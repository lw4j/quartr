# Financial Reports Service (Draft)

Retrieves SEC EDGAR filings for a set of companies, converts selected filings
to PDF, and stores them for use by other applications.

This is an initial architectural draft implementing the design in
`financial_reports_service_spec.txt`. It favors clear component boundaries
and correct control flow over production hardening.

**On AI usage.** This service was built with AI assistance. Rather than a
raw chat transcript, the prompt log is `financial_reports_service_spec.txt`
in the repository root: I wrote that specification first and drove the
implementation from it, so it is both the design document and the record of
what was asked for. Reading it alongside the code shows which decisions were
made up front and which were made while building.

**Contents**

- [Architecture](#architecture)
  - [Component boundaries](#component-boundaries-mirrors-spec-section-25)
  - [Identifiers](#identifiers-spec-section-27)
  - [Concurrency model](#concurrency-model)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
  - [Get the six companies' latest 10-Ks](#get-the-six-companies-latest-10-ks)
  - [Scaling workers](#scaling-workers)
- [Requesting a report](#requesting-a-report)
  - [An amended filing](#an-amended-filing)
  - [Load testing with 100 tickers](#load-testing-with-100-tickers)
- [Checking status](#checking-status)
  - [Downloading the PDF](#downloading-the-pdf)
  - [Addressing artifacts](#addressing-artifacts)
- [Observability](#observability)
- [Configuration](#configuration)
- [Running locally (without Docker)](#running-locally-without-docker)
- [Running tests](#running-tests)
- [Failure and recovery behavior](#failure-and-recovery-behavior)
- [Scope](#scope)

## Architecture

### Component boundaries (mirrors spec section 25)

```
app/
  api/            HTTP API (FastAPI) + admission control + dedup
  companies/      ticker -> CIK mapping
  sec/            SEC HTTP client, submissions API, rate limiting, retry
  reports/        report identity, filing selection policy, report state
  queue/          Redis Streams producer/consumer/recovery
  conversion/     SEC source -> PDF
  storage/        PDF/source artifact + metadata storage
  workers/        orchestrates the above
  observability/  metrics + structured logging helpers
```

### Identifiers (spec section 27)

```
Ticker -> CIK -> logical report path -> SEC accession number
       -> immutable source filing -> generated PDF
```

### Concurrency model

Both the API and the worker run on an asyncio event loop.

- **API handlers are `async def`** and every Redis call is async.
- **The worker** awaits Redis and SEC I/O, while pushing the two blocking steps —
  PDF rendering (CPU-bound, holds the GIL) and artifact writes (no true async
  file API exists) — onto threads via `asyncio.to_thread`.
- `WORKER_CONCURRENCY` (default `1`) controls how many tasks a single worker
  process keeps in flight; the Redis-backed SEC rate limiter bounds outbound
  request rate globally across all coroutines and processes. Defaults to 1.
- `uvloop` is used when available, purely as a drop-in faster asyncio loop.

## Quick start (Docker Compose)

Prerequisites: Docker + Docker Compose (recommended path), **or** Python 3.14
(matching the `python:3.14-slim` image) and a local/remote Redis instance —
see [Running locally](#running-locally-without-docker).

From the repository root:

```bash
docker compose up --build
```

This starts three services:

| Service  | Purpose                                               | Port |
|----------|-------------------------------------------------------|------|
| `redis`  | task stream, task state, dedup keys, rate limiter     | 6379 |
| `api`    | FastAPI HTTP API                                      | 8000 |
| `worker` | background report processing (SEC fetch + PDF convert)| 9100+ (metrics) |

Wait for the logs to show the API and worker are up, then confirm health:

```bash
curl http://localhost:8000/healthz
# {"status": "ok"}
```

### Get the six companies' latest 10-Ks

The one-command path — submits all six, waits for the workers, and
downloads the PDFs:

```bash
./scripts/fetch_six.sh
#   AAPL   done  -> ./pdfs/AAPL-10-K.pdf
#   META   done  -> ./pdfs/META-10-K.pdf
#   ...
#
# 6/6 PDFs in ./pdfs
```

Override with `BASE_URL`, `OUT_DIR` and `TIMEOUT` (default 300s; a cold
run renders six full annual reports, and Goldman Sachs alone is ~750
pages). The script exits non-zero unless all six succeed.

The steps it automates are described under
[Requesting a report](#requesting-a-report) and
[Checking status](#checking-status) below.

To flush the queue and all tasks' state, delete the volumes:

```bash
docker compose down -v
```

### Scaling workers

Workers are independently scalable; the SEC rate limiter is global (Redis-
backed), so adding workers increases throughput without exceeding the
aggregate SEC request rate:

```bash
docker compose up --build --scale worker=3
```

Each worker process can additionally keep several tasks in flight on its own
event loop via `WORKER_CONCURRENCY` (default `1`):

```bash
WORKER_CONCURRENCY=4 docker compose up --build --scale worker=3
```

Both dials are safe to combine: the Redis-backed rate limiter caps SEC traffic
globally regardless of how many coroutines or processes are running. Prefer
raising `WORKER_CONCURRENCY` for SEC-I/O-bound throughput and adding replicas
for PDF-rendering (CPU-bound) throughput, since rendering is offloaded to a
thread and still contends for the GIL within one process.

## Requesting a report

### Latest 10-K for a ticker (year omitted)

```bash
curl -i -X POST http://localhost:8000/reports \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "form": "10-K"}'
```

Possible responses:
- `202 Accepted` with a JSON body containing `task_id`, `logical_path`,
  and `state` (`queued`) — processing has started asynchronously.
- `200 OK` if that report was already produced previously.
- `404 Not Found` if the ticker is unknown.
- `429 Too Many Requests` (with `Retry-After` header) if the queue is at
  capacity — retry later.

`200 OK` applies to requests that name an explicit `year`. A request with
`year` omitted asks for *whatever is latest right now*, so it is always
re-queued and re-checked against SEC rather than served from cache — the
worker then short-circuits the download if that accession is already on
disk, so the repeat cost is one submissions API call.

> **Ticker resolution.** Tickers are resolved against SEC's published
> `company_tickers.json`, which the API and workers download at startup and
> then refresh every `CIK_MAPPING_REFRESH_SECONDS`, persisting the result to
> `CIK_MAPPING_PATH` on the shared volume. Until the first refresh succeeds
> only a small built-in seed list resolves, so a service that cannot reach
> SEC on startup will return `404` for most tickers.
>
> Note that this list keys companies by the ticker they register with SEC,
> which is occasionally not the exchange ticker — Marsh & McLennan, for
> example, is `MRSH` rather than its NYSE symbol `MMC`.

### A specific year

```bash
curl -i -X POST http://localhost:8000/reports \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "form": "10-K", "year": 2025}'
```

### An amended filing

```bash
curl -i -X POST http://localhost:8000/reports \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "form": "10-K", "year": 2025, "include_amended": true}'
```

`include_amended` is part of what identifies the result, not just a filter:
it changes which filing is selected, so it is tracked separately from the
default request for the same ticker/form/year rather than reusing its
cached PDF. Read it back with the matching flag:

```bash
curl -s "http://localhost:8000/reports/AAPL/10-K/2025?include_amended=true" | jq
```

There are three distinct ways to ask about amendments, and they mean
different things:

| Request | Selects | Addressed as |
| --- | --- | --- |
| `{"form": "10-K"}` | the latest `10-K`, ignoring amendments | `AAPL/10-K/2025` |
| `{"form": "10-K", "include_amended": true}` | the latest of `10-K` **or** `10-K/A` | `AAPL/10-K/2025`, read back with `?include_amended=true` |
| `{"form": "10-K/A"}` | only amendments, never the original | `AAPL/10-K_A/2025` |

The first two keep the plain `10-K` spelling: `include_amended` widens
*which* filing qualifies, but the form asked for is still `10-K`. Only the
third changes the form itself, and SEC writes that form with a slash
(`10-K/A`), which would collide with the delimiter in logical paths and
artifact refs. It is encoded as `10-K_A` in both, and read back with the
encoded spelling:

```bash
curl -s -X POST http://localhost:8000/reports \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "form": "10-K/A", "year": 2025}'

curl -s http://localhost:8000/reports/AAPL/10-K_A/2025 | jq
```

The encoding is reversible because the accepted form grammar contains no
underscore. Asking for an amendment that does not exist fails permanently
rather than falling back to the original filing:
`No filing found matching form='10-K/A' ...`.

### The initial six companies' latest 10-Ks

```bash
for ticker in AAPL META GOOGL AMZN NFLX GS; do
  curl -s -X POST http://localhost:8000/reports \
    -H "Content-Type: application/json" \
    -d "{\"ticker\": \"$ticker\", \"form\": \"10-K\"}" | jq
done
```

These six requests are deduplicated and admission-controlled individually,
but all share the same Redis Stream queue and the same global SEC rate
limiter — they will not generate six simultaneous SEC downloads.

### Load testing with 100 tickers

`scripts/request_tickers.sh` submits the latest 10-K for 100 large-cap
tickers, printing the status code and body for each:

```bash
./scripts/request_tickers.sh

# Against another host, or for a different form:
BASE_URL=http://api:8000 FORM=10-Q ./scripts/request_tickers.sh
```

Output is one line per ticker, e.g. `AAPL   202 {"task_id": ...}`. Expect
`202` (queued), `200` (already produced), or `429` (queue at capacity). A
`404` means the ticker did not resolve — see [Ticker
resolution](#requesting-a-report) above.

This exercises the parts of the system that matter under load: admission
control, deduplication, queue depth, and the way a burst of submissions
drains through the global SEC rate limiter. Watch it with:

```bash
# Queue depth drains from the API; per-job counters live on the worker.
watch -n1 'curl -s localhost:8000/metrics | grep -E "^(queue_depth|jobs_accepted_total)"'
```

Two caveats. The script issues requests **sequentially**, so it measures
how the pipeline absorbs a backlog, not API concurrency — use a dedicated
tool against `POST /reports` for that. And because
submission only enqueues work, a fast run of 100 `202`s says nothing about
whether the PDFs were produced; poll `GET /tasks/{task_id}` or watch
`queue_depth` fall back to zero to see the workers actually catch up.

Alternatively just watch the local artifact store fill up with:

```bash
watch -n1 'find reports-data -name "*.pdf" | wc -l'
```

## Checking status

### By task id (returned from the POST response)

```bash
curl http://localhost:8000/tasks/<task_id>
```

Response includes `state` (`accepted` / `queued` / `processing` /
`completed` / `failed`), `accession_number`, `error` if failed, and once
completed an `artifact` handle:

```json
{
  "task_id": "…",
  "logical_path": "/AAPL/10-K/2025",
  "state": "completed",
  "accession_number": "0000320193-25-000079",
  "artifact": {
    "ref": "AAPL/10-K/2025/0000320193-25-000079",
    "url": "/artifacts/AAPL/10-K/2025/0000320193-25-000079",
    "media_type": "application/pdf"
  }
}
```

### Downloading the PDF

```bash
curl -o report.pdf http://localhost:8000/artifacts/AAPL/10-K/2025/0000320193-25-000079
```

- `200 OK` with `Content-Type: application/pdf`.
- `400 Bad Request` if the reference is malformed.
- `404 Not Found` if no artifact has been generated for it yet.

This is a deliberately **basic** read path. Caching (ETag/conditional GET,
`Cache-Control`), authentication, egress rate limiting and reverse-proxy/CDN
offload are intentionally not implemented — spec section 26 defers the public
report-serving API. Since accession-scoped artifacts are immutable, caching is
a safe later addition.


### Accessing artifact store

```bash
open reports-data/reports/AAPL/10-K/2025/accession-0000320193-25-000079/report.pdf
```


### Addressing artifacts

`artifact.ref` is a stable, backend-independent handle built only from the
spec section 27 identifiers (logical path + accession number). The API
deliberately does **not** return a filesystem path: spec section 9 leaves the
physical backend unspecified, so a path would freeze an implementation detail
into the public contract and break every consumer if local disk were swapped
for S3.

Local storage is used at the moment, as it's easier to inspect than e.g. a docker volume.
View a generated report (macOS) directly in the artifact store, e.g.:


```bash
# ref = AAPL/10-K/2025/0000320193-25-000079
open reports-data/reports/AAPL/10-K/2025/accession-0000320193-25-000079/report.pdf
```

The `ArtifactStore.resolve_pdf_path()` is the single place performing that
mapping in code, and it validates the reference before it becomes a path so
user input cannot escape the storage root.

### By logical report path

```bash
curl -i http://localhost:8000/reports/AAPL/10-K/2025
```

- `200 OK` with artifact info if completed.
- `404 Not Found` if not yet requested.
- If currently in flight, returns the associated task's current state.

### Re-requesting the same report

Submitting the same `POST /reports` body again while a report is queued
or processing returns the **same** `task_id` instead of creating a new
job (spec-mandated deduplication). Once completed, subsequent requests
return `200 OK` immediately without re-enqueuing.

## Observability

Metrics are split across two processes, because a Prometheus registry is
per-process and most counters are incremented by the worker:

```bash
# API: queue_depth, jobs_accepted_total
curl http://localhost:8000/metrics

# Worker: jobs_completed/failed/retried_total, sec_requests_total,
# sec_429_total, sec_request_latency_seconds, pdf_conversion_latency_seconds,
# active_workers. The host port is assigned from a range so that
# `--scale worker=N` works, so discover it rather than assuming 9100:
curl "http://localhost:$(docker compose port worker 9100 | cut -d: -f2)/metrics"
```

A counter only appears with a non-zero value in the process that owns it —
`jobs_accepted_total` is an API counter and reads `0` on the worker
endpoint, and vice versa. Scrape both.

Worker and API logs are structured JSON (one line per event) including
`task_id`, `logical_report_path`, `ticker`, `cik`, `form`, `accession`,
`worker_id`, and `state` where applicable — filter with `jq` or your log
aggregator.

## Configuration

Set these as environment variables (see `app/config.py` for full list and
defaults). In `docker-compose.yml` they're set on both `api` and `worker`.

| Variable                      | Default                  | Notes |
|-------------------------------|--------------------------|-------|
| `SEC_USER_AGENT`              | dev placeholder          | **Must** be a real org name + contact email in production (`ENVIRONMENT=production` enforces this at startup) |
| `SEC_RATE_LIMIT_PER_SECOND`   | `8`                      | Keep below SEC's 10 req/s ceiling |
| `MAX_QUEUE_DEPTH`             | `1000`                   | Admission-control threshold; requests beyond this get `429` |
| `WORKER_CONCURRENCY`          | `1`                      | Tasks a single worker process keeps in flight on its event loop |
| `WORKER_METRICS_PORT`         | `9100`                   | Worker's own Prometheus endpoint; `0` disables it |
| `REDIS_URL`                   | `redis://localhost:6379/0` | Shared by API + all workers (compose overrides to `redis://redis:6379/0`) |
| `STORAGE_ROOT`                | `/data/reports`          | PDF/source/metadata artifact root |
| `CIK_MAPPING_PATH`            | `/data/cik_mapping.json` | Persisted ticker→CIK mapping |
| `CIK_MAPPING_REFRESH_SECONDS` | `86400`                  | How often to refresh from SEC |
| `INCLUDE_AMENDED_BY_DEFAULT`  | `false`                  | Default filing-selection policy |
| `SEC_RETRY_MAX_ATTEMPTS`      | `5`                      | Applies to both SEC HTTP retries and task-level retrying |
| `ENVIRONMENT`                 | `development`            | Set to `production` to enforce `SEC_USER_AGENT` validation |

Example production override in `docker-compose.yml` or your orchestrator:

```yaml
environment:
  SEC_USER_AGENT: "AcmeCorp FinancialReports/1.0 (data-eng@acme.com)"
  ENVIRONMENT: production
```

## Running locally (without Docker)

Useful for development/debugging with a local or remote Redis.

WeasyPrint links against system Pango/HarfBuzz, which pip cannot provide. The
Docker image installs them for you; for a local run install them first
(macOS: `brew install pango`, Debian/Ubuntu: `apt install libpango-1.0-0
libpangoft2-1.0-0 libharfbuzz-subset0`). Without them, only the API and the
non-PDF parts of the worker will run.

```bash
# from the repository root
pip install -r requirements.txt

export REDIS_URL=redis://localhost:6379/0
export SEC_USER_AGENT="FinancialReportsService/1.0 (dev; you@example.com)"
export STORAGE_ROOT=./data/reports
export CIK_MAPPING_PATH=./data/cik_mapping.json

# terminal 1: API
uvicorn app.api.main:app --reload --port 8000

# terminal 2: worker (run multiple copies to simulate scaling)
python -m app.workers.main
```

## Running tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
```

Tests use `fakeredis` and do not require a real Redis instance or network
access to SEC.

## Failure and recovery behavior

- If a worker crashes mid-task, the message stays in the Redis Stream's
  pending-entries list; another worker (or the same one after restart)
  reclaims it automatically once the idle timeout
  (`PENDING_CLAIM_TIMEOUT_MS`, default 5 minutes) elapses.
- Transient SEC errors (429, 5xx, timeouts) are retried with exponential
  backoff + jitter; permanent errors (unknown ticker/CIK, filing not
  found) mark the task `failed` immediately without retrying.
- Artifacts are keyed by SEC accession number and are immutable once
  written — re-processing the same accession is a no-op (idempotent).

## Scope

Implements: ticker->CIK resolution, CIK normalization, SEC submissions
client, latest-10-K discovery (excluding 10-K/A by default), accession
handling, filing download, HTML->PDF conversion, persistent PDF storage,
Redis Streams queue with consumer groups, worker with crash recovery,
report state machine, request dedup, admission control/backpressure,
global SEC rate limiter, SEC-compliant User-Agent, retry/backoff.

Deferred (per spec section 26): 10-Q/8-K/other filing types beyond the
data-model support already present, XBRL extraction, scheduled refresh,
advanced public report-serving API(basic endpoint implemented),
full-text search, financial-data normalization.

### Known limitations

- **`year` means filing year, not fiscal year.** Selection keys on
  `filingDate`, so Apple's FY2025 10-K (filed Oct 2025) is `/AAPL/10-K/2025`
  while Meta's FY2025 10-K (filed Jan 2026) is `/META/10-K/2026`. SEC's
  `reportDate` is parsed but not used for addressing. Requesting the latest
  report (omitting `year`) is unaffected.
- **Only `filings.recent` is read.** The submissions payload pages older
  filings into `filings.files`, which is not followed. For very active
  filers `recent` covers roughly the last 12 months, so a specific historic
  `year` may return "no filing found" even though it exists.
- **Images are omitted from generated PDFs.** The converter renders HTML
  without a `base_url`, so relative image references do not resolve. Text
  and tables — the substance of a 10-K — render correctly.
- **PDF metadata is written to a sidecar `metadata.json`**, not embedded in
  the PDF document info dictionary.
- **Plain-text filings paginate poorly.** Modern 10-Ks are HTML; the
  fallback text converter is a placeholder and truncates long documents.
- **Pending entries have no lease renewal.** A task still running after
  `PENDING_CLAIM_TIMEOUT_MS` (default 300s) can be reclaimed and processed
  by a second worker. Rendering is idempotent and keyed by accession, so
  the duplicate wastes work rather than corrupting output, but a very large
  filing can cross that threshold.

## License

Review-only. The source code may be viewed and reviewed for evaluation purposes,
but is not licensed for use, copying, modification, distribution, or incorporation
into other projects.

See [LICENSE](LICENSE) for the full terms.



