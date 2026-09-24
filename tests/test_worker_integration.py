"""End-to-end worker tests: `ReportWorker.process_task` against a canned
SEC submissions payload, fake Redis, a real ArtifactStore and a stubbed
PDF converter.

These cover the seam the component tests missed -- the API/worker handoff,
where the task's persisted state and the queue message meet.
"""
from __future__ import annotations

import json
import os

import fakeredis.aioredis
import pytest

from app.companies.cik_mapping import CikMapping, CompanyRecord
from app.config import settings
from app.queue.stream import ReportQueue
from app.reports.identity import LATEST_YEAR_SENTINEL, ReportIdentity
from app.reports.repository import ReportRepository
from app.reports.state import ReportState
from app.sec.retry import TransientSecError
from app.storage.artifact_store import ArtifactStore
from app.workers.worker import ReportWorker

# Trimmed from the real data.sec.gov/submissions/CIK0000320193.json shape.
SUBMISSIONS = {
    "cik": "320193",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-25-000079", "0000320193-24-000123"],
            "filingDate": ["2025-10-31", "2024-11-01"],
            "reportDate": ["2025-09-27", "2024-09-28"],
            "form": ["10-K", "10-K"],
            "primaryDocument": ["aapl-20250927.htm", "aapl-20240928.htm"],
        }
    },
}

PDF_BYTES = b"%PDF-1.7\nstub\n%%EOF\n"


class StubResponse:
    def __init__(self, payload=None, content=b"", headers=None):
        self._payload = payload
        self.content = content
        self.headers = headers or {}

    def json(self):
        return self._payload


class StubSecClient:
    """Records calls so tests can assert what the worker asked SEC for."""

    def __init__(self, submissions=None, document=b"<html><body>10-K</body></html>"):
        self.submissions = submissions if submissions is not None else SUBMISSIONS
        self.document = document
        self.submissions_calls: list[str] = []
        self.document_calls: list[str] = []

    async def get_submissions(self, cik):
        self.submissions_calls.append(cik)
        return StubResponse(payload=self.submissions)

    async def download_document(self, url):
        self.document_calls.append(url)
        return StubResponse(content=self.document, headers={"Content-Type": "text/html"})

    async def aclose(self):
        pass


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.workers.worker.convert_to_pdf", lambda *a, **kw: PDF_BYTES
    )
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    mapping = CikMapping(path=str(tmp_path / "cik.json"))
    mapping._by_ticker["AAPL"] = CompanyRecord("AAPL", "0000320193", "Apple Inc.")
    sec = StubSecClient()
    worker = ReportWorker(
        worker_id="test-worker",
        queue=ReportQueue(redis),
        repository=ReportRepository(redis),
        sec_client=sec,
        cik_mapping=mapping,
        artifact_store=ArtifactStore(storage_root=str(tmp_path / "store")),
        concurrency=1,
    )
    return worker, sec, redis


async def test_processes_latest_10k_end_to_end(env):
    worker, sec, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=LATEST_YEAR_SENTINEL)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    assert sec.submissions_calls == ["0000320193"]
    # Latest of the two 10-Ks, fetched from the correct Archives URL.
    assert sec.document_calls == [
        "https://www.sec.gov/Archives/edgar/data/320193/"
        "000032019325000079/aapl-20250927.htm"
    ]

    stored = await repo.get_task(task.task_id)
    assert stored.state is ReportState.COMPLETED
    assert stored.filing.accession_number == "0000320193-25-000079"

    # The sentinel year is resolved on the way out.
    assert stored.identity.logical_path == "/AAPL/10-K/2025"
    artifact = await repo.get_current_artifact("/AAPL/10-K/2025")
    assert artifact["accession_number"] == "0000320193-25-000079"

    with open(stored.artifact_path, "rb") as f:
        assert f.read().startswith(b"%PDF-")


async def test_queued_task_from_api_is_processable(env):
    """Regression: the API used to enqueue before persisting QUEUED, so a
    worker could load the task while still ACCEPTED -- every transition then
    failed and the accepted task was silently dropped (spec 24.1)."""
    worker, _, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)

    # Exactly what the API persists before the message becomes visible.
    await repo.transition(task, ReportState.QUEUED)
    reloaded = await repo.get_task(task.task_id)
    assert reloaded.state is ReportState.QUEUED

    await worker.process_task(reloaded)

    assert (await repo.get_task(task.task_id)).state is ReportState.COMPLETED


async def test_latest_request_clears_its_sentinel_dedup_key(env):
    """Regression: a 'latest' task registers dedup under /AAPL/10-K/0 but
    completes under /AAPL/10-K/2025, so the sentinel key leaked and pinned
    the logical path to a finished task for the whole pending TTL."""
    worker, _, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=LATEST_YEAR_SENTINEL)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    assert await repo.get_pending_task_id("/AAPL/10-K/0") == task.task_id

    await worker.process_task(task)

    assert await repo.get_pending_task_id("/AAPL/10-K/0") is None
    # A fresh 'latest' request is therefore a new task, not the stale one.
    task2, created = await repo.get_or_create_task(identity, include_amended=False)
    assert created is True
    assert task2.task_id != task.task_id


async def test_unhandled_error_leaves_message_for_redelivery(env):
    """Regression: an unexpected exception used to ACK+delete the message in
    a `finally`, discarding an accepted task."""
    worker, _, _ = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await queue.enqueue(task.task_id)

    async def boom(_task):
        raise RuntimeError("worker exploded")

    worker.process_task = boom
    messages = await queue.read(worker.worker_id, count=1, block_ms=10)
    assert len(messages) == 1
    await worker._handle_message(*messages[0])

    # Not acked: still pending for this consumer, so it can be reclaimed.
    assert await queue.depth() == 1
    assert (await repo.get_task(task.task_id)).attempts == 1


async def test_poison_message_is_dead_lettered_after_max_attempts(env):
    """Not ACKing must not mean redelivering forever."""
    worker, _, _ = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await queue.enqueue(task.task_id)
    task.attempts = settings.retry_max_attempts - 1
    await repo.save_attempts(task)

    async def boom(_task):
        raise RuntimeError("worker exploded")

    worker.process_task = boom
    messages = await queue.read(worker.worker_id, count=1, block_ms=10)
    await worker._handle_message(*messages[0])

    stored = await repo.get_task(task.task_id)
    assert stored.state is ReportState.FAILED
    assert "worker exploded" in stored.error
    assert await queue.depth() == 0
    assert await repo.get_pending_task_id("/AAPL/10-K/2025") is None


async def test_unacked_message_is_reclaimed_and_counts_one_attempt(env):
    """The premise of leaving a message unacked is that it gets reclaimed.

    Regression: `attempts` was incremented both by `_fail_transiently` and
    again by the unhandled-error handler, so each delivery burned two
    attempts and halved the configured retry budget.
    """
    worker, _, _ = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await queue.enqueue(task.task_id)

    async def boom(_task):
        raise RuntimeError("worker exploded")

    worker.process_task = boom

    messages = await queue.read(worker.worker_id, count=1, block_ms=10)
    await worker._handle_message(*messages[0])
    assert (await repo.get_task(task.task_id)).attempts == 1

    # A second worker reclaims the still-pending entry.
    reclaimed = await queue.claim_abandoned("other-worker", min_idle_ms=0)
    assert len(reclaimed) == 1
    await worker._handle_message(*reclaimed[0])
    assert (await repo.get_task(task.task_id)).attempts == 2


async def test_attempts_counted_once_when_retry_bookkeeping_fails(env):
    """Regression: `_fail_transiently` bumps `attempts` in memory *before*
    persisting. If that persist failed, the unhandled-error handler bumped
    the same object again, so one delivery burned two attempts and halved
    the configured retry budget."""
    worker, sec, _ = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await queue.enqueue(task.task_id)

    async def transient(_cik):
        raise TransientSecError("SEC 503")

    sec.get_submissions = transient

    # The retry write itself fails, so nothing is persisted -- but
    # _fail_transiently has already mutated the in-memory task.
    original = repo.transition

    async def flaky(t, target, **kw):
        if target is ReportState.RETRYING:
            raise ConnectionError("redis down")
        return await original(t, target, **kw)

    repo.transition = flaky

    messages = await queue.read(worker.worker_id, count=1, block_ms=10)
    await worker._handle_message(*messages[0])

    assert (await repo.get_task(task.task_id)).attempts == 1


async def test_failure_after_completion_does_not_demote_the_task(env):
    """Regression: an exception raised *after* COMPLETED was persisted (e.g.
    in publish_artifact) propagated into the failure handlers, which tried
    an illegal COMPLETED -> FAILED transition and dead-lettered a task whose
    PDF was already on disk."""
    worker, _, _ = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await queue.enqueue(task.task_id)

    async def boom(_task):
        raise RuntimeError("redis hiccup in publish_artifact")

    repo.publish_artifact = boom

    messages = await queue.read(worker.worker_id, count=1, block_ms=10)
    await worker._handle_message(*messages[0])

    # Drive it past the retry budget: the demotion only surfaced after
    # repeated redeliveries, so a single pass would not have caught it.
    for _ in range(settings.retry_max_attempts + 1):
        reclaimed = await queue.claim_abandoned("other-worker", min_idle_ms=0)
        if not reclaimed:
            break
        await worker._handle_message(*reclaimed[0])

    stored = await repo.get_task(task.task_id)
    assert stored.state is ReportState.COMPLETED
    assert stored.artifact_path and os.path.isfile(stored.artifact_path)


async def test_force_fail_never_demotes_a_completed_task(env):
    worker, _, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await worker.process_task(task)
    assert task.state is ReportState.COMPLETED

    await repo.force_fail(task, "should be ignored")

    assert (await repo.get_task(task.task_id)).state is ReportState.COMPLETED


async def test_retry_persists_queued_before_republishing(env, monkeypatch):
    """Regression: `_fail_transiently` enqueued the retry before persisting
    QUEUED, so a worker could load it while still RETRYING -- which has no
    legal edge to PROCESSING."""
    worker, _, shared = env
    repo, queue = worker.repository, worker.queue
    await queue.ensure_group()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)
    await repo.transition(task, ReportState.PROCESSING)

    seen = {}
    original = type(queue).enqueue

    async def spy(self, task_id):
        raw = await shared.get(f"report:task:{task_id}")
        seen["state"] = json.loads(raw)["state"] if raw else None
        return await original(self, task_id)

    monkeypatch.setattr(type(queue), "enqueue", spy)

    await worker._fail_transiently(task, RuntimeError("transient"), {})

    assert seen["state"] == "queued"


async def test_unexpected_transition_error_is_not_swallowed(env):
    """Regression: a bare `except: pass` around transition(PROCESSING) hid
    real persistence failures. Only InvalidStateTransition is tolerated."""
    worker, _, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    async def boom(*a, **kw):
        raise ConnectionError("redis down")

    repo.transition = boom

    with pytest.raises(ConnectionError):
        await worker.process_task(task)


async def test_existing_artifact_is_not_redownloaded(env):
    """Idempotency (spec 8/13): reprocessing must not refetch from SEC."""
    worker, sec, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)

    task1, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task1, ReportState.QUEUED)
    await worker.process_task(task1)
    assert len(sec.document_calls) == 1

    await repo.clear_pending("/AAPL/10-K/2025")
    task2, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task2, ReportState.QUEUED)
    await worker.process_task(task2)

    assert len(sec.document_calls) == 1  # no second download


async def test_unknown_ticker_fails_permanently(env):
    worker, sec, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="ZZZZ", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    stored = await repo.get_task(task.task_id)
    assert stored.state is ReportState.FAILED
    assert sec.submissions_calls == []
    assert await repo.get_pending_task_id("/ZZZZ/10-K/2025") is None


async def test_missing_filing_for_requested_year_fails_permanently(env):
    worker, _, _ = env
    repo = worker.repository
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=1999)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    assert (await repo.get_task(task.task_id)).state is ReportState.FAILED


# Same company, with an amendment filed after the original 10-K.
SUBMISSIONS_WITH_AMENDMENT = {
    "cik": "320193",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-25-000999", "0000320193-25-000079"],
            "filingDate": ["2025-12-01", "2025-10-31"],
            "reportDate": ["2025-09-27", "2025-09-27"],
            "form": ["10-K/A", "10-K"],
            "primaryDocument": ["aapl-amended.htm", "aapl-20250927.htm"],
        }
    },
}


async def test_explicit_amended_form_selects_the_amendment(env):
    """`form: "10-K/A"` must reach the selector intact.

    The worker used to pass `form.split("/")[0]`, so this request silently
    selected the original 10-K and reported success with the wrong document.
    """
    worker, sec, _ = env
    sec.submissions = SUBMISSIONS_WITH_AMENDMENT
    repo = worker.repository

    identity = ReportIdentity(ticker="AAPL", form="10-K/A", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    stored = await repo.get_task(task.task_id)
    assert stored.state is ReportState.COMPLETED
    assert stored.filing.accession_number == "0000320193-25-000999"
    assert stored.identity.logical_path == "/AAPL/10-K_A/2025"


async def test_plain_form_still_ignores_an_amendment(env):
    """The default policy must not start picking up amendments."""
    worker, sec, _ = env
    sec.submissions = SUBMISSIONS_WITH_AMENDMENT
    repo = worker.repository

    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    stored = await repo.get_task(task.task_id)
    assert stored.filing.accession_number == "0000320193-25-000079"


async def test_include_amended_widens_to_the_newer_amendment(env):
    worker, sec, _ = env
    sec.submissions = SUBMISSIONS_WITH_AMENDMENT
    repo = worker.repository

    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=True)
    await repo.transition(task, ReportState.QUEUED)

    await worker.process_task(task)

    stored = await repo.get_task(task.task_id)
    assert stored.filing.accession_number == "0000320193-25-000999"
