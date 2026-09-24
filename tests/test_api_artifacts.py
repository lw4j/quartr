"""API-level tests for artifact addressing and serving.

Uses the real ASGI app with a fake Redis and a temporary storage root, so
the ref -> URL -> bytes path is exercised end to end.
"""
from __future__ import annotations

import os
import tempfile

import fakeredis.aioredis
import pytest
import redis.asyncio

ACCESSION = "0000320193-25-000079"
REF = f"AAPL/10-K/2025/{ACCESSION}"
PDF_BYTES = b"%PDF-1.7\n" + b"x" * 512 + b"\n%%EOF\n"


@pytest.fixture
def client(monkeypatch):
    storage_root = tempfile.mkdtemp()
    monkeypatch.setenv("STORAGE_ROOT", storage_root)
    monkeypatch.setenv("CIK_MAPPING_PATH", os.path.join(tempfile.mkdtemp(), "cik.json"))

    shared = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis.asyncio.Redis, "from_url", staticmethod(lambda *a, **k: shared))

    # Imported lazily so the patched env/redis are in place first, and
    # cached singletons are rebuilt per test.
    import app.deps
    from app.storage.artifact_store import ArtifactStore

    factories = (
        app.deps.get_redis, app.deps.get_queue, app.deps.get_report_repository,
        app.deps.get_rate_limiter, app.deps.get_artifact_store,
    )
    for factory in factories:
        factory.cache_clear()

    # `settings` is a frozen dataclass, so the storage root is injected by
    # overriding the factory rather than mutating config.
    monkeypatch.setattr(app.deps, "get_artifact_store", lambda: ArtifactStore(storage_root))

    import app.api.routes
    monkeypatch.setattr(app.api.routes, "get_artifact_store", lambda: ArtifactStore(storage_root))

    from fastapi.testclient import TestClient
    from app.api.main import app as asgi_app

    with TestClient(asgi_app) as c:
        c.storage_root = storage_root
        yield c

    for factory in factories:
        factory.cache_clear()


def _write_artifact(storage_root: str) -> str:
    path = os.path.join(
        storage_root, "AAPL", "10-K", "2025", f"accession-{ACCESSION}", "report.pdf"
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(PDF_BYTES)
    return path


def test_serves_pdf_bytes_for_a_valid_ref(client):
    _write_artifact(client.storage_root)

    resp = client.get(f"/artifacts/{REF}")

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/pdf"
    assert resp.content == PDF_BYTES
    # Rendered in-browser rather than force-downloaded.
    assert resp.headers["content-disposition"].startswith("inline")
    assert f"AAPL-10-K-2025-{ACCESSION}.pdf" in resp.headers["content-disposition"]


def test_returns_404_when_artifact_not_yet_generated(client):
    resp = client.get(f"/artifacts/{REF}")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "bad_ref",
    [
        "AAPL/10-K/2025/not-an-accession",
        "AAPL/10-K/2025",
        "AAPL/10-K/2025/0000320193-25-000079/extra/segments",
    ],
)
def test_rejects_malformed_refs_with_400(client, bad_ref):
    resp = client.get(f"/artifacts/{bad_ref}")
    assert resp.status_code == 400


def test_traversal_ref_cannot_escape_storage_root(client):
    secret_marker = b"%PDF-LEAKED-FILE-CONTENTS"
    secret = os.path.join(client.storage_root, "..", "secret.pdf")
    with open(secret, "wb") as f:
        f.write(secret_marker)

    # The traversal must be percent-encoded: HTTP clients (and proxies)
    # collapse a literal `../` before the request is sent, so an unencoded
    # probe never reaches the endpoint and would pass vacuously.
    resp = client.get("/artifacts/AAPL/10-K/2025/..%2F..%2F..%2Fsecret.pdf")

    assert resp.status_code == 400
    # Assert on the file's contents, not its name: the 400 body legitimately
    # echoes the submitted ref, which contains the name.
    assert secret_marker not in resp.content


def test_unencoded_traversal_is_normalized_away_before_routing(client):
    """Documents why the encoded probe above is the meaningful one."""
    resp = client.get("/artifacts/AAPL/10-K/2025/../../../../secret.pdf")

    # Normalized to /secret.pdf by the client, so no route matches at all.
    assert resp.status_code == 404
    assert resp.request.url.path == "/secret.pdf"


def test_completed_report_response_links_to_the_artifact_url(client):
    _write_artifact(client.storage_root)

    import asyncio
    from app.deps import get_report_repository
    from app.reports.identity import FilingIdentity, ReportIdentity
    from app.reports.state import ReportState

    repo = get_report_repository()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)

    async def seed():
        task, _ = await repo.get_or_create_task(identity, include_amended=False)
        filing = FilingIdentity(
            cik="0000320193", accession_number=ACCESSION, primary_document="a.htm",
            filing_date="2025-11-01", source_url="https://example.invalid",
        )
        await repo.transition(task, ReportState.QUEUED)
        await repo.transition(task, ReportState.PROCESSING)
        await repo.transition(task, ReportState.COMPLETED, filing=filing,
                              artifact_path="/internal/report.pdf")
        await repo.publish_artifact(task)

    asyncio.run(seed())

    body = client.get("/reports/AAPL/10-K/2025").json()

    assert body["state"] == "completed"
    assert body["artifact"]["ref"] == REF
    assert body["artifact"]["url"] == f"/artifacts/{REF}"
    # The internal storage path must never surface in the API contract.
    assert "/internal/" not in str(body)

    # The advertised URL must actually serve the bytes.
    served = client.get(body["artifact"]["url"])
    assert served.status_code == 200
    assert served.content == PDF_BYTES


def _seed_completed(include_amended: bool) -> None:
    """Publish a completed artifact for AAPL/10-K/2025 under one policy."""
    import asyncio

    from app.deps import get_report_repository
    from app.reports.identity import FilingIdentity, ReportIdentity
    from app.reports.state import ReportState

    repo = get_report_repository()
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)

    async def seed():
        task, _ = await repo.get_or_create_task(identity, include_amended=include_amended)
        filing = FilingIdentity(
            cik="0000320193", accession_number=ACCESSION, primary_document="a.htm",
            filing_date="2025-11-01", source_url="https://example.invalid",
        )
        await repo.transition(task, ReportState.QUEUED)
        await repo.transition(task, ReportState.PROCESSING)
        await repo.transition(task, ReportState.COMPLETED, filing=filing,
                              artifact_path="/internal/report.pdf")
        await repo.publish_artifact(task)

    asyncio.run(seed())


def test_amended_request_is_not_served_the_non_amended_cache(client):
    """`include_amended` changes which filing is selected, so a cached
    non-amended PDF must not satisfy a request that asks for amendments.

    Previously both the artifact cache and the dedup key were derived from
    the logical path alone (ticker/form/year), so this returned 200 with
    the already-rendered non-amended report.
    """
    _seed_completed(include_amended=False)

    cached = client.post("/reports", json={"ticker": "AAPL", "form": "10-K", "year": 2025})
    assert cached.status_code == 200
    assert cached.json()["state"] == "completed"

    amended = client.post(
        "/reports",
        json={"ticker": "AAPL", "form": "10-K", "year": 2025, "include_amended": True},
    )

    assert amended.status_code == 202
    assert amended.json()["state"] == "queued"


def test_amended_request_is_not_deduplicated_onto_a_non_amended_task(client):
    """A second, differently-scoped request must create its own task rather
    than attaching to the in-flight one via the pending key."""
    first = client.post("/reports", json={"ticker": "AAPL", "form": "10-K", "year": 2025})
    second = client.post(
        "/reports",
        json={"ticker": "AAPL", "form": "10-K", "year": 2025, "include_amended": True},
    )

    assert first.status_code == 202 and second.status_code == 202
    assert first.json()["task_id"] != second.json()["task_id"]


def test_get_report_can_address_each_policy_separately(client):
    _seed_completed(include_amended=True)

    assert client.get("/reports/AAPL/10-K/2025").status_code == 404
    amended = client.get("/reports/AAPL/10-K/2025?include_amended=true")
    assert amended.status_code == 200
    assert amended.json()["state"] == "completed"


def test_get_report_accepts_the_encoded_amended_form(client):
    """The service emits `10-K_A` in paths, so it must accept it back.

    Previously `GET /reports/AAPL/10-K_A/2025` returned 400 "Invalid form",
    making a completed amended report unaddressable by the status route.
    """
    resp = client.get("/reports/AAPL/10-K_A/2025")

    assert resp.status_code == 404
    assert "Invalid form" not in resp.text
