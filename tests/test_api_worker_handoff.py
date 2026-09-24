"""API -> worker handoff.

The failure these cover is an ordering one: the task must be durably
persisted in a state the worker can act on *before* the queue message
becomes visible. A plain sequential test cannot catch that (the API always
finishes both steps before the test looks), so the ordering is asserted at
the moment of enqueue.
"""
from __future__ import annotations

import json
import os
import tempfile

import fakeredis.aioredis
import pytest
import redis.asyncio

from tests.test_worker_integration import PDF_BYTES, StubSecClient


@pytest.fixture
def env(monkeypatch):
    storage_root = tempfile.mkdtemp()
    monkeypatch.setenv("STORAGE_ROOT", storage_root)
    monkeypatch.setenv("CIK_MAPPING_PATH", os.path.join(tempfile.mkdtemp(), "cik.json"))

    shared = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        redis.asyncio.Redis, "from_url", staticmethod(lambda *a, **k: shared)
    )

    import app.deps
    from app.storage.artifact_store import ArtifactStore

    factories = (
        app.deps.get_redis, app.deps.get_queue, app.deps.get_report_repository,
        app.deps.get_rate_limiter, app.deps.get_artifact_store,
    )
    for factory in factories:
        factory.cache_clear()

    store = ArtifactStore(storage_root)
    monkeypatch.setattr(app.deps, "get_artifact_store", lambda: store)
    import app.api.routes
    monkeypatch.setattr(app.api.routes, "get_artifact_store", lambda: store)

    # Keep the API lifespan off the network.
    from app.companies.cik_mapping import CikMapping, CompanyRecord, cik_mapping

    async def _no_refresh(client=None):
        return None

    monkeypatch.setattr(cik_mapping, "ensure_fresh", _no_refresh)
    monkeypatch.setattr("app.workers.worker.convert_to_pdf", lambda *a, **kw: PDF_BYTES)

    mapping = CikMapping(path=os.path.join(tempfile.mkdtemp(), "cik2.json"))
    mapping._by_ticker["AAPL"] = CompanyRecord("AAPL", "0000320193", "Apple Inc.")

    from fastapi.testclient import TestClient
    from app.api.main import app as asgi_app

    with TestClient(asgi_app) as client:
        yield client, shared, store, mapping

    for factory in factories:
        factory.cache_clear()


def _worker(shared, store, mapping, sec=None):
    from app.queue.stream import ReportQueue
    from app.reports.repository import ReportRepository
    from app.workers.worker import ReportWorker

    return ReportWorker(
        worker_id="handoff-worker",
        queue=ReportQueue(shared),
        repository=ReportRepository(shared),
        sec_client=sec or StubSecClient(),
        cik_mapping=mapping,
        artifact_store=store,
        concurrency=1,
    )


async def test_task_is_persisted_as_queued_before_message_is_published(env, monkeypatch):
    """Regression (spec 24.1): the API used to XADD and only then persist
    QUEUED. A worker blocked on XREADGROUP gets the message within one
    round-trip and would load the task while still ACCEPTED -- no legal
    transition remained, and the accepted task was silently dropped."""
    client, shared, _, _ = env
    from app.queue.stream import ReportQueue

    seen = {}
    original = ReportQueue.enqueue

    async def spy(self, task_id):
        raw = await shared.get(f"report:task:{task_id}")
        seen[task_id] = json.loads(raw)["state"] if raw else None
        return await original(self, task_id)

    monkeypatch.setattr(ReportQueue, "enqueue", spy)

    resp = client.post("/reports", json={"ticker": "AAPL", "form": "10-K", "year": 2025})
    assert resp.status_code == 202

    task_id = resp.json()["task_id"]
    assert seen[task_id] == "queued", (
        f"task was persisted as {seen[task_id]!r} when the message became "
        "visible; a worker consuming it then has no legal transition"
    )
    assert resp.json()["state"] == "queued"


async def test_api_submission_is_processed_by_worker_end_to_end(env):
    client, shared, store, mapping = env

    resp = client.post("/reports", json={"ticker": "AAPL", "form": "10-K"})
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]

    worker = _worker(shared, store, mapping)
    await worker.queue.ensure_group()
    messages = await worker.queue.read(worker.worker_id, count=1, block_ms=10)
    assert len(messages) == 1
    await worker._handle_message(*messages[0])

    status = client.get(f"/tasks/{task_id}")
    assert status.status_code == 200
    body = status.json()
    assert body["state"] == "completed"
    assert body["accession_number"] == "0000320193-25-000079"

    # The advertised artifact ref really serves the PDF.
    pdf = client.get(body["artifact"]["url"])
    assert pdf.status_code == 200
    assert pdf.headers["content-type"] == "application/pdf"
    assert pdf.content.startswith(b"%PDF-")

    assert await worker.queue.depth() == 0


async def test_completed_report_is_served_from_cache_on_repeat_request(env):
    client, shared, store, mapping = env

    first = client.post("/reports", json={"ticker": "AAPL", "form": "10-K", "year": 2025})
    assert first.status_code == 202

    worker = _worker(shared, store, mapping)
    await worker.queue.ensure_group()
    messages = await worker.queue.read(worker.worker_id, count=1, block_ms=10)
    await worker._handle_message(*messages[0])

    repeat = client.post("/reports", json={"ticker": "AAPL", "form": "10-K", "year": 2025})
    assert repeat.status_code == 200
    assert repeat.json()["state"] == "completed"
    assert repeat.json()["artifact"]["url"].endswith("0000320193-25-000079")
    # No new work was queued.
    assert await worker.queue.depth() == 0
