"""FastAPI application entrypoint."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.routes import router
from app.companies.cik_mapping import cik_mapping
from app.config import settings
from app.deps import get_queue, get_redis
from app.observability.logging import configure_logging
from app.observability.metrics import queue_depth

configure_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown.

    The consumer group is created here rather than in `ReportQueue.__init__`
    because it awaits Redis; doing it once at startup keeps the request
    path free of setup checks.

    The CIK mapping is refreshed at startup and then on an interval. The API
    and workers are separate processes with separate in-memory mappings, so
    the API refreshes its own copy rather than relying on the one a worker
    persists to the shared volume.
    """
    settings.validate_for_production()
    await get_queue().ensure_group()
    await cik_mapping.ensure_fresh()
    logger.info("API started (%d tickers resolvable)", cik_mapping.size)
    refresher = asyncio.create_task(_refresh_cik_mapping_periodically())
    yield
    refresher.cancel()
    await asyncio.gather(refresher, return_exceptions=True)
    await get_redis().aclose()
    logger.info("API stopped")


async def _refresh_cik_mapping_periodically() -> None:
    """Keep the API's mapping current without touching the request path."""
    while True:
        await asyncio.sleep(settings.cik_mapping_refresh_seconds)
        await cik_mapping.ensure_fresh()


app = FastAPI(title="Financial Reports Service", version="0.1.0", lifespan=lifespan)
app.include_router(router)


@app.get("/healthz")
async def healthz() -> dict:
    """Liveness probe.

    Deliberately `async def` so it executes directly on the event loop and
    cannot be queued behind thread-pool contention from other endpoints.
    """
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    queue_depth.set(await get_queue().depth())
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
