"""Process-wide dependency wiring (Redis connections, repositories, queue).

Kept minimal and explicit rather than using a full DI framework, since the
service is small; this is the single place that constructs shared,
process-scoped singletons for both the API and worker processes.

The Redis client is `redis.asyncio`, so every call is awaited and the
event loop stays free while waiting on Redis. Both the API and the worker
run exactly one event loop for the lifetime of the process, so caching the
client per-process is safe.
"""
from __future__ import annotations

from functools import cache

import redis.asyncio as aioredis

from app.config import settings
from app.queue.stream import ReportQueue
from app.reports.repository import ReportRepository
from app.sec.client import SecClient
from app.sec.rate_limiter import GlobalSecRateLimiter
from app.storage.artifact_store import ArtifactStore


@cache
def get_redis() -> aioredis.Redis:
    """Async Redis client.

    `from_url` only builds the connection pool; sockets open lazily on
    first use, so this is safe to call before the loop is running.
    """
    return aioredis.Redis.from_url(settings.redis_url, decode_responses=True)


@cache
def get_queue() -> ReportQueue:
    return ReportQueue(get_redis())


@cache
def get_report_repository() -> ReportRepository:
    return ReportRepository(get_redis())


@cache
def get_rate_limiter() -> GlobalSecRateLimiter:
    return GlobalSecRateLimiter(get_redis())


@cache
def get_artifact_store() -> ArtifactStore:
    return ArtifactStore()


def build_sec_client() -> SecClient:
    """Not cached: SEC clients own an httpx.AsyncClient and should be closed
    by whoever creates them (e.g. worker lifecycle)."""
    return SecClient(rate_limiter=get_rate_limiter())
