"""Global, Redis-backed SEC request rate limiter (spec section 15).

The SEC rate limit applies to the service as a whole, not per worker.
Scaling workers must not increase the aggregate SEC request rate, so the
limiter state lives in Redis and is shared by every process.

Implementation: a sliding-window counter using a Redis sorted set. Each
permitted request records a timestamp; requests older than the 1-second
window are trimmed before granting a new slot. This makes the limit
correct even if many worker processes call `acquire()` concurrently.
"""
from __future__ import annotations

import asyncio
import time
import uuid

import redis.asyncio as aioredis

from app.config import settings


_ACQUIRE_LUA = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now_ms, member)
    redis.call('PEXPIRE', key, window_ms * 2)
    return 1
end
return 0
"""


class GlobalSecRateLimiter:
    """Sliding-window limiter shared across all workers via Redis."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        requests_per_second: float = settings.sec_rate_limit_per_second,
        key: str = "sec:rate_limiter",
    ) -> None:
        self._redis = redis_client
        self._limit = max(1, int(requests_per_second))
        self._window_ms = 1000
        self._key = key
        self._script = self._redis.register_script(_ACQUIRE_LUA)

    async def try_acquire(self) -> bool:
        member = f"{time.time_ns()}-{uuid.uuid4().hex}"
        now_ms = int(time.time() * 1000)
        result = await self._script(
            keys=[self._key], args=[now_ms, self._window_ms, self._limit, member]
        )
        return bool(result)

    async def acquire(
        self, poll_interval: float = 0.05, timeout: float | None = None
    ) -> None:
        """Wait until a slot under the global SEC rate limit is available.

        `asyncio.sleep` yields to the event loop instead of blocking a
        thread, so a worker waiting on the rate limiter costs nothing and
        other in-flight tasks keep progressing.
        """
        start = time.monotonic()
        while not await self.try_acquire():
            if timeout is not None and (time.monotonic() - start) > timeout:
                raise TimeoutError("Timed out waiting for SEC rate limiter slot")
            await asyncio.sleep(poll_interval)
