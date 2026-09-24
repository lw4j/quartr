"""Redis Streams work queue (spec sections 11, 20).

Redis Streams are used instead of Pub/Sub because jobs must not disappear
if a worker is unavailable: the stream persists messages, and consumer
groups provide per-message acknowledgement and pending-entry tracking so
abandoned tasks can be reclaimed after a crash.
"""
from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from app.config import settings

logger = logging.getLogger(__name__)


class QueueFullError(Exception):
    """Raised by admission control when MAX_QUEUE_DEPTH would be exceeded."""


class ReportQueue:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        stream_name: str = settings.stream_name,
        consumer_group: str = settings.consumer_group,
        max_queue_depth: int = settings.max_queue_depth,
    ) -> None:
        self._redis = redis_client
        self._stream = stream_name
        self._group = consumer_group
        self._max_queue_depth = max_queue_depth
        self._group_ready = False

    async def ensure_group(self) -> None:
        """Create the consumer group if absent.

        Cannot live in `__init__` because it awaits; the API calls it during
        lifespan startup and the worker calls it before its read loop. It is
        idempotent, so repeated calls are harmless.
        """
        if self._group_ready:
            return
        try:
            await self._redis.xgroup_create(
                name=self._stream, groupname=self._group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    # -- admission control (spec 14) ---------------------------------------

    async def depth(self) -> int:
        """Approximate current queue depth (backlog + in-flight/pending).

        `XLEN` counts every entry still in the stream. Since
        `ack_and_remove()` both XACKs and XDELs, an entry is only removed
        once its result has been durably persisted -- so XLEN already
        reflects "not yet delivered" plus "delivered but not yet
        acknowledged" without needing group introspection (which varies
        across Redis-compatible backends).
        """
        try:
            return int(await self._redis.xlen(self._stream))
        except ResponseError:
            return 0

    async def has_capacity(self) -> bool:
        return await self.depth() < self._max_queue_depth

    # -- producer -----------------------------------------------------------

    async def enqueue(self, task_id: str) -> str:
        depth = await self.depth()
        if depth >= self._max_queue_depth:
            raise QueueFullError(
                f"Queue depth {depth} >= MAX_QUEUE_DEPTH {self._max_queue_depth}"
            )
        return await self._redis.xadd(self._stream, {"task_id": task_id})

    # -- consumer -------------------------------------------------------------

    async def read(self, consumer_name: str, count: int = 1, block_ms: int = 5000):
        """Read new messages for this consumer within the group.

        `block_ms` parks the coroutine on the Redis socket rather than the
        thread, so a waiting worker costs nothing while idle.
        """
        response = await self._redis.xreadgroup(
            groupname=self._group,
            consumername=consumer_name,
            streams={self._stream: ">"},
            count=count,
            block=block_ms,
        )
        if not response:
            return []
        _, messages = response[0]
        return messages  # list of (entry_id, {field: value})

    async def ack_and_remove(self, entry_id: str) -> None:
        # XACK then XDEL, deliberately not atomic. A crash between the two
        # leaves an acked-but-undeleted entry, which is inert: it is no
        # longer pending for any consumer and is never redelivered. The
        # reverse order would be unsafe, so the failure mode is one stale
        # entry's worth of memory rather than a lost or duplicated task.
        await self._redis.xack(self._stream, self._group, entry_id)
        await self._redis.xdel(self._stream, entry_id)

    async def claim_abandoned(self, consumer_name: str, min_idle_ms: Optional[int] = None):
        """Reclaim tasks whose worker crashed before acknowledging (spec 20).

        A future worker (potentially a different process) can claim entries
        that have been pending for longer than `min_idle_ms` and retry them.
        """
        # `or` would treat an explicit 0 ("reclaim immediately") as unset.
        if min_idle_ms is None:
            min_idle_ms = settings.pending_claim_timeout_ms
        pending = await self._redis.xpending_range(
            self._stream, self._group, min="-", max="+", count=100
        )
        reclaimed = []
        for entry in pending:
            if entry["time_since_delivered"] < min_idle_ms:
                continue
            claimed = await self._redis.xclaim(
                self._stream,
                self._group,
                consumer_name,
                min_idle_time=min_idle_ms,
                message_ids=[entry["message_id"]],
            )
            reclaimed.extend(claimed)
        return reclaimed
