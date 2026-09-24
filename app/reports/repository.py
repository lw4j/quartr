"""Redis-backed report repository: state, current-accession pointer, dedup.

Spec section 9: "A database or Redis record should maintain the current
mapping: logical_path -> current_accession -> artifact."

Spec section 13: idempotency/deduplication -- concurrent requests for the
same logical report must produce exactly one task, not one per request.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis

from app.reports.identity import ReportIdentity
from app.reports.models import ReportTask
from app.reports.state import ReportState, assert_transition


def _task_key(task_id: str) -> str:
    return f"report:task:{task_id}"


def _policy_suffix(include_amended: bool) -> str:
    """Namespaces a logical path by the filing-selection policy.

    `include_amended` changes *which* filing satisfies a request, so a
    report fetched with amendments included is not interchangeable with one
    fetched without. Keeping the policy in the key stops an
    `include_amended=true` request from being served the cached
    non-amended PDF, and stops it from being deduplicated onto an in-flight
    task that is applying the other policy.
    """
    return "+amended" if include_amended else ""


def _logical_key(logical_path: str, include_amended: bool = False) -> str:
    return f"report:logical:{logical_path}{_policy_suffix(include_amended)}"


def _pending_task_key(logical_path: str, include_amended: bool = False) -> str:
    """Points at the in-flight task id for a logical path, if any.

    Used for deduplication: while a task is accepted/queued/processing this
    key holds its id, so a second request for the same logical path and
    selection policy is attached to the existing task instead of creating a
    new one.
    """
    return f"report:pending:{logical_path}{_policy_suffix(include_amended)}"


class ReportRepository:
    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    # -- lookup -----------------------------------------------------------

    async def get_current_artifact(
        self, logical_path: str, include_amended: bool = False
    ) -> Optional[dict]:
        raw = await self._redis.hgetall(_logical_key(logical_path, include_amended))
        return raw or None

    async def get_task(self, task_id: str) -> Optional[ReportTask]:
        raw = await self._redis.get(_task_key(task_id))
        return ReportTask.from_json(raw) if raw else None

    async def get_pending_task_id(
        self, logical_path: str, include_amended: bool = False
    ) -> Optional[str]:
        return await self._redis.get(_pending_task_key(logical_path, include_amended))

    # -- creation / dedup ---------------------------------------------------

    async def get_or_create_task(
        self, identity: ReportIdentity, include_amended: bool
    ) -> tuple[ReportTask, bool]:
        """Return (task, created). Deduplicates concurrent identical requests.

        Uses SET NX as an atomic compare-and-set so only the first of a
        burst of concurrent requests creates a task; the rest attach to it.
        """
        logical_path = identity.logical_path
        task_id = str(uuid.uuid4())
        pending_key = _pending_task_key(logical_path, include_amended)

        created = bool(
            await self._redis.set(pending_key, task_id, nx=True, ex=6 * 60 * 60)
        )
        if not created:
            existing_id = await self._redis.get(pending_key)
            existing = await self.get_task(existing_id) if existing_id else None
            if existing is not None:
                return existing, False
            # Pending pointer existed but task expired/missing; fall through
            # and create a fresh one, reclaiming the pointer.
            await self._redis.set(pending_key, task_id, ex=6 * 60 * 60)

        task = ReportTask(
            task_id=task_id, identity=identity, include_amended=include_amended
        )
        await self._save(task)
        return task, True

    async def clear_pending(self, logical_path: str, include_amended: bool = False) -> None:
        await self._redis.delete(_pending_task_key(logical_path, include_amended))

    # -- mutation -----------------------------------------------------------

    async def _save(self, task: ReportTask) -> None:
        await self._redis.set(_task_key(task.task_id), task.to_json())

    async def transition(
        self, task: ReportTask, target: ReportState, **updates
    ) -> ReportTask:
        assert_transition(task.state, target)
        task.state = target
        for key, value in updates.items():
            setattr(task, key, value)
        task.updated_at = datetime.now(timezone.utc).isoformat()
        await self._save(task)
        if target in (ReportState.COMPLETED, ReportState.FAILED):
            await self.clear_pending(task.identity.logical_path, task.include_amended)
        return task

    async def save_attempts(self, task: ReportTask) -> ReportTask:
        """Persist the attempt counter without changing state."""
        task.updated_at = datetime.now(timezone.utc).isoformat()
        await self._save(task)
        return task

    async def force_fail(self, task: ReportTask, error: str) -> ReportTask:
        """Terminal failure that bypasses the state machine.

        Used for poison messages: a task whose handler raised before it
        could record an outcome may be in a state with no legal edge to
        FAILED, and `transition` would raise again. Redelivery is bounded,
        so the task must still be able to reach a terminal state.

        A task that already reached COMPLETED is never demoted -- its PDF
        exists, and reporting it as failed would be a lie.
        """
        if task.state is ReportState.COMPLETED:
            return task
        task.state = ReportState.FAILED
        task.error = error
        task.updated_at = datetime.now(timezone.utc).isoformat()
        await self._save(task)
        await self.clear_pending(task.identity.logical_path, task.include_amended)
        return task

    async def publish_artifact(self, task: ReportTask) -> None:
        """Record the current logical_path -> accession -> artifact mapping."""
        assert task.filing is not None and task.artifact_path is not None
        await self._redis.hset(
            _logical_key(task.identity.logical_path, task.include_amended),
            mapping={
                "accession_number": task.filing.accession_number,
                "artifact_path": task.artifact_path,
                "cik": task.filing.cik,
                "updated_at": str(time.time()),
            },
        )
