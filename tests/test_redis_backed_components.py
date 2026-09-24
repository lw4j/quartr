import fakeredis.aioredis

from app.queue.stream import ReportQueue
from app.reports.identity import ReportIdentity
from app.reports.repository import ReportRepository
from app.reports.state import ReportState
from app.sec.rate_limiter import GlobalSecRateLimiter


def _redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


async def test_rate_limiter_enforces_limit_within_window():
    limiter = GlobalSecRateLimiter(_redis(), requests_per_second=2)
    assert await limiter.try_acquire() is True
    assert await limiter.try_acquire() is True
    assert await limiter.try_acquire() is False  # 3rd request in same window rejected


async def test_report_dedup_creates_single_task_for_concurrent_requests():
    repo = ReportRepository(_redis())
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)

    task1, created1 = await repo.get_or_create_task(identity, include_amended=False)
    task2, created2 = await repo.get_or_create_task(identity, include_amended=False)

    assert created1 is True
    assert created2 is False
    assert task1.task_id == task2.task_id


async def test_queue_admission_control_rejects_when_full():
    queue = ReportQueue(_redis(), max_queue_depth=1)
    await queue.ensure_group()
    await queue.enqueue("task-1")
    assert await queue.has_capacity() is False


async def test_report_state_lifecycle_via_repository():
    repo = ReportRepository(_redis())
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    task, _ = await repo.get_or_create_task(identity, include_amended=False)

    await repo.transition(task, ReportState.QUEUED)
    await repo.transition(task, ReportState.PROCESSING)
    await repo.transition(task, ReportState.COMPLETED)

    reloaded = await repo.get_task(task.task_id)
    assert reloaded.state == ReportState.COMPLETED
    # pending pointer cleared on terminal state (dedup key released)
    assert await repo.get_pending_task_id(identity.logical_path) is None
