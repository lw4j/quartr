"""Worker process entrypoint (`python -m app.workers.main`)."""
from __future__ import annotations

import asyncio
import logging
import signal

from prometheus_client import start_http_server

from app.config import settings
from app.deps import get_redis
from app.observability.logging import configure_logging
from app.workers.worker import ReportWorker

configure_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _run() -> None:
    worker = ReportWorker()
    loop = asyncio.get_running_loop()

    # Worker metrics (jobs_*, sec_*, pdf_conversion_*) are incremented in
    # this process, so they need their own scrape endpoint -- the API's
    # /metrics only ever sees its own registry.
    if settings.worker_metrics_port:
        start_http_server(settings.worker_metrics_port)
        logger.info("Worker metrics on :%d/metrics", settings.worker_metrics_port)

    def _shutdown(signum: int) -> None:
        logger.info(
            "Worker %s received signal %s, shutting down", worker.worker_id, signum
        )
        worker.request_stop()

    # `loop.add_signal_handler` schedules the callback on the loop instead of
    # interrupting arbitrary bytecode, so shutdown is cooperative: the batch
    # in flight finishes and is ACKed before the process exits.
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        await worker.run_forever()
    finally:
        await get_redis().aclose()


def main() -> None:
    settings.validate_for_production()
    try:
        import uvloop
    except ImportError:
        asyncio.run(_run())
    else:
        uvloop.run(_run())


if __name__ == "__main__":
    main()
