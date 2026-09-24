"""Report worker: the loop described in spec section 19.

1. Read task from Redis Stream.
2. Resolve ticker -> CIK.
3. Query SEC submissions history.
4. Select applicable filing.
5. Check whether accession is already stored.
6. Download required SEC source document(s).
7. Convert filing to PDF.
8. Validate generated PDF.
9. Store PDF and metadata.
10. Update report state.
11. ACK Redis Stream message.

The message is only ACKed after the result has been safely persisted
(spec section 19), and abandoned messages are reclaimed via
`ReportQueue.claim_abandoned` (spec section 20).

The worker runs on an event loop: Redis and SEC I/O are awaited, while
GIL-holding work (PDF rendering) and blocking file writes are pushed to a
thread via `asyncio.to_thread` so they cannot stall the loop. With
`WORKER_CONCURRENCY > 1` a single worker process keeps several tasks in
flight, still bounded globally by the shared SEC rate limiter.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid

from app.companies.cik_mapping import CikMapping, TickerNotFoundError
from app.config import settings
from app.conversion.pdf import PdfConversionError, PdfMetadata, convert_to_pdf
from app.deps import build_sec_client, get_artifact_store, get_queue, get_report_repository
from app.observability import metrics
from app.queue.stream import ReportQueue
from app.reports.identity import LATEST_YEAR_SENTINEL, FilingIdentity, ReportIdentity
from app.reports.models import ReportTask
from app.reports.repository import ReportRepository
from app.reports.state import InvalidStateTransition, ReportState
from app.sec.client import SecClient
from app.sec.retry import PermanentSecError, TransientSecError
from app.sec.submissions import FilingSelector, parse_submissions, select_latest
from app.storage.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)


class ReportWorker:
    def __init__(
        self,
        worker_id: str | None = None,
        queue: ReportQueue | None = None,
        repository: ReportRepository | None = None,
        sec_client: SecClient | None = None,
        cik_mapping: CikMapping | None = None,
        artifact_store: ArtifactStore | None = None,
        concurrency: int | None = None,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue = queue or get_queue()
        self.repository = repository or get_report_repository()
        self.sec_client = sec_client or build_sec_client()
        self.cik_mapping = cik_mapping or CikMapping()
        self.artifact_store = artifact_store or get_artifact_store()
        self.concurrency = concurrency or settings.worker_concurrency
        self._stopping = asyncio.Event()

    def request_stop(self) -> None:
        """Ask the read loop to finish the current batch and exit."""
        self._stopping.set()

    async def run_forever(self) -> None:
        logger.info(
            "Worker %s starting (concurrency=%d)", self.worker_id, self.concurrency
        )
        await self.queue.ensure_group()
        await self.cik_mapping.ensure_fresh()
        metrics.active_workers.inc()
        try:
            while not self._stopping.is_set():
                # Cheap timestamp check; only hits SEC once per refresh
                # interval, giving the periodic refresh of spec section 3.2
                # without a separate background task to supervise.
                await self.cik_mapping.ensure_fresh()
                await self._reclaim_abandoned()
                messages = await self.queue.read(
                    self.worker_id, count=self.concurrency, block_ms=5000
                )
                await self._handle_batch(messages)
        finally:
            metrics.active_workers.dec()
            await self.sec_client.aclose()
            logger.info("Worker %s stopped", self.worker_id)

    async def _handle_batch(self, messages) -> None:
        """Process a batch concurrently; one failure must not cancel siblings.

        Each message is fully handled (including its own error handling and
        ACK) inside `_handle_message`, so the TaskGroup only needs to await
        completion.
        """
        if not messages:
            return
        if self.concurrency == 1:
            for entry_id, fields in messages:
                await self._handle_message(entry_id, fields)
            return
        async with asyncio.TaskGroup() as tg:
            for entry_id, fields in messages:
                tg.create_task(self._handle_message(entry_id, fields))

    async def _reclaim_abandoned(self) -> None:
        """ Attemps to claim tasks which spend more time in the queue than max allowed, and handles them. """
        reclaimed = await self.queue.claim_abandoned(self.worker_id)
        for entry_id, fields in reclaimed:
            logger.info("Reclaimed abandoned task %s -> %s", entry_id, fields)
            await self._handle_message(entry_id, fields)

    async def _handle_message(self, entry_id: str, fields: dict) -> None:
        task_id = fields.get("task_id")

        if not task_id:
            logger.warning("Entry doesn't contain required field: task_id, dropping message %s", entry_id)
            await self.queue.ack_and_remove(entry_id)
            return

        task = await self.repository.get_task(task_id)
        if task is None:
            logger.warning("Task %s not found, dropping message %s", task_id, entry_id)
            await self.queue.ack_and_remove(entry_id)
            return

        try:
            await self.process_task(task)
        except asyncio.CancelledError:
            # Shutdown: leave the message unacked so another worker
            # reclaims it.
            raise
        except Exception as exc:
            logger.exception("Unhandled error processing task %s", task_id)
            # The outcome was NOT durably persisted, so ACKing here would
            # silently lose an accepted task. Leave it unacked for reclaim,
            # but bound redelivery so an always-crashing task cannot loop
            # forever.
            #
            # The recovery path itself talks to Redis -- the very thing that
            # may have just failed -- so it must not be able to kill the
            # read loop.
            try:
                await self._recover_from_unhandled(task_id, task, entry_id, exc)
            except Exception:
                logger.exception(
                    "Recovery failed for task %s; leaving message for reclaim",
                    task_id,
                )
            return

        # ACK only after the outcome (success or terminal failure) has been
        # durably persisted via process_task's state transitions.
        await self.queue.ack_and_remove(entry_id)

    async def _recover_from_unhandled(
        self, task_id: str, task: ReportTask, entry_id: str, exc: Exception
    ) -> None:
        # Re-read rather than mutating the in-memory task: `_fail_transiently`
        # increments `attempts` before its own transition, so if it raised
        # part-way the object in hand may already carry that increment.
        # Incrementing it again would halve the configured retry budget.
        current = await self.repository.get_task(task_id) or task
        current.attempts += 1

        if current.state is ReportState.COMPLETED:
            # Succeeded, then something failed afterwards. Never demote.
            logger.warning(
                "Task %s already COMPLETED, ignoring post-completion error",
                task_id,
            )
            await self.queue.ack_and_remove(entry_id)
            return

        if current.attempts >= settings.retry_max_attempts:
            logger.error(
                "Task %s exhausted %d attempts, dead-lettering",
                task_id,
                current.attempts,
            )
            await self.repository.force_fail(current, f"Unhandled error: {exc}")
            metrics.jobs_failed_total.inc()
            await self.queue.ack_and_remove(entry_id)
        else:
            await self.repository.save_attempts(current)

    async def process_task(self, task: ReportTask) -> None:
        log_ctx = {
            "task_id": task.task_id,
            "logical_report_path": task.identity.logical_path,
            "ticker": task.identity.ticker,
            "form": task.identity.form,
            "worker_id": self.worker_id,
        }
        logger.info("Processing task", extra=log_ctx)

        # Captured before the "latest" sentinel is resolved to a real year,
        # because that rewrite changes which dedup key must be cleared.
        original_logical_path = task.identity.logical_path

        try:
            await self.repository.transition(task, ReportState.PROCESSING)
        except InvalidStateTransition:
            # Re-delivered message for a task already past QUEUED. Safe to
            # continue: the work itself is idempotent. Any other error is a
            # real persistence failure and must propagate.
            logger.warning(
                "Unexpected state %s for task %s, processing anyway",
                task.state,
                task.task_id,
                extra=log_ctx,
            )

        try:
            company = self.cik_mapping.resolve(task.identity.ticker)

            metrics.sec_requests_total.inc()
            start = time.monotonic()
            resp = await self.sec_client.get_submissions(company.cik)
            metrics.sec_request_latency_seconds.observe(time.monotonic() - start)
            filings = parse_submissions(resp.json())

            selector = FilingSelector(
                form=task.identity.form,
                include_amended=task.include_amended,
            )
            requested_year = (
                None
                if task.identity.year == LATEST_YEAR_SENTINEL
                else task.identity.year
            )
            filing_record = select_latest(filings, selector, year=requested_year)

            # The real identity of the result with the correct year set.
            resolved_identity = task.identity
            if task.identity.year == LATEST_YEAR_SENTINEL:
                resolved_identity = ReportIdentity(
                    ticker=task.identity.ticker,
                    form=task.identity.form,
                    year=filing_record.filing_date.year,
                    quarter=task.identity.quarter,
                )

            source_url = filing_record.filing_index_url()
            # Construct the storage record.
            filing = FilingIdentity(
                cik=company.cik,
                accession_number=filing_record.accession_number,
                primary_document=filing_record.primary_document,
                filing_date=filing_record.filing_date.isoformat(),
                source_url=source_url,
            )

            _, ext = os.path.splitext(filing_record.primary_document)
            # Get the paths to store the source, artifact and metadata.
            paths = self.artifact_store.paths_for(
                resolved_identity, filing.accession_number, ext or ".html"
            )

            if self.artifact_store.artifact_exists(paths):
                # Idempotency (spec 8/13): artifact already produced for
                # this accession, skip re-download/convert entirely.
                logger.info("Artifact already exists, skipping download", extra=log_ctx)
            else:
                metrics.sec_requests_total.inc()
                start = time.monotonic()
                doc_resp = await self.sec_client.download_document(source_url)
                metrics.sec_request_latency_seconds.observe(time.monotonic() - start)

                pdf_metadata = PdfMetadata.build(resolved_identity, filing, company.name)
                start = time.monotonic()
                # weasyprint/reportlab are CPU-bound and hold the GIL, so
                # rendering runs on a thread to keep the loop responsive.
                pdf_bytes = await asyncio.to_thread(
                    convert_to_pdf,
                    doc_resp.content,
                    doc_resp.headers.get("Content-Type", "text/html"),
                    pdf_metadata,
                )
                metrics.pdf_conversion_latency_seconds.observe(time.monotonic() - start)
                self._validate_pdf(pdf_bytes)

                # Blocking file I/O: no kernel offers a true async path for
                # regular files, so a thread is the honest implementation.
                await asyncio.to_thread(
                    self.artifact_store.write,
                    paths,
                    doc_resp.content,
                    pdf_bytes,
                    pdf_metadata.__dict__,
                )

            task.identity = resolved_identity
            await self.repository.transition(
                task,
                ReportState.COMPLETED,
                filing=filing,
                artifact_path=paths.pdf_path,
            )
            # Past this point the PDF is on disk and success is durably
            # recorded. COMPLETED has no outgoing edges, so letting a
            # bookkeeping failure reach the handlers below would attempt an
            # illegal transition and dead-letter a task that in fact
            # succeeded. Log and keep the success instead.
            try:
                await self.repository.publish_artifact(task)
                if resolved_identity.logical_path != original_logical_path:
                    # A "latest" task was created under the /TICKER/FORM/0
                    # sentinel but completes under its resolved year, so
                    # transition() cleared the wrong dedup key. Clear the
                    # one it was actually registered under, otherwise this
                    # logical path stays pinned for the pending TTL.
                    await self.repository.clear_pending(
                        original_logical_path, task.include_amended
                    )
            except Exception:
                logger.exception(
                    "Post-completion bookkeeping failed; task stays COMPLETED",
                    extra=log_ctx,
                )
            metrics.jobs_completed_total.inc()
            logger.info("Task completed", extra=log_ctx)

        except TickerNotFoundError as exc:
            await self._fail_permanently(task, exc, log_ctx)
        except PermanentSecError as exc:
            await self._fail_permanently(task, exc, log_ctx)
        except PdfConversionError as exc:
            await self._fail_permanently(task, exc, log_ctx)
        except TransientSecError as exc:
            await self._fail_transiently(task, exc, log_ctx)
        except asyncio.CancelledError:
            # Shutdown, not a task failure: leave the message unacked so
            # another worker reclaims it.
            raise
        except Exception as exc:
            await self._fail_transiently(task, exc, log_ctx)

    async def _fail_permanently(self, task: ReportTask, exc: Exception, log_ctx: dict) -> None:
        logger.error("Task failed permanently: %s", exc, extra=log_ctx)
        await self.repository.transition(task, ReportState.FAILED, error=str(exc))
        metrics.jobs_failed_total.inc()

    async def _fail_transiently(self, task: ReportTask, exc: Exception, log_ctx: dict) -> None:
        logger.warning("Task failed transiently: %s", exc, extra=log_ctx)
        # This value will be save as part of the update.
        task.attempts += 1
        max_attempts = settings.retry_max_attempts
        if task.attempts >= max_attempts:
            await self.repository.transition(task, ReportState.FAILED, error=str(exc))
            metrics.jobs_failed_total.inc()
            return
        await self.repository.transition(task, ReportState.RETRYING, error=str(exc))
        # Same ordering rule as the API: persist QUEUED before the message
        # becomes visible, or a worker may load this task while it is still
        # RETRYING (which has no legal edge to PROCESSING).
        await self.repository.transition(task, ReportState.QUEUED)
        await self.queue.enqueue(task.task_id)
        metrics.jobs_retried_total.inc()

    @staticmethod
    def _validate_pdf(pdf_bytes: bytes) -> None:
        # Header check only. The renderer either produces a well-formed
        # document or raises, so this guards against an empty/truncated
        # write rather than against structural corruption; full trailer and
        # catalog validation would mean parsing back every PDF we generate.
        if not pdf_bytes.startswith(b"%PDF-"):
            raise PdfConversionError("Generated artifact is not a valid PDF")
