"""Public HTTP API (spec section 10).

- Validates the request.
- Resolves ticker -> CIK for a fast existence check (fails fast on unknown
  tickers with a 4xx rather than enqueuing an unprocessable task).
- Deduplicates concurrent identical requests to a single task.
- Applies admission control (queue depth) before enqueueing.
- Never blocks on SEC retrieval or PDF generation.

All handlers are `async def` and every Redis call is awaited, so a request
waiting on Redis parks its coroutine frame instead of occupying a thread.
Concurrency is bounded by connections rather than by the 40-slot AnyIO
thread pool that `def` handlers get dispatched to.
"""
from __future__ import annotations

import asyncio
import logging
import os

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import FileResponse

from app.api.schemas import ArtifactRef, CreateReportRequest, ReportTaskResponse
from app.companies.cik_mapping import TickerNotFoundError, cik_mapping
from app.deps import get_artifact_store, get_queue, get_report_repository
from app.observability import metrics
from app.queue.stream import QueueFullError
from app.reports.identity import LATEST_YEAR_SENTINEL, ReportIdentity, decode_form
from app.reports.state import ReportState
from app.storage.artifact_store import InvalidArtifactRefError

logger = logging.getLogger(__name__)
router = APIRouter()


def _artifact_ref(identity: ReportIdentity, accession_number: str | None) -> ArtifactRef | None:
    """Public handle for a completed artifact, or None if not yet produced.

    Physical paths stay internal to the storage layer (spec section 9 leaves
    the backend unspecified), so the API only ever emits this reference.
    """
    if not accession_number:
        return None
    ref = get_artifact_store().ref_for(identity, accession_number)
    return ArtifactRef(ref=ref, url=f"/artifacts/{ref}")


@router.post("/reports", response_model=ReportTaskResponse)
async def create_report(payload: CreateReportRequest, response: Response) -> ReportTaskResponse:
    try:
        cik_mapping.resolve(payload.ticker)
    except TickerNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    year = payload.year if payload.year is not None else LATEST_YEAR_SENTINEL
    try:
        # Ensure valid identity can be constructed.
        identity = ReportIdentity(
            ticker=payload.ticker, form=payload.form, year=year, quarter=payload.quarter
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    repo = get_report_repository()
    queue = get_queue()

    # Already available? Return 200 immediately without touching the queue.
    existing_artifact = await repo.get_current_artifact(
        identity.logical_path, payload.include_amended
    )
    if existing_artifact:
        accession = existing_artifact.get("accession_number")
        response.status_code = 200
        return ReportTaskResponse(
            task_id="",
            logical_path=identity.logical_path,
            state=ReportState.COMPLETED.value,
            accession_number=accession,
            artifact=_artifact_ref(identity, accession),
        )

    task, created = await repo.get_or_create_task(identity, include_amended=payload.include_amended)

    if created:
        if not await queue.has_capacity():
            await repo.clear_pending(identity.logical_path, payload.include_amended)
            response.headers["Retry-After"] = "30"
            raise HTTPException(
                status_code=429,
                detail="System reached maximum capacity, please retry later",
            )
        # Persist QUEUED *before* enqueueing. A worker blocked on
        # XREADGROUP receives the message within one Redis round-trip, and
        # would otherwise load the task while it is still ACCEPTED -- making
        # every downstream transition invalid and causing the accepted task
        # to be dropped (spec 24.1: no accepted task may be silently lost).
        await repo.transition(task, ReportState.QUEUED)
        try:
            await queue.enqueue(task.task_id)
        except QueueFullError as exc:
            await repo.clear_pending(identity.logical_path, payload.include_amended)
            response.headers["Retry-After"] = "30"
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        metrics.jobs_accepted_total.inc()

    response.status_code = 202
    return ReportTaskResponse(
        task_id=task.task_id,
        logical_path=identity.logical_path,
        state=task.state.value,
    )


@router.get("/reports/{ticker}/{form}/{year}", response_model=ReportTaskResponse)
async def get_report(
    ticker: str, form: str, year: int, include_amended: bool = False
) -> ReportTaskResponse:
    try:
        # Amended forms are encoded in paths (`10-K_A`), so decode before
        # validating -- otherwise the encoded spelling the service itself
        # emits would be rejected as an invalid form.
        identity = ReportIdentity(ticker=ticker, form=decode_form(form), year=year)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    repo = get_report_repository()

    artifact = await repo.get_current_artifact(identity.logical_path, include_amended)
    if artifact:
        accession = artifact.get("accession_number")
        return ReportTaskResponse(
            task_id="",
            logical_path=identity.logical_path,
            state=ReportState.COMPLETED.value,
            accession_number=accession,
            artifact=_artifact_ref(identity, accession),
        )

    pending_task_id = await repo.get_pending_task_id(
        identity.logical_path, include_amended
    )
    if pending_task_id:
        task = await repo.get_task(pending_task_id)
        if task:
            return ReportTaskResponse(
                task_id=task.task_id, logical_path=identity.logical_path, state=task.state.value
            )

    raise HTTPException(status_code=404, detail="Report has not been scheduled for retrieval, submit a fetch query first")


@router.get("/tasks/{task_id}", response_model=ReportTaskResponse)
async def get_task(task_id: str) -> ReportTaskResponse:
    repo = get_report_repository()
    task = await repo.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Unknown task id")
    accession = task.filing.accession_number if task.filing else None
    return ReportTaskResponse(
        task_id=task.task_id,
        logical_path=task.identity.logical_path,
        state=task.state.value,
        accession_number=accession,
        # Only completed tasks have a durable artifact; a failed task may
        # still carry a resolved accession from a partial attempt.
        artifact=(
            _artifact_ref(task.identity, accession)
            if task.state == ReportState.COMPLETED
            else None
        ),
        error=task.error,
    )


@router.get(
    "/artifacts/{ref:path}",
    response_class=FileResponse,
    responses={
        200: {"content": {"application/pdf": {}}, "description": "The generated PDF"},
        400: {"description": "Malformed artifact reference"},
        404: {"description": "No artifact stored for this reference"},
    },
)
async def get_artifact(ref: str) -> FileResponse:
    """Serve a generated PDF by its `artifact.ref`.

    Deliberately basic: this is the minimal read path that makes stored
    artifacts retrievable over HTTP. Spec section 26 defers the *public*
    report-serving API, so caching (ETag/conditional GET, `Cache-Control`),
    authentication, egress rate limiting and reverse-proxy/CDN offload
    (`X-Accel-Redirect`, signed URLs) are intentionally not handled here.
    Accession-scoped artifacts are immutable, so caching is a safe and
    obvious later addition.

    Two things it does get right: `resolve_pdf_path` validates the
    reference before it ever becomes a path, so user input cannot escape
    the storage root; and `FileResponse` reads the file on a thread, so
    large PDFs never block the event loop.
    """
    store = get_artifact_store()
    try:
        path = store.resolve_pdf_path(ref)
        identity, accession = store.parse_ref(ref)
    except InvalidArtifactRefError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not await asyncio.to_thread(os.path.isfile, path):
        raise HTTPException(
            status_code=404,
            detail="No artifact stored for this reference; request the report first",
        )

    download_name = (
        f"{identity.ticker}-{identity.form.replace('/', '')}-"
        f"{identity.year}-{accession}.pdf"
    )
    return FileResponse(
        path,
        media_type="application/pdf",
        filename=download_name,
        # Reports are meant to be read, so render in-browser rather than
        # forcing a download.
        content_disposition_type="inline",
    )
