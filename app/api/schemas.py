"""Request/response schemas for the public API (spec section 10)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class CreateReportRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    form: str = Field(default="10-K", examples=["10-K"])
    year: Optional[int] = Field(
        default=None, description="Omit to resolve the latest applicable filing", examples=["2026"]
    )
    quarter: Optional[str] = Field(default=None, examples=["Q1"])
    include_amended: bool = Field(
        default=False,
        description="Include 10-K/A style amendments when selecting the latest filing",
    )


class ArtifactRef(BaseModel):
    """Backend-independent handle to a generated artifact.

    Deliberately *not* a filesystem path: spec section 9 leaves the physical
    backend unspecified, so exposing one would freeze an implementation
    detail into the public contract. `ref` is built from spec section 27
    identifiers only (logical path + accession number).
    """

    ref: str = Field(..., examples=["AAPL/10-K/2025/0000320193-25-000079"])
    media_type: str = "application/pdf"
    url: str = Field(
        ...,
        description="Relative URL serving the artifact bytes",
        examples=["/artifacts/AAPL/10-K/2025/0000320193-25-000079"],
    )


class ReportTaskResponse(BaseModel):
    task_id: str
    logical_path: str
    state: str
    accession_number: Optional[str] = None
    artifact: Optional[ArtifactRef] = None
    error: Optional[str] = None
