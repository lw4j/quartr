"""SEC source document -> PDF conversion (spec section 8).

Kept as a distinct stage from acquisition/storage so the original SEC
source can be preserved and PDF generation repeated without another SEC
request. This draft implementation converts HTML filings via a headless
renderer (`weasyprint`) as a reasonable, dependency-light default; the
converter boundary can be swapped out (e.g. for wkhtmltopdf or a browser
based renderer) without touching the SEC client or worker orchestration.
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import datetime, timezone

from app.reports.identity import FilingIdentity, ReportIdentity


@dataclass
class PdfMetadata:
    ticker: str
    company_name: str
    cik: str
    form: str
    filing_date: str
    accession_number: str
    primary_document: str
    source_url: str
    generation_timestamp: str

    @classmethod
    def build(
        cls,
        identity: ReportIdentity,
        filing: FilingIdentity,
        company_name: str,
    ) -> "PdfMetadata":
        return cls(
            ticker=identity.ticker,
            company_name=company_name,
            cik=filing.cik,
            form=identity.form,
            filing_date=filing.filing_date,
            accession_number=filing.accession_number,
            primary_document=filing.primary_document,
            source_url=filing.source_url,
            generation_timestamp=datetime.now(timezone.utc).isoformat(),
        )


class PdfConversionError(Exception):
    pass


def convert_to_pdf(source_bytes: bytes, source_content_type: str, metadata: PdfMetadata) -> bytes:
    """Convert a downloaded SEC source document to a PDF byte stream.

    `source_content_type` distinguishes HTML filings (the common case for
    modern 10-Ks) from plain text filings. Real embedding of `metadata`
    into PDF document properties is left to the concrete renderer
    implementation used in production; this draft focuses on the pipeline
    boundary rather than rendering fidelity.
    """
    try:
        if "html" in source_content_type:
            return _convert_html(source_bytes, metadata)
        return _convert_text(source_bytes, metadata)
    except Exception as exc:  # pragma: no cover - defensive boundary
        raise PdfConversionError(f"Failed to convert filing to PDF: {exc}") from exc


def _convert_html(source_bytes: bytes, metadata: PdfMetadata) -> bytes:
    from weasyprint import HTML  # imported lazily; heavy optional dependency

    pdf_io = io.BytesIO()
    HTML(string=source_bytes.decode("utf-8", errors="replace")).write_pdf(
        pdf_io,
        # PDF metadata identifying the source (spec section 8).
        # Since this format allows embedding, we can embed the html source as well.
        # pdf_variant="pdf/a-3b",
    )
    return pdf_io.getvalue()


def _convert_text(source_bytes: bytes, metadata: PdfMetadata) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    pdf_io = io.BytesIO()
    c = canvas.Canvas(pdf_io, pagesize=A4)
    c.setTitle(f"{metadata.ticker} {metadata.form} {metadata.filing_date}")
    text = c.beginText(40, 740)
    for line in source_bytes.decode("utf-8", errors="replace").splitlines():
        text.textLine(line[:110])
    c.drawText(text)
    c.showPage()
    c.save()
    return pdf_io.getvalue()
