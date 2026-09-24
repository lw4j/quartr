"""Parsing SEC submissions history and selecting the applicable filing.

Spec section 6: determining the latest 10-K, with explicit, configurable
handling of amendments (10-K/A). Spec section 9's future extensibility
(10-Q, 8-K, 20-F, ...) is supported by keeping form/accession/date generic.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from app.config import settings
from app.sec.retry import PermanentSecError


@dataclass(frozen=True)
class FilingRecord:
    cik: str
    form: str  # e.g. "10-K", "10-K/A"
    filing_date: date
    accession_number: str  # normalized with dashes, e.g. 0000320193-25-000073
    primary_document: str
    company_name: str

    @property
    def accession_no_dashes(self) -> str:
        return self.accession_number.replace("-", "")

    def filing_index_url(self) -> str:
        cik_int = str(int(self.cik))
        return (
            f"{settings.sec_archives_base_url}/{cik_int}/"
            f"{self.accession_no_dashes}/{self.primary_document}"
        )


@dataclass(frozen=True)
class FilingSelector:
    """Configuration-driven filing-selection policy (spec section 6).

    "Latest filed <form>, excluding amendments unless the caller explicitly
    requests amended filings." This is deliberately data, not hard-coded
    branching in the storage model, so future selectors (10-Q, 8-K, 20-F,
    ...) can be added without touching the report state machine.
    """

    form: str = "10-K"
    include_amended: bool = settings.include_amended_by_default

    def matches(self, form: str) -> bool:
        # An explicitly amended form (`10-K/A`) needs no special case: it is
        # matched exactly by the final comparison, and widening it with
        # `include_amended` is a no-op. Only a base form widens.
        if self.include_amended:
            return form == self.form or form == f"{self.form}/A"
        return form == self.form


def parse_submissions(payload: dict) -> list[FilingRecord]:
    """Flatten SEC's columnar `recent` filings structure into records."""
    cik = str(payload["cik"]).zfill(10)
    company_name = payload.get("name", "")
    recent = payload.get("filings", {}).get("recent", {})

    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    records: list[FilingRecord] = []
    for i in range(len(forms)):
        try:
            filing_date = date.fromisoformat(dates[i])
        except (ValueError, IndexError):
            continue
        records.append(
            FilingRecord(
                cik=cik,
                form=forms[i],
                filing_date=filing_date,
                accession_number=accessions[i],
                primary_document=primary_docs[i] if i < len(primary_docs) else "",
                company_name=company_name,
            )
        )
    return records


def select_latest(
    filings: list[FilingRecord],
    selector: FilingSelector,
    year: Optional[int] = None,
) -> FilingRecord:
    """Pick the latest filing matching the selector (and optional year).

    Raises PermanentSecError (non-retryable) if nothing matches -- the
    filing genuinely does not exist, retrying will not help.
    """
    candidates = [f for f in filings if selector.matches(f.form)]
    if year is not None:
        candidates = [f for f in candidates if f.filing_date.year == year]
    # SEC occasionally omits `primaryDocument`. Such a record would build an
    # Archives URL ending in a slash, which resolves to the accession's
    # directory listing -- a real HTML page that converts to a valid-looking
    # PDF of a file index. Excluding it fails honestly instead of reporting
    # the wrong document as a success.
    candidates = [f for f in candidates if f.primary_document]
    if not candidates:
        raise PermanentSecError(
            f"No filing found matching form={selector.form!r} "
            f"include_amended={selector.include_amended} year={year}"
        )
    return max(candidates, key=lambda f: f.filing_date)
