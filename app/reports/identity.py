"""Report identity (spec sections 4 and 5).

The logical path `/{ticker}/{form}/{year}[/{quarter}]` is the external,
logical key. The SEC accession number is the immutable identity of the
concrete filing/version a logical report currently resolves to.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_TICKER_RE = re.compile(r"^[A-Z][A-Z.\-]{0,9}$")
_FORM_RE = re.compile(r"^[A-Z0-9]+(-[A-Z0-9]+)?(/A)?$")

# Placeholder year meaning "resolve whatever the latest applicable filing
# is" (spec section 10: an explicit year may be omitted). The worker
# resolves the concrete filing year and republishes under the real
# ticker/form/year logical path as well.
LATEST_YEAR_SENTINEL = 0


def encode_form(form: str) -> str:
    """`10-K/A` -> `10-K_A` so a form never introduces a path segment.

    Amended forms carry a slash in SEC's own vocabulary, which collides with
    the delimiter of the logical path and of artifact refs. Encoding is
    unambiguous because the form grammar above permits no underscore.
    """
    return form.replace("/", "_")


def decode_form(encoded: str) -> str:
    return encoded.replace("_", "/")


@dataclass(frozen=True)
class ReportIdentity:
    """The logical identity of a requested report.

    Extensible by design: `form`/`year`/`quarter` already support future
    10-Q/8-K style reports without any model changes.
    """

    ticker: str
    form: str
    year: int
    quarter: Optional[str] = None  # e.g. "Q1", only used by quarterly forms

    def __post_init__(self) -> None:
        ticker = self.ticker.upper()
        form = self.form.upper()
        if not _TICKER_RE.match(ticker):
            raise ValueError(f"Invalid ticker: {self.ticker!r}")
        if not _FORM_RE.match(form):
            raise ValueError(f"Invalid form: {self.form!r}")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "form", form)

    @property
    def logical_path(self) -> str:
        parts = [self.ticker, encode_form(self.form), str(self.year)]
        if self.quarter:
            parts.append(self.quarter)
        return "/" + "/".join(parts)

    @classmethod
    def parse(cls, path: str) -> "ReportIdentity":
        segments = [p for p in path.split("/") if p]
        if len(segments) not in (3, 4):
            raise ValueError(f"Invalid report path: {path!r}")
        ticker, form, year = segments[0], segments[1], segments[2]
        quarter = segments[3] if len(segments) == 4 else None
        return cls(
            ticker=ticker, form=decode_form(form), year=int(year), quarter=quarter
        )


@dataclass(frozen=True)
class FilingIdentity:
    """Immutable identity of the concrete SEC filing behind a logical report."""

    cik: str
    accession_number: str
    primary_document: str
    filing_date: str
    source_url: str
