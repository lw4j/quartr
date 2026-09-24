"""Ticker -> CIK resolution (spec section 3.2).

Design:

    SEC CIK mapping
        -> local persisted mapping
        -> API lookup

The mapping is refreshed periodically rather than fetched per report
request, and CIKs are normalized to the SEC-required 10-digit
zero-padded representation.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class TickerNotFoundError(Exception):
    """Raised when a ticker cannot be resolved to a CIK. Non-retryable."""


def normalize_cik(cik: str | int) -> str:
    """Normalize a CIK to the SEC-required 10-digit zero-padded string.

    Example: 320193 -> "0000320193"
    """
    digits = str(cik).strip().lstrip("0") or "0"
    if not digits.isdigit():
        raise ValueError(f"Invalid CIK: {cik!r}")
    return digits.zfill(10)


@dataclass(frozen=True)
class CompanyRecord:
    ticker: str
    cik: str  # normalized, 10-digit
    name: str


# Seed data covering the initial required companies (spec 3.1). This is used
# as a fallback / bootstrap and is overwritten by the persisted mapping once
# it has been refreshed from SEC at least once.
_SEED_COMPANIES = [
    CompanyRecord("AAPL", normalize_cik(320193), "Apple Inc."),
    CompanyRecord("META", normalize_cik(1326801), "Meta Platforms, Inc."),
    CompanyRecord("GOOGL", normalize_cik(1652044), "Alphabet Inc."),
    CompanyRecord("GOOG", normalize_cik(1652044), "Alphabet Inc."),
    CompanyRecord("AMZN", normalize_cik(1018724), "Amazon.com, Inc."),
    CompanyRecord("NFLX", normalize_cik(1065280), "Netflix, Inc."),
    CompanyRecord("GS", normalize_cik(886982), "The Goldman Sachs Group, Inc."),
]


class CikMapping:
    """Local ticker -> CIK mapping with periodic refresh from SEC.

    Multiple tickers may resolve to the same CIK (e.g. GOOGL/GOOG), and the
    mapping never assumes ticker symbols are the canonical SEC identity.
    """

    def __init__(
        self,
        path: str = settings.cik_mapping_path,
        refresh_seconds: int = settings.cik_mapping_refresh_seconds,
    ) -> None:
        self._path = path
        self._refresh_seconds = refresh_seconds
        self._by_ticker: dict[str, CompanyRecord] = {
            c.ticker: c for c in _SEED_COMPANIES
        }
        self._last_refresh: float = 0.0
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r") as f:
                raw = json.load(f)

            records = {
                item["ticker"]: CompanyRecord( item["ticker"], normalize_cik(item["cik"]), item["name"])
                for item in raw.get("companies", [])
            }
            if records:
                self._by_ticker = records
                self._last_refresh = raw.get("refreshed_at", 0.0)
        except (json.JSONDecodeError, KeyError, OSError) as exc:
            logger.warning("Failed to load persisted CIK mapping: %s", exc)

    def _save_to_disk(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        payload = {
            "refreshed_at": self._last_refresh,
            "companies": [
                {"ticker": c.ticker, "cik": c.cik, "name": c.name}
                for c in self._by_ticker.values()
            ],
        }
        tmp_path = f"{self._path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, self._path)

    def needs_refresh(self) -> bool:
        return (time.time() - self._last_refresh) > self._refresh_seconds

    async def refresh(self, client: Optional[httpx.AsyncClient] = None) -> None:
        """Refresh the local mapping from SEC's published ticker list.

        Uses `company_tickers.json`, which the SEC publishes alongside the
        submissions API for mapping tickers to CIKs and entity names.

        Disk persistence stays synchronous: the payload is small, written
        once per refresh interval, and no kernel offers true async file
        I/O anyway (`aiofiles` would just hide a thread here).
        """
        own_client = client is None
        client = client or httpx.AsyncClient(
            headers={"User-Agent": settings.sec_user_agent}, timeout=30.0
        )
        try:
            resp = await client.get(settings.sec_company_tickers_url)
            resp.raise_for_status()
            data = resp.json()
            records: dict[str, CompanyRecord] = {}
            for entry in data.values():
                ticker = str(entry["ticker"]).upper()
                cik = normalize_cik(entry["cik_str"])
                name = entry["title"]
                records[ticker] = CompanyRecord(ticker, cik, name)
            if records:
                self._by_ticker = records
                self._last_refresh = time.time()
                self._save_to_disk()
                logger.info("Refreshed CIK mapping: %d tickers", len(records))
        finally:
            if own_client:
                await client.aclose()

    def resolve(self, ticker: str) -> CompanyRecord:
        record = self._by_ticker.get(ticker.upper())
        if record is None:
            raise TickerNotFoundError(f"Unknown ticker: {ticker}")
        return record

    async def ensure_fresh(self, client: Optional[httpx.AsyncClient] = None) -> None:
        """Refresh from SEC if the local mapping has gone stale.

        Called at API and worker startup, and periodically by the worker, so
        the mapping is refreshed on an interval rather than per request
        (spec section 3.2).

        Failures are non-fatal: without this the service would refuse to
        start whenever SEC is unreachable. The seed/persisted mapping stays
        in use and the next interval retries.
        """
        if not self.needs_refresh():
            return
        try:
            await self.refresh(client)
        except Exception as exc:
            logger.warning(
                "CIK mapping refresh failed, continuing with %d known tickers: %s",
                len(self._by_ticker),
                exc,
            )

    @property
    def size(self) -> int:
        return len(self._by_ticker)


# Module-level singleton used by the API and workers.
cik_mapping = CikMapping()
