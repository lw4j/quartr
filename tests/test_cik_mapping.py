"""CIK mapping refresh behaviour (spec section 3.2).

Regression cover for a bug where `refresh()` existed but was never called,
leaving the mapping permanently on its handful of seed companies so that
almost every ticker 404'd.
"""
from __future__ import annotations

import json
import time

import httpx
import pytest

from app.companies.cik_mapping import CikMapping, TickerNotFoundError

SEC_PAYLOAD = {
    "0": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
    "1": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
}


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=SEC_PAYLOAD)


async def test_unknown_ticker_resolves_after_refresh(tmp_path):
    """The bug: without a refresh, non-seed tickers are unresolvable."""
    mapping = CikMapping(path=str(tmp_path / "cik.json"))

    with pytest.raises(TickerNotFoundError):
        mapping.resolve("MSFT")

    async with _client(_ok) as client:
        await mapping.ensure_fresh(client)

    assert mapping.resolve("MSFT").cik == "0000789019"
    assert mapping.resolve("nvda").cik == "0001045810"


async def test_refresh_persists_and_reloads_without_refetching(tmp_path):
    path = tmp_path / "cik.json"
    async with _client(_ok) as client:
        await CikMapping(path=str(path)).ensure_fresh(client)

    assert json.loads(path.read_text())["companies"]

    def fail(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("should not refetch a fresh mapping")

    reloaded = CikMapping(path=str(path))
    assert not reloaded.needs_refresh()
    async with _client(fail) as client:
        await reloaded.ensure_fresh(client)
    assert reloaded.resolve("MSFT").cik == "0000789019"


async def test_ensure_fresh_is_non_fatal_when_sec_is_unavailable(tmp_path):
    """Startup must not fail just because SEC is down."""
    mapping = CikMapping(path=str(tmp_path / "cik.json"))
    seeded = mapping.size

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sec unreachable")

    async with _client(boom) as client:
        await mapping.ensure_fresh(client)

    assert mapping.size == seeded
    assert mapping.resolve("AAPL").cik


async def test_ensure_fresh_skips_refresh_while_still_fresh(tmp_path):
    mapping = CikMapping(path=str(tmp_path / "cik.json"))
    mapping._last_refresh = time.time()

    def fail(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("should not refresh within the interval")

    assert not mapping.needs_refresh()
    async with _client(fail) as client:
        await mapping.ensure_fresh(client)
