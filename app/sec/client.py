"""SEC EDGAR HTTP client (spec sections 7, 16, 17).

Wraps httpx with:
- the required descriptive User-Agent (never optional in production);
- an injected rate limiter (the service supplies the Redis-backed
  global one; anything with `acquire()` works);
- retry/backoff via `RetryPolicy`;
- conditional-request support (ETag / Last-Modified) where the caller
  supplies cached validators, reducing unnecessary SEC load.

Only downloads what is needed to construct the requested report; it does
not crawl a company's full filing history or EDGAR generally.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol

import httpx

from app.config import settings
from app.observability import metrics
from app.sec.retry import RetryPolicy

logger = logging.getLogger(__name__)


class RateLimiter(Protocol):
    """The only thing `SecClient` needs from a rate limiter.

    Declared structurally rather than importing the Redis-backed
    `GlobalSecRateLimiter`, so the SEC client depends on the 1-method
    contract it actually uses instead of on the distributed implementation
    that happens to satisfy it. That keeps this module testable, and the
    fetch path usable, without Redis.
    """

    async def acquire(self, *, timeout: float | None = None) -> None: ...


@dataclass
class CachedResponse:
    etag: Optional[str] = None
    last_modified: Optional[str] = None


class SecClient:
    def __init__(
        self,
        rate_limiter: RateLimiter,
        retry_policy: Optional[RetryPolicy] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        if not settings.sec_user_agent:
            raise RuntimeError("SEC_USER_AGENT must be configured")
        self._rate_limiter = rate_limiter
        self._retry_policy = retry_policy or RetryPolicy()
        self._http = http_client or httpx.AsyncClient(
            headers={
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=30.0,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _get(
        self, url: str, cached: Optional[CachedResponse] = None
    ) -> httpx.Response:
        headers = {}
        if cached:
            if cached.etag:
                headers["If-None-Match"] = cached.etag
            if cached.last_modified:
                headers["If-Modified-Since"] = cached.last_modified

        async def do_request() -> httpx.Response:
            await self._rate_limiter.acquire(timeout=30.0)
            resp = await self._http.get(url, headers=headers)
            if resp.status_code == 429:
                metrics.sec_429_total.inc()
            if resp.status_code == 304:
                return resp
            resp.raise_for_status()
            return resp

        return await self._retry_policy.run(do_request)

    async def get_submissions(
        self, cik: str, cached: Optional[CachedResponse] = None
    ) -> httpx.Response:
        """Fetch the company's filing history JSON.

        https://data.sec.gov/submissions/CIK##########.json
        """
        url = f"{settings.sec_submissions_base_url}/CIK{cik}.json"
        return await self._get(url, cached=cached)

    async def download_document(self, url: str) -> httpx.Response:
        """Download a single filing document (only what is needed)."""
        return await self._get(url)
