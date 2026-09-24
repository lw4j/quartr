"""Retry policy for SEC requests (spec section 18).

Distinguishes retryable (transient) failures from non-retryable
(permanent) ones, and applies exponential backoff with jitter.
"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class PermanentSecError(Exception):
    """Non-retryable error: invalid CIK/ticker, filing not found, etc."""


class TransientSecError(Exception):
    """Retryable error: rate limited, 5xx, network timeout, DNS blip, etc."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def classify_http_error(exc: httpx.HTTPStatusError) -> Exception:
    status = exc.response.status_code
    if status in _RETRYABLE_STATUS_CODES:
        retry_after = None
        header = exc.response.headers.get("Retry-After")
        if header is not None:
            try:
                retry_after = float(header)
            except ValueError:
                retry_after = None
        return TransientSecError(f"Retryable SEC HTTP error {status}", retry_after)
    return PermanentSecError(f"Non-retryable SEC HTTP error {status}: {exc}")


@dataclass
class RetryPolicy:
    max_attempts: int = settings.retry_max_attempts
    base_delay_seconds: float = settings.retry_base_delay_seconds
    max_delay_seconds: float = settings.retry_max_delay_seconds

    def delay_for_attempt(self, attempt: int, retry_after: float | None = None) -> float:
        if retry_after is not None:
            return min(retry_after, self.max_delay_seconds)
        backoff = min(self.base_delay_seconds * (2 ** (attempt - 1)), self.max_delay_seconds)
        jitter = random.uniform(0, backoff * 0.5)
        return backoff + jitter

    async def run(self, fn: Callable[[], Awaitable[T]]) -> T:
        """Await `fn()`, retrying transient failures with backoff + jitter.

        Backoff uses `asyncio.sleep`, so a task waiting out a 429 releases
        the event loop rather than occupying a thread.
        """
        last_exc: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return await fn()
            except httpx.HTTPStatusError as exc:
                classified = classify_http_error(exc)
                if isinstance(classified, PermanentSecError):
                    raise classified
                last_exc = classified
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as exc:
                last_exc = TransientSecError(f"Transient network error: {exc}")

            retry_after = getattr(last_exc, "retry_after", None)
            if attempt < self.max_attempts:
                delay = self.delay_for_attempt(attempt, retry_after)
                logger.warning(
                    "SEC request failed (attempt %d/%d), retrying in %.2fs: %s",
                    attempt,
                    self.max_attempts,
                    delay,
                    last_exc,
                )
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc
