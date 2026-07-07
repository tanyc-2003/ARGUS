"""The one instrumented HTTP layer (governing decision 6).

Every HTTP source (Polygon, Alpaca, Stooq, EDGAR, symbol directories) goes
through FetchClient: token bucket -> per-run budget -> retry/backoff -> bytes.
Only yfinance bypasses it (its library manages Yahoo auth) and is wrapped by
the same bucket+budget at the job layer.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable

import httpx

from argus.ops.errors import TransportFailure
from argus.ops.ratelimit import RunBudget, TokenBucket

_DEFAULT_UA = "ARGUS/0.1 (personal research data platform)"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class FetchClient:
    def __init__(
        self,
        source: str,
        *,
        bucket: TokenBucket | None = None,
        budget: RunBudget | None = None,
        user_agent: str = _DEFAULT_UA,
        timeout: float = 30.0,
        attempts: int = 3,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.source = source
        self.bucket = bucket
        self.budget = budget
        self.attempts = attempts
        self._sleep = sleep
        self._client = client or httpx.Client(
            timeout=timeout, headers={"User-Agent": user_agent}, follow_redirects=True
        )

    def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """GET with rate limiting, budget accounting, and bounded retries.

        Raises BudgetExhausted (from the budget) or TransportFailure (after
        retries). A non-retryable HTTP error status raises TransportFailure
        immediately with the status in the message.
        """
        last_error = ""
        for attempt in range(self.attempts):
            if self.budget is not None:
                self.budget.spend()  # every wire call counts, including retries
            if self.bucket is not None:
                self.bucket.acquire()
            try:
                resp = self._client.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if resp.status_code < 400:
                    return resp
                if resp.status_code not in _RETRYABLE_STATUS:
                    raise TransportFailure(
                        f"{self.source}: HTTP {resp.status_code} for {url}",
                        source=self.source,
                    )
                last_error = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    with contextlib.suppress(ValueError):
                        self._sleep(min(float(retry_after), 120.0))
            if attempt < self.attempts - 1:
                self._sleep(2.0**attempt)
        raise TransportFailure(
            f"{self.source}: giving up on {url} after {self.attempts} attempts ({last_error})",
            source=self.source,
        )

    def close(self) -> None:
        self._client.close()
