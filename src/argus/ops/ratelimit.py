"""Token-bucket rate limiting + per-run call budgets (v4 §7.2).

The bucket bounds the instantaneous rate (e.g. Polygon 5/min); the budget
bounds total calls per nightly run (patience is the currency — exhaustion is
a normal terminal state, not an error loop).

Clock and sleep are injectable so tests never wait on wall time.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from argus.ops.errors import BudgetExhausted


class TokenBucket:
    def __init__(
        self,
        rate_per_sec: float,
        capacity: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_sec <= 0 or capacity <= 0:
            raise ValueError("rate_per_sec and capacity must be positive")
        self.rate = rate_per_sec
        self.capacity = capacity
        self._clock = clock
        self._sleep = sleep
        self._tokens = capacity
        self._last = clock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def try_acquire(self, n: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

    def acquire(self, n: float = 1.0) -> None:
        """Block (via injected sleep) until `n` tokens are available."""
        if n > self.capacity:
            raise ValueError(f"cannot acquire {n} > capacity {self.capacity}")
        while not self.try_acquire(n):
            deficit = n - self._tokens
            self._sleep(max(deficit / self.rate, 0.01))


class RunBudget:
    """Per-run call counter; raises BudgetExhausted when spent."""

    def __init__(self, source: str, max_calls: int) -> None:
        self.source = source
        self.max_calls = max_calls
        self.used = 0

    @property
    def remaining(self) -> int:
        return max(0, self.max_calls - self.used)

    def spend(self, n: int = 1) -> None:
        if self.used + n > self.max_calls:
            raise BudgetExhausted(
                f"{self.source}: nightly budget of {self.max_calls} calls exhausted",
                source=self.source,
            )
        self.used += n


# Per-source steady-state rates (the free-tier discipline, v4 §2.2/§7.2)
def polygon_bucket(**kw: object) -> TokenBucket:
    return TokenBucket(rate_per_sec=5 / 60, capacity=1, **kw)  # type: ignore[arg-type]


def yfinance_bucket(**kw: object) -> TokenBucket:
    return TokenBucket(rate_per_sec=0.5, capacity=1, **kw)  # type: ignore[arg-type]


def alpaca_bucket(**kw: object) -> TokenBucket:
    return TokenBucket(rate_per_sec=3.0, capacity=3, **kw)  # type: ignore[arg-type]


def edgar_bucket(**kw: object) -> TokenBucket:
    return TokenBucket(rate_per_sec=8.0, capacity=8, **kw)  # type: ignore[arg-type]
