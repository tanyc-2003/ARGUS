"""Bounded retry with exponential backoff (sleep injectable for tests)."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def backoff_retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    factor: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run `fn`, retrying on `retry_on` up to `attempts` total tries.

    Delays: base_delay, base_delay*factor, ... between tries. The final
    failure is re-raised unchanged so callers keep the real error class.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    last_exc: BaseException | None = None
    for i in range(attempts):
        try:
            return fn()
        except retry_on as exc:  # noqa: PERF203
            last_exc = exc
            if i < attempts - 1:
                sleep(base_delay * (factor**i))
    assert last_exc is not None
    raise last_exc
