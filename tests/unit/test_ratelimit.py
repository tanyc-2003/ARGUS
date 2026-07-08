import pytest

from argus.ops.errors import BudgetExhausted
from argus.ops.ratelimit import RunBudget, TokenBucket, polygon_bucket


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def test_burst_up_to_capacity_then_blocked() -> None:
    clock = FakeClock()
    b = TokenBucket(rate_per_sec=1.0, capacity=2.0, clock=clock, sleep=clock.sleep)
    assert b.try_acquire()
    assert b.try_acquire()
    assert not b.try_acquire()


def test_refill_over_time() -> None:
    clock = FakeClock()
    b = TokenBucket(rate_per_sec=1.0, capacity=2.0, clock=clock, sleep=clock.sleep)
    b.acquire()
    b.acquire()
    clock.t += 1.5
    assert b.try_acquire()
    assert not b.try_acquire()


def test_acquire_blocks_until_refill() -> None:
    clock = FakeClock()
    b = TokenBucket(rate_per_sec=2.0, capacity=1.0, clock=clock, sleep=clock.sleep)
    b.acquire()
    start = clock.t
    b.acquire()  # must wait ~0.5s via injected sleep
    assert clock.t - start >= 0.5 - 1e-9


def test_polygon_bucket_never_exceeds_five_per_minute() -> None:
    clock = FakeClock()
    b = polygon_bucket(clock=clock, sleep=clock.sleep)
    stamps: list[float] = []
    for _ in range(6):
        b.acquire()
        stamps.append(clock.t)
    gaps = [b2 - a for a, b2 in zip(stamps, stamps[1:], strict=False)]
    assert all(g >= 12.0 - 1e-6 for g in gaps)


def test_acquire_more_than_capacity_rejected() -> None:
    clock = FakeClock()
    b = TokenBucket(rate_per_sec=1.0, capacity=1.0, clock=clock, sleep=clock.sleep)
    with pytest.raises(ValueError):
        b.acquire(2.0)


def test_run_budget_exhaustion() -> None:
    budget = RunBudget("polygon", 3)
    budget.spend()
    budget.spend()
    budget.spend()
    assert budget.remaining == 0
    with pytest.raises(BudgetExhausted):
        budget.spend()
    assert budget.used == 3  # failed spend does not count
