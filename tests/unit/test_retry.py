import pytest

from argus.ops.retry import backoff_retry


class Flaky:
    def __init__(self, fail_times: int, exc: type[Exception] = ConnectionError) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.exc = exc

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc(f"boom {self.calls}")
        return "ok"


def test_succeeds_after_transient_failures() -> None:
    sleeps: list[float] = []
    fn = Flaky(fail_times=2)
    assert backoff_retry(fn, attempts=3, retry_on=(ConnectionError,), sleep=sleeps.append) == "ok"
    assert fn.calls == 3
    assert sleeps == [1.0, 2.0]


def test_gives_up_and_reraises_last() -> None:
    fn = Flaky(fail_times=99)
    with pytest.raises(ConnectionError, match="boom 3"):
        backoff_retry(fn, attempts=3, retry_on=(ConnectionError,), sleep=lambda _: None)
    assert fn.calls == 3


def test_non_retryable_raises_immediately() -> None:
    fn = Flaky(fail_times=99, exc=ValueError)
    with pytest.raises(ValueError):
        backoff_retry(fn, attempts=3, retry_on=(ConnectionError,), sleep=lambda _: None)
    assert fn.calls == 1
