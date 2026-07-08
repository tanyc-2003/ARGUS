import httpx
import pytest

from argus.ops.errors import BudgetExhausted, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.ratelimit import RunBudget


def _client_with(responses: list[httpx.Response]) -> tuple[FetchClient, list[str]]:
    calls: list[str] = []
    seq = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return next(seq)

    fc = FetchClient(
        "test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    return fc, calls


def test_retries_5xx_then_succeeds() -> None:
    fc, calls = _client_with(
        [httpx.Response(500), httpx.Response(502), httpx.Response(200, text="ok")]
    )
    resp = fc.get("https://x.test/data")
    assert resp.text == "ok"
    assert len(calls) == 3


def test_non_retryable_status_fails_fast() -> None:
    fc, calls = _client_with([httpx.Response(404)])
    with pytest.raises(TransportFailure, match="404"):
        fc.get("https://x.test/missing")
    assert len(calls) == 1


def test_gives_up_after_attempts() -> None:
    fc, calls = _client_with([httpx.Response(503)] * 3)
    with pytest.raises(TransportFailure, match="giving up"):
        fc.get("https://x.test/flaky")
    assert len(calls) == 3


def test_429_honors_retry_after() -> None:
    sleeps: list[float] = []
    seq = iter([httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)])
    fc = FetchClient(
        "test",
        client=httpx.Client(transport=httpx.MockTransport(lambda r: next(seq))),
        sleep=sleeps.append,
    )
    assert fc.get("https://x.test/limited").status_code == 200
    assert 7.0 in sleeps


def test_budget_counts_every_wire_call() -> None:
    budget = RunBudget("test", 2)
    seq = iter([httpx.Response(500), httpx.Response(500), httpx.Response(200)])
    fc = FetchClient(
        "test",
        budget=budget,
        client=httpx.Client(transport=httpx.MockTransport(lambda r: next(seq))),
        sleep=lambda _: None,
    )
    with pytest.raises(BudgetExhausted):
        fc.get("https://x.test/data")
    assert budget.used == 2
