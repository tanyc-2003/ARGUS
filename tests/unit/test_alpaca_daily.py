import json

import httpx
import pytest

from argus.ops.errors import SchemaDrift, SourceDown
from argus.ops.http import FetchClient
from argus.orchestration.build_jobs import _parse_alpaca_daily
from argus.sources import alpaca

BARS_JSON = {
    "bars": [
        {"t": "2026-07-06T04:00:00Z", "o": 100.0, "h": 102.0, "l": 99.0, "c": 101.0,
         "v": 1000000},
        {"t": "2026-07-07T04:00:00Z", "o": 101.0, "h": 103.0, "l": 100.0, "c": 102.0,
         "v": 1100000},
    ],
    "symbol": "AAPL",
    "next_page_token": None,
}


def _fc(handler) -> FetchClient:  # type: ignore[no-untyped-def]
    return FetchClient(
        "alpaca_iex", client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )


def test_requires_credentials(ctx) -> None:
    with pytest.raises(SourceDown, match="not configured"):
        alpaca.capture_daily_bars(ctx)


def test_capture_lands_per_ticker(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(
        update={"alpaca_key_id": "k", "alpaca_secret_key": "s"}
    )
    result = alpaca.capture_daily_bars(
        ctx, client=_fc(lambda r: httpx.Response(200, json=BARS_JSON))
    )
    assert result.rows_out == 2  # SPY + AAPL
    again = alpaca.capture_daily_bars(
        ctx, client=_fc(lambda r: httpx.Response(200, json=BARS_JSON))
    )
    assert again.rows_out == 0


def test_capture_missing_bars_key_is_drift(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(
        update={"alpaca_key_id": "k", "alpaca_secret_key": "s"}
    )
    with pytest.raises(SchemaDrift):
        alpaca.capture_daily_bars(ctx, client=_fc(lambda r: httpx.Response(200, json={})))


def test_parse_golden() -> None:
    from datetime import date

    df = _parse_alpaca_daily(json.dumps(BARS_JSON).encode(), "AAPL")
    assert df.height == 2
    # 04:00 UTC = midnight ET -> the bar's exchange-local date
    assert df["bar_date"].to_list() == [date(2026, 7, 6), date(2026, 7, 7)]
    assert df["close"].to_list() == [101.0, 102.0]


def test_parse_empty_bars_ok() -> None:
    df = _parse_alpaca_daily(b'{"bars": [], "symbol": "X"}', "X")
    assert df.is_empty()


def test_parse_malformed_row_is_drift() -> None:
    bad = json.dumps({"bars": [{"t": "2026-07-06T04:00:00Z", "o": 1.0}]}).encode()
    with pytest.raises(SchemaDrift, match="malformed bar row"):
        _parse_alpaca_daily(bad, "X")
