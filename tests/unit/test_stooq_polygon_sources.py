import httpx
import pytest

from argus.normalize.actions import parse_polygon_actions
from argus.normalize.daily import parse_stooq_csv
from argus.ops.errors import SchemaDrift, SourceDown
from argus.ops.http import FetchClient
from argus.sources import polygon_ref, stooq

STOOQ_CSV = (
    "Date,Open,High,Low,Close,Volume\n"
    "2020-08-28,126.01,126.44,124.58,124.81,187629920\n"
    "2020-08-31,127.58,131.00,126.00,129.04,225702700\n"
)

SPLITS_JSON = (
    b'{"status":"OK","results":[{"execution_date":"2020-08-31",'
    b'"split_from":1,"split_to":4,"ticker":"AAPL"}]}'
)
DIVS_JSON = (
    b'{"status":"OK","results":[{"cash_amount":0.82,"ex_dividend_date":"2024-08-12",'
    b'"declaration_date":"2024-08-01","ticker":"AAPL"}]}'
)


def _fc(handler) -> FetchClient:  # type: ignore[no-untyped-def]
    return FetchClient(
        "test", client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )


# ---- stooq ------------------------------------------------------------------

def test_parse_stooq_golden() -> None:
    df = parse_stooq_csv(STOOQ_CSV, "aapl")
    assert df["ticker"].to_list() == ["AAPL", "AAPL"]
    assert df["close"].to_list() == [124.81, 129.04]
    assert df["volume"][0] == 187629920.0


def test_parse_stooq_html_is_drift() -> None:
    with pytest.raises(SchemaDrift):
        parse_stooq_csv("<html>rate limited</html>", "AAPL")


def test_parse_stooq_missing_volume_tolerated() -> None:
    df = parse_stooq_csv("Date,Open,High,Low,Close\n2020-01-02,1,2,0.5,1.5\n", "THIN")
    assert df["volume"][0] is None


def test_stooq_capture_lands_per_universe_ticker(ctx) -> None:
    result = stooq.capture(ctx, client=_fc(lambda r: httpx.Response(200, text=STOOQ_CSV)))
    assert result.rows_out == 2  # test universe: SPY + AAPL
    again = stooq.capture(ctx, client=_fc(lambda r: httpx.Response(200, text=STOOQ_CSV)))
    assert again.rows_out == 0 and "already=2" in again.detail


def test_stooq_no_data_response_skipped_not_landed(ctx) -> None:
    result = stooq.capture(ctx, client=_fc(lambda r: httpx.Response(200, text="No data")))
    assert result.rows_out == 0
    assert "no_data=2" in result.detail


def test_stooq_partial_drift_lands_the_good_tickers(ctx) -> None:
    """Stooq's HTML rate-limit page for one ticker must not kill the rest."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, text="<html>daily hits limit</html>")
        return httpx.Response(200, text=STOOQ_CSV)

    result = stooq.capture(ctx, client=_fc(handler))
    assert result.rows_out == 1
    assert "drifted=1" in result.detail


def test_stooq_total_drift_fails_loudly(ctx) -> None:
    with pytest.raises(SchemaDrift):
        stooq.capture(ctx, client=_fc(lambda r: httpx.Response(200, text="<html>limit</html>")))


# ---- polygon ----------------------------------------------------------------

def test_polygon_capture_requires_key(ctx) -> None:
    with pytest.raises(SourceDown, match="not configured"):
        polygon_ref.capture_corporate_actions(ctx)


def test_polygon_capture_lands_both_kinds(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"polygon_api_key": "k"})

    def handler(request: httpx.Request) -> httpx.Response:
        body = SPLITS_JSON if "splits" in str(request.url) else DIVS_JSON
        return httpx.Response(200, content=body)

    result = polygon_ref.capture_corporate_actions(ctx, client=_fc(handler))
    assert result.rows_out == 4  # 2 tickers x (splits + dividends)


def test_parse_polygon_splits_golden() -> None:
    df = parse_polygon_actions(SPLITS_JSON, kind="polygon_splits", ticker="AAPL",
                               landing_key="k")
    assert df.height == 1
    assert df["action_type"][0] == "split"
    assert df["ratio"][0] == 4.0
    # knowledge is stamped at the ex-date (exchange-local end of day)
    assert df["knowledge_time"][0].date() >= df["ex_date"][0]


def test_parse_polygon_dividends_golden() -> None:
    df = parse_polygon_actions(DIVS_JSON, kind="polygon_dividends", ticker="AAPL",
                               landing_key="k")
    assert df["action_type"][0] == "dividend"
    assert df["cash_amount"][0] == 0.82
    assert df["ratio"][0] is None


def test_parse_polygon_missing_status_is_drift() -> None:
    with pytest.raises(SchemaDrift):
        parse_polygon_actions(b'{"results": []}', kind="polygon_splits", ticker="A",
                              landing_key="k")


def test_parse_polygon_empty_results_ok() -> None:
    df = parse_polygon_actions(b'{"status":"OK","results":[]}', kind="polygon_splits",
                               ticker="A", landing_key="k")
    assert df.is_empty()


def test_parse_polygon_malformed_row_is_drift() -> None:
    bad = b'{"status":"OK","results":[{"ticker":"A","split_to":4}]}'
    with pytest.raises(SchemaDrift):
        parse_polygon_actions(bad, kind="polygon_splits", ticker="A", landing_key="k")
