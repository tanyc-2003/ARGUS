import json
from datetime import date

import httpx
import polars as pl
import pytest

from argus.canonical import daily_bars
from argus.ops.errors import SourceDown
from argus.ops.http import FetchClient
from argus.orchestration import trust_jobs
from argus.sources import edgar

COMPANY_TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    # SPY deliberately absent: ETFs have no EDGAR company entry
}
SUBMISSIONS = {"sic": "3571", "sicDescription": "Electronic Computers", "name": "Apple Inc."}


def _fc(handler) -> FetchClient:  # type: ignore[no-untyped-def]
    return FetchClient(
        "test", client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )


# ---- EDGAR --------------------------------------------------------------------

def test_edgar_requires_user_agent(ctx) -> None:
    with pytest.raises(SourceDown, match="EDGAR_USER_AGENT"):
        edgar.capture(ctx)


def test_edgar_capture_map_and_submissions(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"edgar_user_agent": "test test@x.com"})

    def handler(request: httpx.Request) -> httpx.Response:
        if "company_tickers" in str(request.url):
            return httpx.Response(200, json=COMPANY_TICKERS)
        return httpx.Response(200, json=SUBMISSIONS)

    result = edgar.capture(ctx, client=_fc(handler))
    # the map + AAPL's submissions; SPY has no CIK -> skipped silently
    assert result.rows_out == 2

    trust_jobs.sector_seal(ctx)  # AAPL now has a sector
    again = edgar.capture(ctx, client=_fc(handler))
    assert again.rows_out == 0  # map landed for this date, AAPL known


def test_edgar_map_drift(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"edgar_user_agent": "test test@x.com"})
    from argus.ops.errors import SchemaDrift

    with pytest.raises(SchemaDrift):
        edgar.capture(ctx, client=_fc(lambda r: httpx.Response(200, json={"0": {"nope": 1}})))


# ---- parity -------------------------------------------------------------------

def _seed_bars(ctx, values: dict[str, float]) -> None:
    daily_bars.upsert_bars(
        ctx.conn,
        pl.DataFrame(
            {
                "ticker": ["AAPL"], "bar_date": [date(2026, 7, 6)],
                "open": [values["o"]], "high": [values["h"]], "low": [values["l"]],
                "close": [values["c"]], "volume": [values["v"]],
            }
        ),
    )


def test_parity_within_tolerance(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"polygon_api_key": "k"})
    ours = {"o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1e6}
    _seed_bars(ctx, ours)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "results": [
            {"o": 100.02, "h": 101.0, "l": 99.0, "c": 100.52, "v": 1.02e6}
        ]})

    result = trust_jobs.parity_sample(ctx, client=_fc(handler))
    assert result.rows_out == 1
    assert "field_breaches=0" in result.detail


def test_parity_detects_drift(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"polygon_api_key": "k"})
    _seed_bars(ctx, {"o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1e6})

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "results": [
            {"o": 100.0, "h": 101.0, "l": 99.0, "c": 102.0, "v": 1e6}  # close off ~1.5%
        ]})

    result = trust_jobs.parity_sample(ctx, client=_fc(handler))
    assert "field_breaches=1" in result.detail
    breach = ctx.conn.execute(
        "SELECT field, within_tol FROM parity_scores WHERE NOT within_tol"
    ).fetchone()
    assert breach == ("close", False)


def test_parity_weekly_gate(ctx) -> None:
    ctx.settings = ctx.settings.model_copy(update={"polygon_api_key": "k"})
    _seed_bars(ctx, {"o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0})
    handler = _fc(lambda r: httpx.Response(200, json={"status": "OK", "results": [
        {"o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}]}))

    from argus.ops.jobs import run_job
    run_job(ctx, "j13_parity_sample", lambda c: trust_jobs.parity_sample(c, client=handler))
    gated = trust_jobs.parity_sample(ctx, client=handler)
    assert trust_jobs.NOT_DUE in gated.detail


def test_edgar_submissions_payload_shape() -> None:
    # keep the fixture honest against the parser expectations
    assert "sic" in SUBMISSIONS and "sicDescription" in SUBMISSIONS
    assert json.dumps(COMPANY_TICKERS)  # serializable
