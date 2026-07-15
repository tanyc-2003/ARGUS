import gzip
import json
from datetime import date

import httpx
import pytest

from argus.ops.errors import BudgetExhausted, PayloadTooLarge, SourceDown
from argus.ops.http import FetchClient
from argus.settings import Settings
from argus.sources import alpaca


def _settings_with_keys(settings: Settings) -> Settings:
    return settings.model_copy(
        update={"alpaca_key_id": "key", "alpaca_secret_key": "secret"}
    )


def _endless_client() -> tuple[FetchClient, list[httpx.Request]]:
    """Always hands back another page token — i.e. a session bigger than the cap."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"quotes": [{"t": "a", "bp": 1.0, "ap": 1.1}], "next_page_token": "more"}
        )

    fc = FetchClient(
        "alpaca_iex",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    return fc, requests


def _paged_client() -> tuple[FetchClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        token = request.url.params.get("page_token")
        if token is None:
            return httpx.Response(
                200,
                json={"quotes": [{"t": "a", "bp": 1.0, "ap": 1.1}], "next_page_token": "p2"},
            )
        return httpx.Response(
            200, json={"quotes": [{"t": "b", "bp": 1.0, "ap": 1.2}], "next_page_token": None}
        )

    fc = FetchClient(
        "alpaca_iex",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    return fc, requests


def test_missing_credentials_is_source_down(ctx) -> None:
    with pytest.raises(SourceDown, match="not configured"):
        alpaca.capture(ctx)


def test_pagination_aggregated_into_one_payload(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    client, requests = _paged_client()
    result = alpaca.capture(ctx, client=client)

    # 2 tickers x LOOKBACK_SESSIONS sessions, 2 pages each
    n_payloads = ctx.conn.execute(
        "SELECT COUNT(*) FROM landing_manifest WHERE dataset='quote_ticks'"
    ).fetchone()[0]
    assert result.rows_out == n_payloads == 2 * alpaca.LOOKBACK_SESSIONS
    assert len(requests) == 2 * n_payloads

    path = ctx.conn.execute(
        "SELECT path FROM landing_manifest WHERE dataset='quote_ticks' LIMIT 1"
    ).fetchone()[0]
    with open(path, "rb") as fh:
        body = json.loads(gzip.decompress(fh.read()))
    assert body["n_quotes"] == 2  # both pages merged
    assert body["feed"] == "iex"


def test_rerun_is_idempotent(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    alpaca.capture(ctx, client=_paged_client()[0])
    client, requests = _paged_client()
    second = alpaca.capture(ctx, client=client)
    assert second.rows_out == 0
    assert len(requests) == 0  # never fetched twice


def test_page_cap_raises_payload_too_large_not_schema_drift(monkeypatch) -> None:
    """Pin the classification at the source: exceeding the cap is a volume signal.
    If this ever regresses to SchemaDrift it would both mask real vendor drift and
    (before containment) take down the whole watchlist."""
    from argus.core import calendars

    monkeypatch.setattr(alpaca, "MAX_PAGES_PER_SESSION", 2)
    client, _ = _endless_client()
    session = calendars.session_info(date(2026, 7, 7))
    assert session is not None

    with pytest.raises(PayloadTooLarge, match="exceeded 2 pages"):
        alpaca._fetch_session_quotes(client, "SPY", session)


def test_oversized_session_is_contained_not_schema_drift(ctx, monkeypatch) -> None:
    """A session past the page cap is a volume problem, not vendor drift: it must
    be classified PayloadTooLarge, contained per-pair, and recorded in the DLQ."""
    monkeypatch.setattr(alpaca, "MAX_PAGES_PER_SESSION", 3)
    ctx.settings = _settings_with_keys(ctx.settings)
    client, requests = _endless_client()

    result = alpaca.capture(ctx, client=client)

    n_pairs = 2 * alpaca.LOOKBACK_SESSIONS  # 2 tickers x 5 sessions
    assert result.rows_out == 0
    assert f"oversized={n_pairs}" in result.detail
    assert "drifted=0" in result.detail  # NOT misfiled as schema drift
    assert len(requests) == 3 * n_pairs  # capped at 3 pages each, then gave up

    rows = ctx.conn.execute(
        "SELECT error_class, request_key FROM dead_letter WHERE resolved_at IS NULL"
    ).fetchall()
    assert len(rows) == n_pairs
    assert {r[0] for r in rows} == {"source_oversized"}


def test_oversized_session_is_never_refetched(ctx, monkeypatch) -> None:
    """The permanence guarantee: a doomed pair costs its calls ONCE, not every
    night forever. This is the regression that burned ~1.2k calls/night."""
    monkeypatch.setattr(alpaca, "MAX_PAGES_PER_SESSION", 3)
    ctx.settings = _settings_with_keys(ctx.settings)
    alpaca.capture(ctx, client=_endless_client()[0])

    client, requests = _endless_client()
    second = alpaca.capture(ctx, client=client)

    assert len(requests) == 0  # suppressed by the open DLQ entry — zero calls burned
    assert second.rows_out == 0
    assert f"oversized={2 * alpaca.LOOKBACK_SESSIONS}" in second.detail


def test_dlq_resolve_rearms_an_oversized_pair(ctx, monkeypatch) -> None:
    """Suppression must be reversible, else raising the cap could never recover."""
    monkeypatch.setattr(alpaca, "MAX_PAGES_PER_SESSION", 3)
    ctx.settings = _settings_with_keys(ctx.settings)
    alpaca.capture(ctx, client=_endless_client()[0])
    ctx.conn.execute("UPDATE dead_letter SET resolved_at = now()")

    # cap now high enough for the (2-page) client to finish
    monkeypatch.setattr(alpaca, "MAX_PAGES_PER_SESSION", 400)
    client, requests = _paged_client()
    third = alpaca.capture(ctx, client=client)

    assert third.rows_out == 2 * alpaca.LOOKBACK_SESSIONS  # all pairs land
    assert len(requests) > 0


def test_budget_never_starts_a_fetch_it_cannot_finish(ctx) -> None:
    """Payloads land atomically, so a fetch cut off mid-pagination spends its calls
    and writes nothing. Below the reserve the job must stop BEFORE spending."""
    ctx.settings = _settings_with_keys(ctx.settings).model_copy(
        update={"alpaca_nightly_budget": alpaca.MAX_PAGES_PER_SESSION - 1}
    )
    client, requests = _paged_client()

    with pytest.raises(BudgetExhausted, match="reserve"):
        alpaca.capture(ctx, client=client)

    assert len(requests) == 0  # not a single wasted call


def test_newest_session_is_captured_for_every_ticker_first(ctx) -> None:
    """Fairness: ticker-major order starved the tail of the watchlist every night.
    Session-major means a short budget drops the oldest backfill instead."""
    ctx.settings = _settings_with_keys(ctx.settings)
    client, requests = _paged_client()
    alpaca.capture(ctx, client=client)

    # order of (ticker, session-start) as first seen on the wire
    seen: list[tuple[str, str]] = []
    for r in requests:
        pair = (str(r.url).split("/stocks/")[1].split("/")[0], r.url.params["start"])
        if pair not in seen:
            seen.append(pair)

    sessions = [s for _, s in seen]
    assert sessions == sorted(sessions, reverse=True)  # newest session first
    # both tickers get the newest session before anything gets the 2nd-newest
    assert {t for t, s in seen if s == sessions[0]} == {"SPY", "AAPL"}


def test_schema_drift_on_missing_quotes_key(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    fc = FetchClient(
        "alpaca_iex",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": []}))
        ),
        sleep=lambda _: None,
    )
    from argus.ops.errors import SchemaDrift

    with pytest.raises(SchemaDrift):
        alpaca.capture(ctx, client=fc)
