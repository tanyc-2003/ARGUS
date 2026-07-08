"""Alpaca IEX quote harvest for the watchlist (capture-only in M0).

Tick-level IEX quotes per (ticker, session) are paginated, aggregated into one
gzipped JSON payload, and landed atomically — a crashed night simply refetches
the whole session next run (the manifest has no partial entries). M5 buckets
these into quote_bars_1m (BBO at minute close + time-weighted mean).

Requires free Alpaca keys; without them the job records skipped_source_down.
"""

from __future__ import annotations

import gzip
import json

from argus.config_files import load_watchlist
from argus.core import calendars
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, alpaca_bucket

SOURCE = "alpaca_iex"
DATASET = "quote_ticks"
DAILY_DATASET = "alpaca_daily"
BASE_URL = "https://data.alpaca.markets/v2/stocks"
LOOKBACK_SESSIONS = 5  # gap recovery after missed nights
DAILY_LOOKBACK_DAYS = 12  # matches the yfinance revision window
PAGE_LIMIT = 10_000
MAX_PAGES_PER_SESSION = 200  # hard stop ≈ 2M quotes; far above any IEX single-name day


def _fetch_session_quotes(
    client: FetchClient, ticker: str, session: calendars.Session
) -> list[dict[str, object]]:
    quotes: list[dict[str, object]] = []
    page_token: str | None = None
    for _ in range(MAX_PAGES_PER_SESSION):
        params = {
            "start": session.open_utc.isoformat().replace("+00:00", "Z"),
            "end": session.close_utc.isoformat().replace("+00:00", "Z"),
            "feed": "iex",
            "limit": str(PAGE_LIMIT),
        }
        if page_token:
            params["page_token"] = page_token
        payload = client.get(f"{BASE_URL}/{ticker}/quotes", params=params).json()
        if "quotes" not in payload:
            raise SchemaDrift(
                f"{SOURCE}: response for {ticker} missing 'quotes' key", source=SOURCE
            )
        quotes.extend(payload["quotes"] or [])
        page_token = payload.get("next_page_token")
        if not page_token:
            return quotes
    raise SchemaDrift(
        f"{SOURCE}: {ticker} {session.session_date} exceeded {MAX_PAGES_PER_SESSION} pages",
        source=SOURCE,
    )


def _authed_client(ctx: JobContext, client: FetchClient | None, budget: RunBudget) -> FetchClient:
    s = ctx.settings
    if not s.alpaca_key_id or not s.alpaca_secret_key:
        raise SourceDown(f"{SOURCE}: ARGUS_ALPACA_KEY_ID/SECRET_KEY not configured", source=SOURCE)
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    client = client or FetchClient(SOURCE, bucket=alpaca_bucket(), budget=budget)
    client._client.headers.update(
        {"APCA-API-KEY-ID": s.alpaca_key_id, "APCA-API-SECRET-KEY": s.alpaca_secret_key}
    )
    return client


def capture(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    budget = RunBudget(SOURCE, ctx.settings.alpaca_nightly_budget)
    client = _authed_client(ctx, client, budget)

    watchlist = load_watchlist(ctx.settings)
    session_dates = calendars.previous_sessions(ctx.trade_date, LOOKBACK_SESSIONS)

    landed = 0
    skipped = 0
    try:
        for ticker in watchlist:
            for session_date in session_dates:
                request_key = f"{ticker}:{session_date.isoformat()}"
                if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
                    skipped += 1
                    continue
                session = calendars.session_info(session_date)
                if session is None:  # defensive; previous_sessions only yields sessions
                    continue
                quotes = _fetch_session_quotes(client, ticker, session)
                body = {
                    "ticker": ticker,
                    "session": session_date.isoformat(),
                    "feed": "iex",
                    "n_quotes": len(quotes),
                    "quotes": quotes,
                }
                store.write(
                    ctx.conn, ctx.settings,
                    dataset=DATASET, source=SOURCE, request_key=request_key,
                    payload=gzip.compress(json.dumps(body).encode("utf-8")),
                    ext="json.gz", partition_date=session_date,
                    knowledge_time=pull_knowledge_time(),
                    content_type="application/gzip",
                )
                landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} already={skipped}",
    )


def capture_daily_bars(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    """Raw (truly unadjusted) IEX daily bars for the universe — the third daily
    observation. Stored as L2 events only until M3 voting; `adjustment=raw`
    also makes this the independent validator of the split reversal: a missed
    split shows up as a ~ratio-sized disagreement instead of being canonized.
    """
    from datetime import timedelta

    from argus.config_files import load_universe

    budget = RunBudget(SOURCE, ctx.settings.alpaca_nightly_budget)
    client = _authed_client(ctx, client, budget)

    start = ctx.trade_date - timedelta(days=DAILY_LOOKBACK_DAYS)
    landed = 0
    skipped = 0
    try:
        for row in load_universe(ctx.settings):
            ticker = row["ticker"]
            request_key = f"{ticker}:{ctx.trade_date.isoformat()}"
            if store.ensure(ctx.conn, DAILY_DATASET, SOURCE, request_key) is not None:
                skipped += 1
                continue
            payload = client.get(
                f"{BASE_URL}/{ticker}/bars",
                params={
                    "timeframe": "1Day", "adjustment": "raw", "feed": "iex",
                    "start": start.isoformat(),
                    # a bare date means midnight UTC; the trade date's own bar is
                    # timestamped ~04:00Z, so the bound must be the NEXT day
                    "end": (ctx.trade_date + timedelta(days=1)).isoformat(),
                    "limit": "1000",
                },
            ).json()
            if "bars" not in payload:
                raise SchemaDrift(
                    f"{SOURCE}: daily bars for {ticker} missing 'bars' key", source=SOURCE
                )
            store.write(
                ctx.conn, ctx.settings,
                dataset=DAILY_DATASET, source=SOURCE, request_key=request_key,
                payload=json.dumps(payload).encode("utf-8"), ext="json",
                partition_date=ctx.trade_date,
                knowledge_time=pull_knowledge_time(), content_type="application/json",
            )
            landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} already={skipped}",
    )
