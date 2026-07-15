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
from argus.ops import dlq, health
from argus.ops.errors import (
    BudgetExhausted,
    ErrorClass,
    PayloadTooLarge,
    SchemaDrift,
    SourceDown,
    TransportFailure,
)
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, alpaca_bucket

SOURCE = "alpaca_iex"
QUOTES_JOB = "j05_alpaca_quotes"  # registry name; used for the DLQ entries below
DATASET = "quote_ticks"
DAILY_DATASET = "alpaca_daily"
BASE_URL = "https://data.alpaca.markets/v2/stocks"
LOOKBACK_SESSIONS = 5  # gap recovery after missed nights
DAILY_LOOKBACK_DAYS = 12  # matches the yfinance revision window
PAGE_LIMIT = 10_000
# Runaway guard ONLY — it must sit far above a real session, never near it.
# Measured IEX single-name days (2026-07, quiet -> busy):
#     HYG 0.05M (5p)   TLT 0.27M (28p)   IWD 0.91M (91p)
#     SPY 1.48-2.27M (149-228p)          QQQ 1.13-2.95M (113-295p)
# A busy day is ~2.6x the same name's quiet day, so sizing off quiet days is how
# the old cap of 200 (~2M) ended up BELOW real volume: busy sessions blew through
# it, spent 200 calls and landed nothing — permanently unfetchable, every night.
# 800 pages (~8M quotes) is ~2.7x the observed peak (QQQ 2.95M), leaving room for
# a genuinely wild session while still terminating a runaway pagination loop.
MAX_PAGES_PER_SESSION = 800


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
    raise PayloadTooLarge(
        f"{SOURCE}: {ticker} {session.session_date} exceeded {MAX_PAGES_PER_SESSION} pages "
        f"({len(quotes):,} quotes fetched before the cap)",
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
    # previous_sessions() is ascending (oldest first); walk it newest-first so the
    # most recent session is captured for EVERY ticker before any backfill runs.
    session_dates = list(reversed(calendars.previous_sessions(ctx.trade_date, LOOKBACK_SESSIONS)))

    landed = 0
    skipped = 0
    oversized = 0
    drifted = 0
    last_drift: SchemaDrift | None = None
    try:
        # session-major, NOT ticker-major: with `for ticker: for session:` an early,
        # expensive name (SPY ~170 calls/session) drained the budget and the tail of
        # the watchlist was starved every single night, deterministically. Iterating
        # sessions outermost means a short budget drops the OLDEST backfill instead.
        for session_date in session_dates:
            session = calendars.session_info(session_date)
            if session is None:  # defensive; previous_sessions only yields sessions
                continue
            for ticker in watchlist:
                request_key = f"{ticker}:{session_date.isoformat()}"
                if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
                    skipped += 1
                    continue
                if dlq.has_open(
                    ctx.conn, source=SOURCE, request_key=request_key,
                    error_class=ErrorClass.SOURCE_OVERSIZED,
                ):
                    # known too big: it cost its calls once, never pay again.
                    # `argus dlq-resolve <id>` re-arms it after the cap is raised.
                    oversized += 1
                    continue
                # Never start a fetch we cannot afford to finish. The payload lands
                # atomically, so a run cut off mid-pagination spends every call and
                # writes nothing — that partial-fetch waste is what exhausted the
                # budget. Reserving the worst case makes any started fetch completable.
                if budget.remaining < MAX_PAGES_PER_SESSION:
                    raise BudgetExhausted(
                        f"{SOURCE}: {budget.remaining} calls left, below the "
                        f"{MAX_PAGES_PER_SESSION}-page reserve needed to complete a session",
                        source=SOURCE,
                    )
                try:
                    quotes = _fetch_session_quotes(client, ticker, session)
                except PayloadTooLarge as exc:
                    # Permanent for this pair until the cap changes. Record it ONCE so
                    # it is visible in `argus dlq-list` and never re-fetched; otherwise
                    # it burns MAX_PAGES_PER_SESSION calls every night, forever.
                    oversized += 1
                    dlq.push(
                        ctx.conn, job_name=QUOTES_JOB, error_class=exc.error_class,
                        detail=str(exc), source=SOURCE, request_key=request_key,
                    )
                    ctx.log.warning(
                        "alpaca_quotes_oversized", ticker=ticker,
                        session=session_date.isoformat(), detail=str(exc),
                    )
                    continue
                except SchemaDrift as exc:
                    # a genuinely malformed response for one pair must not kill the
                    # rest of the watchlist; systemic drift still fails loudly below
                    drifted += 1
                    last_drift = exc
                    ctx.log.warning(
                        "alpaca_quotes_drifted", ticker=ticker,
                        session=session_date.isoformat(), detail=str(exc),
                    )
                    continue
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
    if drifted and landed == 0 and skipped == 0:
        health.record_failure(ctx.conn, SOURCE)
        assert last_drift is not None
        raise last_drift  # every single session was bad — that IS schema drift
    health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} already={skipped} oversized={oversized} drifted={drifted}",
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
