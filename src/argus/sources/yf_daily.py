"""yfinance daily incrementals + the T−2/T−5 revision re-fetch (v4 §5.1.2).

One payload per (ticker, trade_date) covering the trailing week: the fresh T−1
bar plus re-observations of recent bars. Because the request_key carries the
trade date, every night lands a NEW observation of the same past sessions —
the build job hash-compares them against canonical and opens SCD-2 revisions
on mismatch. Yahoo serves split-adjusted, dividend-unadjusted prices
(auto_adjust=False); the Adj Close column rides along for M3's implied-
dividend cross-check.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from argus.config_files import load_universe
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SourceDown
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, TokenBucket, yfinance_bucket

SOURCE = "yfinance"
DATASET = "yf_daily"
LOOKBACK_DAYS = 12  # calendar days: T-5 SESSIONS can span 11 days across a holiday week
HISTORY_START = date(1990, 1, 1)  # bootstrap depth (R1 needs >= 10y)

Downloader = Callable[[str, date, date], Any]  # (ticker, start, end) -> pandas DataFrame


def _default_downloader(ticker: str, start: date, end: date) -> Any:
    import yfinance as yf

    from argus.sources._yf import quiet_vendor_deprecations, yahoo_symbol

    with quiet_vendor_deprecations():
        return yf.download(
            yahoo_symbol(ticker),
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )


def _to_parquet_bytes(df: Any) -> bytes:
    frame = df.reset_index()
    frame.columns = [
        "_".join(str(p) for p in col if str(p)) if isinstance(col, tuple) else str(col)
        for col in frame.columns
    ]
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    return buf.getvalue()


def capture(
    ctx: JobContext,
    downloader: Downloader | None = None,
    bucket: TokenBucket | None = None,
) -> JobResult:
    start = ctx.trade_date - timedelta(days=LOOKBACK_DAYS)
    return _capture_window(ctx, start, downloader=downloader, bucket=bucket, key_tag="")


def capture_history(
    ctx: JobContext,
    downloader: Downloader | None = None,
    bucket: TokenBucket | None = None,
) -> JobResult:
    """Deep-history bootstrap capture (b01): the full Yahoo daily archive per
    universe ticker. Became the bootstrap spine when Stooq closed its endpoints
    behind a JS proof-of-work challenge (2026-07: a source-permanence event the
    multi-source design absorbs). Same landing dataset — the payloads flow
    through the identical parse -> reverse -> events -> vote path."""
    return _capture_window(
        ctx, HISTORY_START, downloader=downloader, bucket=bucket, key_tag="history:"
    )


def backfill_new_tickers(
    ctx: JobContext,
    downloader: Downloader | None = None,
    bucket: TokenBucket | None = None,
) -> JobResult:
    """Deep history for universe names that have never had any (j02c, nightly).

    b01_yf_history only runs under `argus bootstrap`, so a ticker ADDED to
    universe.yaml afterwards would silently accrue nothing but the rolling
    12-day nightly window — no 10y spine, forever. This finds names with no
    history payload and backfills them; already-bootstrapped tickers are left
    completely untouched (their existing data is never re-fetched or rewritten).

    Runs before j08/j09 in the nightly, so a new ticker's deep history is split-
    reversed against the corporate actions j06 lands the same night.
    """
    return _capture_window(
        ctx, HISTORY_START, downloader=downloader, bucket=bucket,
        key_tag="history:", only_without_history=True,
    )


def _has_history(ctx: JobContext, ticker: str) -> bool:
    """Has this ticker ever had a deep-history payload landed?

    The manifest is the record of what we already hold, so it is also the
    cheapest answer to 'is this name new?' — no extra state to drift.
    """
    row = ctx.conn.execute(
        "SELECT 1 FROM landing_manifest WHERE dataset = ? AND source = ? "
        "AND request_key LIKE ? LIMIT 1",
        [DATASET, SOURCE, f"{ticker}:history:%"],
    ).fetchone()
    return row is not None


def _capture_window(
    ctx: JobContext,
    start: date,
    *,
    downloader: Downloader | None,
    bucket: TokenBucket | None,
    key_tag: str,
    only_without_history: bool = False,
) -> JobResult:
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    downloader = downloader or _default_downloader
    bucket = bucket or yfinance_bucket()
    budget = RunBudget(SOURCE, ctx.settings.yfinance_nightly_budget)

    end = ctx.trade_date + timedelta(days=1)  # yfinance end is exclusive

    landed = 0
    skipped = 0
    failures = 0
    empty = 0
    for row in load_universe(ctx.settings):
        ticker = row["ticker"]
        if only_without_history and _has_history(ctx, ticker):
            skipped += 1  # already has its spine — never re-fetch or rewrite it
            continue
        # the trade-date suffix keeps history payloads visible to the same
        # per-trade-date build queue as the nightly incrementals
        request_key = f"{ticker}:{key_tag}{ctx.trade_date.isoformat()}"
        if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
            skipped += 1
            continue
        budget.spend()
        bucket.acquire()
        try:
            df = downloader(ticker, start, end)
        except Exception:
            failures += 1
            continue
        if df is None or len(df) == 0:
            empty += 1
            continue
        store.write(
            ctx.conn, ctx.settings,
            dataset=DATASET, source=SOURCE, request_key=request_key,
            payload=_to_parquet_bytes(df), ext="parquet", partition_date=ctx.trade_date,
            knowledge_time=pull_knowledge_time(), content_type="application/parquet",
        )
        landed += 1

    if landed == 0 and failures > 0 and empty == 0 and skipped == 0:
        health.record_failure(ctx.conn, SOURCE)
    else:
        health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} empty={empty} already={skipped} failures={failures}",
    )
