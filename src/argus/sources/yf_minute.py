"""yfinance 1-minute bar harvest for the watchlist (capture-only in M0).

Yahoo serves 1-minute bars only ~30 days back — every day of delay is minute
history permanently lost, which is why this job starts on day 1 with no
processing behind it. One payload per (ticker, session) lands as Parquet.

pandas is quarantined to this module (yfinance returns pandas frames); the
frame is stored as-received. The downloader is injectable so tests never
import network paths.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from argus.config_files import load_watchlist
from argus.core import calendars
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SourceDown
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, TokenBucket, yfinance_bucket

SOURCE = "yfinance"
DATASET = "minute_bars"
LOOKBACK_DAYS = 25  # stay inside Yahoo's ~30-day 1m window with margin

Downloader = Callable[[str, date], Any]  # returns a pandas DataFrame (possibly empty)


def _default_downloader(ticker: str, session: date) -> Any:
    import yfinance as yf  # deferred: keeps import cost/network surface out of tests

    return yf.download(
        ticker,
        start=session.isoformat(),
        end=(session + timedelta(days=1)).isoformat(),
        interval="1m",
        auto_adjust=False,
        prepost=False,
        progress=False,
        threads=False,
    )


def _to_parquet_bytes(df: Any) -> bytes:
    frame = df.reset_index()
    # yfinance sometimes returns MultiIndex columns even for one ticker; flatten for Parquet.
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
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    downloader = downloader or _default_downloader
    bucket = bucket or yfinance_bucket()
    budget = RunBudget(SOURCE, ctx.settings.yfinance_nightly_budget)

    watchlist = load_watchlist(ctx.settings)
    sessions = calendars.sessions_within(ctx.trade_date, LOOKBACK_DAYS)

    landed = 0
    empty = 0
    skipped = 0
    failures = 0
    for ticker in watchlist:
        for session in sessions:
            request_key = f"{ticker}:{session.isoformat()}"
            if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
                skipped += 1
                continue
            budget.spend()
            bucket.acquire()
            try:
                df = downloader(ticker, session)
            except Exception:  # yfinance raises loosely-typed errors; isolate per request
                failures += 1
                continue
            if df is None or len(df) == 0:
                empty += 1
                continue
            store.write(
                ctx.conn, ctx.settings,
                dataset=DATASET, source=SOURCE, request_key=request_key,
                payload=_to_parquet_bytes(df), ext="parquet", partition_date=session,
                knowledge_time=pull_knowledge_time(), content_type="application/parquet",
            )
            landed += 1

    if landed == 0 and failures > 0 and empty == 0:
        health.record_failure(ctx.conn, SOURCE)
    else:
        health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed,
        budget_used=budget.used,
        detail=f"landed={landed} empty={empty} already={skipped} failures={failures}",
    )
