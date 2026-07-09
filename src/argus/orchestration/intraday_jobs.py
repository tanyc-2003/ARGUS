"""Intraday seal: landed minute/quote payloads -> canonical -> hybrid serving frame.

Incremental over the L0 archive: each payload processes once (marker table),
so the nightly cost stays flat as the archive compounds. The serving frame is
a full re-projection (bars x BBO join + CS fallback) — cheap at this scale;
revisit when the watchlist grows past ~100 names.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from argus.core import calendars
from argus.core.clocks import utc_now
from argus.derive.spreads import corwin_schultz_daily, hybrid_intraday
from argus.normalize.minute import bucket_quotes, parse_yf_minute_parquet
from argus.ops.jobs import JobContext, JobResult

MINUTE_DATASET = "minute_bars"
QUOTE_DATASET = "quote_ticks"


def _unprocessed(ctx: JobContext, dataset: str) -> list[tuple[str, str]]:
    rows = ctx.conn.execute(
        """
        SELECT lm.request_key, lm.path FROM landing_manifest lm
        WHERE lm.dataset = ? AND NOT EXISTS (
            SELECT 1 FROM intraday_processed p
            WHERE p.dataset = lm.dataset AND p.request_key = lm.request_key
        )
        ORDER BY lm.request_key
        """,
        [dataset],
    ).fetchall()
    return [(str(r[0]), str(r[1])) for r in rows]


def _mark(ctx: JobContext, dataset: str, request_key: str) -> None:
    ctx.conn.execute(
        "INSERT OR IGNORE INTO intraday_processed VALUES (?, ?, ?)",
        [dataset, request_key, utc_now()],
    )


def _insert_frame(ctx: JobContext, table: str, frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    ctx.conn.register("intraday_incoming", frame.to_arrow())
    try:
        cols = ", ".join(frame.columns)
        ctx.conn.execute(
            f"INSERT OR IGNORE INTO {table} ({cols}) SELECT {cols} FROM intraday_incoming"
        )
    finally:
        ctx.conn.unregister("intraday_incoming")
    return frame.height


def intraday_seal(ctx: JobContext) -> JobResult:
    minute_rows = 0
    for request_key, path in _unprocessed(ctx, MINUTE_DATASET):
        ticker = request_key.split(":", 1)[0]
        with open(path, "rb") as fh:
            frame = parse_yf_minute_parquet(fh.read(), ticker)
        # a minute bar is world-knowable at its own minute
        frame = frame.with_columns(
            pl.lit("yfinance").alias("source"),
            pl.col("minute_ts").alias("knowledge_time"),
        )
        minute_rows += _insert_frame(ctx, "bars_minute", frame)
        _mark(ctx, MINUTE_DATASET, request_key)

    quote_rows = 0
    for request_key, path in _unprocessed(ctx, QUOTE_DATASET):
        ticker, session_str = request_key.split(":", 1)
        session = calendars.session_info(date.fromisoformat(session_str))
        if session is None:
            _mark(ctx, QUOTE_DATASET, request_key)
            continue
        with open(path, "rb") as fh:
            frame = bucket_quotes(fh.read(), ticker, session.close_utc)
        frame = frame.with_columns(pl.col("minute_ts").alias("knowledge_time"))
        quote_rows += _insert_frame(ctx, "quote_bars_1m", frame)
        _mark(ctx, QUOTE_DATASET, request_key)

    # re-project the hybrid serving frame (v4 §5.2)
    minutes = ctx.conn.execute(
        "SELECT ticker, minute_ts, close, volume FROM bars_minute"
    ).pl()
    quotes = ctx.conn.execute(
        "SELECT ticker, minute_ts, bid_close, ask_close FROM quote_bars_1m"
    ).pl()
    daily = ctx.conn.execute(
        """
        SELECT ticker, bar_date, high, low FROM bars_daily
        WHERE is_current AND grade <> 'quarantined' AND high > 0 AND low > 0
        """
    ).pl()
    # DuckDB returns TIMESTAMPTZ in the SESSION timezone — normalize both sides
    # to UTC unconditionally (an empty frame casts for free) or the join breaks
    hybrid = hybrid_intraday(
        minutes.with_columns(pl.col("minute_ts").cast(pl.Datetime("us", "UTC"))),
        quotes.with_columns(pl.col("minute_ts").cast(pl.Datetime("us", "UTC"))),
        corwin_schultz_daily(daily),
    )
    ctx.conn.execute("DELETE FROM serving_intraday")
    served = _insert_frame(ctx, "serving_intraday", hybrid)

    bbo = 0 if hybrid.is_empty() else hybrid.filter(
        pl.col("derivation") == "iex_bbo"
    ).height
    return JobResult(
        rows_out=served,
        detail=(
            f"new_minute_rows={minute_rows} new_quote_rows={quote_rows} "
            f"served={served} iex_bbo={bbo} corwin_schultz={served - bbo}"
        ),
    )
