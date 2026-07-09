"""Two simulated nights through the real pipeline (events -> vote -> canonical):
fresh bars, a vendor rewrite detected on night two, grade upgrades, conflicts.
"""

import io
from datetime import date

import polars as pl
import structlog

from argus.canonical import daily_bars
from argus.core.clocks import pull_knowledge_time, utc_now
from argus.landing import store
from argus.ops import dlq
from argus.ops.jobs import JobContext
from argus.orchestration.build_jobs import (
    build_daily_bars,
    build_daily_incrementals,
    vote_and_seal,
)

NIGHT1 = date(2026, 7, 6)  # Monday
NIGHT2 = date(2026, 7, 7)  # Tuesday


def _yf_payload(closes: dict[date, float]) -> bytes:
    df = pl.DataFrame(
        {
            "Date": list(closes.keys()),
            "Open": list(closes.values()),
            "High": [c * 1.01 for c in closes.values()],
            "Low": [c * 0.99 for c in closes.values()],
            "Close": list(closes.values()),
            "Adj Close": list(closes.values()),
            "Volume": [1e6] * len(closes),
        }
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def _land_yf(ctx: JobContext, ticker: str, closes: dict[date, float]) -> None:
    store.write(
        ctx.conn, ctx.settings,
        dataset="yf_daily", source="yfinance",
        request_key=f"{ticker}:{ctx.trade_date.isoformat()}",
        payload=_yf_payload(closes), ext="parquet", partition_date=ctx.trade_date,
        knowledge_time=pull_knowledge_time(),
    )


def _land_stooq(ctx: JobContext, ticker: str, closes: dict[date, float]) -> None:
    lines = ["Date,Open,High,Low,Close,Volume"] + [
        f"{d},{c},{c * 1.01:.4f},{c * 0.99:.4f},{c},1000000" for d, c in closes.items()
    ]
    store.write(
        ctx.conn, ctx.settings,
        dataset="stooq_daily", source="stooq",
        request_key=f"{ticker}:{ctx.trade_date.isoformat()}",
        payload="\n".join(lines).encode(), ext="csv", partition_date=ctx.trade_date,
        knowledge_time=pull_knowledge_time(),
    )


def _ctx_for(settings, conn, trade_date: date) -> JobContext:
    return JobContext(settings=settings, conn=conn, trade_date=trade_date,
                      log=structlog.get_logger("argus-test"))


def test_two_night_revision_lifecycle(settings, conn) -> None:
    d1, d2 = date(2026, 7, 2), date(2026, 7, 6)

    ctx1 = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx1, "AAPL", {d1: 210.00, d2: 212.50})
    build_daily_incrementals(ctx1)
    seal1 = vote_and_seal(ctx1)
    assert seal1.rows_out == 2
    t_between = utc_now()

    ctx2 = _ctx_for(settings, conn, NIGHT2)
    _land_yf(ctx2, "AAPL", {d1: 209.40, d2: 212.50})
    build_daily_incrementals(ctx2)
    seal2 = vote_and_seal(ctx2)
    assert seal2.rows_out == 1  # only the rewritten bar revises

    versions = conn.execute(
        """
        SELECT revision_seq, is_current, close FROM bars_daily
        WHERE ticker='AAPL' AND bar_date=? ORDER BY revision_seq
        """,
        [d1],
    ).fetchall()
    assert [(v[0], v[1], v[2]) for v in versions] == [(1, False, 210.00), (2, True, 209.40)]

    # time travel: between the nights we still believed the original value
    believed = daily_bars.bars_asof(conn, "AAPL", d1, t_between)
    assert believed is not None and believed[0] == 210.00
    now_believed = daily_bars.bars_asof(conn, "AAPL", d1, utc_now())
    assert now_believed is not None and now_believed[0] == 209.40


def test_second_source_upgrades_grade(settings, conn) -> None:
    d = date(2026, 7, 2)
    ctx1 = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx1, "AAPL", {d: 210.0})
    build_daily_incrementals(ctx1)
    vote_and_seal(ctx1)
    first = conn.execute(
        "SELECT grade, single_source FROM bars_daily WHERE is_current"
    ).fetchone()
    assert first == ("degraded", True)

    ctx2 = _ctx_for(settings, conn, NIGHT2)
    _land_stooq(ctx2, "AAPL", {d: 210.05})  # within ±0.1% -> agreement
    build_daily_bars(ctx2)
    vote_and_seal(ctx2)
    upgraded = conn.execute(
        "SELECT grade, single_source, revision_seq FROM bars_daily WHERE is_current"
    ).fetchone()
    assert upgraded == ("good", False, 2)  # belief improved -> revision, not overwrite


def test_vendor_conflict_quarantines_and_dead_letters(settings, conn) -> None:
    d = date(2026, 7, 2)
    ctx = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx, "AAPL", {d: 210.0})
    _land_stooq(ctx, "AAPL", {d: 215.0})  # ~2.4% apart: no agreeing pair
    build_daily_incrementals(ctx)
    build_daily_bars(ctx)
    vote_and_seal(ctx)

    row = conn.execute("SELECT grade FROM bars_daily WHERE is_current").fetchone()
    assert row[0] == "quarantined"
    served = conn.execute("SELECT COUNT(*) FROM vw_mad_daily_ohlcv").fetchone()[0]
    assert served == 0  # quarantined rows never reach the consumer
    assert dlq.open_depth(conn) == 1

    # a second seal must not spam the DLQ with the same conflict
    vote_and_seal(ctx)
    assert dlq.open_depth(conn) == 1

    verdict = conn.execute(
        "SELECT verdict, n_sources FROM vote_results WHERE ticker='AAPL'"
    ).fetchone()
    assert verdict == ("conflict", 2)
