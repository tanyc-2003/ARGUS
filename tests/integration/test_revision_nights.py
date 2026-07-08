"""Two simulated nights through the real build jobs: fresh bars on night one,
a vendor rewrite detected on night two -> SCD-2 revision + correct time travel.
"""

import io
from datetime import UTC, date, datetime

import polars as pl
import structlog

from argus.canonical import daily_bars
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops.jobs import JobContext
from argus.orchestration.build_jobs import build_daily_incrementals

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


def _ctx_for(settings, conn, trade_date: date) -> JobContext:
    return JobContext(settings=settings, conn=conn, trade_date=trade_date,
                      log=structlog.get_logger("argus-test"))


def test_two_night_revision_lifecycle(settings, conn) -> None:
    d1, d2 = date(2026, 7, 2), date(2026, 7, 6)

    # ---- night one: fresh observations
    ctx1 = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx1, "AAPL", {d1: 210.00, d2: 212.50})
    r1 = build_daily_incrementals(ctx1)
    assert r1.rows_out == 2

    # ---- night two: vendor silently rewrote d1
    ctx2 = _ctx_for(settings, conn, NIGHT2)
    _land_yf(ctx2, "AAPL", {d1: 209.40, d2: 212.50})
    r2 = build_daily_incrementals(ctx2)
    assert "'revised': 1" in r2.detail and "'unchanged': 1" in r2.detail

    versions = conn.execute(
        """
        SELECT revision_seq, is_current, close FROM bars_daily
        WHERE ticker='AAPL' AND bar_date=? ORDER BY revision_seq
        """,
        [d1],
    ).fetchall()
    assert [(v[0], v[1], v[2]) for v in versions] == [(1, False, 210.00), (2, True, 209.40)]

    # time travel: before night two's detection we still believed 210.00
    believed_night1 = daily_bars.bars_asof(
        conn, "AAPL", d1, datetime(2026, 7, 7, 6, 0, tzinfo=UTC)
    )
    assert believed_night1 is not None and believed_night1[0] == 210.00

    # d2 was untouched: still a single version
    n_d2 = conn.execute(
        "SELECT COUNT(*) FROM bars_daily WHERE ticker='AAPL' AND bar_date=?", [d2]
    ).fetchone()[0]
    assert n_d2 == 1


def test_yf_never_revises_the_stooq_spine(settings, conn) -> None:
    d = date(2026, 7, 2)
    # bootstrap owns this key with a slightly different vendor value
    daily_bars.upsert_bars(
        conn,
        pl.DataFrame(
            {"ticker": ["AAPL"], "bar_date": [d], "open": [210.1], "high": [210.1],
             "low": [210.1], "close": [210.1], "volume": [1e6]}
        ),
        source_set="stooq",
    )
    ctx = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx, "AAPL", {d: 210.0})
    result = build_daily_incrementals(ctx)
    assert "foreign_keys_skipped=1" in result.detail

    row = conn.execute(
        "SELECT source_set, close, revision_seq FROM bars_daily WHERE is_current"
    ).fetchone()
    assert row == ("stooq", 210.1, 1)  # spine intact, no flip-flop revision

    # the yfinance observation still exists in L2 for M3's voting
    from argus.events import schemas
    from argus.events import store as event_store

    events = event_store.scan(settings, schemas.BAR_EVENTS).collect()
    assert events.filter(pl.col("source") == "yfinance").height == 1


def test_stooq_bootstrap_never_stomps_yf_owned_keys(settings, conn) -> None:
    """The ownership rule holds in BOTH directions: a late bootstrap must not
    revise keys the incremental feed already owns."""
    from argus.orchestration.build_jobs import build_daily_bars

    d = date(2026, 7, 2)
    ctx = _ctx_for(settings, conn, NIGHT1)
    _land_yf(ctx, "AAPL", {d: 210.0})
    build_daily_incrementals(ctx)  # yfinance now owns (AAPL, d)

    stooq_csv = f"Date,Open,High,Low,Close,Volume\n{d},209.9,210.5,209.0,209.9,1000000\n"
    store.write(
        ctx.conn, ctx.settings,
        dataset="stooq_daily", source="stooq",
        request_key=f"AAPL:{ctx.trade_date.isoformat()}",
        payload=stooq_csv.encode(), ext="csv", partition_date=ctx.trade_date,
        knowledge_time=pull_knowledge_time(),
    )
    result = build_daily_bars(ctx)
    assert "foreign_keys_skipped=1" in result.detail

    row = conn.execute(
        "SELECT source_set, close, revision_seq FROM bars_daily WHERE is_current"
    ).fetchone()
    assert row == ("yfinance", 210.0, 1)  # untouched
