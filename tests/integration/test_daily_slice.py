"""End-to-end M1 slice on fixtures: land -> build -> serve -> contract.

This is the dashboard-shaped read: the exact polars call the ArgusConnector
will make, against the atomically-published serving copy.
"""

from datetime import date

import duckdb

from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.orchestration.build_jobs import build_actions, build_daily_bars, vote_and_seal
from argus.serving import contracts
from argus.serving.publish import publish

STOOQ_AAPL = (
    "Date,Open,High,Low,Close,Volume\n"
    "2020-08-27,125.24,127.48,123.83,125.01,155552400\n"
    "2020-08-28,126.01,126.44,124.58,124.81,187629920\n"  # vendor = raw/4 (split-adjusted)
    "2020-08-31,127.58,131.00,126.00,129.04,225702700\n"
)
STOOQ_SPY = (
    "Date,Open,High,Low,Close,Volume\n"
    "2020-08-28,350.35,351.99,349.52,350.58,44813700\n"
    "2020-08-31,350.40,351.30,349.10,349.31,61305100\n"
)
SPLITS_AAPL = (
    b'{"status":"OK","results":[{"execution_date":"2020-08-31",'
    b'"split_from":1,"split_to":4,"ticker":"AAPL"}]}'
)
EMPTY_CA = b'{"status":"OK","results":[]}'


def _land(ctx, dataset: str, ticker: str, payload: bytes, ext: str) -> None:
    store.write(
        ctx.conn, ctx.settings,
        dataset=dataset, source="polygon" if dataset.startswith("polygon") else "stooq",
        request_key=f"{ticker}:{ctx.trade_date.isoformat()}",
        payload=payload, ext=ext, partition_date=ctx.trade_date,
        knowledge_time=pull_knowledge_time(),
    )


def test_full_slice(ctx) -> None:
    # 1. land raw payloads exactly as capture jobs would
    _land(ctx, "polygon_splits", "AAPL", SPLITS_AAPL, "json")
    _land(ctx, "polygon_dividends", "AAPL", EMPTY_CA, "json")
    _land(ctx, "polygon_splits", "SPY", EMPTY_CA, "json")
    _land(ctx, "polygon_dividends", "SPY", EMPTY_CA, "json")
    _land(ctx, "stooq_daily", "AAPL", STOOQ_AAPL.encode(), "csv")
    _land(ctx, "stooq_daily", "SPY", STOOQ_SPY.encode(), "csv")

    # 2. build: actions first (the reversal consumes them), bars -> events,
    #    vote seals canonical, then publish
    r1 = build_actions(ctx)
    assert r1.rows_out == 1  # one split canonicalized
    r2 = build_daily_bars(ctx)
    assert r2.rows_out == 5  # 3 AAPL + 2 SPY event bars
    seal = vote_and_seal(ctx)
    assert seal.rows_out == 5  # stooq-only: admitted single_source/degraded
    r3 = publish(ctx)
    assert r3.rows_out == 5

    # 3. the sealed serving copy exists and honors the frozen contract
    serving = ctx.settings.serving_db_path
    assert serving.exists()
    assert contracts.assert_daily_ohlcv(serving) == 5

    # 4. dashboard-shaped read
    con = duckdb.connect(str(serving), read_only=True)
    df = con.execute(
        "SELECT * FROM vw_mad_daily_ohlcv WHERE ticker='AAPL' ORDER BY effective_date"
    ).pl()
    meta = con.execute("SELECT sealed_trade_date FROM serving_meta").fetchone()
    con.close()
    assert dict(df.schema) == contracts.DAILY_OHLCV_SCHEMA
    assert meta[0] == ctx.trade_date

    # 5. golden PIT values: raw reconstructed, adjusted-as-of correct on both sides
    by_date = {r["effective_date"]: r for r in df.iter_rows(named=True)}
    assert abs(by_date[date(2020, 8, 28)]["close"] - 124.81 * 4) < 1e-6  # raw = vendor x 4
    assert abs(by_date[date(2020, 8, 31)]["close"] - 129.04 * 4) < 1e-6  # ex-date: factor on
    # volume: pre-split raw shares = vendor/4, served as raw; post-split rescaled by /4
    assert abs(by_date[date(2020, 8, 28)]["volume"] - 187629920.0 / 4) < 1e-3
    assert abs(by_date[date(2020, 8, 31)]["volume"] - 225702700.0 / 4) < 1e-3

    # 6. re-running is harmless: actions no-op (SCD-2); bars re-append events
    #    (deduped at the vote by latest-per-source), the vote itself no-ops
    assert build_actions(ctx).rows_out == 0
    build_daily_bars(ctx)
    assert vote_and_seal(ctx).rows_out == 0
    assert publish(ctx).rows_out == 5


def test_pit_report_on_the_slice(ctx) -> None:
    _land(ctx, "polygon_splits", "AAPL", SPLITS_AAPL, "json")
    _land(ctx, "polygon_dividends", "AAPL", EMPTY_CA, "json")
    _land(ctx, "polygon_splits", "SPY", EMPTY_CA, "json")
    _land(ctx, "polygon_dividends", "SPY", EMPTY_CA, "json")
    _land(ctx, "stooq_daily", "AAPL", STOOQ_AAPL.encode(), "csv")
    _land(ctx, "stooq_daily", "SPY", STOOQ_SPY.encode(), "csv")
    build_actions(ctx)
    build_daily_bars(ctx)
    vote_and_seal(ctx)

    from argus.factors.adjustment import pit_report

    pre = pit_report(ctx.conn, "AAPL", date(2020, 8, 28))
    assert pre.no_lookahead
    assert pre.cum_factor == 1.0  # split ex 08-31 not applied to the 08-28 bar
    post = pit_report(ctx.conn, "AAPL", date(2020, 8, 31))
    assert post.no_lookahead
    assert post.cum_factor == 4.0
