"""M4 exit criteria: the graveyard projection, its guards, terminal returns,
coverage arithmetic, and the served shapes."""

import gzip
import json
from datetime import date

import polars as pl

from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.orchestration.universe_jobs import universe_seal
from argus.serving import contracts
from argus.serving.publish import publish

D1, D2, D3 = date(2026, 7, 2), date(2026, 7, 6), date(2026, 7, 7)


def _nasdaq_file(tickers: list[str]) -> str:
    header = ("Symbol|Security Name|Market Category|Test Issue|Financial Status|"
              "Round Lot Size|ETF|NextShares")
    rows = [f"{t}|{t} Common Stock|Q|N|N|100|N|N" for t in tickers]
    return "\n".join([header, *rows, "File Creation Time: 0709202622:01|||||||"])


def _other_file(tickers: list[str]) -> str:
    header = ("ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|"
              "Test Issue|NASDAQ Symbol")
    rows = [f"{t}|{t} ETF|P|{t}|Y|100|N|{t}" for t in tickers]
    return "\n".join([header, *rows, "File Creation Time: 0709202622:01|||||||"])


def _land_dirs(ctx, snapshot_date: date, nasdaq: list[str], other: list[str]) -> None:
    for kind, text in (("nasdaqlisted", _nasdaq_file(nasdaq)),
                       ("otherlisted", _other_file(other))):
        store.write(
            ctx.conn, ctx.settings,
            dataset="symbol_dirs", source="nasdaqtrader",
            request_key=f"{kind}:{snapshot_date.isoformat()}",
            payload=text.encode(), ext="txt", partition_date=snapshot_date,
            knowledge_time=pull_knowledge_time(),
        )


def test_disappearance_enters_graveyard(ctx) -> None:
    _land_dirs(ctx, D1, ["AAPL", "DOOMD"], ["SPY"])
    _land_dirs(ctx, D2, ["AAPL"], ["SPY"])  # DOOMD gone
    result = universe_seal(ctx)
    assert result.rows_out == 1
    row = ctx.conn.execute(
        "SELECT ticker, termination_date, termination_reason, reason_confidence, first_seen "
        "FROM graveyard"
    ).fetchone()
    assert row[:4] == ("DOOMD", D2, "unknown", "pending")

    # regression: the SECOND seal must survive existing rows (session-tz types)
    # and keep first_seen stable
    assert universe_seal(ctx).rows_out == 1
    row2 = ctx.conn.execute("SELECT first_seen FROM graveyard").fetchone()
    assert row2[0] == row[4]


def test_relisting_self_corrects(ctx) -> None:
    _land_dirs(ctx, D1, ["AAPL", "MOVER"], ["SPY"])
    _land_dirs(ctx, D2, ["AAPL"], ["SPY"])          # MOVER gone -> would terminate
    _land_dirs(ctx, D3, ["AAPL"], ["SPY", "MOVER"])  # back on NYSE: exchange move
    universe_seal(ctx)
    n = ctx.conn.execute("SELECT COUNT(*) FROM graveyard").fetchone()[0]
    assert n == 0  # projection self-corrected: a move is not a death


def test_incomplete_snapshot_never_fakes_delisting(ctx) -> None:
    _land_dirs(ctx, D1, ["AAPL", "SAFE"], ["SPY"])
    # D2: only ONE file landed (otherlisted missing) — must not count as a snapshot
    store.write(
        ctx.conn, ctx.settings,
        dataset="symbol_dirs", source="nasdaqtrader",
        request_key=f"nasdaqlisted:{D2.isoformat()}",
        payload=_nasdaq_file(["AAPL"]).encode(), ext="txt", partition_date=D2,
        knowledge_time=pull_knowledge_time(),
    )
    universe_seal(ctx)
    n = ctx.conn.execute("SELECT COUNT(*) FROM graveyard").fetchone()[0]
    assert n == 0  # SAFE (and SPY) survive the partial capture


def test_terminal_return_from_sealed_bars(ctx) -> None:
    from argus.canonical import daily_bars

    closes = [(date(2026, 6, 30), 10.0), (date(2026, 7, 1), 8.0)]  # -20% final day
    daily_bars.upsert_bars(
        ctx.conn,
        pl.DataFrame(
            {
                "ticker": ["DOOMD"] * 2,
                "bar_date": [c[0] for c in closes],
                "open": [c[1] for c in closes], "high": [c[1] for c in closes],
                "low": [c[1] for c in closes], "close": [c[1] for c in closes],
                "volume": [1e6] * 2,
            }
        ),
    )
    _land_dirs(ctx, D1, ["DOOMD", "FILLER"], ["SPY"])
    _land_dirs(ctx, D2, ["FILLER"], ["SPY"])
    universe_seal(ctx)
    row = ctx.conn.execute(
        "SELECT terminal_return FROM graveyard WHERE ticker='DOOMD'"
    ).fetchone()
    assert row is not None and abs(row[0] - (-0.2)) < 1e-9

    cov = ctx.conn.execute(
        "SELECT coverage FROM coverage_metrics WHERE audit_window='since_golive'"
    ).fetchone()
    assert cov[0] == 1.0  # the one delisted name has bars -> fully covered


def test_coverage_honest_when_bars_missing(ctx) -> None:
    _land_dirs(ctx, D1, ["GHOST", "DOOMD", "FILLER"], ["SPY"])
    _land_dirs(ctx, D2, ["FILLER"], ["SPY"])  # both die; no bars exist for either
    universe_seal(ctx)
    rows = dict(ctx.conn.execute(
        "SELECT audit_window, coverage FROM coverage_metrics"
    ).fetchall())
    assert rows["since_golive"] == 0.0  # the unflattering truth, served
    assert rows["10y"] == 0.0


def test_polygon_delisted_date_wins(ctx) -> None:
    _land_dirs(ctx, D1, ["DOOMD", "FILLER"], ["SPY"])
    _land_dirs(ctx, D2, ["FILLER"], ["SPY"])  # diff says D2
    payload = {"results": [{"ticker": "DOOMD", "delisted_utc": "2026-07-01T00:00:00Z"}],
               "complete": True, "pages": 1}
    store.write(
        ctx.conn, ctx.settings,
        dataset="polygon_delisted", source="polygon",
        request_key=f"delisted:{ctx.trade_date.isoformat()}",
        payload=gzip.compress(json.dumps(payload).encode()), ext="json.gz",
        partition_date=ctx.trade_date, knowledge_time=pull_knowledge_time(),
    )
    universe_seal(ctx)
    row = ctx.conn.execute(
        "SELECT termination_date, detection_source FROM graveyard WHERE ticker='DOOMD'"
    ).fetchone()
    assert row == (date(2026, 7, 1), "polygon")  # authoritative date preferred


def test_served_shapes_and_publish_gates(ctx) -> None:
    _land_dirs(ctx, D1, ["AAPL", "DOOMD"], ["SPY"])
    _land_dirs(ctx, D2, ["AAPL"], ["SPY"])
    universe_seal(ctx)
    publish(ctx)

    serving = ctx.settings.serving_db_path
    assert contracts.assert_delisted(serving) == 1
    assert contracts.assert_coverage(serving) == 2  # both audit windows served

    import duckdb

    con = duckdb.connect(str(serving), read_only=True)
    delisted = con.execute("SELECT * FROM vw_mad_delisted").pl()
    coverage = con.execute("SELECT * FROM vw_mad_coverage").pl()
    con.close()
    assert dict(delisted.schema) == contracts.DELISTED_SCHEMA
    assert dict(coverage.schema) == contracts.COVERAGE_SCHEMA
    assert set(delisted["termination_reason"].to_list()) <= contracts.TERMINATION_REASONS
