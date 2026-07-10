"""End-to-end M5 slice: landed minute + quote payloads -> canonical -> hybrid
serving frame -> published vw_mad_intraday in the exact dashboard shape."""

import gzip
import io
import json
from datetime import UTC, date, datetime

import duckdb
import pandas as pd

from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.orchestration.intraday_jobs import intraday_seal
from argus.orchestration.universe_jobs import universe_seal
from argus.serving import contracts
from argus.serving.publish import publish

SESSION = date(2026, 7, 7)


def _minute_payload() -> bytes:
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-07-07 09:30"), pd.Timestamp("2026-07-07 09:31")],
        name="Datetime",
    ).tz_localize("America/New_York")
    frame = pd.DataFrame(
        {"Open": [500.0, 500.5], "High": [500.6, 500.9], "Low": [499.8, 500.2],
         "Close": [500.5, 500.8], "Volume": [1_200_000, 950_000]},
        index=idx,
    ).reset_index()
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    return buf.getvalue()


def _quote_payload() -> bytes:
    quotes = [
        {"t": "2026-07-07T13:30:05Z", "bp": 500.40, "ap": 500.44},
        {"t": "2026-07-07T13:30:50Z", "bp": 500.48, "ap": 500.52},
        # nothing for 13:31 -> that minute falls back to corwin_schultz
    ]
    return gzip.compress(json.dumps({"quotes": quotes}).encode())


def test_intraday_end_to_end(ctx) -> None:
    store.write(
        ctx.conn, ctx.settings, dataset="minute_bars", source="yfinance",
        request_key=f"SPY:{SESSION.isoformat()}", payload=_minute_payload(),
        ext="parquet", partition_date=SESSION, knowledge_time=pull_knowledge_time(),
    )
    store.write(
        ctx.conn, ctx.settings, dataset="quote_ticks", source="alpaca_iex",
        request_key=f"SPY:{SESSION.isoformat()}", payload=_quote_payload(),
        ext="json.gz", partition_date=SESSION, knowledge_time=pull_knowledge_time(),
    )

    result = intraday_seal(ctx)
    assert result.rows_out == 2
    assert "iex_bbo=1" in result.detail and "corwin_schultz=1" in result.detail

    # incremental: a second seal re-processes nothing but re-projects the same frame
    again = intraday_seal(ctx)
    assert "new_minute_rows=0 new_quote_rows=0" in again.detail
    assert again.rows_out == 2

    universe_seal(ctx)  # publish gates on coverage
    publish(ctx)
    serving = ctx.settings.serving_db_path
    assert contracts.assert_intraday(serving) == 2

    con = duckdb.connect(str(serving), read_only=True)
    df = con.execute(
        "SELECT * FROM vw_mad_intraday WHERE ticker='SPY' ORDER BY minute"
    ).pl()
    con.close()

    assert dict(df.schema) == contracts.INTRADAY_SCHEMA
    first, second = df.row(0, named=True), df.row(1, named=True)
    # naive UTC minutes (the dashboard's plain pl.Datetime)
    assert first["minute"] == datetime(2026, 7, 7, 13, 30)
    assert first["derivation"] == "iex_bbo"
    assert (first["bid"], first["ask"]) == (500.48, 500.52)  # last quote in the minute
    assert first["volume"] == 1_200_000.0  # consolidated yfinance volume

    assert second["derivation"] == "corwin_schultz"
    assert second["bid"] <= second["ask"]
    assert second["volume"] == 950_000.0


def test_minute_knowledge_is_the_minute_itself(ctx) -> None:
    store.write(
        ctx.conn, ctx.settings, dataset="minute_bars", source="yfinance",
        request_key=f"SPY:{SESSION.isoformat()}", payload=_minute_payload(),
        ext="parquet", partition_date=SESSION, knowledge_time=pull_knowledge_time(),
    )
    intraday_seal(ctx)
    row = ctx.conn.execute(
        "SELECT minute_ts, knowledge_time FROM bars_minute ORDER BY minute_ts LIMIT 1"
    ).fetchone()
    assert row[0] == row[1] == datetime(2026, 7, 7, 13, 30, tzinfo=UTC)


def test_quote_only_minutes_do_not_serve(ctx) -> None:
    """A minute with BBO but no consolidated bar must not appear: volume would
    be IEX-only or null — the poison the whole hybrid design exists to avoid."""
    store.write(
        ctx.conn, ctx.settings, dataset="quote_ticks", source="alpaca_iex",
        request_key=f"SPY:{SESSION.isoformat()}", payload=_quote_payload(),
        ext="json.gz", partition_date=SESSION, knowledge_time=pull_knowledge_time(),
    )
    result = intraday_seal(ctx)
    assert result.rows_out == 0  # quotes canonicalized, nothing served without bars
    n_quotes = ctx.conn.execute("SELECT COUNT(*) FROM quote_bars_1m").fetchone()[0]
    assert n_quotes == 1
