"""Minute-bar and quote-tick normalization (R2 inputs).

Minute OHLCV comes from the landed yfinance parquet payloads (consolidated
volume — the reason this feed exists, v4 §5.2). Quote ticks come from the
landed Alpaca IEX json.gz payloads and bucket into per-minute BBO: last quote
at minute close plus a time-weighted mean (v3 §4.8 shape).

Everything lands in UTC; RTH filtering happens against market_sessions at the
build job (a session's exact UTC window handles DST structurally).
"""

from __future__ import annotations

import gzip
import io
import json
from datetime import UTC, datetime

import polars as pl

from argus.ops.errors import SchemaDrift

MINUTE_SCHEMA: dict[str, object] = {
    "ticker": pl.Utf8,
    "minute_ts": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}

QUOTE_BAR_SCHEMA: dict[str, object] = {
    "ticker": pl.Utf8,
    "minute_ts": pl.Datetime("us", "UTC"),
    "bid_close": pl.Float64,
    "ask_close": pl.Float64,
    "bid_twm": pl.Float64,
    "ask_twm": pl.Float64,
    "n_quotes": pl.Int32,
}


def parse_yf_minute_parquet(payload: bytes, ticker: str) -> pl.DataFrame:
    """Landed yfinance 1m payload -> MINUTE_SCHEMA frame (UTC minutes)."""
    try:
        df = pl.read_parquet(io.BytesIO(payload))
    except Exception as exc:
        raise SchemaDrift(f"yfinance:{ticker} minute payload unreadable: {exc}",
                          source="yfinance") from exc

    def find(name: str) -> str | None:
        for c in df.columns:
            if c == name or c.startswith(f"{name}_"):
                return c
        return None

    ts_col = find("Datetime") or find("Date") or find("index")
    cols = {n: find(n) for n in ["Open", "High", "Low", "Close", "Volume"]}
    missing = [n for n, c in cols.items() if c is None and n != "Volume"]
    if ts_col is None or missing:
        raise SchemaDrift(
            f"yfinance:{ticker} minute payload missing timestamp/{missing} "
            f"(got {df.columns})",
            source="yfinance",
        )

    ts = pl.col(ts_col)
    ts_dtype = df.schema[ts_col]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        minute_expr = ts.dt.convert_time_zone("UTC")
    elif isinstance(ts_dtype, pl.Datetime):
        # naive timestamps from Yahoo are exchange-local wall time
        minute_expr = ts.dt.replace_time_zone("America/New_York").dt.convert_time_zone("UTC")
    else:
        raise SchemaDrift(
            f"yfinance:{ticker} minute timestamp column has dtype {ts_dtype}",
            source="yfinance",
        )

    open_c, high_c, low_c, close_c = cols["Open"], cols["High"], cols["Low"], cols["Close"]
    assert open_c and high_c and low_c and close_c
    volume_c = cols["Volume"]
    out = df.select(
        pl.lit(ticker.upper()).alias("ticker"),
        minute_expr.cast(pl.Datetime("us", "UTC")).alias("minute_ts"),
        pl.col(open_c).cast(pl.Float64).alias("open"),
        pl.col(high_c).cast(pl.Float64).alias("high"),
        pl.col(low_c).cast(pl.Float64).alias("low"),
        pl.col(close_c).cast(pl.Float64).alias("close"),
        (
            pl.col(volume_c).cast(pl.Float64) if volume_c else pl.lit(None, dtype=pl.Float64)
        ).alias("volume"),
    ).drop_nulls(subset=["minute_ts", "close"])
    return out.sort("minute_ts")


def bucket_quotes(payload: bytes, ticker: str, session_close_utc: datetime) -> pl.DataFrame:
    """Landed Alpaca quote-tick payload -> QUOTE_BAR_SCHEMA minute buckets.

    bid/ask close = last quote in the minute; the time-weighted mean weights
    each quote by how long it stood (capped at the minute boundary / session
    close). Zero-priced quotes (empty book) are dropped.
    """
    try:
        body = json.loads(gzip.decompress(payload))
    except Exception as exc:
        raise SchemaDrift(f"alpaca_iex:{ticker} quote payload unreadable: {exc}",
                          source="alpaca_iex") from exc
    quotes = body.get("quotes")
    if quotes is None:
        raise SchemaDrift(f"alpaca_iex:{ticker} quote payload missing 'quotes'",
                          source="alpaca_iex")

    rows = []
    for q in quotes:
        try:
            ts = datetime.fromisoformat(str(q["t"]).replace("Z", "+00:00")).astimezone(UTC)
            bid, ask = float(q.get("bp") or 0.0), float(q.get("ap") or 0.0)
        except (KeyError, TypeError, ValueError) as exc:
            raise SchemaDrift(f"alpaca_iex:{ticker} malformed quote {q}: {exc}",
                              source="alpaca_iex") from exc
        if bid <= 0.0 or ask <= 0.0:
            continue
        rows.append({"ts": ts, "bid": bid, "ask": ask})
    if not rows:
        return pl.DataFrame(schema=QUOTE_BAR_SCHEMA)  # type: ignore[arg-type]

    df = pl.DataFrame(rows).sort("ts").with_columns(
        pl.col("ts").dt.truncate("1m").alias("minute_ts"),
    )
    # duration each quote stands: until the next quote, capped at its minute end
    df = df.with_columns(
        pl.col("ts").shift(-1).fill_null(session_close_utc).alias("next_ts"),
        (pl.col("minute_ts") + pl.duration(minutes=1)).alias("minute_end"),
    ).with_columns(
        pl.min_horizontal("next_ts", "minute_end").alias("stand_until")
    ).with_columns(
        (pl.col("stand_until") - pl.col("ts")).dt.total_microseconds()
        .clip(lower_bound=0).alias("weight_us")
    )
    out = (
        df.group_by("minute_ts")
        .agg(
            pl.col("bid").last().alias("bid_close"),
            pl.col("ask").last().alias("ask_close"),
            ((pl.col("bid") * pl.col("weight_us")).sum()
             / pl.col("weight_us").sum().clip(lower_bound=1)).alias("bid_twm"),
            ((pl.col("ask") * pl.col("weight_us")).sum()
             / pl.col("weight_us").sum().clip(lower_bound=1)).alias("ask_twm"),
            pl.len().cast(pl.Int32).alias("n_quotes"),
        )
        .with_columns(pl.lit(ticker.upper()).alias("ticker"))
        .select(list(QUOTE_BAR_SCHEMA))
        .sort("minute_ts")
    )
    return out.with_columns(pl.col("minute_ts").cast(pl.Datetime("us", "UTC")))
