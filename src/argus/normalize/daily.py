"""Daily-bar normalization: Stooq CSV -> typed frame -> RAW prices.

`reverse_split_adjustment` is the most PIT-critical function in the repo
(v4 §5.1): Stooq (and Yahoo) serve retroactively split-adjusted prices, so
ingesting them naively bakes look-ahead into the archive. Split adjustment is
a known multiplicative factor and therefore invertible:

    raw(D)        = vendor(D) x PROD{ ratio(s) : s.ex_date > D }
    raw_volume(D) = vendor_volume(D) / PROD{ ratio(s) : s.ex_date > D }

where ratio = split_to/split_from (2.0 for a 2-for-1). Dividends never appear
in these vendor prices, so they live purely in the factor layer.
"""

from __future__ import annotations

import io

import polars as pl

from argus.ops.errors import SchemaDrift

_STOOQ_REQUIRED = ["Date", "Open", "High", "Low", "Close"]


def parse_stooq_csv(text: str, ticker: str) -> pl.DataFrame:
    """Stooq CSV -> (ticker, bar_date, open, high, low, close, volume) or SchemaDrift."""
    try:
        df = pl.read_csv(io.StringIO(text), try_parse_dates=True)
    except Exception as exc:
        raise SchemaDrift(f"stooq:{ticker} CSV unparseable: {exc}", source="stooq") from exc
    missing = [c for c in _STOOQ_REQUIRED if c not in df.columns]
    if missing:
        raise SchemaDrift(
            f"stooq:{ticker} missing columns {missing} (got {df.columns})", source="stooq"
        )
    if "Volume" not in df.columns:  # a few thin names come without volume
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("Volume"))
    out = df.select(
        pl.lit(ticker.upper()).alias("ticker"),
        pl.col("Date").cast(pl.Date).alias("bar_date"),
        pl.col("Open").cast(pl.Float64).alias("open"),
        pl.col("High").cast(pl.Float64).alias("high"),
        pl.col("Low").cast(pl.Float64).alias("low"),
        pl.col("Close").cast(pl.Float64).alias("close"),
        pl.col("Volume").cast(pl.Float64).alias("volume"),
    ).drop_nulls(subset=["bar_date", "close"])
    if out.is_empty():
        raise SchemaDrift(f"stooq:{ticker} parsed to zero rows", source="stooq")
    return out.sort("bar_date")


def parse_yf_daily_parquet(payload: bytes, ticker: str) -> pl.DataFrame:
    """Landed yfinance daily parquet -> (ticker, bar_date, o/h/l/c, volume, adj_close).

    yfinance sometimes returns MultiIndex columns even for one ticker; the
    capture flattens them to 'Open_AAPL' style, so match by exact name OR
    prefix. Prices are split-adjusted, dividend-unadjusted (auto_adjust=False);
    adj_close rides along for M3's implied-dividend cross-check.
    """
    import io as _io

    try:
        df = pl.read_parquet(_io.BytesIO(payload))
    except Exception as exc:
        raise SchemaDrift(
            f"yfinance:{ticker} daily payload unreadable: {exc}", source="yfinance"
        ) from exc

    def find(name: str) -> str | None:
        for c in df.columns:
            if c == name or c.startswith(f"{name}_"):
                return c
        return None

    cols = {n: find(n) for n in ["Date", "Open", "High", "Low", "Close", "Volume", "Adj Close"]}
    missing = [n for n in ["Date", "Open", "High", "Low", "Close"] if cols[n] is None]
    if missing:
        raise SchemaDrift(
            f"yfinance:{ticker} daily payload missing {missing} (got {df.columns})",
            source="yfinance",
        )
    date_c, open_c, high_c, low_c, close_c = (
        cols["Date"], cols["Open"], cols["High"], cols["Low"], cols["Close"],
    )
    assert date_c and open_c and high_c and low_c and close_c  # narrowed by the check above
    volume_c, adj_c = cols["Volume"], cols["Adj Close"]
    out = df.select(
        pl.lit(ticker.upper()).alias("ticker"),
        pl.col(date_c).cast(pl.Date).alias("bar_date"),
        pl.col(open_c).cast(pl.Float64).alias("open"),
        pl.col(high_c).cast(pl.Float64).alias("high"),
        pl.col(low_c).cast(pl.Float64).alias("low"),
        pl.col(close_c).cast(pl.Float64).alias("close"),
        (
            pl.col(volume_c).cast(pl.Float64) if volume_c else pl.lit(None, dtype=pl.Float64)
        ).alias("volume"),
        (
            pl.col(adj_c).cast(pl.Float64) if adj_c else pl.lit(None, dtype=pl.Float64)
        ).alias("adj_close"),
    ).drop_nulls(subset=["bar_date", "close"])
    if out.is_empty():
        raise SchemaDrift(f"yfinance:{ticker} daily payload parsed to zero rows",
                          source="yfinance")
    return out.sort("bar_date")


def _with_reversal_factor(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Attach reversal_factor(D) = PROD{ ratio : ex_date > D } per (ticker, bar_date).

    Implementation: per-ticker suffix product over splits sorted by ex_date,
    then join_asof(forward). join_asof matches the NEAREST key >= bar_date;
    keying on (ex_date - 1 day) makes that equivalent to ex_date > bar_date.
    """
    if splits.is_empty():
        return bars.with_columns(pl.lit(1.0).alias("reversal_factor"))
    sp = (
        splits.select("ticker", "ex_date", "ratio")
        .sort(["ticker", "ex_date"])
        .with_columns(
            pl.col("ratio").log().cum_sum(reverse=True).exp().over("ticker")
            .alias("suffix_factor"),
            (pl.col("ex_date") - pl.duration(days=1)).cast(pl.Date).alias("join_key"),
        )
        .select("ticker", "join_key", "suffix_factor")
        .sort(["ticker", "join_key"])
    )
    return (
        bars.sort(["ticker", "bar_date"])
        .join_asof(sp, left_on="bar_date", right_on="join_key", by="ticker", strategy="forward")
        .with_columns(pl.col("suffix_factor").fill_null(1.0).alias("reversal_factor"))
        .drop("join_key", "suffix_factor", strict=False)
    )


def reverse_split_adjustment(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Reconstruct RAW prices from vendor split-adjusted bars.

    bars:   (ticker, bar_date, open, high, low, close, volume) — vendor-adjusted
    splits: (ticker, ex_date: Date, ratio: Float64) — ratio = to/from, e.g. 4.0

    Returns bars with raw prices and the `reversal_factor` applied (1.0 = untouched).
    """
    return _with_reversal_factor(bars, splits).with_columns(
        (pl.col("open") * pl.col("reversal_factor")).alias("open"),
        (pl.col("high") * pl.col("reversal_factor")).alias("high"),
        (pl.col("low") * pl.col("reversal_factor")).alias("low"),
        (pl.col("close") * pl.col("reversal_factor")).alias("close"),
        (pl.col("volume") / pl.col("reversal_factor")).alias("volume"),
    )


def apply_split_adjustment(raw: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Inverse of reverse_split_adjustment: the vendor's split-adjusted view of raw
    bars (property-test helper proving the round trip)."""
    return (
        _with_reversal_factor(raw, splits)
        .with_columns(
            (pl.col("open") / pl.col("reversal_factor")).alias("open"),
            (pl.col("high") / pl.col("reversal_factor")).alias("high"),
            (pl.col("low") / pl.col("reversal_factor")).alias("low"),
            (pl.col("close") / pl.col("reversal_factor")).alias("close"),
            (pl.col("volume") * pl.col("reversal_factor")).alias("volume"),
        )
        .drop("reversal_factor")
    )
