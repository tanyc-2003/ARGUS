"""L5.5 spread derivation: IEX BBO when we have it, Corwin–Schultz when we don't.

The hybrid intraday frame (v4 §5.2): minute OHLCV (consolidated volume) joined
with IEX BBO on (ticker, minute). Minutes without a quote — thin names, and
every name during the 4–6 week baseline cold-start — fall back to a synthetic
bid/ask around the minute close, spread from the Corwin–Schultz daily high/low
estimator. Every row carries `derivation` so the consumer's CS-proxy and
real-spread paths stay separate (their own invariant).
"""

from __future__ import annotations

import math

import polars as pl

_CS_DENOM = 3.0 - 2.0 * math.sqrt(2.0)


def corwin_schultz_daily(daily: pl.DataFrame) -> pl.DataFrame:
    """(ticker, bar_date, high, low) -> (ticker, bar_date, cs_spread).

    Corwin & Schultz (2012): from two consecutive days' high/low ranges.
    beta  = sum over both days of ln(H/L)^2
    gamma = ln(H2/L2)^2 over the two-day combined range
    alpha = (sqrt(2*beta) - sqrt(beta)) / (3 - 2*sqrt(2)) - sqrt(gamma / (3 - 2*sqrt(2)))
    S     = 2 * (e^alpha - 1) / (1 + e^alpha), floored at 0 (negative estimates
            are set to zero per the paper's practice).
    The estimate for day D uses days (D-1, D); day D's row carries it.
    """
    if daily.is_empty():
        return pl.DataFrame(
            schema={"ticker": pl.Utf8, "bar_date": pl.Date, "cs_spread": pl.Float64}
        )
    df = daily.sort(["ticker", "bar_date"]).with_columns(
        pl.col("high").log().alias("_lh"),
        pl.col("low").log().alias("_ll"),
    )
    df = df.with_columns(
        ((pl.col("_lh") - pl.col("_ll")) ** 2).alias("_hl2"),
        pl.col("_lh").shift(1).over("ticker").alias("_lh_prev"),
        pl.col("_ll").shift(1).over("ticker").alias("_ll_prev"),
    )
    df = df.with_columns(
        (pl.col("_hl2") + (pl.col("_lh_prev") - pl.col("_ll_prev")) ** 2).alias("_beta"),
        (
            (pl.max_horizontal("_lh", "_lh_prev") - pl.min_horizontal("_ll", "_ll_prev")) ** 2
        ).alias("_gamma"),
    )
    df = df.with_columns(
        (
            ((2.0 * pl.col("_beta")).sqrt() - pl.col("_beta").sqrt()) / _CS_DENOM
            - (pl.col("_gamma") / _CS_DENOM).sqrt()
        ).alias("_alpha")
    )
    df = df.with_columns(
        (2.0 * (pl.col("_alpha").exp() - 1.0) / (1.0 + pl.col("_alpha").exp()))
        .clip(lower_bound=0.0)
        .alias("cs_spread")
    )
    return df.select("ticker", "bar_date", "cs_spread").drop_nulls()


def hybrid_intraday(
    minutes: pl.DataFrame, quote_bars: pl.DataFrame, cs_daily: pl.DataFrame
) -> pl.DataFrame:
    """Join minute bars x BBO; CS fallback for quoteless minutes.

    minutes:    (ticker, minute_ts, ..., close, volume)
    quote_bars: (ticker, minute_ts, bid_close, ask_close, ...)
    cs_daily:   (ticker, bar_date, cs_spread)

    Returns (ticker, minute_ts, bid, ask, volume, derivation).
    """
    if minutes.is_empty():
        return pl.DataFrame(
            schema={
                "ticker": pl.Utf8, "minute_ts": pl.Datetime("us", "UTC"),
                "bid": pl.Float64, "ask": pl.Float64, "volume": pl.Float64,
                "derivation": pl.Utf8,
            }
        )
    joined = minutes.join(
        quote_bars.select("ticker", "minute_ts", "bid_close", "ask_close"),
        on=["ticker", "minute_ts"], how="left",
    ).with_columns(
        # the bar's exchange-local session date keys the CS fallback
        pl.col("minute_ts").dt.convert_time_zone("America/New_York").dt.date()
        .alias("bar_date")
    ).join(cs_daily, on=["ticker", "bar_date"], how="left")

    has_bbo = pl.col("bid_close").is_not_null() & pl.col("ask_close").is_not_null()
    cs_half = (pl.col("cs_spread").fill_null(0.0) / 2.0).clip(lower_bound=0.0)
    return joined.with_columns(
        pl.when(has_bbo).then(pl.col("bid_close"))
        .otherwise(pl.col("close") * (1.0 - cs_half)).alias("bid"),
        pl.when(has_bbo).then(pl.col("ask_close"))
        .otherwise(pl.col("close") * (1.0 + cs_half)).alias("ask"),
        pl.when(has_bbo).then(pl.lit("iex_bbo")).otherwise(pl.lit("corwin_schultz"))
        .alias("derivation"),
    ).select("ticker", "minute_ts", "bid", "ask", "volume", "derivation").sort(
        ["ticker", "minute_ts"]
    )
