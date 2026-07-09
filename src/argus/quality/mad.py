"""MAD outlier screen (v4 §6, k≈15): a bad tick from a lone vendor must not
poison the spine, while a real crash confirmed by two vendors must survive.

Flags a bar when its log-return deviates from the ticker's median return by
more than k times the median absolute deviation. Consequence is graded by
confidence: single-source flagged bars quarantine; confirmed bars keep their
grade (two vendors agreeing on a wild move is a market event, not an error).
"""

from __future__ import annotations

import polars as pl

K = 15.0
MIN_BARS = 30  # below this the estimate is noise — screen stays quiet


def apply_mad_screen(canonical: pl.DataFrame) -> pl.DataFrame:
    """Adds mad_flag and downgrades single-source outliers to quarantined."""
    if canonical.is_empty():
        return canonical.with_columns(pl.lit(False).alias("mad_flag"))

    scored = (
        canonical.sort(["ticker", "bar_date"])
        .with_columns(
            (pl.col("close").log() - pl.col("close").log().shift(1).over("ticker"))
            .alias("_r")
        )
        .with_columns(
            pl.col("_r").median().over("ticker").alias("_med"),
            (pl.col("_r") - pl.col("_r").median().over("ticker")).abs()
            .median().over("ticker").alias("_mad"),
            pl.col("_r").count().over("ticker").alias("_n"),
        )
        .with_columns(
            (
                (pl.col("_n") >= MIN_BARS)
                & (pl.col("_mad") > 0)
                & ((pl.col("_r") - pl.col("_med")).abs() > K * pl.col("_mad"))
            )
            .fill_null(False)
            .alias("mad_flag")
        )
    )
    return scored.with_columns(
        pl.when(pl.col("mad_flag") & pl.col("single_source"))
        .then(pl.lit("quarantined"))
        .otherwise(pl.col("grade"))
        .alias("grade")
    ).drop("_r", "_med", "_mad", "_n")
