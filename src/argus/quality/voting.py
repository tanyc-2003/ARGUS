"""Cross-source voting — the free-data defense (v4 §6, Principle 9).

Nothing enters the canonical layer from a single free source unconfirmed
without being tagged as such. The vote runs over the LATEST observation per
(source, ticker, bar_date) in the L2 event store and produces the full
canonical candidate state — which makes it double as the replay function:
`argus rebuild` is literally "re-run the vote over L2".

Rules (v4 §6):
  * close agreement: relative diff <= 0.1% (midpoint denominator)
  * volume agreement: <= 5%, and IEX volume is STRUCTURALLY excluded — an
    alpaca_iex row can never contribute a served volume
  * >= 2 sources agreeing -> verdict 'confirmed', grade 'good'
  * exactly one source -> 'single_source', grade 'degraded' (served, tagged)
  * a bar only IEX saw -> 'alpaca_only_skipped', NOT canonized (an IEX-only
    print with IEX-only volume would poison downstream volume features)
  * >= 2 sources, no agreeing pair -> 'conflict', grade 'quarantined'
    (kept, never served) + dead letter
"""

from __future__ import annotations

import polars as pl

from argus.events import schemas as event_schemas
from argus.events import store as event_store
from argus.settings import Settings

CLOSE_TOL = 0.001  # ±0.1%
VOLUME_TOL = 0.05  # ±5%

# priority when several sources agree: consolidated-volume vendors first
_PRIORITY = ["yfinance", "stooq", "alpaca_iex"]


def latest_observations(settings: Settings) -> pl.DataFrame:
    """Latest daily-bar observation per (source, ticker, bar_date) from L2.

    Deterministic: ties on knowledge_time break on written_at, then
    payload_hash — replay yields the same state every time.
    """
    lf = event_store.scan(settings, event_schemas.BAR_EVENTS)
    return (
        lf.filter(pl.col("interval") == "1d")
        .sort(["knowledge_time", "written_at", "payload_hash"])
        .group_by(["source", "ticker", "bar_date"], maintain_order=True)
        .last()
        .collect()
    )


def _rel_close(a: str, b: str) -> pl.Expr:
    mid = (pl.col(a).abs() + pl.col(b).abs()) / 2.0
    return ((pl.col(a) - pl.col(b)).abs() / pl.max_horizontal(mid, pl.lit(1e-12))).le(CLOSE_TOL)


def vote_bars(obs: pl.DataFrame) -> pl.DataFrame:
    """Vote per (ticker, bar_date). Returns one row per key with the chosen
    OHLCV, verdict, grade, chosen_source, per-source closes, volume_agrees."""
    if obs.is_empty():
        return pl.DataFrame()

    per_source: dict[str, pl.DataFrame] = {}
    for src in _PRIORITY:
        per_source[src] = (
            obs.filter(pl.col("source") == src)
            .select(
                "ticker", "bar_date",
                pl.col("open").alias(f"open_{src}"),
                pl.col("high").alias(f"high_{src}"),
                pl.col("low").alias(f"low_{src}"),
                pl.col("close").alias(f"close_{src}"),
                pl.col("volume").alias(f"volume_{src}"),
            )
        )
    wide = per_source["yfinance"]
    for src in ["stooq", "alpaca_iex"]:
        wide = wide.join(per_source[src], on=["ticker", "bar_date"], how="full",
                         coalesce=True)

    wide = wide.with_columns(
        pl.col("close_yfinance").is_not_null().alias("has_yf"),
        pl.col("close_stooq").is_not_null().alias("has_st"),
        pl.col("close_alpaca_iex").is_not_null().alias("has_al"),
    ).with_columns(
        (pl.col("has_yf").cast(pl.Int32) + pl.col("has_st").cast(pl.Int32)
         + pl.col("has_al").cast(pl.Int32)).alias("n_sources"),
        (pl.col("has_yf") & pl.col("has_st")
         & _rel_close("close_yfinance", "close_stooq")).alias("agree_yf_st"),
        (pl.col("has_yf") & pl.col("has_al")
         & _rel_close("close_yfinance", "close_alpaca_iex")).alias("agree_yf_al"),
        (pl.col("has_st") & pl.col("has_al")
         & _rel_close("close_stooq", "close_alpaca_iex")).alias("agree_st_al"),
    )

    yf_in_pair = pl.col("agree_yf_st") | pl.col("agree_yf_al")
    st_in_pair = pl.col("agree_yf_st") | pl.col("agree_st_al")
    al_in_pair = pl.col("agree_yf_al") | pl.col("agree_st_al")
    any_pair = yf_in_pair | st_in_pair | al_in_pair

    verdict = (
        pl.when(pl.col("n_sources") == 0)
        .then(pl.lit("empty"))
        .when((pl.col("n_sources") == 1) & pl.col("has_al"))
        .then(pl.lit("alpaca_only_skipped"))
        .when(pl.col("n_sources") == 1)
        .then(pl.lit("single_source"))
        .when(any_pair)
        .then(pl.lit("confirmed"))
        .otherwise(pl.lit("conflict"))
    )
    chosen = (
        pl.when((pl.col("n_sources") == 1) & pl.col("has_yf")).then(pl.lit("yfinance"))
        .when((pl.col("n_sources") == 1) & pl.col("has_st")).then(pl.lit("stooq"))
        .when(yf_in_pair).then(pl.lit("yfinance"))
        .when(st_in_pair).then(pl.lit("stooq"))
        .when(al_in_pair).then(pl.lit("alpaca_iex"))
        # conflict rows keep a deterministic record source (never served)
        .when(pl.col("has_yf")).then(pl.lit("yfinance"))
        .when(pl.col("has_st")).then(pl.lit("stooq"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )
    wide = wide.with_columns(verdict.alias("verdict"), chosen.alias("chosen_source"))

    def pick(field: str) -> pl.Expr:
        return (
            pl.when(pl.col("chosen_source") == "yfinance").then(pl.col(f"{field}_yfinance"))
            .when(pl.col("chosen_source") == "stooq").then(pl.col(f"{field}_stooq"))
            .when(pl.col("chosen_source") == "alpaca_iex").then(pl.col(f"{field}_alpaca_iex"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias(field)
        )

    # volume: IEX structurally excluded — fall back across consolidated vendors only
    volume_expr = (
        pl.when(pl.col("chosen_source") == "yfinance")
        .then(pl.coalesce(pl.col("volume_yfinance"), pl.col("volume_stooq")))
        .otherwise(pl.coalesce(pl.col("volume_stooq"), pl.col("volume_yfinance")))
        .alias("volume")
    )
    vol_mid = (pl.col("volume_yfinance").abs() + pl.col("volume_stooq").abs()) / 2.0
    volume_agrees = (
        pl.when(pl.col("volume_yfinance").is_null() | pl.col("volume_stooq").is_null())
        .then(pl.lit(None, dtype=pl.Boolean))
        .otherwise(
            ((pl.col("volume_yfinance") - pl.col("volume_stooq")).abs()
             / pl.max_horizontal(vol_mid, pl.lit(1.0))).le(VOLUME_TOL)
        )
        .alias("volume_agrees")
    )

    out = wide.with_columns(
        pick("open"), pick("high"), pick("low"), pick("close"), volume_expr, volume_agrees,
    ).with_columns(
        pl.when(pl.col("verdict") == "confirmed")
        .then(
            # a failed volume vote degrades the row without quarantining prices
            pl.when(pl.col("volume_agrees").not_().fill_null(False))
            .then(pl.lit("degraded"))
            .otherwise(pl.lit("good"))
        )
        .when(pl.col("verdict") == "single_source").then(pl.lit("degraded"))
        .when(pl.col("verdict") == "conflict").then(pl.lit("quarantined"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("grade"),
        (pl.col("verdict") == "single_source").alias("single_source"),
    )

    # source_set: comma-joined present sources (audit trail on the canonical row)
    out = out.with_columns(
        pl.concat_list(
            pl.when(pl.col("has_yf")).then(pl.lit("yfinance")).otherwise(pl.lit(None)),
            pl.when(pl.col("has_st")).then(pl.lit("stooq")).otherwise(pl.lit(None)),
            pl.when(pl.col("has_al")).then(pl.lit("alpaca_iex")).otherwise(pl.lit(None)),
        ).list.drop_nulls().list.join(",").alias("source_set")
    )
    return out.filter(~pl.col("verdict").is_in(["empty", "alpaca_only_skipped"])).select(
        "ticker", "bar_date", "open", "high", "low", "close", "volume",
        "source_set", "grade", "single_source", "verdict", "chosen_source",
        "n_sources", "close_stooq", "close_yfinance",
        pl.col("close_alpaca_iex").alias("close_alpaca"), "volume_agrees",
    ).sort(["ticker", "bar_date"])


def skipped_alpaca_only(obs: pl.DataFrame) -> int:
    """Count of bars only IEX saw (reported, never canonized)."""
    if obs.is_empty():
        return 0
    counts = obs.group_by(["ticker", "bar_date"]).agg(
        pl.col("source").n_unique().alias("n"),
        pl.col("source").first().alias("only"),
    )
    return counts.filter((pl.col("n") == 1) & (pl.col("only") == "alpaca_iex")).height
