"""corporate_actions canonical builder.

M1 has a single CA source (Polygon free tier), so every action carries
confidence='single_source' — never silently presented as confirmed. M3's
cross-check against Yahoo's adjusted/raw ratio upgrades confidence.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from argus.canonical import scd2

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

KEY_COLS = ["ticker", "action_type", "ex_date"]
VALUE_COLS = ["ratio", "cash_amount", "declared_date", "confidence", "source_set"]


def upsert_actions(
    conn: duckdb.DuckDBPyConnection,
    action_events: pl.DataFrame,
    *,
    confidence: str = "single_source",
) -> dict[str, int]:
    """SCD-2 upsert of normalized action events into corporate_actions."""
    if action_events.is_empty():
        return {"revised": 0, "inserted": 0, "unchanged": 0}
    incoming = action_events.select(
        "ticker", "action_type", "ex_date", "ratio", "cash_amount", "declared_date",
        "payload_hash", "knowledge_time",
    ).with_columns(
        pl.lit(confidence).alias("confidence"),
        pl.lit("polygon").alias("source_set"),
    )
    return scd2.upsert(conn, "corporate_actions", KEY_COLS, VALUE_COLS, incoming)


def current_splits(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Current split events as (ticker, ex_date, ratio) — the reversal input."""
    df = conn.execute(
        """
        SELECT ticker, ex_date, ratio FROM corporate_actions
        WHERE is_current AND action_type = 'split' AND ratio IS NOT NULL AND ratio > 0
        ORDER BY ticker, ex_date
        """
    ).pl()
    return df.with_columns(pl.col("ex_date").cast(pl.Date), pl.col("ratio").cast(pl.Float64))
