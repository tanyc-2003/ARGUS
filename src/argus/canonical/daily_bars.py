"""bars_daily canonical builder (M1: single-source Stooq spine, honestly graded).

Rows enter graded 'degraded' + single_source=True until cross-source voting
exists (M3) — the dashboard can filter on the tag; nothing pretends to be
better than it is (v4 Principle 9).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl

from argus.canonical import scd2
from argus.core.hashing import canonical_hash

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

KEY_COLS = ["ticker", "bar_date"]
VALUE_COLS = ["open", "high", "low", "close", "volume", "source_set", "grade", "single_source"]


def bar_knowledge_time(frame: pl.DataFrame) -> pl.DataFrame:
    """Vectorized clocks.asof_knowledge_time: 23:59:59 exchange-local on bar_date, in UTC.

    23:59:59 is never DST-ambiguous, so the conversion is total. A unit test
    pins this against the scalar implementation in core/clocks.py.
    """
    return frame.with_columns(
        (pl.col("bar_date").cast(pl.Datetime("us")) + pl.duration(hours=23, minutes=59, seconds=59))
        .dt.replace_time_zone("America/New_York")
        .dt.convert_time_zone("UTC")
        .alias("knowledge_time")
    )


def row_hashes(frame: pl.DataFrame) -> pl.DataFrame:
    """Canonical per-bar content hash.

    Covers grade/source columns when present, not just prices: an upgraded
    belief (single-source -> confirmed) is a real SCD-2 revision even when the
    price didn't move.
    """
    hash_cols = [c for c in ["open", "high", "low", "close", "volume",
                             "source_set", "grade", "single_source"] if c in frame.columns]
    hashes = [
        canonical_hash({c: r[c] for c in hash_cols})
        for r in frame.select(hash_cols).iter_rows(named=True)
    ]
    return frame.with_columns(pl.Series("payload_hash", hashes, dtype=pl.Utf8))


def upsert_bars(
    conn: duckdb.DuckDBPyConnection,
    raw_bars: pl.DataFrame,
    *,
    source_set: str = "stooq",
    grade: str = "degraded",
    single_source: bool = True,
    revision_knowledge: datetime | None = None,
) -> dict[str, int]:
    """SCD-2 upsert of raw bars (ticker, bar_date, open..volume) into bars_daily.

    revision_knowledge (detection time) must be passed by incremental feeds so
    corrections become knowable when detected, not backdated to the bar date.
    """
    if raw_bars.is_empty():
        return {"revised": 0, "inserted": 0, "unchanged": 0}
    incoming = raw_bars.select("ticker", "bar_date", "open", "high", "low", "close", "volume")
    incoming = incoming.with_columns(
        pl.lit(source_set).alias("source_set"),
        pl.lit(grade).alias("grade"),
        pl.lit(single_source).alias("single_source"),
    )
    incoming = bar_knowledge_time(row_hashes(incoming))
    return scd2.upsert(
        conn, "bars_daily", KEY_COLS, VALUE_COLS, incoming,
        revision_knowledge=revision_knowledge,
    )


def bars_asof(
    conn: duckdb.DuckDBPyConnection, ticker: str, bar_date: object, as_of: datetime
) -> tuple[float, int] | None:
    """(close, revision_seq) as believed at knowledge time `as_of` — the time machine."""
    row = conn.execute(
        """
        SELECT close, revision_seq FROM bars_daily
        WHERE ticker = ? AND bar_date = ?
          AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
        """,
        [ticker.upper(), bar_date, as_of, as_of],
    ).fetchone()
    return (float(row[0]), int(row[1])) if row else None
