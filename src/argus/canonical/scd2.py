"""Generic set-based SCD-2 upsert — the ONLY mutation path into canonical tables.

Semantics per natural key:
  * no current row            -> insert version 1
  * current row, same hash    -> no-op (idempotent re-runs)
  * current row, differs      -> close it (valid_to = knowledge_time) and insert
                                 the next revision_seq. Never an in-place UPDATE
                                 of values, so as-of queries can time-travel.

The incoming frame must contain the natural-key columns, the value columns in
DDL order, plus payload_hash and knowledge_time. SCD-2 bookkeeping columns are
derived here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


def upsert(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    key_cols: list[str],
    value_cols: list[str],
    incoming: pl.DataFrame,
) -> dict[str, int]:
    """Set-based SCD-2 upsert. Returns {'revised': n, 'inserted': n, 'unchanged': n}."""
    required = [*key_cols, *value_cols, "payload_hash", "knowledge_time"]
    missing = set(required) - set(incoming.columns)
    if missing:
        raise ValueError(f"{table}: incoming frame missing {sorted(missing)}")
    if incoming.is_empty():
        return {"revised": 0, "inserted": 0, "unchanged": 0}

    dedup = incoming.unique(subset=key_cols, keep="last")
    conn.register("scd2_incoming", dedup.to_arrow())
    key_join = " AND ".join(f"t.{c} = i.{c}" for c in key_cols)
    try:
        unchanged_row = conn.execute(
            f"""
            SELECT COUNT(*) FROM scd2_incoming i
            JOIN {table} t ON {key_join}
            WHERE t.is_current AND t.payload_hash = i.payload_hash
            """
        ).fetchone()
        assert unchanged_row is not None
        unchanged = unchanged_row[0]

        revised = conn.execute(
            f"""
            UPDATE {table} AS t
            SET valid_to = i.knowledge_time, is_current = FALSE
            FROM scd2_incoming i
            WHERE {key_join} AND t.is_current AND t.payload_hash <> i.payload_hash
            RETURNING 1
            """
        ).fetchall()

        cols = ", ".join(required)
        seq_key = " AND ".join(f"b.{c} = i.{c}" for c in key_cols)
        inserted = conn.execute(
            f"""
            INSERT INTO {table} ({cols}, valid_from, valid_to, is_current, revision_seq)
            SELECT {", ".join("i." + c for c in required)},
                   i.knowledge_time, NULL, TRUE,
                   COALESCE((SELECT MAX(b.revision_seq) FROM {table} b WHERE {seq_key}), 0) + 1
            FROM scd2_incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM {table} t WHERE {key_join} AND t.is_current
            )
            RETURNING 1
            """
        ).fetchall()
    finally:
        conn.unregister("scd2_incoming")

    return {"revised": len(revised), "inserted": len(inserted), "unchanged": int(unchanged)}
