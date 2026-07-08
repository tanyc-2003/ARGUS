"""Generic set-based SCD-2 upsert — the ONLY mutation path into canonical tables.

Semantics per natural key:
  * no history at all         -> insert version 1 with the row's OWN
                                 knowledge_time (a backfilled world fact keeps
                                 its historical knowledge stamp)
  * current row, same hash    -> no-op (idempotent re-runs)
  * current row, differs      -> a REVISION: close the current version and open
                                 the next one at `revision_knowledge` (detection
                                 time). A correction only becomes knowable when
                                 it is detected — stamping it at the bar's own
                                 date would rewrite the past (look-ahead).

Never an in-place UPDATE of values, so as-of queries can time-travel.

The incoming frame must contain the natural-key columns, the value columns,
plus payload_hash and knowledge_time. SCD-2 bookkeeping columns derive here.
"""

from __future__ import annotations

from datetime import datetime
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
    *,
    revision_knowledge: datetime | None = None,
) -> dict[str, int]:
    """Set-based SCD-2 upsert. Returns {'revised': n, 'inserted': n, 'unchanged': n}.

    revision_knowledge: knowledge_time for REVISIONS of existing keys (detection
    time). Defaults to each row's own knowledge_time when not given — correct
    only for feeds whose knowledge_time already is the observation time.
    """
    required = [*key_cols, *value_cols, "payload_hash", "knowledge_time"]
    missing = set(required) - set(incoming.columns)
    if missing:
        raise ValueError(f"{table}: incoming frame missing {sorted(missing)}")
    if incoming.is_empty():
        return {"revised": 0, "inserted": 0, "unchanged": 0}

    dedup = incoming.unique(subset=key_cols, keep="last")
    conn.register("scd2_incoming", dedup.to_arrow())
    key_join = " AND ".join(f"t.{c} = i.{c}" for c in key_cols)
    seq_key = " AND ".join(f"b.{c} = i.{c}" for c in key_cols)
    cols_no_kt = [*key_cols, *value_cols, "payload_hash"]
    cols_sql = ", ".join([*cols_no_kt, "knowledge_time"])
    i_cols = ", ".join("i." + c for c in cols_no_kt)
    next_seq = f"COALESCE((SELECT MAX(b.revision_seq) FROM {table} b WHERE {seq_key}), 0) + 1"
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

        # a revision becomes knowable at detection time, not at the fact's own date
        rev_kt_sql = "?" if revision_knowledge is not None else "i.knowledge_time"
        rev_params = [revision_knowledge] if revision_knowledge is not None else []

        revised = conn.execute(
            f"""
            UPDATE {table} AS t
            SET valid_to = {rev_kt_sql}, is_current = FALSE
            FROM scd2_incoming i
            WHERE {key_join} AND t.is_current AND t.payload_hash <> i.payload_hash
            RETURNING 1
            """,
            rev_params,
        ).fetchall()

        # revised keys: history exists but no current row (just closed above)
        inserted_rev = conn.execute(
            f"""
            INSERT INTO {table} ({cols_sql}, valid_from, valid_to, is_current, revision_seq)
            SELECT {i_cols}, {rev_kt_sql}, {rev_kt_sql}, NULL, TRUE, {next_seq}
            FROM scd2_incoming i
            WHERE EXISTS (SELECT 1 FROM {table} t WHERE {key_join})
              AND NOT EXISTS (SELECT 1 FROM {table} t WHERE {key_join} AND t.is_current)
            RETURNING 1
            """,
            [*rev_params, *rev_params],
        ).fetchall()

        # brand-new keys: keep the row's own (possibly historical) knowledge stamp
        inserted_new = conn.execute(
            f"""
            INSERT INTO {table} ({cols_sql}, valid_from, valid_to, is_current, revision_seq)
            SELECT {i_cols}, i.knowledge_time, i.knowledge_time, NULL, TRUE, 1
            FROM scd2_incoming i
            WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {key_join})
            RETURNING 1
            """
        ).fetchall()
    finally:
        conn.unregister("scd2_incoming")

    return {
        "revised": len(revised),
        "inserted": len(inserted_rev) + len(inserted_new),
        "unchanged": int(unchanged),
    }
