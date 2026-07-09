"""Rebuild the canonical DuckDB state from the L2 event store.

The Parquet event store is the system of record; the DuckDB file is a
disposable projection (v4 §4.1). Rebuilding = wipe canonical tables, replay
corporate actions from action_events, then run the same vote-and-seal
projection the nightly uses. Deterministic by construction: observations are
ordered by (knowledge_time, written_at, payload_hash) and fresh inserts carry
world-knowledge stamps derived from the data itself.

Note: the rebuilt state is the CURRENT belief. Full revision-by-revision
history remains replayable from L2 (events carry every observation), but the
rebuilt tables start at revision_seq 1 — record this in ops notes, not a bug.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl
import structlog

from argus.canonical import actions_store
from argus.events import schemas as event_schemas
from argus.events import store as event_store
from argus.ops.jobs import JobContext, JobResult
from argus.orchestration.build_jobs import vote_and_seal
from argus.serving.publish import publish
from argus.settings import Settings

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

CANONICAL_TABLES = ["bars_daily", "corporate_actions", "vote_results"]


def replay_actions(settings: Settings, conn: duckdb.DuckDBPyConnection) -> int:
    """Latest action observation per (ticker, action_type, ex_date) -> corporate_actions."""
    events = event_store.scan(settings, event_schemas.ACTION_EVENTS).collect()
    if events.is_empty():
        return 0
    latest = (
        events.sort(["knowledge_time", "written_at", "payload_hash"])
        .unique(subset=["ticker", "action_type", "ex_date"], keep="last")
        .sort(["ticker", "ex_date"])
    )
    counts = actions_store.upsert_actions(conn, latest)
    return counts["inserted"]


def rebuild_canonical(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    trade_date: date,
    *,
    do_publish: bool = True,
) -> dict[str, object]:
    """Wipe + replay. Returns a summary dict for the CLI/tests."""
    log = structlog.get_logger("argus").bind(op="rebuild")
    for table in CANONICAL_TABLES:
        conn.execute(f"DELETE FROM {table}")
    log.info("canonical_wiped", tables=CANONICAL_TABLES)

    n_actions = replay_actions(settings, conn)
    ctx = JobContext(settings=settings, conn=conn, trade_date=trade_date, log=log)
    seal: JobResult = vote_and_seal(ctx)
    pub: JobResult | None = publish(ctx) if do_publish else None

    bars_row = conn.execute("SELECT COUNT(*) FROM bars_daily WHERE is_current").fetchone()
    summary = {
        "actions_replayed": n_actions,
        "bars_current": int(bars_row[0]) if bars_row else 0,
        "seal_detail": seal.detail,
        "published_rows": pub.rows_out if pub else None,
    }
    log.info("rebuild_done", **summary)
    return summary


def canonical_state_fingerprint(conn: duckdb.DuckDBPyConnection) -> str:
    """Deterministic hash of the rebuilt state's stable columns (test + drill aid).

    Excludes volatile columns (written_at-derived timestamps like voted_at);
    includes everything a consumer can observe through the serving views.
    """
    from argus.core.hashing import canonical_hash

    bars = pl.DataFrame(
        conn.execute(
            """
            SELECT ticker, bar_date, open, high, low, close, volume,
                   source_set, grade, single_source, payload_hash, knowledge_time
            FROM bars_daily WHERE is_current
            ORDER BY ticker, bar_date
            """
        ).pl()
    )
    actions = pl.DataFrame(
        conn.execute(
            """
            SELECT ticker, action_type, ex_date, ratio, cash_amount, confidence,
                   payload_hash, knowledge_time
            FROM corporate_actions WHERE is_current
            ORDER BY ticker, action_type, ex_date
            """
        ).pl()
    )
    return canonical_hash({"bars": bars.rows(), "actions": actions.rows()})
