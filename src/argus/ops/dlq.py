"""Dead-letter queue: every failure lands here with its raw context (kept principle)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from argus.core.clocks import utc_now
from argus.ops.errors import ErrorClass

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


def push(
    conn: duckdb.DuckDBPyConnection,
    *,
    job_name: str,
    error_class: ErrorClass,
    detail: str,
    source: str | None = None,
    request_key: str | None = None,
    payload_path: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO dead_letter
            (job_name, source, request_key, payload_path, error_class, detail, first_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [job_name, source, request_key, payload_path, str(error_class), detail[:4000], utc_now()],
    )


def list_open(conn: duckdb.DuckDBPyConnection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, job_name, source, request_key, error_class, detail, first_seen, retry_count
        FROM dead_letter WHERE resolved_at IS NULL
        ORDER BY first_seen DESC LIMIT ?
        """,
        [limit],
    ).fetchall()
    cols = ["id", "job_name", "source", "request_key", "error_class", "detail", "first_seen",
            "retry_count"]
    return [dict(zip(cols, r, strict=True)) for r in rows]


def resolve(conn: duckdb.DuckDBPyConnection, dlq_id: int) -> None:
    conn.execute(
        "UPDATE dead_letter SET resolved_at = ? WHERE id = ?",
        [utc_now(), dlq_id],
    )


def open_depth(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM dead_letter WHERE resolved_at IS NULL").fetchone()
    return int(row[0]) if row else 0
