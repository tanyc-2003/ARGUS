"""Per-source circuit breaker.

A source that keeps failing is skipped (`skipped_source_down`) rather than
failing the night: voting proceeds with the remaining sources and the gap is
disclosed, never silent (integration doc §4.4). The circuit re-closes after a
cooldown so a transient outage self-heals the next night.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from argus.core.clocks import utc_now

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

OPEN_AFTER_FAILURES = 3
COOLDOWN = timedelta(hours=20)  # shorter than one nightly cadence


def record_success(conn: duckdb.DuckDBPyConnection, source: str) -> None:
    conn.execute(
        """
        INSERT INTO source_health VALUES (?, 'closed', 0, ?, NULL, NULL)
        ON CONFLICT (source) DO UPDATE SET
            state = 'closed', consecutive_failures = 0, last_ok = excluded.last_ok,
            opened_at = NULL
        """,
        [source, utc_now()],
    )


def record_failure(conn: duckdb.DuckDBPyConnection, source: str) -> None:
    now = utc_now()
    row = conn.execute(
        "SELECT consecutive_failures FROM source_health WHERE source = ?", [source]
    ).fetchone()
    failures = (int(row[0]) if row else 0) + 1
    state = "open" if failures >= OPEN_AFTER_FAILURES else "closed"
    opened_at = now if state == "open" else None
    conn.execute(
        """
        INSERT INTO source_health VALUES (?, ?, ?, NULL, ?, ?)
        ON CONFLICT (source) DO UPDATE SET
            state = excluded.state,
            consecutive_failures = excluded.consecutive_failures,
            last_failure = excluded.last_failure,
            opened_at = excluded.opened_at
        """,
        [source, state, failures, now, opened_at],
    )


def is_open(conn: duckdb.DuckDBPyConnection, source: str) -> bool:
    """True while the circuit is open and the cooldown has not elapsed."""
    row = conn.execute(
        "SELECT state, opened_at FROM source_health WHERE source = ?", [source]
    ).fetchone()
    if row is None or row[0] != "open":
        return False
    opened_at = row[1]
    cooled_down = opened_at is not None and utc_now() - opened_at >= COOLDOWN
    return not cooled_down  # cooled down => half-open: allow the next attempt to probe
