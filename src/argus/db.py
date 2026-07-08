"""DuckDB connection management and idempotent schema migrations.

`argus.duckdb` is the build/writer database. It is a disposable projection —
the Parquet landing zone (L0) and event store (L2) are the system of record,
and later milestones add `argus rebuild` to regenerate this file from them.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from argus.core.clocks import utc_now

# Ordered, append-only migration list. Never edit an entry after it has shipped;
# add a new one. Version = 1-based index.
MIGRATIONS: list[str] = [
    # v1 — M0 backbone tables
    """
    CREATE SEQUENCE IF NOT EXISTS dead_letter_seq;

    CREATE TABLE IF NOT EXISTS landing_manifest (
        dataset        VARCHAR NOT NULL,
        source         VARCHAR NOT NULL,
        request_key    VARCHAR NOT NULL,
        payload_hash   VARCHAR NOT NULL,
        path           VARCHAR NOT NULL,
        content_type   VARCHAR,
        n_bytes        BIGINT,
        partition_date DATE,
        knowledge_time TIMESTAMPTZ NOT NULL,
        written_at     TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (dataset, source, request_key)
    );

    CREATE TABLE IF NOT EXISTS job_runs (
        job_name    VARCHAR NOT NULL,
        trade_date  DATE NOT NULL,
        run_id      VARCHAR NOT NULL,
        status      VARCHAR NOT NULL,
        started_at  TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        error_class VARCHAR,
        detail      VARCHAR,
        rows_out    BIGINT,
        budget_used BIGINT,
        PRIMARY KEY (job_name, trade_date, run_id)
    );

    CREATE TABLE IF NOT EXISTS dead_letter (
        id           BIGINT PRIMARY KEY DEFAULT nextval('dead_letter_seq'),
        job_name     VARCHAR NOT NULL,
        source       VARCHAR,
        request_key  VARCHAR,
        payload_path VARCHAR,
        error_class  VARCHAR NOT NULL,
        detail       VARCHAR,
        first_seen   TIMESTAMPTZ NOT NULL,
        retry_count  INTEGER NOT NULL DEFAULT 0,
        resolved_at  TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS source_health (
        source               VARCHAR PRIMARY KEY,
        state                VARCHAR NOT NULL,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_ok              TIMESTAMPTZ,
        last_failure         TIMESTAMPTZ,
        opened_at            TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS market_sessions (
        exchange     VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        open_utc     TIMESTAMPTZ NOT NULL,
        close_utc    TIMESTAMPTZ NOT NULL,
        is_half_day  BOOLEAN NOT NULL,
        PRIMARY KEY (exchange, session_date)
    );
    """,
]


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def migrate(conn: duckdb.DuckDBPyConnection) -> int:
    """Apply pending migrations; returns the schema version now in effect."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)"
    )
    row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
    current = int(row[0]) if row else 0
    for version, sql in enumerate(MIGRATIONS, start=1):
        if version > current:
            conn.execute(sql)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?, ?)",
                [version, utc_now()],
            )
    return len(MIGRATIONS)


def open_migrated(db_path: Path) -> duckdb.DuckDBPyConnection:
    conn = connect(db_path)
    migrate(conn)
    return conn
