"""Atomic publish: materialize the serving views into a sealed copy.

DuckDB is single-writer, so the dashboard must never read the build database.
The publish job ATTACHes a temp file from the build connection, materializes
each vw_mad_* view as a TABLE (a nightly snapshot — tables are the contract,
the "vw_" names are preserved), runs the contract gate against the sealed
file, then atomically replaces `argus_serving.duckdb`. A crashed run leaves
yesterday's good copy in place; a contract violation blocks the swap.
"""

from __future__ import annotations

import os
import time

from argus.core.clocks import utc_now
from argus.ops.jobs import JobContext, JobResult
from argus.serving import contracts

_REPLACE_ATTEMPTS = 4
_REPLACE_DELAY_S = 2.0


def publish(ctx: JobContext) -> JobResult:
    settings = ctx.settings
    tmp = settings.serving_db_path.with_name(settings.serving_db_path.name + ".tmp")
    if tmp.exists():
        tmp.unlink()

    tmp_sql = str(tmp).replace("\\", "/").replace("'", "''")
    conn = ctx.conn
    conn.execute(f"ATTACH '{tmp_sql}' AS sv")
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE sv.{contracts.DAILY_OHLCV} AS "
            f"SELECT * FROM {contracts.DAILY_OHLCV} ORDER BY ticker, effective_date"
        )
        conn.execute(
            f"CREATE OR REPLACE TABLE sv.{contracts.DELISTED} AS "
            f"SELECT * FROM {contracts.DELISTED} ORDER BY ticker, termination_date"
        )
        conn.execute(
            f"CREATE OR REPLACE TABLE sv.{contracts.COVERAGE} AS "
            f"SELECT * FROM {contracts.COVERAGE} ORDER BY audit_window"
        )
        version_row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
        conn.execute(
            "CREATE OR REPLACE TABLE sv.serving_meta AS "
            "SELECT ? AS sealed_trade_date, ? AS published_at, ? AS argus_schema_version",
            [ctx.trade_date, utc_now(), version_row[0] if version_row else None],
        )
    finally:
        conn.execute("DETACH sv")

    # gates BEFORE the swap — any contract violation leaves yesterday's copy live
    rows = contracts.assert_daily_ohlcv(tmp)
    n_delisted = contracts.assert_delisted(tmp)
    contracts.assert_coverage(tmp)

    # the dashboard may hold the previous copy open (read-only); Windows can
    # refuse the replace briefly — bounded retry, then fail loudly.
    for attempt in range(_REPLACE_ATTEMPTS):
        try:
            os.replace(tmp, settings.serving_db_path)
            break
        except PermissionError:
            if attempt == _REPLACE_ATTEMPTS - 1:
                raise
            time.sleep(_REPLACE_DELAY_S)

    return JobResult(
        rows_out=rows,
        detail=(
            f"published {rows} daily rows, {n_delisted} delisted -> "
            f"{settings.serving_db_path.name}"
        ),
    )
