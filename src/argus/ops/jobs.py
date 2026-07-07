"""Job bookkeeping: the `job_runs` table and the run_job wrapper.

Idempotency key = (job_name, trade_date): a job that already has an 'ok' row
for the trade date is skipped, which is what makes double-fired schedules and
catch-up runs harmless.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any

import structlog

from argus.core.clocks import utc_now
from argus.ops import dlq
from argus.ops.errors import ArgusError, BudgetExhausted, ErrorClass, SourceDown
from argus.settings import Settings

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


@dataclass
class JobContext:
    settings: Settings
    conn: duckdb.DuckDBPyConnection
    trade_date: date
    log: structlog.stdlib.BoundLogger
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobResult:
    rows_out: int = 0
    budget_used: int = 0
    detail: str = ""


# statuses: ok | failed | budget_exhausted | skipped_already_done | skipped_source_down
JobFn = Callable[[JobContext], JobResult]


def already_ok(conn: duckdb.DuckDBPyConnection, job_name: str, trade_date: date) -> bool:
    row = conn.execute(
        "SELECT 1 FROM job_runs WHERE job_name = ? AND trade_date = ? AND status = 'ok' LIMIT 1",
        [job_name, trade_date],
    ).fetchone()
    return row is not None


def _record(
    conn: duckdb.DuckDBPyConnection,
    job_name: str,
    trade_date: date,
    run_id: str,
    status: str,
    *,
    started_at: Any,
    error_class: str | None = None,
    detail: str = "",
    rows_out: int = 0,
    budget_used: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO job_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [job_name, trade_date, run_id, status, started_at, utc_now(), error_class,
         detail[:4000], rows_out, budget_used],
    )


def run_job(ctx: JobContext, job_name: str, fn: JobFn, *, force: bool = False) -> str:
    """Execute one job with full bookkeeping; returns the recorded status."""
    log = ctx.log.bind(job=job_name, trade_date=str(ctx.trade_date))
    if not force and already_ok(ctx.conn, job_name, ctx.trade_date):
        log.info("job_skipped_already_done")
        return "skipped_already_done"

    run_id = uuid.uuid4().hex[:12]
    started_at = utc_now()
    log.info("job_started", run_id=run_id)
    try:
        result = fn(ctx)
    except BudgetExhausted as exc:
        # normal terminal state: resume tomorrow (v4 §7.2)
        _record(ctx.conn, job_name, ctx.trade_date, run_id, "budget_exhausted",
                started_at=started_at, error_class=str(exc.error_class), detail=str(exc))
        log.warning("job_budget_exhausted", detail=str(exc))
        return "budget_exhausted"
    except SourceDown as exc:
        _record(ctx.conn, job_name, ctx.trade_date, run_id, "skipped_source_down",
                started_at=started_at, error_class=str(exc.error_class), detail=str(exc))
        log.warning("job_skipped_source_down", detail=str(exc))
        return "skipped_source_down"
    except ArgusError as exc:
        _record(ctx.conn, job_name, ctx.trade_date, run_id, "failed",
                started_at=started_at, error_class=str(exc.error_class), detail=str(exc))
        dlq.push(ctx.conn, job_name=job_name, error_class=exc.error_class,
                 detail=str(exc), source=exc.source)
        log.error("job_failed", error_class=str(exc.error_class), detail=str(exc))
        return "failed"
    except Exception as exc:
        _record(ctx.conn, job_name, ctx.trade_date, run_id, "failed",
                started_at=started_at, error_class=str(ErrorClass.UNKNOWN), detail=repr(exc))
        dlq.push(ctx.conn, job_name=job_name, error_class=ErrorClass.UNKNOWN, detail=repr(exc))
        log.error("job_failed", error_class="unknown", detail=repr(exc))
        return "failed"

    _record(ctx.conn, job_name, ctx.trade_date, run_id, "ok",
            started_at=started_at, detail=result.detail,
            rows_out=result.rows_out, budget_used=result.budget_used)
    log.info("job_ok", rows_out=result.rows_out, budget_used=result.budget_used,
             detail=result.detail)
    return "ok"
