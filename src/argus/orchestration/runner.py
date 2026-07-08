"""The nightly runner: calendar gate -> ordered jobs -> summary.

Idempotent per trade date; safe to double-fire (scheduler + catch-up trigger).
Exit code 0 unless a job recorded `failed`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb

from argus import db, logging_setup
from argus.core import calendars
from argus.core.clocks import utc_now
from argus.ops.jobs import JobContext, run_job
from argus.orchestration.nightly import JobSpec, build_registry
from argus.settings import Settings

SESSIONS_BACK = 60  # market_sessions refresh window, days
SESSIONS_FORWARD = 370


def run_nightly(
    settings: Settings,
    *,
    only: list[str] | None = None,
    force: bool = False,
    now: datetime | None = None,
    registry: list[JobSpec] | None = None,
) -> int:
    log = logging_setup.configure(settings)
    settings.ensure_dirs()
    now = now or utc_now()
    try:
        conn = db.open_migrated(settings.db_path)
    except duckdb.IOException as exc:
        # DuckDB is single-writer: the nightly and catch-up tasks are separate
        # scheduler entries and can fire concurrently. The other instance is
        # doing the work — bow out quietly instead of crashing.
        if "lock" in str(exc).lower():
            log.warning("another_argus_instance_holds_the_db", detail=str(exc))
            return 0
        raise
    try:
        calendars.refresh_market_sessions(
            conn,
            now.date() - timedelta(days=SESSIONS_BACK),
            now.date() + timedelta(days=SESSIONS_FORWARD),
        )

        trade_date = calendars.latest_completed_session(now)
        if trade_date is None:
            log.warning("no_completed_session_in_window", now=now.isoformat())
            return 0
        log.info("nightly_start", trade_date=str(trade_date), now=now.isoformat())

        jobs = registry if registry is not None else build_registry()
        if only:
            wanted = set(only)
            unknown = wanted - {j.name for j in jobs}
            if unknown:
                raise SystemExit(f"unknown job(s): {sorted(unknown)}")
            jobs = [j for j in jobs if j.name in wanted]

        ctx = JobContext(settings=settings, conn=conn, trade_date=trade_date, log=log)
        statuses: dict[str, str] = {}
        for spec in jobs:
            statuses[spec.name] = run_job(ctx, spec.name, spec.fn, force=force)

        failed = [name for name, s in statuses.items() if s == "failed"]
        log.info("nightly_done", trade_date=str(trade_date), statuses=statuses)
        return 1 if failed else 0
    finally:
        conn.close()
