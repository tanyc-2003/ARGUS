from datetime import UTC, date, datetime

from argus import db as db_module
from argus.ops.jobs import JobContext, JobResult
from argus.orchestration.nightly import JobSpec
from argus.orchestration.runner import run_nightly

NOW = datetime(2026, 7, 7, 22, 0, tzinfo=UTC)  # Tuesday, after close


def _ok(ctx: JobContext) -> JobResult:
    return JobResult(rows_out=1)


def _boom(ctx: JobContext) -> JobResult:
    raise RuntimeError("kaput")


def test_all_ok_exits_zero_and_records(settings) -> None:
    registry = [JobSpec("t_ok_a", _ok), JobSpec("t_ok_b", _ok)]
    assert run_nightly(settings, now=NOW, registry=registry) == 0
    conn = db_module.open_migrated(settings.db_path)
    rows = conn.execute(
        "SELECT job_name, status, trade_date FROM job_runs ORDER BY job_name"
    ).fetchall()
    conn.close()
    assert [(r[0], r[1]) for r in rows] == [("t_ok_a", "ok"), ("t_ok_b", "ok")]
    assert all(r[2] == date(2026, 7, 7) for r in rows)


def test_failing_job_sets_exit_code_and_dlq(settings) -> None:
    registry = [JobSpec("t_ok", _ok), JobSpec("t_boom", _boom)]
    assert run_nightly(settings, now=NOW, registry=registry) == 1
    conn = db_module.open_migrated(settings.db_path)
    dlq_depth = conn.execute(
        "SELECT COUNT(*) FROM dead_letter WHERE resolved_at IS NULL"
    ).fetchone()[0]
    ok_still_ran = conn.execute(
        "SELECT status FROM job_runs WHERE job_name='t_ok'"
    ).fetchone()[0]
    conn.close()
    assert dlq_depth == 1
    assert ok_still_ran == "ok"  # one bad job never blocks the others


def test_double_fire_is_harmless(settings) -> None:
    registry = [JobSpec("t_ok", _ok)]
    run_nightly(settings, now=NOW, registry=registry)
    assert run_nightly(settings, now=NOW, registry=registry) == 0
    conn = db_module.open_migrated(settings.db_path)
    n = conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name='t_ok'").fetchone()[0]
    conn.close()
    assert n == 1  # second fire skipped, not re-recorded


def test_always_jobs_rerun_on_double_fire(settings) -> None:
    registry = [JobSpec("t_always", _ok, always=True)]
    run_nightly(settings, now=NOW, registry=registry)
    run_nightly(settings, now=NOW, registry=registry)
    conn = db_module.open_migrated(settings.db_path)
    n = conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name='t_always'").fetchone()[0]
    conn.close()
    assert n == 2  # projections refresh every run (publish must see late-built rows)


def test_market_sessions_refreshed(settings) -> None:
    run_nightly(settings, now=NOW, registry=[])
    conn = db_module.open_migrated(settings.db_path)
    n = conn.execute("SELECT COUNT(*) FROM market_sessions").fetchone()[0]
    conn.close()
    assert n > 200  # ~60 back + ~370 forward days of sessions


def test_unknown_only_job_rejected(settings) -> None:
    import pytest

    with pytest.raises(SystemExit):
        run_nightly(settings, now=NOW, registry=[JobSpec("t_ok", _ok)], only=["nope"])


def test_concurrent_instance_bows_out_gracefully(settings) -> None:
    # hold the single-writer lock like a concurrently-fired runner would
    settings.ensure_dirs()
    holder = db_module.connect(settings.db_path)
    try:
        assert run_nightly(settings, now=NOW, registry=[JobSpec("t_ok", _ok)]) == 0
    finally:
        holder.close()
    # nothing ran and nothing crashed; a later run does the work
    assert run_nightly(settings, now=NOW, registry=[JobSpec("t_ok", _ok)]) == 0
    conn = db_module.open_migrated(settings.db_path)
    n = conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name='t_ok'").fetchone()[0]
    conn.close()
    assert n == 1
