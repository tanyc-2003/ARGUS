from argus.ops import dlq
from argus.ops.errors import BudgetExhausted, SourceDown, TransportFailure
from argus.ops.jobs import JobContext, JobResult, run_job


def _ok(ctx: JobContext) -> JobResult:
    return JobResult(rows_out=7, budget_used=3, detail="fine")


def _boom(ctx: JobContext) -> JobResult:
    raise ValueError("unexpected explosion")


def test_ok_recorded(ctx) -> None:
    assert run_job(ctx, "job_a", _ok) == "ok"
    row = ctx.conn.execute(
        "SELECT status, rows_out, budget_used FROM job_runs WHERE job_name='job_a'"
    ).fetchone()
    assert row == ("ok", 7, 3)


def test_second_run_skipped(ctx) -> None:
    run_job(ctx, "job_a", _ok)
    assert run_job(ctx, "job_a", _ok) == "skipped_already_done"
    n = ctx.conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name='job_a'").fetchone()[0]
    assert n == 1  # the skip is not re-recorded


def test_force_reruns(ctx) -> None:
    run_job(ctx, "job_a", _ok)
    assert run_job(ctx, "job_a", _ok, force=True) == "ok"
    n = ctx.conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name='job_a'").fetchone()[0]
    assert n == 2


def test_unexpected_exception_fails_and_dlqs(ctx) -> None:
    assert run_job(ctx, "job_b", _boom) == "failed"
    row = ctx.conn.execute(
        "SELECT status, error_class FROM job_runs WHERE job_name='job_b'"
    ).fetchone()
    assert row == ("failed", "unknown")
    assert dlq.open_depth(ctx.conn) == 1


def test_classified_error_keeps_class(ctx) -> None:
    def fail(ctx_: JobContext) -> JobResult:
        raise TransportFailure("wire snapped", source="stooq")

    assert run_job(ctx, "job_c", fail) == "failed"
    entry = dlq.list_open(ctx.conn)[0]
    assert entry["error_class"] == "transport"
    assert entry["source"] == "stooq"


def test_budget_exhausted_is_not_a_failure(ctx) -> None:
    def exhausted(ctx_: JobContext) -> JobResult:
        raise BudgetExhausted("spent", source="polygon")

    assert run_job(ctx, "job_d", exhausted) == "budget_exhausted"
    assert dlq.open_depth(ctx.conn) == 0  # resume tomorrow, no dead letter


def test_source_down_skips_without_dlq(ctx) -> None:
    def down(ctx_: JobContext) -> JobResult:
        raise SourceDown("no creds", source="alpaca_iex")

    assert run_job(ctx, "job_e", down) == "skipped_source_down"
    assert dlq.open_depth(ctx.conn) == 0


def test_failed_job_is_retried_next_run(ctx) -> None:
    run_job(ctx, "job_f", _boom)
    # a failed status must not satisfy the idempotency check
    assert run_job(ctx, "job_f", _ok) == "ok"
