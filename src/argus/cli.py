"""ARGUS command-line interface."""

from __future__ import annotations

import typer

from argus import db as db_module
from argus.settings import load_settings

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command()
def init_db() -> None:
    """Create the data root and the build database with current schema."""
    settings = load_settings()
    settings.ensure_dirs()
    conn = db_module.open_migrated(settings.db_path)
    version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    conn.close()
    typer.echo(f"data root : {settings.data_root}")
    typer.echo(f"database  : {settings.db_path} (schema v{version[0] if version else '?'})")


@app.command()
def nightly(
    only: list[str] = typer.Option(None, "--only", help="run only these job names"),
    force: bool = typer.Option(False, "--force", help="re-run jobs already ok for the trade date"),
) -> None:
    """Run the nightly pipeline for the latest completed session (idempotent)."""
    from argus.orchestration.runner import run_nightly

    settings = load_settings()
    raise SystemExit(run_nightly(settings, only=list(only) if only else None, force=force))


@app.command()
def job(name: str, force: bool = typer.Option(False, "--force")) -> None:
    """Run a single named job (see `argus jobs-list`)."""
    from argus.orchestration.runner import run_nightly

    settings = load_settings()
    raise SystemExit(run_nightly(settings, only=[name], force=force))


@app.command("jobs-list")
def jobs_list() -> None:
    """Show the registered nightly jobs in execution order."""
    from argus.orchestration.nightly import build_registry

    for spec in build_registry():
        typer.echo(spec.name)


@app.command()
def bootstrap(force: bool = typer.Option(False, "--force")) -> None:
    """One-off daily-spine bootstrap: Polygon CAs -> Stooq history -> serve.

    Requires ARGUS_POLYGON_API_KEY: bootstrapping bars without the split feed
    would serve split-adjusted prices as raw — refused outright.
    """
    from argus.orchestration.nightly import bootstrap_registry
    from argus.orchestration.runner import run_nightly

    settings = load_settings()
    if not settings.polygon_api_key:
        typer.echo("refusing to bootstrap: ARGUS_POLYGON_API_KEY is not set (see .env.example).")
        typer.echo("Without the split feed the reversal cannot run and the served")
        typer.echo("prices would silently bake in look-ahead (v4 §5.1).")
        raise SystemExit(2)
    raise SystemExit(run_nightly(settings, force=force, registry=bootstrap_registry()))


@app.command()
def rebuild(yes: bool = typer.Option(False, "--yes", help="confirm the canonical wipe")) -> None:
    """Rebuild canonical DuckDB state from the L2 event store (deterministic replay).

    The Parquet event store is the system of record; this wipes bars_daily,
    corporate_actions and vote_results, replays L2, re-votes, and republishes.
    """
    if not yes:
        typer.echo("This wipes the canonical tables and replays them from L2 Parquet.")
        typer.echo("The event store is untouched. Re-run with --yes to proceed.")
        raise SystemExit(2)

    from argus.core import calendars
    from argus.core.clocks import utc_now
    from argus.orchestration.rebuild import rebuild_canonical

    settings = load_settings()
    trade_date = calendars.latest_completed_session(utc_now())
    if trade_date is None:
        typer.echo("no completed session found — aborting")
        raise SystemExit(1)
    conn = db_module.open_migrated(settings.db_path)
    try:
        summary = rebuild_canonical(settings, conn, trade_date)
    finally:
        conn.close()
    for k, v in summary.items():
        typer.echo(f"{k}: {v}")


@app.command("verify-pit")
def verify_pit(
    ticker: str = typer.Option(..., "--ticker"),
    on_date: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
) -> None:
    """Show exactly how the served value for (ticker, date) was built — factor by factor."""
    from datetime import date as date_type

    from argus.factors.adjustment import pit_report

    settings = load_settings()
    conn = db_module.open_migrated(settings.db_path)
    report = pit_report(conn, ticker, date_type.fromisoformat(on_date))
    conn.close()

    typer.echo(f"{report.ticker} @ {report.bar_date}")
    typer.echo(f"  raw close      : {report.raw_close}")
    for f in report.factors:
        mark = "APPLIED" if f.applied else "excluded"
        typer.echo(
            f"  factor {f.factor_type:<9} ex={f.ex_date} x{f.factor:.6f} "
            f"knowledge={f.knowledge_time.isoformat()} [{mark}]"
        )
    typer.echo(f"  cum factor     : {report.cum_factor:.6f}")
    typer.echo(f"  adjusted close : {report.adjusted_close}")
    typer.echo(f"  no look-ahead  : {report.no_lookahead}")
    if not report.no_lookahead:
        raise SystemExit(1)


@app.command()
def status(limit: int = typer.Option(20, "--limit")) -> None:
    """Recent job runs and open DLQ depth."""
    from argus.ops import dlq

    settings = load_settings()
    conn = db_module.open_migrated(settings.db_path)
    rows = conn.execute(
        """
        SELECT trade_date, job_name, status, started_at, rows_out, error_class
        FROM job_runs ORDER BY started_at DESC LIMIT ?
        """,
        [limit],
    ).fetchall()
    if not rows:
        typer.echo("no job runs recorded yet")
    for r in rows:
        typer.echo(f"{r[0]}  {r[1]:<20} {r[2]:<22} rows={r[4] or 0:<6} {r[5] or ''}")
    typer.echo(f"open DLQ entries: {dlq.open_depth(conn)}")
    conn.close()


@app.command("dlq-list")
def dlq_list(limit: int = typer.Option(20, "--limit")) -> None:
    """Show open dead-letter entries."""
    from argus.ops import dlq

    settings = load_settings()
    conn = db_module.open_migrated(settings.db_path)
    entries = dlq.list_open(conn, limit=limit)
    if not entries:
        typer.echo("DLQ is empty")
    for e in entries:
        typer.echo(f"#{e['id']}  {e['first_seen']}  {e['job_name']}  "
                   f"[{e['error_class']}] {e['detail'][:120]}")
    conn.close()


@app.command()
def check() -> None:
    """Environment sanity: data root, keys presence, calendar access."""
    settings = load_settings()
    settings.ensure_dirs()
    alpaca = "set" if settings.alpaca_key_id else "MISSING (j05 will skip)"
    polygon = "set" if settings.polygon_api_key else "MISSING (needed from M1)"
    edgar = "set" if settings.edgar_user_agent else "MISSING (needed from M4)"
    typer.echo(f"data root        : {settings.data_root} (exists: {settings.data_root.exists()})")
    typer.echo(f"alpaca keys      : {alpaca}")
    typer.echo(f"polygon key      : {polygon}")
    typer.echo(f"edgar user-agent : {edgar}")
    from argus.core import calendars
    from argus.core.clocks import utc_now

    td = calendars.latest_completed_session(utc_now())
    typer.echo(f"latest completed US session: {td}")


if __name__ == "__main__":
    app()
