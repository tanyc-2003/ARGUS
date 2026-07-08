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
