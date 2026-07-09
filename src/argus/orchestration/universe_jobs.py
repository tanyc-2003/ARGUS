"""Survivorship: universe snapshots -> graveyard -> terminal returns -> coverage.

The graveyard is a nightly PROJECTION over immutable inputs (the landed
symbol-directory snapshots plus Polygon's delisted list), rebuilt wholesale —
the snapshots are the audit history, so SCD-2 here would duplicate it. A
ticker that reappears (rename, exchange move round-trip) simply drops out of
the projection on the next build: self-correcting by construction.

Diff semantics: a date participates only when BOTH directory files landed
(otherwise a missing file would fake a mass delisting), and a gap of missed
nights collapses into one diff at the next good snapshot.
"""

from __future__ import annotations

import gzip
import json
from datetime import date, timedelta

import polars as pl

from argus.core.clocks import utc_now
from argus.normalize.universe import PARSERS
from argus.ops.errors import SchemaDrift
from argus.ops.jobs import JobContext, JobResult
from argus.sources.polygon_ref import DELISTED_DATASET

AUDIT_WINDOW_10Y_DAYS = 3653
REASON_DEFAULT = "unknown"  # EDGAR/CA reason classification arrives in a later milestone


def _load_snapshots(ctx: JobContext) -> int:
    """Parse every landed symbol-dir payload not yet in universe_snapshots."""
    landed = ctx.conn.execute(
        """
        SELECT request_key, path FROM landing_manifest
        WHERE dataset = 'symbol_dirs' ORDER BY request_key
        """
    ).fetchall()
    known = {
        (r[0], r[1])
        for r in ctx.conn.execute(
            "SELECT DISTINCT source, snapshot_date FROM universe_snapshots"
        ).fetchall()
    }
    added = 0
    for request_key, path in landed:
        kind, date_str = str(request_key).split(":", 1)
        snapshot_date = date.fromisoformat(date_str)
        if kind not in PARSERS:
            raise SchemaDrift(f"unknown symbol-dir kind {kind!r}", source="nasdaqtrader")
        if (kind, snapshot_date) in known:
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            frame = PARSERS[kind](fh.read())
        rows = frame.with_columns(
            pl.lit(kind).alias("source"),
            pl.lit(snapshot_date).alias("snapshot_date"),
        ).select("source", "snapshot_date", "ticker", "security_name", "exchange", "is_etf")
        ctx.conn.register("snap_incoming", rows.to_arrow())
        try:
            ctx.conn.execute(
                "INSERT OR IGNORE INTO universe_snapshots "
                "SELECT source, snapshot_date, ticker, security_name, exchange, is_etf "
                "FROM snap_incoming"
            )
        finally:
            ctx.conn.unregister("snap_incoming")
        added += rows.height
    return added


def _diff_graveyard(ctx: JobContext) -> pl.DataFrame:
    """Disappearances from the unioned snapshots -> (ticker, termination_date)."""
    complete_dates = [
        r[0]
        for r in ctx.conn.execute(
            """
            SELECT snapshot_date FROM universe_snapshots
            GROUP BY snapshot_date HAVING COUNT(DISTINCT source) = 2
            ORDER BY snapshot_date
            """
        ).fetchall()
    ]
    if len(complete_dates) < 2:
        return pl.DataFrame(schema={"ticker": pl.Utf8, "termination_date": pl.Date})

    presence = ctx.conn.execute(
        """
        SELECT DISTINCT ticker, snapshot_date FROM universe_snapshots
        WHERE snapshot_date IN (SELECT snapshot_date FROM universe_snapshots
                                GROUP BY snapshot_date HAVING COUNT(DISTINCT source) = 2)
        """
    ).pl()
    latest = complete_dates[-1]
    per_ticker = presence.group_by("ticker").agg(pl.col("snapshot_date").max().alias("last_seen"))
    gone = per_ticker.filter(pl.col("last_seen") < latest)
    if gone.is_empty():
        return pl.DataFrame(schema={"ticker": pl.Utf8, "termination_date": pl.Date})

    # termination_date = the first complete snapshot AFTER the last sighting
    dates_df = pl.DataFrame({"termination_date": complete_dates}).sort("termination_date")
    return (
        gone.sort("last_seen")
        .join_asof(
            dates_df.with_columns(
                (pl.col("termination_date") - pl.duration(days=1)).cast(pl.Date)
                .alias("join_key")
            ).sort("join_key"),
            left_on="last_seen", right_on="join_key", strategy="forward",
        )
        .select("ticker", "termination_date")
        .drop_nulls()
    )


def _polygon_graveyard(ctx: JobContext) -> pl.DataFrame:
    """Delisted names from the most recent Polygon delisted-list payload."""
    row = ctx.conn.execute(
        """
        SELECT path FROM landing_manifest WHERE dataset = ?
        ORDER BY request_key DESC LIMIT 1
        """,
        [DELISTED_DATASET],
    ).fetchone()
    empty = pl.DataFrame(schema={"ticker": pl.Utf8, "termination_date": pl.Date})
    if row is None:
        return empty
    with open(row[0], "rb") as fh:
        body = json.loads(gzip.decompress(fh.read()))
    rows = []
    for r in body.get("results", []):
        delisted = r.get("delisted_utc")
        ticker = r.get("ticker")
        if not delisted or not ticker:
            continue
        rows.append({"ticker": str(ticker).upper(),
                     "termination_date": date.fromisoformat(str(delisted)[:10])})
    return pl.DataFrame(rows, schema={"ticker": pl.Utf8, "termination_date": pl.Date}) \
        if rows else empty


def _terminal_returns(ctx: JobContext, grave: pl.DataFrame) -> pl.DataFrame:
    """terminal_return = last sealed return before delisting, when we hold bars."""
    if grave.is_empty():
        return grave.with_columns(pl.lit(None, dtype=pl.Float64).alias("terminal_return"))
    bars = ctx.conn.execute(
        """
        SELECT ticker, bar_date, close FROM bars_daily
        WHERE is_current AND grade <> 'quarantined' ORDER BY ticker, bar_date
        """
    ).pl()
    if bars.is_empty():
        return grave.with_columns(pl.lit(None, dtype=pl.Float64).alias("terminal_return"))
    rets = bars.with_columns(
        (pl.col("close") / pl.col("close").shift(1).over("ticker") - 1.0).alias("ret")
    )
    last_rets = (
        rets.join(grave, on="ticker", how="semi")
        .join(grave.select("ticker", "termination_date"), on="ticker")
        .filter(pl.col("bar_date") <= pl.col("termination_date"))
        .sort("bar_date")
        .group_by("ticker")
        .last()
        .select("ticker", pl.col("ret").alias("terminal_return"))
    )
    return grave.join(last_rets, on="ticker", how="left")


def universe_seal(ctx: JobContext) -> JobResult:
    """Snapshots -> graveyard projection -> terminal returns -> coverage metrics."""
    n_snap = _load_snapshots(ctx)

    diff = _diff_graveyard(ctx).with_columns(pl.lit("symbol_dirs_diff").alias("detection_source"))
    poly = _polygon_graveyard(ctx).with_columns(pl.lit("polygon").alias("detection_source"))
    # polygon's delist date is authoritative when both saw the same ticker die
    grave = pl.concat([poly, diff], how="vertical").unique(subset=["ticker"], keep="first")
    grave = _terminal_returns(ctx, grave)

    # projection rebuild; keep first_seen stable across rebuilds.
    # DuckDB returns TIMESTAMPTZ in the SESSION timezone — normalize to UTC
    # before mixing with utc_now() values (empty frames cast for free).
    existing = ctx.conn.execute(
        "SELECT ticker, termination_date, first_seen FROM graveyard"
    ).pl().with_columns(pl.col("first_seen").cast(pl.Datetime("us", "UTC")))
    now = utc_now()
    if not existing.is_empty():
        grave = grave.join(existing, on=["ticker", "termination_date"], how="left")
        grave = grave.with_columns(pl.col("first_seen").fill_null(now))
    else:
        grave = grave.with_columns(pl.lit(now).alias("first_seen"))
    grave = grave.with_columns(
        pl.lit(REASON_DEFAULT).alias("termination_reason"),
        pl.lit("pending").alias("reason_confidence"),
        pl.lit(None, dtype=pl.Utf8).alias("reason_source"),
    ).select(
        "ticker", "termination_date", "termination_reason", "reason_confidence",
        "reason_source", "terminal_return", "detection_source", "first_seen",
    )

    ctx.conn.execute("DELETE FROM graveyard")
    if not grave.is_empty():
        ctx.conn.register("grave_incoming", grave.to_arrow())
        try:
            ctx.conn.execute("INSERT INTO graveyard SELECT * FROM grave_incoming")
        finally:
            ctx.conn.unregister("grave_incoming")

    coverage = _compute_coverage(ctx)
    return JobResult(
        rows_out=grave.height,
        detail=f"snapshot_rows_added={n_snap} graveyard={grave.height} coverage={coverage}",
    )


def _compute_coverage(ctx: JobContext) -> dict[str, float]:
    """Delisted coverage per audit window (v4 §5.3.4) — always the true number."""
    golive_row = ctx.conn.execute(
        "SELECT MIN(snapshot_date) FROM universe_snapshots"
    ).fetchone()
    golive = golive_row[0] if golive_row and golive_row[0] else ctx.trade_date
    windows = {
        "10y": ctx.trade_date - timedelta(days=AUDIT_WINDOW_10Y_DAYS),
        "since_golive": golive,
    }
    out: dict[str, float] = {}
    ctx.conn.execute("DELETE FROM coverage_metrics")
    for name, start in windows.items():
        row = ctx.conn.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM bars_daily b
                       WHERE b.ticker = g.ticker AND b.is_current
                   ))
            FROM graveyard g WHERE g.termination_date >= ?
            """,
            [start],
        ).fetchone()
        expected, covered = (int(row[0]), int(row[1])) if row else (0, 0)
        coverage = (covered / expected) if expected else 1.0  # auditor semantics: none due = pass
        ctx.conn.execute(
            "INSERT INTO coverage_metrics VALUES (?, ?, ?, ?, ?, ?)",
            [name, start, expected, covered, coverage, utc_now()],
        )
        out[name] = round(coverage, 4)
    return out
