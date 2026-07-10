"""M6 trust layer: sectors, the Polygon parity drift alarm, the gap ledger,
the monthly Stooq re-pull gate, and the append-only backup mirror.

Cadenced jobs (weekly parity, monthly re-pull) gate on the last genuinely-run
success in job_runs — a "not due" night records ok with a marker detail that
the gate ignores.
"""

from __future__ import annotations

import json
import random
from datetime import timedelta
from pathlib import Path

from argus.config_files import load_sic_map, load_universe, sic_to_sector
from argus.core.clocks import utc_now
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, polygon_bucket
from argus.sources import stooq
from argus.sources.edgar import SUBMISSIONS_DATASET

NOT_DUE = "not due"
PARITY_SAMPLE_SIZE = 25
PARITY_WINDOW_DAYS = 600  # stay comfortably inside Polygon's ~2y free window
PARITY_TOL = {"open": 0.001, "high": 0.001, "low": 0.001, "close": 0.001, "volume": 0.05}


def _due(ctx: JobContext, job_name: str, min_days: int, *, statuses: str = "'ok'") -> bool:
    row = ctx.conn.execute(
        f"""
        SELECT MAX(trade_date) FROM job_runs
        WHERE job_name = ? AND status IN ({statuses}) AND detail NOT LIKE ?
        """,
        [job_name, f"{NOT_DUE}%"],
    ).fetchone()
    last = row[0] if row else None
    return last is None or (ctx.trade_date - last).days >= min_days


def stooq_monthly(ctx: JobContext) -> JobResult:
    """Monthly full-history re-pull (v4 §7.1): the re-landed payloads flow
    through the normal build+vote path, so silent vendor rewrites surface as
    SCD-2 revisions — the diff alarm is the revision count itself.

    Note 2026-07: Stooq gates its endpoints behind a JS proof-of-work challenge
    (which ARGUS respects, not defeats). The failure backoff below keeps a
    blocked source from spamming the DLQ nightly; if Stooq reopens, this heals.
    """
    if not _due(ctx, "j02b_stooq_monthly", 28):
        return JobResult(detail=f"{NOT_DUE} (last full pull < 28 days ago)")
    if not _due(ctx, "j02b_stooq_monthly", 7, statuses="'failed'"):
        return JobResult(detail=f"{NOT_DUE} (blocked source: retrying weekly, not nightly)")
    return stooq.capture(ctx)


def sector_seal(ctx: JobContext) -> JobResult:
    """Landed EDGAR submissions -> sectors projection (SIC -> sector ETF)."""
    known = {r[0] for r in ctx.conn.execute("SELECT ticker FROM sectors").fetchall()}
    landed = ctx.conn.execute(
        "SELECT request_key, path FROM landing_manifest WHERE dataset = ? "
        "ORDER BY request_key",
        [SUBMISSIONS_DATASET],
    ).fetchall()
    ranges = load_sic_map(ctx.settings) if landed else []
    added = 0
    for request_key, path in landed:
        ticker, cik = str(request_key).split(":", 1)
        if ticker in known:
            continue
        with open(path, encoding="utf-8") as fh:
            body = json.load(fh)
        sic = body.get("sic")
        ctx.conn.execute(
            "INSERT OR REPLACE INTO sectors VALUES (?, ?, ?, ?, ?, 'edgar', ?)",
            [ticker, cik, str(sic) if sic else None,
             sic_to_sector(sic, ranges), body.get("sicDescription"), utc_now()],
        )
        added += 1
    return JobResult(rows_out=added, detail=f"sectors_added={added}")


def parity_sample(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    """Weekly ~25-bar spot check against Polygon EOD aggregates (v4 §6).

    Interpreted as a drift ALARM on the free spine, not a parity target:
    sustained divergence demotes a source, it never rewrites our data.
    Sampling is seeded by trade date — a forced re-run compares the same bars.
    """
    if not ctx.settings.polygon_api_key:
        raise SourceDown("polygon: ARGUS_POLYGON_API_KEY not configured", source="polygon")
    if not _due(ctx, "j13_parity_sample", 7):
        return JobResult(detail=f"{NOT_DUE} (last sample < 7 days ago)")
    if health.is_open(ctx.conn, "polygon"):
        raise SourceDown("polygon: circuit open", source="polygon")

    candidates = ctx.conn.execute(
        """
        SELECT ticker, bar_date, open, high, low, close, volume FROM bars_daily
        WHERE is_current AND grade <> 'quarantined' AND bar_date >= ?
        ORDER BY ticker, bar_date
        """,
        [ctx.trade_date - timedelta(days=PARITY_WINDOW_DAYS)],
    ).fetchall()
    if not candidates:
        return JobResult(detail="no eligible bars to sample")
    rng = random.Random(ctx.trade_date.isoformat())  # deterministic per trade date
    sample = rng.sample(candidates, min(PARITY_SAMPLE_SIZE, len(candidates)))

    budget = RunBudget("polygon", ctx.settings.polygon_nightly_budget)
    client = client or FetchClient("polygon", bucket=polygon_bucket(), budget=budget)

    ctx.conn.execute("DELETE FROM parity_scores WHERE sample_date = ?", [ctx.trade_date])
    checked = 0
    breaches = 0
    try:
        for ticker, bar_date, *ours in sample:
            resp = client.get(
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
                f"{bar_date}/{bar_date}",
                params={"adjusted": "false", "apiKey": ctx.settings.polygon_api_key},
            )
            payload = resp.json()
            if "status" not in payload:
                raise SchemaDrift("polygon: aggs response missing 'status'", source="polygon")
            store.write(
                ctx.conn, ctx.settings, dataset="polygon_parity", source="polygon",
                request_key=f"{ticker}:{bar_date}:{ctx.trade_date.isoformat()}",
                payload=resp.content, ext="json", partition_date=ctx.trade_date,
                knowledge_time=utc_now(),
            )
            results = payload.get("results") or []
            theirs = results[0] if results else {}
            field_map = zip(
                ["open", "high", "low", "close", "volume"],
                ours,
                [theirs.get(k) for k in ("o", "h", "l", "c", "v")],
                strict=True,
            )
            for field, our_v, their_v in field_map:
                if our_v is None or their_v is None:
                    rel = None
                    ok = their_v is None and our_v is None
                else:
                    mid = (abs(our_v) + abs(their_v)) / 2 or 1.0
                    rel = abs(our_v - their_v) / mid
                    ok = rel <= PARITY_TOL[field]
                ctx.conn.execute(
                    "INSERT OR REPLACE INTO parity_scores VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [ctx.trade_date, ticker, bar_date, field, our_v,
                     float(their_v) if their_v is not None else None, rel, bool(ok)],
                )
                breaches += 0 if ok else 1
            checked += 1
    except TransportFailure:
        health.record_failure(ctx.conn, "polygon")
        raise
    health.record_success(ctx.conn, "polygon")
    return JobResult(rows_out=checked, budget_used=budget.used,
                     detail=f"bars_checked={checked} field_breaches={breaches}")


def gap_ledger_seal(ctx: JobContext) -> JobResult:
    """v4 §10: what $0 cannot buy, measured — served, never discovered."""
    conn = ctx.conn
    now = utc_now()

    def one(sql: str, params: list | None = None) -> float | None:
        row = conn.execute(sql, params or []).fetchone()
        return None if row is None or row[0] is None else float(row[0])

    intraday_iex = one(
        "SELECT AVG(CASE WHEN derivation = 'iex_bbo' THEN 1.0 ELSE 0.0 END) "
        "FROM serving_intraday"
    )
    single_source = one(
        "SELECT AVG(CASE WHEN single_source THEN 1.0 ELSE 0.0 END) "
        "FROM bars_daily WHERE is_current"
    )
    quarantined = one(
        "SELECT COUNT(*) FROM bars_daily WHERE is_current AND grade = 'quarantined'"
    )
    coverage_10y = one(
        "SELECT coverage FROM coverage_metrics WHERE audit_window = '10y'"
    )
    reasons_unknown = one(
        "SELECT AVG(CASE WHEN termination_reason = 'unknown' THEN 1.0 ELSE 0.0 END) "
        "FROM graveyard"
    )
    universe_n = len(load_universe(ctx.settings))
    sectors_n = one("SELECT COUNT(*) FROM sectors WHERE sector IS NOT NULL") or 0.0
    parity_worst = one(
        "SELECT MAX(rel_diff) FROM parity_scores WHERE sample_date = "
        "(SELECT MAX(sample_date) FROM parity_scores)"
    )
    circuits_open = one(
        "SELECT COUNT(*) FROM source_health WHERE state = 'open'"
    )

    rows: list[tuple[str, str, float | None, str, str]] = [
        ("intraday_iex_bbo_share",
         "share of served intraday minutes with real IEX BBO (rest: Corwin-Schultz proxy)",
         intraday_iex, "ratio",
         "info" if (intraday_iex or 0) > 0 else "warn"),
        ("daily_single_source_share",
         "share of current daily bars confirmed by only one source",
         single_source, "ratio",
         "warn" if (single_source or 0) > 0.5 else "info"),
        ("daily_quarantined_count",
         "current daily bars quarantined by vote conflict or MAD screen",
         quarantined, "rows", "warn" if (quarantined or 0) > 0 else "info"),
        ("delisted_coverage_10y",
         "delisted names with price history over the 10y audit window "
         "(dashboard audit blocks below 0.95)",
         coverage_10y, "ratio",
         "blocker" if (coverage_10y or 0.0) < 0.95 else "info"),
        ("delisted_reasons_unknown_share",
         "graveyard rows still reason='unknown' (EDGAR classification pending)",
         reasons_unknown, "ratio", "info"),
        ("sectors_missing_count",
         "universe tickers without a mapped sector (ETFs have no SIC)",
         float(universe_n) - sectors_n, "rows", "info"),
        ("parity_worst_rel_diff",
         "worst field divergence vs Polygon in the latest weekly sample",
         parity_worst, "ratio",
         "warn" if (parity_worst or 0) > 0.001 else "info"),
        ("sources_circuit_open",
         "sources currently circuit-open (dead or blocked; voting degrades gracefully)",
         circuits_open, "sources",
         "warn" if (circuits_open or 0) > 0 else "info"),
    ]
    conn.execute("DELETE FROM gap_ledger")
    for key, desc, metric, unit, severity in rows:
        conn.execute("INSERT INTO gap_ledger VALUES (?, ?, ?, ?, ?, ?)",
                     [key, desc, metric, unit, severity, now])
    return JobResult(rows_out=len(rows), detail=f"entries={len(rows)}")


def backup(ctx: JobContext) -> JobResult:
    """Mirror the append-only stores (L0 landing + L2 events) into backup/.

    Files are immutable by construction, so copy-if-absent IS a correct
    incremental backup. The DuckDB file is deliberately excluded: it is a
    disposable projection (argus rebuild regenerates it).
    """
    import shutil

    copied = 0
    for src_root in (ctx.settings.landing_dir, ctx.settings.events_dir):
        if not src_root.exists():
            continue
        dst_root: Path = ctx.settings.data_root / "backup" / src_root.name
        for src in src_root.rglob("*"):
            if not src.is_file() or src.suffix == ".tmp":
                continue
            dst = dst_root / src.relative_to(src_root)
            if dst.exists() and dst.stat().st_size == src.stat().st_size:
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied += 1
    return JobResult(rows_out=copied, detail=f"files_copied={copied}")
