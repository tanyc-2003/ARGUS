"""SEC EDGAR: CIK mapping + SIC codes -> sectors (R4, v4 §5.4).

Two endpoints, both free, both requiring a descriptive User-Agent with contact
info per SEC fair-access policy (ARGUS_EDGAR_USER_AGENT; the job skips cleanly
until it is configured):

  * company_tickers.json  — ticker -> CIK map, one snapshot per trade date
  * submissions/CIK{10d}  — per-company profile incl. sic + sicDescription;
                            fetched only for universe tickers missing a sector
                            (profiles barely change — no nightly re-pull)
"""

from __future__ import annotations

import json

from argus.config_files import load_universe
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, edgar_bucket

SOURCE = "edgar"
TICKERS_DATASET = "edgar_company_tickers"
SUBMISSIONS_DATASET = "edgar_submissions"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:0>10}.json"


def _client(ctx: JobContext, budget: RunBudget) -> FetchClient:
    if not ctx.settings.edgar_user_agent:
        raise SourceDown(
            f"{SOURCE}: ARGUS_EDGAR_USER_AGENT not configured (SEC fair-access policy "
            "requires a contact address)",
            source=SOURCE,
        )
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    return FetchClient(
        SOURCE, bucket=edgar_bucket(), budget=budget,
        user_agent=ctx.settings.edgar_user_agent,
    )


def _tickers_missing_sector(ctx: JobContext) -> list[str]:
    known = {
        r[0] for r in ctx.conn.execute("SELECT ticker FROM sectors").fetchall()
    }
    return [r["ticker"] for r in load_universe(ctx.settings) if r["ticker"] not in known]


def capture(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    # 1 call per universe ticker still missing a sector, +1 for the ticker map.
    # Must clear the universe size: at 112 names (2026-07) the old 100 could not
    # even cover a first pass, so the last names silently never got a sector.
    budget = RunBudget(SOURCE, 250)
    client = client or _client(ctx, budget)

    landed = 0
    skipped = 0
    try:
        map_key = f"company_tickers:{ctx.trade_date.isoformat()}"
        if store.ensure(ctx.conn, TICKERS_DATASET, SOURCE, map_key) is None:
            resp = client.get(TICKERS_URL)
            payload = resp.json()
            if not isinstance(payload, dict) or not payload:
                raise SchemaDrift(f"{SOURCE}: company_tickers.json empty/unexpected",
                                  source=SOURCE)
            first = next(iter(payload.values()))
            if "cik_str" not in first or "ticker" not in first:
                raise SchemaDrift(f"{SOURCE}: company_tickers.json shape changed",
                                  source=SOURCE)
            store.write(
                ctx.conn, ctx.settings, dataset=TICKERS_DATASET, source=SOURCE,
                request_key=map_key, payload=resp.content, ext="json",
                partition_date=ctx.trade_date, knowledge_time=pull_knowledge_time(),
            )
            landed += 1
            cik_map = {str(v["ticker"]).upper(): int(v["cik_str"]) for v in payload.values()}
        else:
            skipped += 1
            row = ctx.conn.execute(
                "SELECT path FROM landing_manifest WHERE dataset = ? AND request_key = ?",
                [TICKERS_DATASET, map_key],
            ).fetchone()
            assert row is not None  # ensure() above said it exists
            with open(row[0], encoding="utf-8") as fh:
                cik_map = {
                    str(v["ticker"]).upper(): int(v["cik_str"])
                    for v in json.load(fh).values()
                }

        for ticker in _tickers_missing_sector(ctx):
            cik = cik_map.get(ticker)
            if cik is None:
                continue  # ETFs and some funds are absent — the gap ledger counts them
            request_key = f"{ticker}:{cik}"
            if store.ensure(ctx.conn, SUBMISSIONS_DATASET, SOURCE, request_key) is not None:
                skipped += 1
                continue
            resp = client.get(SUBMISSIONS_URL.format(cik=cik))
            body = resp.json()
            if "sic" not in body:
                raise SchemaDrift(f"{SOURCE}: submissions for {ticker} missing 'sic'",
                                  source=SOURCE)
            store.write(
                ctx.conn, ctx.settings, dataset=SUBMISSIONS_DATASET, source=SOURCE,
                request_key=request_key, payload=resp.content, ext="json",
                partition_date=ctx.trade_date, knowledge_time=pull_knowledge_time(),
            )
            landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)
    return JobResult(rows_out=landed, budget_used=budget.used,
                     detail=f"landed={landed} already={skipped}")
