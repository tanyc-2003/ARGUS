"""Polygon free-tier reference drip: splits + dividends (R4 corporate actions).

5 calls/min is the entire budget discipline (v4 §7.2): one snapshot per
(kind, ticker) per trade date, drip-fed through the shared token bucket.
The seed universe costs ~30 calls ≈ 6 minutes; the nightly job simply
re-snapshots so late-announced actions are picked up (SCD-2 makes unchanged
rows no-ops downstream).
"""

from __future__ import annotations

from argus.config_files import load_universe
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, polygon_bucket

SOURCE = "polygon"
BASE = "https://api.polygon.io/v3/reference"
KINDS: dict[str, str] = {
    "polygon_splits": f"{BASE}/splits",
    "polygon_dividends": f"{BASE}/dividends",
}
DELISTED_DATASET = "polygon_delisted"
MAX_DELISTED_PAGES = 25  # 1000 names/page; the drip budget is the real cap


def capture_corporate_actions(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    s = ctx.settings
    if not s.polygon_api_key:
        raise SourceDown(f"{SOURCE}: ARGUS_POLYGON_API_KEY not configured", source=SOURCE)
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)

    budget = RunBudget(SOURCE, s.polygon_nightly_budget)
    client = client or FetchClient(SOURCE, bucket=polygon_bucket(), budget=budget)

    landed = 0
    skipped = 0
    try:
        for row in load_universe(ctx.settings):
            ticker = row["ticker"]
            for dataset, url in KINDS.items():
                request_key = f"{ticker}:{ctx.trade_date.isoformat()}"
                if store.ensure(ctx.conn, dataset, SOURCE, request_key) is not None:
                    skipped += 1
                    continue
                resp = client.get(
                    url,
                    params={"ticker": ticker, "limit": "1000", "apiKey": s.polygon_api_key},
                )
                payload = resp.json()
                if "status" not in payload:
                    raise SchemaDrift(
                        f"{SOURCE}: {dataset} for {ticker} missing 'status' key", source=SOURCE
                    )
                store.write(
                    ctx.conn, ctx.settings,
                    dataset=dataset, source=SOURCE, request_key=request_key,
                    payload=resp.content, ext="json", partition_date=ctx.trade_date,
                    knowledge_time=pull_knowledge_time(), content_type="application/json",
                )
                landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} already={skipped}",
    )


def capture_delisted(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    """Delisted-ticker enumeration (R3 backward coverage): /v3/reference/tickers
    ?active=false, paginated through the 5/min bucket, aggregated into one
    gzipped payload per trade date. Budget exhaustion mid-pagination loses the
    partial page set and retries tomorrow — patience is the currency (v4 §7.2).
    """
    import gzip
    import json

    s = ctx.settings
    if not s.polygon_api_key:
        raise SourceDown(f"{SOURCE}: ARGUS_POLYGON_API_KEY not configured", source=SOURCE)
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)

    request_key = f"delisted:{ctx.trade_date.isoformat()}"
    if store.ensure(ctx.conn, DELISTED_DATASET, SOURCE, request_key) is not None:
        return JobResult(detail="already landed for this trade date")

    budget = RunBudget(SOURCE, s.polygon_nightly_budget)
    client = client or FetchClient(SOURCE, bucket=polygon_bucket(), budget=budget)

    results: list[dict[str, object]] = []
    url: str | None = f"{BASE}/tickers"
    params: dict[str, str] | None = {
        "active": "false", "market": "stocks", "limit": "1000", "apiKey": s.polygon_api_key,
    }
    pages = 0
    complete = False
    try:
        while url and pages < MAX_DELISTED_PAGES:
            payload = client.get(url, params=params).json()
            if "status" not in payload:
                raise SchemaDrift(f"{SOURCE}: delisted page missing 'status'", source=SOURCE)
            results.extend(payload.get("results") or [])
            pages += 1
            next_url = payload.get("next_url")
            if not next_url:
                complete = True
                break
            url, params = next_url, {"apiKey": s.polygon_api_key}
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)

    body = {"results": results, "complete": complete, "pages": pages}
    store.write(
        ctx.conn, ctx.settings,
        dataset=DELISTED_DATASET, source=SOURCE, request_key=request_key,
        payload=gzip.compress(json.dumps(body).encode("utf-8")), ext="json.gz",
        partition_date=ctx.trade_date, knowledge_time=pull_knowledge_time(),
        content_type="application/gzip",
    )
    return JobResult(
        rows_out=len(results), budget_used=budget.used,
        detail=f"names={len(results)} pages={pages} complete={complete}",
    )
