"""Stooq daily-history capture (the R1 bootstrap spine).

Per-symbol CSV endpoint (https://stooq.com/q/d/l/?s=aapl.us&i=d): decades of
daily OHLCV, no key, one polite request per ticker. Stooq prices are
split-adjusted and dividend-unadjusted — L1 reverses the split adjustment
using Polygon's split feed before anything reaches the canonical layer.

The v4 bulk-file download remains an option when the universe grows past the
point where per-symbol requests are polite.
"""

from __future__ import annotations

from argus.config_files import load_universe
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult
from argus.ops.ratelimit import RunBudget, TokenBucket

SOURCE = "stooq"
DATASET = "stooq_daily"
URL = "https://stooq.com/q/d/l/"
_EXPECTED_HEADER = "Date,Open,High,Low,Close"


def stooq_bucket(**kw: object) -> TokenBucket:
    return TokenBucket(rate_per_sec=0.5, capacity=1, **kw)  # type: ignore[arg-type]


def _validate(ticker: str, text: str) -> None:
    first = text.strip().splitlines()[0] if text.strip() else ""
    if not first.startswith(_EXPECTED_HEADER):
        raise SchemaDrift(
            f"{SOURCE}:{ticker} response does not start with '{_EXPECTED_HEADER}' "
            f"(got: {first[:80]!r})",
            source=SOURCE,
        )


def capture(
    ctx: JobContext,
    client: FetchClient | None = None,
    bucket: TokenBucket | None = None,
) -> JobResult:
    """Land one full-history CSV per universe ticker (keyed per trade date)."""
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    budget = RunBudget(SOURCE, 400)
    client = client or FetchClient(SOURCE, bucket=bucket or stooq_bucket(), budget=budget)

    landed = 0
    skipped = 0
    no_data = 0
    drifted = 0
    last_drift: SchemaDrift | None = None
    try:
        for row in load_universe(ctx.settings):
            ticker = row["ticker"]
            request_key = f"{ticker}:{ctx.trade_date.isoformat()}"
            if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
                skipped += 1
                continue
            resp = client.get(URL, params={"s": f"{ticker.lower()}.us", "i": "d"})
            text = resp.text
            if "no data" in text.strip().lower()[:40]:
                no_data += 1
                ctx.log.warning("stooq_no_data", ticker=ticker)
                continue
            try:
                _validate(ticker, text)
            except SchemaDrift as exc:
                # one bad response (Stooq's HTML rate-limit page, typically) must
                # not kill the other tickers — count it and move on; systemic
                # drift (everything bad) still fails the job loudly below
                drifted += 1
                last_drift = exc
                ctx.log.warning("stooq_drifted_response", ticker=ticker)
                continue
            store.write(
                ctx.conn, ctx.settings,
                dataset=DATASET, source=SOURCE, request_key=request_key,
                payload=resp.content, ext="csv", partition_date=ctx.trade_date,
                knowledge_time=pull_knowledge_time(), content_type="text/csv",
            )
            landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    if drifted and landed == 0 and skipped == 0 and no_data == 0:
        health.record_failure(ctx.conn, SOURCE)
        assert last_drift is not None
        raise last_drift  # every single response was bad — that IS schema drift
    health.record_success(ctx.conn, SOURCE)
    return JobResult(
        rows_out=landed, budget_used=budget.used,
        detail=f"landed={landed} already={skipped} no_data={no_data} drifted={drifted}",
    )
