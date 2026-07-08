"""Symbol-directory snapshots (Nasdaq Trader): the forward graveyard's raw feed.

Capture-only in M0: one snapshot of each directory file per trade date lands in
L0. M4 diffs consecutive snapshots into universe/graveyard events. The first
snapshot is the graveyard baseline — this is the job that must run from day 1.
"""

from __future__ import annotations

from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops import health
from argus.ops.errors import SchemaDrift, SourceDown, TransportFailure
from argus.ops.http import FetchClient
from argus.ops.jobs import JobContext, JobResult

SOURCE = "nasdaqtrader"
DATASET = "symbol_dirs"

FILES: dict[str, str] = {
    "nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
    "otherlisted": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
}


def _validate(name: str, text: str) -> None:
    """Cheap shape check: pipe-delimited with a header row and a creation-time footer."""
    lines = text.strip().splitlines()
    if len(lines) < 10 or "|" not in lines[0]:
        raise SchemaDrift(
            f"{SOURCE}:{name} does not look like a pipe-delimited symbol directory",
            source=SOURCE,
        )
    if "file creation time" not in lines[-1].lower():
        raise SchemaDrift(
            f"{SOURCE}:{name} footer missing 'File Creation Time' — format changed?",
            source=SOURCE,
        )


def capture(ctx: JobContext, client: FetchClient | None = None) -> JobResult:
    if health.is_open(ctx.conn, SOURCE):
        raise SourceDown(f"{SOURCE}: circuit open", source=SOURCE)
    client = client or FetchClient(SOURCE)

    landed = 0
    skipped = 0
    try:
        for name, url in FILES.items():
            request_key = f"{name}:{ctx.trade_date.isoformat()}"
            if store.ensure(ctx.conn, DATASET, SOURCE, request_key) is not None:
                skipped += 1
                continue
            resp = client.get(url)
            _validate(name, resp.text)
            store.write(
                ctx.conn, ctx.settings,
                dataset=DATASET, source=SOURCE, request_key=request_key,
                payload=resp.content, ext="txt", partition_date=ctx.trade_date,
                knowledge_time=pull_knowledge_time(), content_type="text/plain",
            )
            landed += 1
    except TransportFailure:
        health.record_failure(ctx.conn, SOURCE)
        raise
    health.record_success(ctx.conn, SOURCE)
    return JobResult(rows_out=landed, detail=f"landed={landed} already={skipped}")
