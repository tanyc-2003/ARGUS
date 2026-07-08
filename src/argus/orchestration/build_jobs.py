"""Processing jobs: landed L0 payloads -> L2 events -> L4 canonical tables.

Each is a normal registry job (idempotent per trade date via job_runs). They
process only payloads landed FOR the current trade date — the L0 manifest is
the queue, request keys end in the trade-date ISO string.
"""

from __future__ import annotations

import uuid

import polars as pl

from argus.canonical import actions_store, daily_bars
from argus.core.clocks import utc_now
from argus.events import schemas as event_schemas
from argus.events import store as event_store
from argus.normalize.actions import parse_polygon_actions
from argus.normalize.daily import parse_stooq_csv, reverse_split_adjustment
from argus.ops.jobs import JobContext, JobResult
from argus.sources.polygon_ref import KINDS as POLYGON_CA_KINDS
from argus.sources.stooq import DATASET as STOOQ_DATASET


def _landed_for_trade_date(ctx: JobContext, datasets: list[str]) -> list[tuple[str, str, str]]:
    """(dataset, request_key, path) rows landed for this trade date."""
    placeholders = ", ".join("?" for _ in datasets)
    rows = ctx.conn.execute(
        f"""
        SELECT dataset, request_key, path FROM landing_manifest
        WHERE dataset IN ({placeholders}) AND request_key LIKE '%:' || ?
        ORDER BY dataset, request_key
        """,
        [*datasets, ctx.trade_date.isoformat()],
    ).fetchall()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


def build_actions(ctx: JobContext) -> JobResult:
    """Polygon CA payloads -> action_events (L2) -> corporate_actions (L4, SCD-2)."""
    landed = _landed_for_trade_date(ctx, list(POLYGON_CA_KINDS))
    frames: list[pl.DataFrame] = []
    for dataset, request_key, path in landed:
        ticker = request_key.split(":", 1)[0]
        with open(path, "rb") as fh:
            payload = fh.read()
        frames.append(
            parse_polygon_actions(
                payload, kind=dataset, ticker=ticker,
                landing_key=f"{dataset}:polygon:{request_key}",
            )
        )
    if not frames:
        return JobResult(detail="no CA payloads landed for this trade date")
    events = pl.concat([f for f in frames if not f.is_empty()], how="vertical") if any(
        not f.is_empty() for f in frames
    ) else pl.DataFrame(schema=event_schemas.ACTION_EVENT_SCHEMA)

    event_store.append(ctx.settings, event_schemas.ACTION_EVENTS, events)
    counts = actions_store.upsert_actions(ctx.conn, events)
    return JobResult(
        rows_out=counts["inserted"] + counts["revised"],
        detail=f"events={events.height} {counts}",
    )


def build_daily_bars(ctx: JobContext) -> JobResult:
    """Stooq payloads -> split reversal -> bar_events (L2) -> bars_daily (L4, SCD-2).

    Ordering matters: build_actions must run first in the same night so the
    reversal sees every split Polygon knows about. A missed split cannot be
    detected until a second bar source votes (M3) — rows stay 'degraded'.
    """
    landed = _landed_for_trade_date(ctx, [STOOQ_DATASET])
    if not landed:
        return JobResult(detail="no stooq payloads landed for this trade date")

    frames: list[pl.DataFrame] = []
    for _dataset, request_key, path in landed:
        ticker = request_key.split(":", 1)[0]
        with open(path, encoding="utf-8") as fh:
            frames.append(parse_stooq_csv(fh.read(), ticker))
    vendor_bars = pl.concat(frames, how="vertical")

    splits = actions_store.current_splits(ctx.conn)
    raw = reverse_split_adjustment(vendor_bars, splits)

    hashed = daily_bars.bar_knowledge_time(daily_bars.row_hashes(raw))
    events = hashed.with_columns(
        pl.Series("event_id", [uuid.uuid4().hex for _ in range(hashed.height)]),
        pl.lit("stooq").alias("source"),
        pl.lit("1d").alias("interval"),
        pl.lit(True).alias("vendor_adjusted"),
        pl.lit(utc_now()).alias("written_at"),
        pl.lit(f"{STOOQ_DATASET}:{ctx.trade_date.isoformat()}").alias("landing_key"),
    )
    event_store.append(ctx.settings, event_schemas.BAR_EVENTS, events)

    counts = daily_bars.upsert_bars(ctx.conn, raw)
    return JobResult(
        rows_out=counts["inserted"] + counts["revised"],
        detail=f"tickers={len(landed)} bars={raw.height} {counts}",
    )
