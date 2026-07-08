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
from argus.normalize.daily import (
    parse_stooq_csv,
    parse_yf_daily_parquet,
    reverse_split_adjustment,
)
from argus.ops.jobs import JobContext, JobResult
from argus.sources.alpaca import DAILY_DATASET as ALPACA_DAILY_DATASET
from argus.sources.polygon_ref import KINDS as POLYGON_CA_KINDS
from argus.sources.stooq import DATASET as STOOQ_DATASET
from argus.sources.yf_daily import DATASET as YF_DAILY_DATASET


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

    event_store.append(
        ctx.settings, event_schemas.BAR_EVENTS,
        _bar_events_frame(raw, source="stooq", vendor_adjusted=True,
                          landing_key=f"{STOOQ_DATASET}:{ctx.trade_date.isoformat()}"),
    )

    # ownership rule (until M3 voting): the bootstrap must not stomp keys the
    # incremental feed already owns — same anti-join as the yfinance side.
    foreign = daily_bars.keys_owned_by_other_sources(ctx.conn, "stooq")
    owned = (
        raw.join(foreign, on=["ticker", "bar_date"], how="anti")
        if not foreign.is_empty() else raw
    )
    counts = daily_bars.upsert_bars(ctx.conn, owned)
    return JobResult(
        rows_out=counts["inserted"] + counts["revised"],
        detail=(
            f"tickers={len(landed)} bars={raw.height} {counts} "
            f"foreign_keys_skipped={raw.height - owned.height}"
        ),
    )


def _bar_events_frame(raw: pl.DataFrame, *, source: str, vendor_adjusted: bool,
                      landing_key: str) -> pl.DataFrame:
    hashed = daily_bars.bar_knowledge_time(daily_bars.row_hashes(raw))
    if "reversal_factor" not in hashed.columns:
        hashed = hashed.with_columns(pl.lit(1.0).alias("reversal_factor"))
    return hashed.with_columns(
        pl.Series("event_id", [uuid.uuid4().hex for _ in range(hashed.height)]),
        pl.lit(source).alias("source"),
        pl.lit("1d").alias("interval"),
        pl.lit(vendor_adjusted).alias("vendor_adjusted"),
        pl.lit(utc_now()).alias("written_at"),
        pl.lit(landing_key).alias("landing_key"),
    )


def build_daily_incrementals(ctx: JobContext) -> JobResult:
    """yfinance T−1 + revision window -> canonical; Alpaca raw daily -> L2 events.

    Ownership rule (until M3 voting): yfinance only writes keys it already owns
    or keys nobody owns — it never revises the Stooq bootstrap spine, so vendor
    disagreement cannot masquerade as a revision. Alpaca bars land as events
    only: the third observation voting consumes in M3.
    """
    pull_kt = utc_now()
    revised_detail: dict[str, int] = {"revised": 0, "inserted": 0, "unchanged": 0}
    yf_rows = 0
    dropped_foreign = 0

    yf_landed = _landed_for_trade_date(ctx, [YF_DAILY_DATASET])
    if yf_landed:
        frames = []
        for _dataset, request_key, path in yf_landed:
            ticker = request_key.split(":", 1)[0]
            with open(path, "rb") as fh:
                frames.append(parse_yf_daily_parquet(fh.read(), ticker))
        vendor = pl.concat(frames, how="vertical")
        splits = actions_store.current_splits(ctx.conn)
        raw = reverse_split_adjustment(vendor.drop("adj_close"), splits)
        yf_rows = raw.height

        event_store.append(
            ctx.settings, event_schemas.BAR_EVENTS,
            _bar_events_frame(raw, source="yfinance", vendor_adjusted=True,
                              landing_key=f"{YF_DAILY_DATASET}:{ctx.trade_date.isoformat()}"),
        )

        foreign = daily_bars.keys_owned_by_other_sources(ctx.conn, "yfinance")
        owned = (
            raw.join(foreign, on=["ticker", "bar_date"], how="anti")
            if not foreign.is_empty() else raw
        )
        dropped_foreign = raw.height - owned.height
        revised_detail = daily_bars.upsert_bars(
            ctx.conn, owned, source_set="yfinance", revision_knowledge=pull_kt,
        )

    alpaca_rows = 0
    alpaca_landed = _landed_for_trade_date(ctx, [ALPACA_DAILY_DATASET])
    for _dataset, request_key, path in alpaca_landed:
        ticker = request_key.split(":", 1)[0]
        with open(path, "rb") as fh:
            frame = _parse_alpaca_daily(fh.read(), ticker)
        if frame.is_empty():
            continue
        alpaca_rows += frame.height
        event_store.append(
            ctx.settings, event_schemas.BAR_EVENTS,
            _bar_events_frame(frame, source="alpaca_iex", vendor_adjusted=False,
                              landing_key=f"{ALPACA_DAILY_DATASET}:{request_key}"),
        )

    return JobResult(
        rows_out=revised_detail["inserted"] + revised_detail["revised"],
        detail=(
            f"yf_bars={yf_rows} {revised_detail} foreign_keys_skipped={dropped_foreign} "
            f"alpaca_event_bars={alpaca_rows}"
        ),
    )


def _parse_alpaca_daily(payload: bytes, ticker: str) -> pl.DataFrame:
    """Alpaca raw daily-bars JSON -> (ticker, bar_date, o/h/l/c, volume)."""
    import json
    from datetime import date as date_type
    from datetime import datetime as dt_type

    from argus.core.clocks import ET
    from argus.ops.errors import SchemaDrift

    try:
        body = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SchemaDrift(f"alpaca_iex:{ticker} daily payload not JSON: {exc}",
                          source="alpaca_iex") from exc
    if "bars" not in body:
        raise SchemaDrift(f"alpaca_iex:{ticker} daily payload missing 'bars'",
                          source="alpaca_iex")
    rows: list[dict[str, object]] = []
    for b in body["bars"] or []:
        try:
            ts = dt_type.fromisoformat(str(b["t"]).replace("Z", "+00:00"))
            bar_date: date_type = ts.astimezone(ET).date()
            rows.append(
                {
                    "ticker": ticker.upper(), "bar_date": bar_date,
                    "open": float(b["o"]), "high": float(b["h"]),
                    "low": float(b["l"]), "close": float(b["c"]),
                    "volume": float(b["v"]),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SchemaDrift(f"alpaca_iex:{ticker} malformed bar row {b}: {exc}",
                              source="alpaca_iex") from exc
    if not rows:
        return pl.DataFrame(
            schema={"ticker": pl.Utf8, "bar_date": pl.Date, "open": pl.Float64,
                    "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                    "volume": pl.Float64}
        )
    return pl.DataFrame(rows)
