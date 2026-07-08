"""Polygon corporate-action payload normalization (splits + dividends).

knowledge_time for a corporate action = when the world could know it. We stamp
conservatively at the ex-date (the world certainly knew by then; declaration
is earlier, so this never manufactures early knowledge for as-of queries).
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import polars as pl

from argus.core.clocks import asof_knowledge_time, utc_now
from argus.core.hashing import canonical_hash
from argus.events.schemas import ACTION_EVENT_SCHEMA
from argus.ops.errors import SchemaDrift


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def parse_polygon_actions(
    payload_bytes: bytes, *, kind: str, ticker: str, landing_key: str
) -> pl.DataFrame:
    """One landed Polygon splits/dividends JSON -> action_events rows.

    kind: 'polygon_splits' | 'polygon_dividends' (the landing dataset name).
    """
    try:
        payload = json.loads(payload_bytes)
    except json.JSONDecodeError as exc:
        raise SchemaDrift(f"polygon:{kind}:{ticker} not JSON: {exc}", source="polygon") from exc
    if "status" not in payload:
        raise SchemaDrift(f"polygon:{kind}:{ticker} missing 'status'", source="polygon")
    results = payload.get("results") or []

    rows: list[dict[str, Any]] = []
    written = utc_now()
    for r in results:
        if kind == "polygon_splits":
            ex = _parse_date(r.get("execution_date"))
            frm = r.get("split_from")
            to = r.get("split_to")
            if ex is None or not frm or not to:
                raise SchemaDrift(
                    f"polygon:splits:{ticker} row missing execution_date/split_from/split_to: {r}",
                    source="polygon",
                )
            ratio = float(to) / float(frm)
            rows.append(
                {
                    "action_type": "split", "ex_date": ex, "ratio": ratio,
                    "cash_amount": None, "declared_date": None,
                }
            )
        else:
            ex = _parse_date(r.get("ex_dividend_date"))
            cash = r.get("cash_amount")
            if ex is None or cash is None:
                raise SchemaDrift(
                    f"polygon:dividends:{ticker} row missing ex_dividend_date/cash_amount: {r}",
                    source="polygon",
                )
            rows.append(
                {
                    "action_type": "dividend", "ex_date": ex, "ratio": None,
                    "cash_amount": float(cash),
                    "declared_date": _parse_date(r.get("declaration_date")),
                }
            )

    if not rows:
        return pl.DataFrame(schema=ACTION_EVENT_SCHEMA)

    full = [
        {
            "event_id": uuid.uuid4().hex,
            "source": "polygon",
            "ticker": ticker.upper(),
            **row,
            "knowledge_time": asof_knowledge_time(row["ex_date"]),
            "written_at": written,
            "payload_hash": canonical_hash(
                {
                    "ticker": ticker.upper(), "type": row["action_type"],
                    "ex": row["ex_date"], "ratio": row["ratio"], "cash": row["cash_amount"],
                }
            ),
            "landing_key": landing_key,
        }
        for row in rows
    ]
    return pl.DataFrame(full).select(
        [pl.col(c).cast(dt) for c, dt in ACTION_EVENT_SCHEMA.items()]
    )
