"""L2 event schemas. Events are immutable observations with the two clocks.

Every event row carries: knowledge_time (when the world could know),
written_at (when ARGUS wrote it), payload_hash (canonical content hash) and
enough lineage to trace back to the L0 landing that produced it.
"""

from __future__ import annotations

import polars as pl

# polars dtypes are classes (pl.Float64) or parametrized instances (pl.Datetime(...))
PolarsType = pl.DataType | type[pl.DataType]

BAR_EVENTS = "bar_events"
ACTION_EVENTS = "action_events"

BAR_EVENT_SCHEMA: dict[str, PolarsType] = {
    "event_id": pl.Utf8,
    "source": pl.Utf8,
    "ticker": pl.Utf8,
    "interval": pl.Utf8,  # '1d' (minute events arrive in M5)
    "bar_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,  # RAW (post split-reversal for vendor-adjusted sources)
    "volume": pl.Float64,
    "vendor_adjusted": pl.Boolean,  # True if the vendor served split-adjusted prices
    "reversal_factor": pl.Float64,  # factor applied at L1 to reconstruct raw (1.0 = none)
    "knowledge_time": pl.Datetime(time_unit="us", time_zone="UTC"),
    "written_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "payload_hash": pl.Utf8,
    "landing_key": pl.Utf8,  # dataset:source:request_key of the L0 payload
}

ACTION_EVENT_SCHEMA: dict[str, PolarsType] = {
    "event_id": pl.Utf8,
    "source": pl.Utf8,
    "ticker": pl.Utf8,
    "action_type": pl.Utf8,  # 'split' | 'dividend'
    "ex_date": pl.Date,
    "ratio": pl.Float64,  # splits: to/from (2.0 for a 2:1); dividends: null
    "cash_amount": pl.Float64,  # dividends; splits: null
    "declared_date": pl.Date,
    "knowledge_time": pl.Datetime(time_unit="us", time_zone="UTC"),
    "written_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "payload_hash": pl.Utf8,
    "landing_key": pl.Utf8,
}

SCHEMAS: dict[str, dict[str, PolarsType]] = {
    BAR_EVENTS: BAR_EVENT_SCHEMA,
    ACTION_EVENTS: ACTION_EVENT_SCHEMA,
}
