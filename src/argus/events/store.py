"""L2 immutable event store: append-only Parquet, the system of record.

There is no update or delete API — `append` writes a new part file, `scan`
reads them all. The DuckDB canonical layer is a disposable projection that
`argus rebuild` (M3) regenerates from these files.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl

from argus.events.schemas import SCHEMAS
from argus.settings import Settings


def _event_dir(settings: Settings, event_type: str) -> Path:
    if event_type not in SCHEMAS:
        raise ValueError(f"unknown event type: {event_type}")
    return settings.events_dir / event_type


def append(settings: Settings, event_type: str, df: pl.DataFrame) -> Path | None:
    """Append a batch of events as a new Parquet part file. Returns None on empty."""
    schema = SCHEMAS[event_type]
    if df.is_empty():
        return None
    missing = set(schema) - set(df.columns)
    if missing:
        raise ValueError(f"{event_type}: missing columns {sorted(missing)}")
    out = df.select([pl.col(c).cast(dt) for c, dt in schema.items()])
    target_dir = _event_dir(settings, event_type)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"part-{uuid.uuid4().hex}.parquet"
    tmp = path.with_suffix(".parquet.tmp")
    out.write_parquet(tmp)
    tmp.replace(path)
    return path


def scan(settings: Settings, event_type: str) -> pl.LazyFrame:
    """Lazy view over every event of a type; empty frame (right schema) if none."""
    target_dir = _event_dir(settings, event_type)  # validates event_type first
    schema = SCHEMAS[event_type]
    parts = sorted(target_dir.glob("part-*.parquet")) if target_dir.exists() else []
    if not parts:
        return pl.DataFrame(schema=schema).lazy()
    return pl.scan_parquet([str(p) for p in parts])
