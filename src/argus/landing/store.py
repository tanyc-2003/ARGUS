"""L0 landing zone: raw payloads exactly as received, append-only, never fetched twice.

Layout:  {data_root}/landing/{dataset}/date=YYYY-MM-DD/source={source}/{key_hash}.{ext}

The never-fetch-twice guarantee lives HERE (v4 §2.2): jobs call ensure() with a
deterministic request_key before fetching; a manifest hit means the payload is
already on disk and the wire call is skipped. write() is the only writer and it
refuses to overwrite — immutability by construction.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from argus.core.clocks import utc_now
from argus.core.hashing import raw_bytes_hash
from argus.settings import Settings

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

_SAFE = re.compile(r"[^A-Za-z0-9_-]+")  # dots excluded so '..' can never appear in a slug


@dataclass(frozen=True)
class LandedRef:
    dataset: str
    source: str
    request_key: str
    path: Path
    payload_hash: str


def _filename(request_key: str, ext: str) -> str:
    slug = _SAFE.sub("_", request_key)[:80]
    digest = hashlib.sha256(request_key.encode("utf-8")).hexdigest()[:16]
    return f"{slug}.{digest}.{ext}"


def payload_path(
    settings: Settings, dataset: str, source: str, partition_date: date, request_key: str, ext: str
) -> Path:
    return (
        settings.landing_dir
        / dataset
        / f"date={partition_date.isoformat()}"
        / f"source={source}"
        / _filename(request_key, ext)
    )


def ensure(
    conn: duckdb.DuckDBPyConnection, dataset: str, source: str, request_key: str
) -> str | None:
    """Return the landed path if this request has already been captured, else None."""
    row = conn.execute(
        "SELECT path FROM landing_manifest WHERE dataset = ? AND source = ? AND request_key = ?",
        [dataset, source, request_key],
    ).fetchone()
    return str(row[0]) if row else None


def write(
    conn: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    dataset: str,
    source: str,
    request_key: str,
    payload: bytes,
    ext: str,
    partition_date: date,
    knowledge_time: datetime,
    content_type: str = "",
) -> LandedRef:
    """Land a payload. Raises if the request_key was already landed (append-only)."""
    if ensure(conn, dataset, source, request_key) is not None:
        raise FileExistsError(
            f"landing_manifest already has ({dataset}, {source}, {request_key}) — "
            "callers must ensure() before fetching"
        )
    path = payload_path(settings, dataset, source, partition_date, request_key, ext)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, path)  # atomic within a volume

    conn.execute(
        "INSERT INTO landing_manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [dataset, source, request_key, raw_bytes_hash(payload), str(path), content_type,
         len(payload), partition_date, knowledge_time, utc_now()],
    )
    return LandedRef(dataset, source, request_key, path, raw_bytes_hash(payload))
