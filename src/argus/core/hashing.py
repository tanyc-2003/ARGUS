"""Canonical payload hashing.

Revision detection compares payload hashes across nights; the hash must
therefore be invariant to representation noise (dict ordering, float
formatting like 0.30000000000000004) while remaining sensitive to real value
changes. Floats are rounded to 6 decimal places before hashing — well below
any price/volume tolerance we act on.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from typing import Any


def _canonicalize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _canonicalize(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, (list, tuple)):
        return [_canonicalize(v) for v in obj]
    if isinstance(obj, bool):  # bool before int: bool is an int subclass
        return obj
    if isinstance(obj, float):
        if math.isnan(obj):
            return "NaN"
        if math.isinf(obj):
            return "Inf" if obj > 0 else "-Inf"
        rounded = round(obj, 6)
        if rounded == 0.0:
            rounded = 0.0  # collapse -0.0
        return f"{rounded:.6f}"
    if isinstance(obj, (int, str)) or obj is None:
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return hashlib.sha256(obj).hexdigest()
    return str(obj)


def canonical_hash(obj: Any) -> str:
    """SHA-256 over the canonical JSON form of `obj`."""
    canon = _canonicalize(obj)
    payload = json.dumps(canon, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def raw_bytes_hash(payload: bytes) -> str:
    """SHA-256 of raw landed bytes (L0 manifest)."""
    return hashlib.sha256(payload).hexdigest()
