from datetime import UTC, date, datetime

import polars as pl
import pytest

from argus.events import schemas, store


def _action_rows(n: int = 2) -> pl.DataFrame:
    now = datetime(2026, 7, 8, 1, 0, tzinfo=UTC)
    return pl.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(n)],
            "source": ["polygon"] * n,
            "ticker": ["AAPL"] * n,
            "action_type": ["split"] * n,
            "ex_date": [date(2020, 8, 31)] * n,
            "ratio": [4.0] * n,
            "cash_amount": [None] * n,
            "declared_date": [None] * n,
            "knowledge_time": [now] * n,
            "written_at": [now] * n,
            "payload_hash": [f"h{i}" for i in range(n)],
            "landing_key": ["k"] * n,
        }
    )


def test_append_then_scan_roundtrip(settings) -> None:
    settings.ensure_dirs()
    assert store.append(settings, schemas.ACTION_EVENTS, _action_rows(2)) is not None
    assert store.append(settings, schemas.ACTION_EVENTS, _action_rows(3)) is not None
    got = store.scan(settings, schemas.ACTION_EVENTS).collect()
    assert got.height == 5
    assert dict(got.schema) == schemas.ACTION_EVENT_SCHEMA


def test_empty_append_is_noop(settings) -> None:
    settings.ensure_dirs()
    assert store.append(settings, schemas.ACTION_EVENTS,
                        pl.DataFrame(schema=schemas.ACTION_EVENT_SCHEMA)) is None


def test_scan_without_data_returns_typed_empty(settings) -> None:
    got = store.scan(settings, schemas.BAR_EVENTS).collect()
    assert got.is_empty()
    assert dict(got.schema) == schemas.BAR_EVENT_SCHEMA


def test_missing_columns_rejected(settings) -> None:
    settings.ensure_dirs()
    with pytest.raises(ValueError, match="missing columns"):
        store.append(settings, schemas.ACTION_EVENTS, pl.DataFrame({"event_id": ["x"]}))


def test_unknown_event_type_rejected(settings) -> None:
    with pytest.raises(ValueError, match="unknown event type"):
        store.scan(settings, "nope")
