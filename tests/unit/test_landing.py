from datetime import date

import pytest

from argus.core.clocks import pull_knowledge_time
from argus.landing import store


def _land(
    conn, settings, key: str = "spy:2026-07-07", payload: bytes = b"hello"
) -> store.LandedRef:
    return store.write(
        conn, settings,
        dataset="test_ds", source="test_src", request_key=key,
        payload=payload, ext="txt", partition_date=date(2026, 7, 7),
        knowledge_time=pull_knowledge_time(),
    )


def test_write_then_ensure_roundtrip(conn, settings) -> None:
    ref = _land(conn, settings)
    assert ref.path.exists()
    assert ref.path.read_bytes() == b"hello"
    assert store.ensure(conn, "test_ds", "test_src", "spy:2026-07-07") == str(ref.path)


def test_ensure_miss_returns_none(conn, settings) -> None:
    assert store.ensure(conn, "test_ds", "test_src", "nope") is None


def test_duplicate_write_refused(conn, settings) -> None:
    _land(conn, settings)
    with pytest.raises(FileExistsError):
        _land(conn, settings, payload=b"different bytes")


def test_partition_layout(conn, settings) -> None:
    ref = _land(conn, settings)
    rel = ref.path.relative_to(settings.landing_dir)
    parts = rel.parts
    assert parts[0] == "test_ds"
    assert parts[1] == "date=2026-07-07"
    assert parts[2] == "source=test_src"


def test_hostile_request_key_sanitized(conn, settings) -> None:
    ref = _land(conn, settings, key='../..\\weird key?*|"<>:')
    assert ref.path.exists()
    assert ".." not in ref.path.name
    assert ref.path.parent.name == "source=test_src"


def test_distinct_keys_land_distinct_files(conn, settings) -> None:
    a = _land(conn, settings, key="k1", payload=b"a")
    b = _land(conn, settings, key="k2", payload=b"b")
    assert a.path != b.path
    assert a.payload_hash != b.payload_hash
