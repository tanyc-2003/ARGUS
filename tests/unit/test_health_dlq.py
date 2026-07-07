from datetime import timedelta

import argus.ops.health as health
from argus.core.clocks import utc_now
from argus.ops import dlq
from argus.ops.errors import ErrorClass


def test_opens_after_three_failures(conn) -> None:
    for _ in range(2):
        health.record_failure(conn, "stooq")
    assert not health.is_open(conn, "stooq")
    health.record_failure(conn, "stooq")
    assert health.is_open(conn, "stooq")


def test_success_closes_circuit(conn) -> None:
    for _ in range(3):
        health.record_failure(conn, "stooq")
    health.record_success(conn, "stooq")
    assert not health.is_open(conn, "stooq")


def test_cooldown_reopens_for_probe(conn, monkeypatch) -> None:
    for _ in range(3):
        health.record_failure(conn, "stooq")
    assert health.is_open(conn, "stooq")
    later = utc_now() + health.COOLDOWN + timedelta(minutes=1)
    monkeypatch.setattr(health, "utc_now", lambda: later)
    assert not health.is_open(conn, "stooq")  # half-open: next attempt may probe


def test_unknown_source_is_closed(conn) -> None:
    assert not health.is_open(conn, "never_seen")


def test_dlq_push_list_resolve(conn) -> None:
    dlq.push(conn, job_name="j", error_class=ErrorClass.TRANSPORT, detail="d1", source="s")
    dlq.push(conn, job_name="j", error_class=ErrorClass.UNKNOWN, detail="d2")
    assert dlq.open_depth(conn) == 2
    entries = dlq.list_open(conn)
    assert len(entries) == 2
    dlq.resolve(conn, entries[0]["id"])
    assert dlq.open_depth(conn) == 1
