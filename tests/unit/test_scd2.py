from datetime import UTC, datetime

import polars as pl

from argus.canonical import scd2

KEY = ["ticker", "bar_date"]
VALS = ["open", "high", "low", "close", "volume", "source_set", "grade", "single_source"]


def _frame(close: float, hash_: str, kt: datetime) -> pl.DataFrame:
    from datetime import date

    return pl.DataFrame(
        {
            "ticker": ["AAPL"], "bar_date": [date(2020, 8, 28)],
            "open": [close], "high": [close], "low": [close], "close": [close],
            "volume": [1e6], "source_set": ["stooq"], "grade": ["degraded"],
            "single_source": [True], "payload_hash": [hash_], "knowledge_time": [kt],
        }
    )


def test_first_insert_is_version_one(conn) -> None:
    t0 = datetime(2026, 7, 7, 23, 0, tzinfo=UTC)
    counts = scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(100.0, "h1", t0))
    assert counts == {"revised": 0, "inserted": 1, "unchanged": 0}
    row = conn.execute(
        "SELECT revision_seq, is_current, valid_to FROM bars_daily"
    ).fetchone()
    assert row == (1, True, None)


def test_same_hash_is_noop(conn) -> None:
    t0 = datetime(2026, 7, 7, 23, 0, tzinfo=UTC)
    scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(100.0, "h1", t0))
    counts = scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(100.0, "h1", t0))
    assert counts == {"revised": 0, "inserted": 0, "unchanged": 1}
    n = conn.execute("SELECT COUNT(*) FROM bars_daily").fetchone()[0]
    assert n == 1


def test_changed_hash_opens_revision_with_abutting_validity(conn) -> None:
    t0 = datetime(2026, 7, 7, 23, 0, tzinfo=UTC)
    t1 = datetime(2026, 7, 8, 23, 0, tzinfo=UTC)
    scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(100.0, "h1", t0))
    counts = scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(101.0, "h2", t1))
    assert counts == {"revised": 1, "inserted": 1, "unchanged": 0}

    rows = conn.execute(
        """
        SELECT revision_seq, is_current, valid_from, valid_to, close
        FROM bars_daily ORDER BY revision_seq
        """
    ).fetchall()
    assert len(rows) == 2
    v1, v2 = rows
    assert v1[1] is False and v2[1] is True
    assert v1[3] == v2[2]  # intervals abut exactly
    assert (v1[4], v2[4]) == (100.0, 101.0)
    assert (v1[0], v2[0]) == (1, 2)


def test_exactly_one_current_per_key(conn) -> None:
    t = datetime(2026, 7, 7, 23, 0, tzinfo=UTC)
    for i, h in enumerate(["h1", "h2", "h3"]):
        scd2.upsert(conn, "bars_daily", KEY, VALS, _frame(100.0 + i, h, t))
    n = conn.execute(
        "SELECT COUNT(*) FROM bars_daily WHERE is_current"
    ).fetchone()[0]
    assert n == 1


def test_missing_column_rejected(conn) -> None:
    import pytest

    with pytest.raises(ValueError, match="missing"):
        scd2.upsert(conn, "bars_daily", KEY, VALS, pl.DataFrame({"ticker": ["A"]}))
