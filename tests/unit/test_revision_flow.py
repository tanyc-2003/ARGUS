"""M2 exit criteria: revisions open SCD-2 versions with detection-time knowledge,
the as-of query time-travels correctly, and the ownership rule prevents vendor
flip-flop against the bootstrap spine."""

from datetime import UTC, date, datetime

import polars as pl

from argus.canonical import daily_bars
from argus.core.clocks import asof_knowledge_time


def _bar(close: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["AAPL"], "bar_date": [date(2026, 7, 6)],
            "open": [close], "high": [close], "low": [close], "close": [close],
            "volume": [1e6],
        }
    )


def test_fresh_insert_keeps_world_knowledge(conn) -> None:
    daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance")
    row = conn.execute(
        "SELECT knowledge_time, revision_seq FROM bars_daily WHERE is_current"
    ).fetchone()
    assert row[0] == asof_knowledge_time(date(2026, 7, 6))  # bar knowable at its own close
    assert row[1] == 1


def test_revision_carries_detection_knowledge(conn) -> None:
    detection = datetime(2026, 7, 8, 0, 30, tzinfo=UTC)  # the night we noticed
    daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance")
    counts = daily_bars.upsert_bars(
        conn, _bar(100.7), source_set="yfinance", revision_knowledge=detection
    )
    assert counts == {"revised": 1, "inserted": 1, "unchanged": 0}

    rows = conn.execute(
        """
        SELECT revision_seq, is_current, knowledge_time, valid_from, valid_to, close
        FROM bars_daily ORDER BY revision_seq
        """
    ).fetchall()
    v1, v2 = rows
    assert v1[4] == detection  # v1 closed at detection
    assert v2[2] == detection  # v2 becomes knowable at detection, NOT at the bar date
    assert v2[3] == detection
    assert (v1[5], v2[5]) == (100.0, 100.7)


def test_asof_time_travel_across_revision(conn) -> None:
    detection = datetime(2026, 7, 8, 0, 30, tzinfo=UTC)
    daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance")
    daily_bars.upsert_bars(
        conn, _bar(100.7), source_set="yfinance", revision_knowledge=detection
    )
    before = daily_bars.bars_asof(
        conn, "AAPL", date(2026, 7, 6), datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
    )
    after = daily_bars.bars_asof(
        conn, "AAPL", date(2026, 7, 6), datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
    )
    assert before == (100.0, 1)  # what we believed before the correction was detected
    assert after == (100.7, 2)


def test_unchanged_rerun_is_noop(conn) -> None:
    daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance")
    counts = daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance")
    assert counts == {"revised": 0, "inserted": 0, "unchanged": 1}


def test_grade_upgrade_is_a_revision(conn) -> None:
    """Same price, better belief (single-source -> confirmed) must open a
    revision: the hash covers grade/source columns, not just prices."""
    from datetime import UTC, datetime

    detection = datetime(2026, 7, 8, 0, 30, tzinfo=UTC)
    daily_bars.upsert_bars(conn, _bar(100.0), source_set="yfinance",
                           grade="degraded", single_source=True)
    counts = daily_bars.upsert_bars(
        conn, _bar(100.0), source_set="yfinance,stooq", grade="good",
        single_source=False, revision_knowledge=detection,
    )
    assert counts == {"revised": 1, "inserted": 1, "unchanged": 0}
    row = conn.execute(
        "SELECT grade, single_source, revision_seq FROM bars_daily WHERE is_current"
    ).fetchone()
    assert row == ("good", False, 2)
