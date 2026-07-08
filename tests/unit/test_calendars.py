from datetime import UTC, date, datetime

import pytest

from argus.core import calendars


def test_half_day_detected_2024_07_03() -> None:
    s = calendars.session_info(date(2024, 7, 3))
    assert s is not None
    assert s.is_half_day
    assert s.close_utc == datetime(2024, 7, 3, 17, 0, tzinfo=UTC)  # 13:00 EDT


def test_independence_day_closed() -> None:
    assert not calendars.is_session(date(2024, 7, 4))


def test_regular_summer_close_is_2000_utc() -> None:
    s = calendars.session_info(date(2024, 7, 8))
    assert s is not None
    assert not s.is_half_day
    assert s.close_utc == datetime(2024, 7, 8, 20, 0, tzinfo=UTC)  # 16:00 EDT


def test_regular_winter_close_is_2100_utc() -> None:
    s = calendars.session_info(date(2024, 1, 8))
    assert s is not None
    assert s.close_utc == datetime(2024, 1, 8, 21, 0, tzinfo=UTC)  # 16:00 EST


def test_latest_completed_after_close_is_today() -> None:
    # Tuesday 2026-07-07 at 21:45 UTC — well after the 20:00 UTC close
    now = datetime(2026, 7, 7, 21, 45, tzinfo=UTC)
    assert calendars.latest_completed_session(now) == date(2026, 7, 7)


def test_latest_completed_before_close_is_previous_session() -> None:
    now = datetime(2026, 7, 7, 15, 0, tzinfo=UTC)  # during RTH
    assert calendars.latest_completed_session(now) == date(2026, 7, 6)


def test_latest_completed_on_weekend_is_friday() -> None:
    now = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)  # Sunday
    assert calendars.latest_completed_session(now) == date(2026, 7, 10)


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError):
        calendars.latest_completed_session(datetime(2026, 7, 7, 21, 45))


def test_previous_sessions_skip_non_sessions() -> None:
    # 2026-07-03 (July 4 observed) and the weekend must not appear
    got = calendars.previous_sessions(date(2026, 7, 7), 3)
    assert got == sorted(got)
    assert len(got) == 3
    assert got[-1] == date(2026, 7, 7)
    assert date(2026, 7, 4) not in got
    assert date(2026, 7, 5) not in got


def test_refresh_market_sessions_idempotent(conn) -> None:
    n1 = calendars.refresh_market_sessions(conn, date(2026, 6, 1), date(2026, 7, 31))
    n2 = calendars.refresh_market_sessions(conn, date(2026, 6, 1), date(2026, 7, 31))
    assert n1 == n2 > 0
    count = conn.execute(
        "SELECT COUNT(*) FROM market_sessions WHERE session_date BETWEEN ? AND ?",
        [date(2026, 6, 1), date(2026, 7, 31)],
    ).fetchone()[0]
    assert count == n1
