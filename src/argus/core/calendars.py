"""Exchange sessions and the trade-date resolver — the ONLY ET<->UTC seam.

Storage is UTC everywhere; exchange-local time exists only inside this module.
The trade date is always computed from the XNYS calendar, never from the
machine's local clock (this box runs on UK time).

`exchange_calendars` returns pandas objects; pandas is quarantined here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import TYPE_CHECKING

import exchange_calendars as xcals

from argus.core.clocks import ET, utc_now

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

EXCHANGE = "XNYS"
_CAL_START = "2005-01-01"  # comfortably before any 10y+ bootstrap window
_CAL_END_DAYS = 730  # build the calendar ~2y forward (default is 1y and runner needs +370d)
_FULL_DAY_CLOSE_ET_HOUR = 16


@dataclass(frozen=True)
class Session:
    session_date: date
    open_utc: datetime
    close_utc: datetime
    is_half_day: bool


@lru_cache(maxsize=1)
def _calendar() -> xcals.ExchangeCalendar:
    end = utc_now().date() + timedelta(days=_CAL_END_DAYS)
    return xcals.get_calendar(EXCHANGE, start=_CAL_START, end=str(end))


def sessions_between(start: date, end: date) -> list[Session]:
    """All XNYS sessions with `start <= session_date <= end`, UTC open/close.

    The range is clamped to the calendar's own bounds — asking past the built
    horizon returns what exists rather than raising DateOutOfBounds.
    """
    cal = _calendar()
    start = max(start, cal.first_session.date())
    end = min(end, cal.last_session.date())
    if start > end:
        return []
    out: list[Session] = []
    for ts in cal.sessions_in_range(str(start), str(end)):
        session_date = ts.date()
        open_utc = cal.opens.loc[ts].to_pydatetime().astimezone(UTC)
        close_utc = cal.closes.loc[ts].to_pydatetime().astimezone(UTC)
        close_et = close_utc.astimezone(ET)
        out.append(
            Session(
                session_date=session_date,
                open_utc=open_utc,
                close_utc=close_utc,
                is_half_day=close_et.hour < _FULL_DAY_CLOSE_ET_HOUR,
            )
        )
    return out


def is_session(d: date) -> bool:
    return bool(_calendar().is_session(str(d)))


def latest_completed_session(now_utc: datetime) -> date | None:
    """The most recent session whose close is <= now — the trade date to seal.

    Pure function of its argument: callers pass clocks.utc_now() (or a test value).
    """
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    window = sessions_between(now_utc.date() - timedelta(days=10), now_utc.date())
    completed = [s for s in window if s.close_utc <= now_utc]
    return completed[-1].session_date if completed else None


def previous_sessions(reference: date, n: int) -> list[date]:
    """The last `n` session dates with session_date <= reference, ascending."""
    window = sessions_between(reference - timedelta(days=max(2 * n + 10, 15)), reference)
    return [s.session_date for s in window][-n:]


def sessions_within(reference: date, days: int) -> list[date]:
    """Session dates in the trailing `days` calendar days, ascending (incl. reference)."""
    return [s.session_date for s in sessions_between(reference - timedelta(days=days), reference)]


def session_info(d: date) -> Session | None:
    found = sessions_between(d, d)
    return found[0] if found else None


def refresh_market_sessions(conn: duckdb.DuckDBPyConnection, start: date, end: date) -> int:
    """Idempotently (re)write the market_sessions rows for [start, end]."""
    rows = sessions_between(start, end)
    conn.execute(
        "DELETE FROM market_sessions WHERE exchange = ? AND session_date BETWEEN ? AND ?",
        [EXCHANGE, start, end],
    )
    for s in rows:
        conn.execute(
            "INSERT INTO market_sessions VALUES (?, ?, ?, ?, ?)",
            [EXCHANGE, s.session_date, s.open_utc, s.close_utc, s.is_half_day],
        )
    return len(rows)
