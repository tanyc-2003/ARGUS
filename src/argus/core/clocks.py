"""The two clocks (v4 kept-principles): `knowledge_time` vs `written_at`.

`knowledge_time` models when the WORLD could know a fact; `written_at` records
when ARGUS learned it. Only knowledge_time participates in as-of logic:

  * fresh observations (nightly pulls)  -> pull time           (pull_knowledge_time)
  * backfilled world facts (bootstrap bars, historical splits/dividends)
                                        -> the fact's own date (asof_knowledge_time) —
       a daily bar is knowable at its own close; a corporate action by its
       ex-date. This is what makes historical as-of adjustment reconstructable
       and maps directly onto the dashboard's `knowledge_date = bar close date`.
  * corrections/revisions (M2+)         -> detection time      (pull_knowledge_time) —
       as-of queries before the detection instant return the pre-revision value.

Every module that needs "now" goes through this file — a source-tree test
enforces that nothing else calls datetime.now()/utcnow() directly.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def utc_now() -> datetime:
    """The single wall-clock read for the whole codebase."""
    return datetime.now(tz=UTC)


def pull_knowledge_time() -> datetime:
    """Knowledge time for data observed by a live pull: the pull moment itself."""
    return utc_now()


def asof_knowledge_time(asof: date) -> datetime:
    """Knowledge time for bulk/bootstrap data carrying a vendor as-of date.

    We honestly did not know a 2016 bar before 2016 ended: stamp end-of-day
    23:59:59 America/New_York on the as-of date, expressed in UTC.
    """
    return datetime.combine(asof, time(23, 59, 59), tzinfo=ET).astimezone(UTC)


def end_of_day_utc(d: date) -> datetime:
    """End of the exchange-local calendar day `d` in UTC (as-of query upper bound)."""
    return asof_knowledge_time(d)
