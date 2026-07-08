"""The two clocks (v4 kept-principles): `knowledge_time` vs `written_at`.

Every module that needs "now" goes through this file — a source-tree test
enforces that nothing else calls datetime.now()/utcnow() directly. That keeps
knowledge-stamping rules in one auditable place:

  * nightly pulls        -> knowledge_time = pull time            (pull_knowledge_time)
  * bulk-file bootstraps -> knowledge_time = file's as-of date    (asof_knowledge_time)
  * revision detection   -> knowledge_time = detection time       (pull_knowledge_time)

`written_at` is always wall clock and never participates in as-of logic.
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
