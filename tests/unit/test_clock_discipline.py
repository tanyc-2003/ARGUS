"""Two-clock discipline: wall-clock reads happen ONLY in core/clocks.py."""

from datetime import UTC, date, datetime
from pathlib import Path

from argus.core.clocks import asof_knowledge_time, end_of_day_utc

SRC = Path(__file__).resolve().parents[2] / "src" / "argus"
FORBIDDEN = ("datetime.now(", "datetime.utcnow(", ".today()")


def test_no_direct_wall_clock_reads_outside_clocks() -> None:
    offenders: list[str] = []
    for py in SRC.rglob("*.py"):
        if py.name == "clocks.py":
            continue
        text = py.read_text(encoding="utf-8")
        for pattern in FORBIDDEN:
            if pattern in text:
                offenders.append(f"{py.relative_to(SRC)}: {pattern}")
    assert not offenders, f"wall-clock reads outside core/clocks.py: {offenders}"


def test_asof_knowledge_time_is_end_of_day_eastern() -> None:
    # summer (EDT, UTC-4): 23:59:59 ET -> 03:59:59 UTC next day
    assert asof_knowledge_time(date(2016, 6, 15)) == datetime(2016, 6, 16, 3, 59, 59, tzinfo=UTC)
    # winter (EST, UTC-5): 23:59:59 ET -> 04:59:59 UTC next day
    assert asof_knowledge_time(date(2016, 1, 15)) == datetime(2016, 1, 16, 4, 59, 59, tzinfo=UTC)


def test_end_of_day_utc_alias() -> None:
    assert end_of_day_utc(date(2020, 8, 28)) == asof_knowledge_time(date(2020, 8, 28))
