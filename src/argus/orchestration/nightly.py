"""The ordered nightly job registry (v4 §7.1).

M0 ships the calendar gate plus the three capture-only, calendar-compounding
jobs. Later milestones append processing jobs to this list; names are stable
because (job_name, trade_date) is the idempotency key.
"""

from __future__ import annotations

from dataclasses import dataclass

from argus.ops.jobs import JobFn
from argus.sources import alpaca, symbol_dirs, yf_minute


@dataclass(frozen=True)
class JobSpec:
    name: str
    fn: JobFn


def build_registry() -> list[JobSpec]:
    return [
        JobSpec("j01_symbol_dirs", symbol_dirs.capture),
        JobSpec("j04_yf_minute", yf_minute.capture),
        JobSpec("j05_alpaca_quotes", alpaca.capture),
    ]
