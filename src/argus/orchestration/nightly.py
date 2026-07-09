"""The ordered nightly job registry (v4 §7.1) + the bootstrap sequence.

Job names are stable across milestones — (job_name, trade_date) is the
idempotency key. The bootstrap reuses the same runner/bookkeeping with extra
one-off steps (b01/b02), so a crashed bootstrap resumes where it stopped.
"""

from __future__ import annotations

from dataclasses import dataclass

from argus.ops.jobs import JobFn
from argus.orchestration import build_jobs
from argus.serving import publish
from argus.sources import alpaca, polygon_ref, stooq, symbol_dirs, yf_daily, yf_minute


@dataclass(frozen=True)
class JobSpec:
    name: str
    fn: JobFn
    # always-run jobs are cheap projections (publish): a partial re-run of the
    # same trade date must refresh them even though an 'ok' row already exists
    always: bool = False


def build_registry() -> list[JobSpec]:
    return [
        JobSpec("j01_symbol_dirs", symbol_dirs.capture),
        JobSpec("j02_yf_daily", yf_daily.capture),
        JobSpec("j03_alpaca_daily", alpaca.capture_daily_bars),
        JobSpec("j04_yf_minute", yf_minute.capture),
        JobSpec("j05_alpaca_quotes", alpaca.capture),
        JobSpec("j06_polygon_ca", polygon_ref.capture_corporate_actions),
        JobSpec("j08_build_actions", build_jobs.build_actions),
        JobSpec("j09_build_daily", build_jobs.build_daily_incrementals),
        JobSpec("j10_vote_seal", build_jobs.vote_and_seal, always=True),
        JobSpec("j12_publish", publish.publish, always=True),
    ]


def bootstrap_registry() -> list[JobSpec]:
    """One-off spine bootstrap: CA drip -> actions -> Stooq history -> bars -> publish.

    Ordering is load-bearing: the split reversal in b02 consumes the splits
    canonicalized by j08, which consumes the payloads landed by j06.
    """
    return [
        JobSpec("j06_polygon_ca", polygon_ref.capture_corporate_actions),
        JobSpec("j08_build_actions", build_jobs.build_actions),
        JobSpec("b01_stooq_capture", stooq.capture),
        JobSpec("b02_build_daily_bars", build_jobs.build_daily_bars),
        JobSpec("j10_vote_seal", build_jobs.vote_and_seal, always=True),
        JobSpec("j12_publish", publish.publish, always=True),
    ]
