"""The ordered nightly job registry (v4 §7.1) + the bootstrap sequence.

Job names are stable across milestones — (job_name, trade_date) is the
idempotency key. The bootstrap reuses the same runner/bookkeeping with extra
one-off steps (b01/b02), so a crashed bootstrap resumes where it stopped.
"""

from __future__ import annotations

from dataclasses import dataclass

from argus.ops.jobs import JobFn
from argus.orchestration import build_jobs, intraday_jobs, trust_jobs, universe_jobs
from argus.serving import publish
from argus.sources import alpaca, edgar, polygon_ref, symbol_dirs, yf_daily, yf_minute


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
        JobSpec("j02b_stooq_monthly", trust_jobs.stooq_monthly),  # monthly re-pull gate
        # deep history for names newly added to universe.yaml; no-op once each
        # has its spine. Must precede j08/j09 so the new history is split-
        # reversed against the corporate actions j06 lands the same night.
        JobSpec("j02c_yf_backfill", yf_daily.backfill_new_tickers),
        JobSpec("j03_alpaca_daily", alpaca.capture_daily_bars),
        JobSpec("j04_yf_minute", yf_minute.capture),
        JobSpec(alpaca.QUOTES_JOB, alpaca.capture),
        JobSpec("j06_polygon_ca", polygon_ref.capture_corporate_actions),
        JobSpec("j07_polygon_delisted", polygon_ref.capture_delisted),
        JobSpec("j07b_edgar", edgar.capture),
        JobSpec("j08_build_actions", build_jobs.build_actions),
        JobSpec("j09_build_daily", build_jobs.build_daily_incrementals),
        # processes the monthly stooq re-pull payloads (no-op on other nights);
        # a silent vendor rewrite surfaces as SCD-2 revisions through the vote
        JobSpec("j09b_build_stooq", build_jobs.build_daily_bars),
        JobSpec("j10_vote_seal", build_jobs.vote_and_seal, always=True),
        JobSpec("j11_universe_seal", universe_jobs.universe_seal, always=True),
        JobSpec("j11b_intraday_seal", intraday_jobs.intraday_seal, always=True),
        JobSpec("j11c_sector_seal", trust_jobs.sector_seal, always=True),
        JobSpec("j11d_gap_ledger", trust_jobs.gap_ledger_seal, always=True),
        JobSpec("j12_publish", publish.publish, always=True),
        JobSpec("j13_parity_sample", trust_jobs.parity_sample),  # weekly gate
        JobSpec("j15_backup", trust_jobs.backup, always=True),
    ]


def bootstrap_registry() -> list[JobSpec]:
    """One-off spine bootstrap: CA drip -> actions -> deep history -> vote -> publish.

    Ordering is load-bearing: the split reversal consumes the splits
    canonicalized by j08, which consumes the payloads landed by j06.
    The history source is yfinance (period-max) since Stooq closed its
    endpoints behind a proof-of-work challenge (2026-07).
    """
    return [
        JobSpec("j06_polygon_ca", polygon_ref.capture_corporate_actions),
        JobSpec("j08_build_actions", build_jobs.build_actions),
        JobSpec("b01_yf_history", yf_daily.capture_history),
        JobSpec("b02_build_daily", build_jobs.build_daily_incrementals),
        JobSpec("j10_vote_seal", build_jobs.vote_and_seal, always=True),
        JobSpec("j11_universe_seal", universe_jobs.universe_seal, always=True),
        JobSpec("j12_publish", publish.publish, always=True),
    ]
