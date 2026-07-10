"""Chaos drill: every external source dead at once (v4 §9, M7 exit criteria).

pytest-socket blocks the network for the whole suite, so running the REAL
nightly registry here means every capture job hits a dead wire. The night must
COMPLETE: failures recorded per job, dead letters filed, the seals and publish
still run on whatever local state exists, and nothing raises unhandled.
Silence-is-not-success: the exit code must be non-zero so the scheduler's
Last Run Result flags the degradation.
"""

from datetime import UTC, datetime

import pytest

from argus import db as db_module
from argus.ops.ratelimit import TokenBucket
from argus.orchestration.runner import run_nightly

NOW = datetime(2026, 7, 7, 22, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    # the wire is dead in these drills — real token-bucket sleeps between
    # doomed requests would stretch the drill to minutes for nothing
    monkeypatch.setattr(TokenBucket, "acquire", lambda self, n=1.0: None)


def test_all_sources_dead_night_still_completes(settings) -> None:
    exit_code = run_nightly(settings, now=NOW)  # the real registry, no keys, no network

    conn = db_module.open_migrated(settings.db_path)
    try:
        statuses = dict(conn.execute(
            "SELECT job_name, status FROM job_runs WHERE trade_date = DATE '2026-07-07'"
        ).fetchall())

        # keyless sources skip cleanly; keyed-off network sources fail loudly
        assert statuses["j03_alpaca_daily"] == "skipped_source_down"
        assert statuses["j05_alpaca_quotes"] == "skipped_source_down"
        assert statuses["j06_polygon_ca"] == "skipped_source_down"
        assert statuses["j07_polygon_delisted"] == "skipped_source_down"
        assert statuses["j07b_edgar"] == "skipped_source_down"
        assert statuses["j13_parity_sample"] == "skipped_source_down"
        assert statuses["j01_symbol_dirs"] == "failed"  # network dead -> loud, not silent

        # the local seals and the publish still complete on empty state
        assert statuses["j10_vote_seal"] == "ok"
        assert statuses["j11_universe_seal"] == "ok"
        assert statuses["j11b_intraday_seal"] == "ok"
        assert statuses["j11c_sector_seal"] == "ok"
        assert statuses["j11d_gap_ledger"] == "ok"
        assert statuses["j12_publish"] == "ok"
        assert statuses["j15_backup"] == "ok"

        # failures are visible: dead letters + non-zero exit
        n_dlq = conn.execute(
            "SELECT COUNT(*) FROM dead_letter WHERE resolved_at IS NULL"
        ).fetchone()[0]
        assert n_dlq >= 1
        assert exit_code == 1

        # and the consumer still got a sealed, contract-valid (empty) copy
        assert settings.serving_db_path.exists()
    finally:
        conn.close()


def test_second_dead_night_is_stable(settings) -> None:
    """A crashloop would open the circuit and spam the DLQ — a second identical
    night must be no worse than the first."""
    run_nightly(settings, now=NOW)
    conn = db_module.open_migrated(settings.db_path)
    dlq_after_first = conn.execute("SELECT COUNT(*) FROM dead_letter").fetchone()[0]
    conn.close()

    exit_code = run_nightly(settings, now=NOW)  # same trade date: captures retry, seals re-run
    conn = db_module.open_migrated(settings.db_path)
    dlq_after_second = conn.execute("SELECT COUNT(*) FROM dead_letter").fetchone()[0]
    conn.close()

    assert exit_code == 1
    # the DLQ grows at most linearly with failing jobs, never explodes
    assert dlq_after_second <= dlq_after_first * 2 + 2
