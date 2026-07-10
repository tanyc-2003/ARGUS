import json
from datetime import date

import polars as pl
import pytest

from argus.config_files import load_sic_map, sic_to_sector
from argus.core.clocks import pull_knowledge_time
from argus.landing import store
from argus.ops.jobs import JobResult
from argus.orchestration import trust_jobs

# ---- SIC mapping -------------------------------------------------------------

def _repo_ranges():  # the real map, loaded via a settings stub
    class S:
        config_dir = __import__("pathlib").Path(__file__).resolve().parents[2] / "config"

    return load_sic_map(S())


def test_sic_mapping_golden() -> None:
    ranges = _repo_ranges()
    assert sic_to_sector(3571, ranges) == "XLK"   # Apple: electronic computers
    assert sic_to_sector("6022", ranges) == "XLF"  # banks
    assert sic_to_sector(2834, ranges) == "XLV"   # pharma
    assert sic_to_sector(9999, ranges) is None    # unmapped -> not served
    assert sic_to_sector(None, ranges) is None
    assert sic_to_sector("garbage", ranges) is None


# ---- sector seal ---------------------------------------------------------------

def _land_submission(ctx, ticker: str, cik: str, sic: str, desc: str) -> None:
    store.write(
        ctx.conn, ctx.settings, dataset="edgar_submissions", source="edgar",
        request_key=f"{ticker}:{cik}",
        payload=json.dumps({"sic": sic, "sicDescription": desc}).encode(),
        ext="json", partition_date=ctx.trade_date, knowledge_time=pull_knowledge_time(),
    )


def test_sector_seal_projects_landed_submissions(ctx, test_config_dir) -> None:
    (test_config_dir / "sic_sector_map.yaml").write_text(
        "ranges:\n  - {lo: 3570, hi: 3579, sector: XLK}\n", encoding="utf-8"
    )
    _land_submission(ctx, "AAPL", "320193", "3571", "Electronic Computers")
    result = trust_jobs.sector_seal(ctx)
    assert result.rows_out == 1
    row = ctx.conn.execute(
        "SELECT ticker, sector, industry FROM sectors"
    ).fetchone()
    assert row == ("AAPL", "XLK", "Electronic Computers")

    # idempotent: already-known tickers are skipped
    assert trust_jobs.sector_seal(ctx).rows_out == 0


# ---- cadence gate --------------------------------------------------------------

def test_due_gating(ctx, monkeypatch) -> None:
    assert trust_jobs._due(ctx, "j13_parity_sample", 7)  # never ran -> due

    from argus.ops.jobs import run_job

    run_job(ctx, "j13_parity_sample", lambda c: JobResult(detail="bars_checked=3"))
    assert not trust_jobs._due(ctx, "j13_parity_sample", 7)  # ran today -> not due

    # a 'not due' marker run must NOT satisfy the gate
    run_job(ctx, "j99_gate_test", lambda c: JobResult(detail=f"{trust_jobs.NOT_DUE} (x)"))
    assert trust_jobs._due(ctx, "j99_gate_test", 7)


def test_stooq_monthly_gate(ctx, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(trust_jobs.stooq, "capture",
                        lambda c: (calls.append(1), JobResult(rows_out=2))[1])
    first = trust_jobs.stooq_monthly(ctx)
    assert first.rows_out == 2 and len(calls) == 1

    from argus.ops.jobs import run_job
    run_job(ctx, "j02b_stooq_monthly", trust_jobs.stooq_monthly, force=True)
    gated = trust_jobs.stooq_monthly(ctx)
    assert trust_jobs.NOT_DUE in gated.detail and len(calls) == 2


def test_stooq_monthly_backs_off_after_failure(ctx, monkeypatch) -> None:
    """A blocked source (PoW challenge) must retry weekly, not spam nightly."""
    from argus.ops.errors import SchemaDrift
    from argus.ops.jobs import run_job

    def blocked(c):  # type: ignore[no-untyped-def]
        raise SchemaDrift("stooq: html challenge page", source="stooq")

    monkeypatch.setattr(trust_jobs.stooq, "capture", blocked)
    assert run_job(ctx, "j02b_stooq_monthly", trust_jobs.stooq_monthly) == "failed"

    gated = trust_jobs.stooq_monthly(ctx)  # same trade date: failure < 7 days ago
    assert "blocked source" in gated.detail


def test_yf_history_capture_keys_and_window(ctx) -> None:
    import pandas as pd

    from argus.ops.ratelimit import TokenBucket
    from argus.sources import yf_daily

    clock = {"t": 0.0}

    def tick() -> float:
        clock["t"] += 0.001
        return clock["t"]

    bucket = TokenBucket(rate_per_sec=1e6, capacity=1e6, clock=tick, sleep=lambda _: None)
    windows: list[tuple] = []

    def downloader(t, s, e):  # type: ignore[no-untyped-def]
        windows.append((t, s, e))
        idx = pd.DatetimeIndex([pd.Timestamp("2026-07-06")], name="Date")
        return pd.DataFrame(
            {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0],
             "Adj Close": [1.0], "Volume": [1.0]}, index=idx,
        )

    result = yf_daily.capture_history(ctx, downloader=downloader, bucket=bucket)
    assert result.rows_out == 2  # SPY + AAPL in the test universe
    assert all(w[1] == yf_daily.HISTORY_START for w in windows)

    # keys carry the history tag AND end with the trade date (build-queue visible)
    keys = [r[0] for r in ctx.conn.execute(
        "SELECT request_key FROM landing_manifest WHERE dataset='yf_daily'"
    ).fetchall()]
    assert all(":history:" in k and k.endswith(ctx.trade_date.isoformat()) for k in keys)


# ---- gap ledger ---------------------------------------------------------------

def test_gap_ledger_metrics(ctx) -> None:
    from argus.canonical import daily_bars

    daily_bars.upsert_bars(
        ctx.conn,
        pl.DataFrame(
            {"ticker": ["AAPL"], "bar_date": [date(2026, 7, 6)], "open": [1.0],
             "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1.0]}
        ),
        grade="degraded", single_source=True,
    )
    result = trust_jobs.gap_ledger_seal(ctx)
    assert result.rows_out >= 6
    ledger = dict(ctx.conn.execute(
        "SELECT gap_key, severity FROM gap_ledger"
    ).fetchall())
    assert ledger["daily_single_source_share"] == "warn"  # 100% single source
    assert ledger["delisted_coverage_10y"] == "blocker"   # no coverage rows yet


# ---- backup -------------------------------------------------------------------

def test_backup_copy_if_absent(ctx) -> None:
    ctx.settings.ensure_dirs()
    f1 = ctx.settings.landing_dir / "ds" / "a.parquet"
    f1.parent.mkdir(parents=True, exist_ok=True)
    f1.write_bytes(b"payload-a")
    (ctx.settings.landing_dir / "ds" / "junk.tmp").write_bytes(b"ignore me")

    first = trust_jobs.backup(ctx)
    assert first.rows_out == 1  # .tmp excluded
    mirrored = ctx.settings.data_root / "backup" / "landing" / "ds" / "a.parquet"
    assert mirrored.read_bytes() == b"payload-a"

    assert trust_jobs.backup(ctx).rows_out == 0  # immutable store: nothing to re-copy

    f2 = ctx.settings.landing_dir / "ds" / "b.parquet"
    f2.write_bytes(b"payload-b")
    assert trust_jobs.backup(ctx).rows_out == 1


# ---- parity gating -------------------------------------------------------------

def test_parity_requires_key(ctx) -> None:
    from argus.ops.errors import SourceDown

    with pytest.raises(SourceDown):
        trust_jobs.parity_sample(ctx)
