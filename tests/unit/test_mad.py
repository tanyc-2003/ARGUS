"""MAD screen: lone-vendor spikes quarantine; confirmed crashes survive."""

from datetime import date, timedelta

import polars as pl

from argus.quality.mad import MIN_BARS, apply_mad_screen


def _series(n: int, spike_at: int | None, *, single_source: bool,
            grade: str) -> pl.DataFrame:
    # smooth pseudo-noise: |r - median| stays within a few MADs (genuinely clean)
    import math

    closes: list[float] = [100.0]
    for i in range(1, n):
        closes.append(closes[-1] * (1.0 + 0.002 * math.sin(i * 1.7)))
    if spike_at is not None:
        closes[spike_at] = closes[spike_at] * 3.0  # +200% in one day
    start = date(2026, 1, 5)
    return pl.DataFrame(
        {
            "ticker": ["X"] * n,
            "bar_date": [start + timedelta(days=i) for i in range(n)],
            "open": closes, "high": closes, "low": closes, "close": closes,
            "volume": [1e6] * n,
            "source_set": ["stooq"] * n,
            "grade": [grade] * n,
            "single_source": [single_source] * n,
        }
    )


def test_single_source_spike_quarantined() -> None:
    out = apply_mad_screen(_series(60, 45, single_source=True, grade="degraded"))
    spiked = out.filter(pl.col("mad_flag"))
    assert spiked.height >= 1
    assert set(spiked["grade"].to_list()) == {"quarantined"}


def test_confirmed_spike_survives() -> None:
    out = apply_mad_screen(_series(60, 45, single_source=False, grade="good"))
    spiked = out.filter(pl.col("mad_flag"))
    assert spiked.height >= 1
    assert set(spiked["grade"].to_list()) == {"good"}  # two vendors agreeing = market event


def test_short_history_stays_quiet() -> None:
    out = apply_mad_screen(_series(MIN_BARS - 5, 10, single_source=True, grade="degraded"))
    assert out.filter(pl.col("mad_flag")).is_empty()


def test_clean_series_unflagged() -> None:
    out = apply_mad_screen(_series(60, None, single_source=True, grade="degraded"))
    assert out.filter(pl.col("mad_flag")).is_empty()
    assert set(out["grade"].to_list()) == {"degraded"}
