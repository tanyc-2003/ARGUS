"""Corwin–Schultz estimator + the hybrid intraday join."""

from datetime import UTC, date, datetime

import polars as pl

from argus.derive.spreads import corwin_schultz_daily, hybrid_intraday


def _daily(rows: list[tuple[date, float, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["X"] * len(rows),
            "bar_date": [r[0] for r in rows],
            "high": [r[1] for r in rows],
            "low": [r[2] for r in rows],
        }
    )


def test_cs_identical_range_days_golden() -> None:
    # two identical-range days (H=101, L=99): hand-computed S ≈ 0.02
    out = corwin_schultz_daily(
        _daily([(date(2026, 7, 6), 101.0, 99.0), (date(2026, 7, 7), 101.0, 99.0)])
    )
    assert out.height == 1  # first day has no predecessor
    assert abs(out["cs_spread"][0] - 0.02) < 1e-3


def test_cs_negative_estimates_floor_at_zero() -> None:
    # a large overnight gap makes gamma dominate -> negative alpha -> floored
    out = corwin_schultz_daily(
        _daily([(date(2026, 7, 6), 102.0, 100.0), (date(2026, 7, 7), 103.0, 101.0)])
    )
    assert out["cs_spread"][0] == 0.0


def test_cs_zero_range_is_zero_spread() -> None:
    out = corwin_schultz_daily(
        _daily([(date(2026, 7, 6), 100.0, 100.0), (date(2026, 7, 7), 100.0, 100.0)])
    )
    assert out["cs_spread"][0] == 0.0


def _minute(ts: datetime, close: float, volume: float = 1e5) -> dict:
    return {"ticker": "X", "minute_ts": ts, "close": close, "volume": volume}


def test_hybrid_prefers_bbo_and_falls_back_to_cs() -> None:
    m1 = datetime(2026, 7, 7, 14, 30, tzinfo=UTC)
    m2 = datetime(2026, 7, 7, 14, 31, tzinfo=UTC)
    minutes = pl.DataFrame([_minute(m1, 100.0), _minute(m2, 100.5)]).with_columns(
        pl.col("minute_ts").cast(pl.Datetime("us", "UTC"))
    )
    quotes = pl.DataFrame(
        [{"ticker": "X", "minute_ts": m1, "bid_close": 99.98, "ask_close": 100.02}]
    ).with_columns(pl.col("minute_ts").cast(pl.Datetime("us", "UTC")))
    cs = pl.DataFrame(
        {"ticker": ["X"], "bar_date": [date(2026, 7, 7)], "cs_spread": [0.01]}
    )
    out = hybrid_intraday(minutes, quotes, cs).sort("minute_ts")

    bbo = out.row(0, named=True)
    assert bbo["derivation"] == "iex_bbo"
    assert (bbo["bid"], bbo["ask"]) == (99.98, 100.02)

    fallback = out.row(1, named=True)
    assert fallback["derivation"] == "corwin_schultz"
    assert abs(fallback["bid"] - 100.5 * (1 - 0.005)) < 1e-9
    assert abs(fallback["ask"] - 100.5 * (1 + 0.005)) < 1e-9
    assert fallback["bid"] <= fallback["ask"]


def test_hybrid_without_any_quotes_or_cs_serves_flat() -> None:
    m1 = datetime(2026, 7, 7, 14, 30, tzinfo=UTC)
    minutes = pl.DataFrame([_minute(m1, 100.0)]).with_columns(
        pl.col("minute_ts").cast(pl.Datetime("us", "UTC"))
    )
    empty_q = pl.DataFrame(
        schema={"ticker": pl.Utf8, "minute_ts": pl.Datetime("us", "UTC"),
                "bid_close": pl.Float64, "ask_close": pl.Float64}
    )
    empty_cs = pl.DataFrame(
        schema={"ticker": pl.Utf8, "bar_date": pl.Date, "cs_spread": pl.Float64}
    )
    out = hybrid_intraday(minutes, empty_q, empty_cs)
    row = out.row(0, named=True)
    assert row["derivation"] == "corwin_schultz"
    assert row["bid"] == row["ask"] == 100.0  # zero-width proxy, honestly tagged
