"""The voting truth table (v4 §6)."""

from datetime import date

import polars as pl

from argus.quality.voting import skipped_alpaca_only, vote_bars

D = date(2026, 7, 6)


def _obs(rows: list[tuple[str, float, float | None]]) -> pl.DataFrame:
    """rows: (source, close, volume) for one (AAPL, D) bar."""
    return pl.DataFrame(
        {
            "source": [r[0] for r in rows],
            "ticker": ["AAPL"] * len(rows),
            "bar_date": [D] * len(rows),
            "open": [r[1] for r in rows],
            "high": [r[1] * 1.01 for r in rows],
            "low": [r[1] * 0.99 for r in rows],
            "close": [r[1] for r in rows],
            "volume": [r[2] for r in rows],
        }
    )


def _one(df: pl.DataFrame) -> dict:
    assert df.height == 1, df
    return df.row(0, named=True)


def test_three_sources_agree() -> None:
    out = _one(vote_bars(_obs([
        ("yfinance", 100.00, 1e6), ("stooq", 100.05, 1.02e6), ("alpaca_iex", 100.02, 5e4),
    ])))
    assert out["verdict"] == "confirmed"
    assert out["grade"] == "good"
    assert out["chosen_source"] == "yfinance"
    assert out["close"] == 100.00
    assert out["volume"] == 1e6  # yfinance consolidated, never IEX


def test_two_agree_third_dissents_still_confirmed() -> None:
    out = _one(vote_bars(_obs([
        ("yfinance", 100.00, 1e6), ("stooq", 100.03, 1e6), ("alpaca_iex", 150.0, 5e4),
    ])))
    assert out["verdict"] == "confirmed"
    assert out["grade"] == "good"
    assert out["close"] == 100.00


def test_single_consolidated_source_admitted_degraded() -> None:
    out = _one(vote_bars(_obs([("stooq", 100.0, 1e6)])))
    assert out["verdict"] == "single_source"
    assert out["grade"] == "degraded"
    assert out["single_source"] is True
    assert out["chosen_source"] == "stooq"


def test_alpaca_only_bar_never_canonized() -> None:
    obs = _obs([("alpaca_iex", 100.0, 5e4)])
    assert vote_bars(obs).is_empty()
    assert skipped_alpaca_only(obs) == 1


def test_all_disagree_is_conflict_quarantined() -> None:
    out = _one(vote_bars(_obs([
        ("yfinance", 100.0, 1e6), ("stooq", 101.5, 1e6), ("alpaca_iex", 103.0, 5e4),
    ])))
    assert out["verdict"] == "conflict"
    assert out["grade"] == "quarantined"


def test_close_tolerance_boundary() -> None:
    # 1000.0 vs 1001.0 -> rel diff ~0.09995% -> inside ±0.1%
    inside = _one(vote_bars(_obs([("yfinance", 1000.0, 1e6), ("stooq", 1001.0, 1e6)])))
    assert inside["verdict"] == "confirmed"
    # 1000.0 vs 1002.5 -> ~0.2497% -> outside
    outside = _one(vote_bars(_obs([("yfinance", 1000.0, 1e6), ("stooq", 1002.5, 1e6)])))
    assert outside["verdict"] == "conflict"


def test_volume_disagreement_degrades_but_serves() -> None:
    out = _one(vote_bars(_obs([("yfinance", 100.0, 1e6), ("stooq", 100.0, 1.2e6)])))
    assert out["verdict"] == "confirmed"
    assert out["volume_agrees"] is False
    assert out["grade"] == "degraded"  # prices fine, volume suspect — tagged, not hidden
    assert out["close"] == 100.0


def test_iex_price_vote_counts_but_volume_never_serves() -> None:
    # stooq + alpaca agree on price, yfinance absent: prices confirmed via IEX,
    # but the served volume must be stooq's (consolidated), never the IEX print
    out = _one(vote_bars(_obs([("stooq", 100.0, 1e6), ("alpaca_iex", 100.01, 5e4)])))
    assert out["verdict"] == "confirmed"
    assert out["chosen_source"] == "stooq"
    assert out["volume"] == 1e6


def test_yf_alpaca_pair_serves_yf_volume() -> None:
    out = _one(vote_bars(_obs([("yfinance", 100.0, 1e6), ("alpaca_iex", 100.02, 5e4)])))
    assert out["verdict"] == "confirmed"
    assert out["chosen_source"] == "yfinance"
    assert out["volume"] == 1e6


def test_multiple_keys_vote_independently() -> None:
    a = _obs([("yfinance", 100.0, 1e6), ("stooq", 100.01, 1e6)])
    b = _obs([("yfinance", 50.0, 1e6), ("stooq", 55.0, 1e6)]).with_columns(
        pl.lit("MSFT").alias("ticker")
    )
    out = vote_bars(pl.concat([a, b])).sort("ticker")
    assert out["ticker"].to_list() == ["AAPL", "MSFT"]
    assert out["verdict"].to_list() == ["confirmed", "conflict"]
