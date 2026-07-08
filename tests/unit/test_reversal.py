"""Split-reversal correctness: the round-trip property and the AAPL 2020 golden case."""

from datetime import date, timedelta

import polars as pl
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from argus.normalize.daily import apply_split_adjustment, reverse_split_adjustment


def _bars(ticker: str, start: date, closes: list[float], volumes: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": [ticker] * len(closes),
            "bar_date": [start + timedelta(days=i) for i in range(len(closes))],
            "open": closes,
            "high": [c * 1.01 for c in closes],
            "low": [c * 0.99 for c in closes],
            "close": closes,
            "volume": volumes,
        }
    )


def test_aapl_2020_golden() -> None:
    # raw closes around the 4:1 split (ex 2020-08-31)
    raw = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 2,
            "bar_date": [date(2020, 8, 28), date(2020, 8, 31)],
            "open": [504.05, 127.58],
            "high": [505.77, 131.00],
            "low": [498.31, 126.00],
            "close": [499.23, 129.04],
            "volume": [46_907_479.0, 225_702_700.0],
        }
    )
    splits = pl.DataFrame(
        {"ticker": ["AAPL"], "ex_date": [date(2020, 8, 31)], "ratio": [4.0]}
    )
    vendor = apply_split_adjustment(raw, splits)
    # the vendor view divides pre-split prices by 4 and multiplies volume by 4
    assert abs(vendor["close"][0] - 499.23 / 4) < 1e-9
    assert abs(vendor["close"][1] - 129.04) < 1e-9
    assert abs(vendor["volume"][0] - 46_907_479.0 * 4) < 1e-6

    back = reverse_split_adjustment(vendor, splits)
    assert abs(back["close"][0] - 499.23) < 1e-9
    assert abs(back["close"][1] - 129.04) < 1e-9
    assert abs(back["volume"][0] - 46_907_479.0) < 1e-6
    assert back["reversal_factor"][0] == 4.0
    assert back["reversal_factor"][1] == 1.0  # ex-date bar is already post-split


def test_no_splits_is_identity() -> None:
    bars = _bars("SPY", date(2024, 1, 2), [470.0, 472.5], [1e6, 1.1e6])
    out = reverse_split_adjustment(bars, pl.DataFrame(schema={"ticker": pl.Utf8,
                                                              "ex_date": pl.Date,
                                                              "ratio": pl.Float64}))
    assert out["close"].to_list() == [470.0, 472.5]
    assert out["reversal_factor"].to_list() == [1.0, 1.0]


def test_stacked_splits_compound() -> None:
    # two splits: 2:1 then 3:1 — a bar before both carries factor 6
    bars = _bars("X", date(2020, 1, 1), [600.0, 300.0, 100.0], [1e6, 2e6, 6e6])
    splits = pl.DataFrame(
        {"ticker": ["X", "X"], "ex_date": [date(2020, 1, 2), date(2020, 1, 3)],
         "ratio": [2.0, 3.0]}
    )
    out = reverse_split_adjustment(bars, splits)
    got = out["reversal_factor"].to_list()
    assert all(abs(a - b) < 1e-9 for a, b in zip(got, [6.0, 3.0, 1.0], strict=True))


def test_other_tickers_untouched() -> None:
    bars = pl.concat(
        [
            _bars("A", date(2020, 1, 1), [10.0, 5.0], [1e6, 2e6]),
            _bars("B", date(2020, 1, 1), [20.0, 21.0], [1e6, 1e6]),
        ]
    )
    splits = pl.DataFrame({"ticker": ["A"], "ex_date": [date(2020, 1, 2)], "ratio": [2.0]})
    out = reverse_split_adjustment(bars, splits).sort(["ticker", "bar_date"])
    assert out.filter(pl.col("ticker") == "B")["reversal_factor"].to_list() == [1.0, 1.0]
    assert out.filter(pl.col("ticker") == "A")["reversal_factor"].to_list() == [2.0, 1.0]


@given(
    closes=st.lists(st.floats(min_value=1.0, max_value=5000.0), min_size=3, max_size=30),
    split_offsets=st.lists(st.integers(min_value=0, max_value=35), min_size=0, max_size=4,
                           unique=True),
    ratios=st.lists(st.sampled_from([0.25, 0.5, 2.0, 3.0, 4.0, 10.0]), min_size=4, max_size=4),
)
@hyp_settings(max_examples=60, deadline=None)
def test_roundtrip_property(closes: list[float], split_offsets: list[int],
                            ratios: list[float]) -> None:
    """apply(reverse) == identity for arbitrary split stacks (incl. reverse splits)."""
    start = date(2022, 1, 1)
    bars = _bars("T", start, closes, [1e6] * len(closes))
    splits = pl.DataFrame(
        {
            "ticker": ["T"] * len(split_offsets),
            "ex_date": [start + timedelta(days=o) for o in split_offsets],
            "ratio": ratios[: len(split_offsets)],
        },
        schema={"ticker": pl.Utf8, "ex_date": pl.Date, "ratio": pl.Float64},
    )
    vendor = apply_split_adjustment(bars, splits)
    back = reverse_split_adjustment(vendor, splits).sort("bar_date")
    for col in ("open", "high", "low", "close", "volume"):
        orig = bars.sort("bar_date")[col].to_list()
        recovered = back[col].to_list()
        assert all(abs(a - b) <= 1e-9 * max(abs(a), 1.0)
                   for a, b in zip(orig, recovered, strict=True))
