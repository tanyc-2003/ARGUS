"""The vectorized bar knowledge stamp must equal the scalar clocks implementation."""

from datetime import date

import polars as pl

from argus.canonical.daily_bars import bar_knowledge_time
from argus.core.clocks import asof_knowledge_time

CASES = [
    date(2016, 6, 15),   # EDT (summer)
    date(2016, 1, 15),   # EST (winter)
    date(2024, 3, 10),   # spring-forward day
    date(2024, 11, 3),   # fall-back day
    date(2020, 8, 31),   # the AAPL split date
]


def test_vectorized_matches_scalar_across_dst() -> None:
    frame = pl.DataFrame({"bar_date": CASES})
    out = bar_knowledge_time(frame)
    for bar_date, kt in zip(out["bar_date"].to_list(), out["knowledge_time"].to_list(),
                            strict=True):
        assert kt == asof_knowledge_time(bar_date), bar_date
