"""The PIT core, tested end-to-end at the serving view.

P1 (no look-ahead): the served value for (ticker, D) is a pure function of
facts knowable (exchange-local) by end of day D — factors arriving with later
knowledge can never change it. P2 (served-value immutability) is checked by
comparing the full view before/after a late-knowledge factor lands.
"""

from datetime import date, datetime

import polars as pl

from argus.canonical import daily_bars
from argus.core.clocks import asof_knowledge_time


def _load_bars(conn, ticker: str, rows: list[tuple[date, float]]) -> None:
    df = pl.DataFrame(
        {
            "ticker": [ticker] * len(rows),
            "bar_date": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [r[1] for r in rows],
            "low": [r[1] for r in rows],
            "close": [r[1] for r in rows],
            "volume": [1_000_000.0] * len(rows),
        }
    )
    daily_bars.upsert_bars(conn, df)


def _insert_action(
    conn,
    ticker: str,
    action_type: str,
    ex: date,
    *,
    ratio: float | None = None,
    cash: float | None = None,
    knowledge_time: datetime | None = None,
) -> None:
    kt = knowledge_time or asof_knowledge_time(ex)
    conn.execute(
        "INSERT INTO corporate_actions VALUES (?, ?, ?, ?, ?, NULL, 'single_source', "
        "'polygon', ?, ?, ?, NULL, TRUE, 1)",
        [ticker, action_type, ex, ratio, cash, f"test-{ticker}-{action_type}-{ex}", kt, kt],
    )


def _served(conn, ticker: str) -> pl.DataFrame:
    return conn.execute(
        "SELECT * FROM vw_mad_daily_ohlcv WHERE ticker = ? ORDER BY effective_date",
        [ticker],
    ).pl()


def test_split_applies_from_ex_date_only(conn) -> None:
    _load_bars(conn, "AAPL", [(date(2020, 8, 28), 499.23), (date(2020, 8, 31), 129.04)])
    _insert_action(conn, "AAPL", "split", date(2020, 8, 31), ratio=4.0)
    out = _served(conn, "AAPL")
    assert abs(out["close"][0] - 499.23) < 1e-9  # pre-ex bar untouched
    assert abs(out["close"][1] - 129.04 * 4) < 1e-9  # ex-date bar carries the factor
    # cross-split return equals the true total return
    assert abs(out["close"][1] / out["close"][0] - (129.04 * 4) / 499.23) < 1e-12


def test_split_volume_consistency(conn) -> None:
    _load_bars(conn, "AAPL", [(date(2020, 8, 28), 499.23), (date(2020, 8, 31), 129.04)])
    _insert_action(conn, "AAPL", "split", date(2020, 8, 31), ratio=4.0)
    out = _served(conn, "AAPL")
    assert abs(out["volume"][0] - 1_000_000.0) < 1e-6
    assert abs(out["volume"][1] - 1_000_000.0 / 4) < 1e-6  # post-split shares rescaled


def test_dividend_total_return_semantics(conn) -> None:
    # close drops exactly by the dividend -> adjusted return must be 0%
    _load_bars(conn, "DIVX", [(date(2024, 3, 1), 100.0), (date(2024, 3, 4), 99.0)])
    _insert_action(conn, "DIVX", "dividend", date(2024, 3, 4), cash=1.0)
    out = _served(conn, "DIVX")
    assert abs(out["close"][0] - 100.0) < 1e-9
    assert abs(out["close"][1] - 99.0 * (100.0 / 99.0)) < 1e-9  # == 100.0
    assert abs(out["volume"][1] - 1_000_000.0) < 1e-6  # dividends never touch volume


def test_no_lookahead_late_knowledge_excluded(conn) -> None:
    """A factor with ex_date <= D but knowledge AFTER end-of-day D must not touch bar D."""
    _load_bars(conn, "PITX", [(date(2020, 8, 28), 200.0), (date(2020, 9, 16), 210.0)])
    _insert_action(
        conn, "PITX", "split", date(2020, 8, 20), ratio=2.0,
        knowledge_time=asof_knowledge_time(date(2020, 9, 15)),  # learned weeks later
    )
    out = _served(conn, "PITX")
    # bar 2020-08-28: ex_date (08-20) <= D but knowledge (09-15) > D -> excluded
    assert abs(out["close"][0] - 200.0) < 1e-9
    # bar 2020-09-16: knowledge arrived by then -> applied
    assert abs(out["close"][1] - 210.0 * 2.0) < 1e-9


def test_served_history_immutable_under_late_knowledge(conn) -> None:
    """P2: landing a late-knowledge factor leaves already-served history bit-identical."""
    days = [(date(2024, 1, 2), 50.0), (date(2024, 1, 3), 51.0), (date(2024, 6, 3), 55.0)]
    _load_bars(conn, "IMMU", days)
    before = _served(conn, "IMMU").filter(pl.col("effective_date") < date(2024, 6, 1))

    _insert_action(
        conn, "IMMU", "dividend", date(2024, 1, 3), cash=0.5,
        knowledge_time=asof_knowledge_time(date(2024, 6, 2)),  # long after the ex-date
    )
    after = _served(conn, "IMMU").filter(pl.col("effective_date") < date(2024, 6, 1))
    assert before.equals(after)  # January's served values did not move
    # ...but the June bar (knowledge arrived) does carry the factor
    june = _served(conn, "IMMU").filter(pl.col("effective_date") == date(2024, 6, 3))
    assert june["close"][0] > 55.0


def test_quarantined_rows_never_served(conn) -> None:
    _load_bars(conn, "QQQ", [(date(2024, 1, 2), 400.0)])
    conn.execute("UPDATE bars_daily SET grade = 'quarantined' WHERE ticker = 'QQQ'")
    assert _served(conn, "QQQ").is_empty()


def test_ex_date_on_bar_date_boundary_is_applied(conn) -> None:
    """knowledge stamped 23:59:59 ET on the ex-date counts as known that day."""
    _load_bars(conn, "EDGE", [(date(2024, 3, 8), 10.0)])
    _insert_action(conn, "EDGE", "split", date(2024, 3, 8), ratio=2.0)
    out = _served(conn, "EDGE")
    assert abs(out["close"][0] - 20.0) < 1e-9
