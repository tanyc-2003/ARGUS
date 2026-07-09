"""Factor-layer inspection: the PIT audit trail behind vw_mad_daily_ohlcv.

The factors themselves are a VIEW over corporate_actions (+ prior closes for
dividends) — see db.VIEWS — so there is exactly one source of truth with full
SCD-2 history. This module answers "show your work" questions: which factors
were applied to a bar, with what knowledge, and what the adjusted value is.

Adjustment convention (pinned in the build plan): forward, total-return style.
    adj(D) = raw(D) x cum(D),  cum(D) = PROD{ f.factor : f.ex_date <= D and
                                              f known (ET) by end of day D }
The served value for (ticker, D) is immutable once written: later-knowledge
factors can never change it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from argus.core.clocks import ET

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


@dataclass(frozen=True)
class FactorRow:
    ex_date: date
    factor_type: str
    factor: float
    knowledge_time: datetime
    applied: bool


@dataclass(frozen=True)
class PitReport:
    ticker: str
    bar_date: date
    raw_close: float | None
    adjusted_close: float | None
    cum_factor: float
    factors: list[FactorRow]

    @property
    def no_lookahead(self) -> bool:
        """True iff every applied factor was knowable (exchange-local) by end of bar_date."""
        return all(
            f.knowledge_time.astimezone(ET).date() <= self.bar_date
            for f in self.factors
            if f.applied
        )


def pit_report(conn: duckdb.DuckDBPyConnection, ticker: str, bar_date: date) -> PitReport:
    """Reconstruct exactly how the served value for (ticker, bar_date) was built."""
    raw = conn.execute(
        """
        SELECT close FROM bars_daily
        WHERE ticker = ? AND bar_date = ? AND is_current AND grade <> 'quarantined'
        """,
        [ticker.upper(), bar_date],
    ).fetchone()
    raw_close = float(raw[0]) if raw else None

    rows = conn.execute(
        """
        SELECT ex_date, factor_type, factor, knowledge_time,
               (ex_date <= ?
                AND CAST(timezone('America/New_York', knowledge_time) AS DATE) <= ?) AS applied
        FROM vw_adjustment_factors
        WHERE ticker = ?
        ORDER BY ex_date
        """,
        [bar_date, bar_date, ticker.upper()],
    ).fetchall()
    factors = [
        FactorRow(ex_date=r[0], factor_type=r[1], factor=float(r[2]),
                  knowledge_time=r[3], applied=bool(r[4]))
        for r in rows
    ]
    cum = 1.0
    for f in factors:
        if f.applied:
            cum *= f.factor
    return PitReport(
        ticker=ticker.upper(),
        bar_date=bar_date,
        raw_close=raw_close,
        adjusted_close=None if raw_close is None else raw_close * cum,
        cum_factor=cum,
        factors=factors,
    )
