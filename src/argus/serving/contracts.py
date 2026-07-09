"""FROZEN serving contracts — the byte-for-byte shapes the dashboard reads.

These dicts mirror the dashboard's `_OHLCV_SCHEMA` (polars) exactly. They are
asserted in CI (contract tests) AND inside the publish job every night: a
schema regression can fail a build or block a publish, but can never reach the
consumer. Any widening must be additive-only — never rename or retype.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

DAILY_OHLCV = "vw_mad_daily_ohlcv"

PolarsType = pl.DataType | type[pl.DataType]

DAILY_OHLCV_SCHEMA: dict[str, PolarsType] = {
    "ticker": pl.Utf8,
    "effective_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}


class ContractViolation(AssertionError):
    """A serving shape does not match the frozen contract."""


def assert_daily_ohlcv(db_path: Path) -> int:
    """Assert the daily view/table in `db_path` honors the contract; returns row count.

    Checks: exact polars schema; (ticker, effective_date) uniqueness; no null
    keys or prices; high >= low on every row.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sample = con.execute(f"SELECT * FROM {DAILY_OHLCV} LIMIT 10000").pl()
        got = dict(sample.schema)
        if got != DAILY_OHLCV_SCHEMA:
            raise ContractViolation(
                f"{DAILY_OHLCV} schema drift:\n  got      {got}\n  expected {DAILY_OHLCV_SCHEMA}"
            )
        counts = con.execute(
            f"""
            SELECT COUNT(*),
                   COUNT(DISTINCT (ticker, effective_date)),
                   COUNT(*) FILTER (
                       WHERE ticker IS NULL OR effective_date IS NULL
                          OR close IS NULL OR high < low
                   )
            FROM {DAILY_OHLCV}
            """
        ).fetchone()
        assert counts is not None
        total, distinct, bad = counts
        if total != distinct:
            raise ContractViolation(
                f"{DAILY_OHLCV}: (ticker, effective_date) not unique ({total} rows, "
                f"{distinct} distinct keys)"
            )
        if bad:
            raise ContractViolation(f"{DAILY_OHLCV}: {bad} rows with null keys/prices or high<low")
        return int(total)
    finally:
        con.close()
