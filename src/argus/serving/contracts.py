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
DELISTED = "vw_mad_delisted"
COVERAGE = "vw_mad_coverage"

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

DELISTED_SCHEMA: dict[str, PolarsType] = {
    "ticker": pl.Utf8,
    "termination_date": pl.Date,
    "termination_reason": pl.Utf8,
    "terminal_return": pl.Float64,
}

# the dashboard's delisted_tickers DDL enforces this CHECK constraint —
# a value outside this set must fail inside ARGUS, never at the consumer
TERMINATION_REASONS = frozenset({"merger", "bankruptcy", "acquisition", "voluntary", "unknown"})

COVERAGE_SCHEMA: dict[str, PolarsType] = {
    "audit_window": pl.Utf8,
    "coverage": pl.Float64,
}

SECTORS = "vw_mad_sectors"

SECTORS_SCHEMA: dict[str, PolarsType] = {
    "ticker": pl.Utf8,
    "sector": pl.Utf8,
    "industry": pl.Utf8,
}

INTRADAY = "vw_mad_intraday"

# minute is NAIVE UTC — the dashboard's fetch_intraday_bars schema is plain
# pl.Datetime; the derivation tag is the additive disclosure column (§3.2)
INTRADAY_SCHEMA: dict[str, PolarsType] = {
    "ticker": pl.Utf8,
    "minute": pl.Datetime("us"),
    "bid": pl.Float64,
    "ask": pl.Float64,
    "volume": pl.Float64,
    "derivation": pl.Utf8,
}

DERIVATIONS = frozenset({"iex_bbo", "corwin_schultz"})


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


def assert_delisted(db_path: Path) -> int:
    """vw_mad_delisted: exact schema, unique (ticker, termination_date), enum reasons."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sample = con.execute(f"SELECT * FROM {DELISTED} LIMIT 10000").pl()
        got = dict(sample.schema)
        if got != DELISTED_SCHEMA:
            raise ContractViolation(
                f"{DELISTED} schema drift:\n  got      {got}\n  expected {DELISTED_SCHEMA}"
            )
        counts = con.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT (ticker, termination_date)),
                   COUNT(*) FILTER (WHERE termination_reason NOT IN
                       ('merger', 'bankruptcy', 'acquisition', 'voluntary', 'unknown'))
            FROM {DELISTED}
            """
        ).fetchone()
        assert counts is not None
        total, distinct, bad_reason = counts
        if total != distinct:
            raise ContractViolation(f"{DELISTED}: (ticker, termination_date) not unique")
        if bad_reason:
            raise ContractViolation(
                f"{DELISTED}: {bad_reason} rows outside the dashboard's reason CHECK set"
            )
        return int(total)
    finally:
        con.close()


def assert_sectors(db_path: Path) -> int:
    """vw_mad_sectors: exact schema; unique tickers; no null sectors served."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(f"SELECT * FROM {SECTORS}").pl()
        got = dict(df.schema)
        if got != SECTORS_SCHEMA:
            raise ContractViolation(
                f"{SECTORS} schema drift:\n  got      {got}\n  expected {SECTORS_SCHEMA}"
            )
        if df.height != df["ticker"].n_unique():
            raise ContractViolation(f"{SECTORS}: duplicate tickers")
        if not df.is_empty() and df["sector"].null_count():
            raise ContractViolation(f"{SECTORS}: null sectors must not be served")
        return df.height
    finally:
        con.close()


def assert_intraday(db_path: Path) -> int:
    """vw_mad_intraday: exact schema; bid <= ask; known derivations; unique keys."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sample = con.execute(f"SELECT * FROM {INTRADAY} LIMIT 10000").pl()
        got = dict(sample.schema)
        if got != INTRADAY_SCHEMA:
            raise ContractViolation(
                f"{INTRADAY} schema drift:\n  got      {got}\n  expected {INTRADAY_SCHEMA}"
            )
        counts = con.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT (ticker, minute)),
                   COUNT(*) FILTER (WHERE bid > ask),
                   COUNT(*) FILTER (WHERE derivation NOT IN ('iex_bbo', 'corwin_schultz'))
            FROM {INTRADAY}
            """
        ).fetchone()
        assert counts is not None
        total, distinct, crossed, bad_tag = counts
        if total != distinct:
            raise ContractViolation(f"{INTRADAY}: (ticker, minute) not unique")
        if crossed:
            raise ContractViolation(f"{INTRADAY}: {crossed} rows with bid > ask")
        if bad_tag:
            raise ContractViolation(f"{INTRADAY}: {bad_tag} rows with unknown derivation")
        return int(total)
    finally:
        con.close()


def assert_coverage(db_path: Path) -> int:
    """vw_mad_coverage: exact schema; coverage in [0, 1]; windows present."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(f"SELECT * FROM {COVERAGE}").pl()
        got = dict(df.schema)
        if got != COVERAGE_SCHEMA:
            raise ContractViolation(
                f"{COVERAGE} schema drift:\n  got      {got}\n  expected {COVERAGE_SCHEMA}"
            )
        if df.is_empty():
            raise ContractViolation(f"{COVERAGE}: no audit windows served")
        out_of_range = df.filter((df["coverage"] < 0) | (df["coverage"] > 1))
        if not out_of_range.is_empty():
            raise ContractViolation(f"{COVERAGE}: coverage outside [0,1]: {out_of_range}")
        return df.height
    finally:
        con.close()
