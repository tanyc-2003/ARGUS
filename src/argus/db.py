"""DuckDB connection management and idempotent schema migrations.

`argus.duckdb` is the build/writer database. It is a disposable projection —
the Parquet landing zone (L0) and event store (L2) are the system of record,
and later milestones add `argus rebuild` to regenerate this file from them.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from argus.core.clocks import utc_now

# Ordered, append-only migration list. Never edit an entry after it has shipped;
# add a new one. Version = 1-based index.
MIGRATIONS: list[str] = [
    # v1 — M0 backbone tables
    """
    CREATE SEQUENCE IF NOT EXISTS dead_letter_seq;

    CREATE TABLE IF NOT EXISTS landing_manifest (
        dataset        VARCHAR NOT NULL,
        source         VARCHAR NOT NULL,
        request_key    VARCHAR NOT NULL,
        payload_hash   VARCHAR NOT NULL,
        path           VARCHAR NOT NULL,
        content_type   VARCHAR,
        n_bytes        BIGINT,
        partition_date DATE,
        knowledge_time TIMESTAMPTZ NOT NULL,
        written_at     TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (dataset, source, request_key)
    );

    CREATE TABLE IF NOT EXISTS job_runs (
        job_name    VARCHAR NOT NULL,
        trade_date  DATE NOT NULL,
        run_id      VARCHAR NOT NULL,
        status      VARCHAR NOT NULL,
        started_at  TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        error_class VARCHAR,
        detail      VARCHAR,
        rows_out    BIGINT,
        budget_used BIGINT,
        PRIMARY KEY (job_name, trade_date, run_id)
    );

    CREATE TABLE IF NOT EXISTS dead_letter (
        id           BIGINT PRIMARY KEY DEFAULT nextval('dead_letter_seq'),
        job_name     VARCHAR NOT NULL,
        source       VARCHAR,
        request_key  VARCHAR,
        payload_path VARCHAR,
        error_class  VARCHAR NOT NULL,
        detail       VARCHAR,
        first_seen   TIMESTAMPTZ NOT NULL,
        retry_count  INTEGER NOT NULL DEFAULT 0,
        resolved_at  TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS source_health (
        source               VARCHAR PRIMARY KEY,
        state                VARCHAR NOT NULL,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_ok              TIMESTAMPTZ,
        last_failure         TIMESTAMPTZ,
        opened_at            TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS market_sessions (
        exchange     VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        open_utc     TIMESTAMPTZ NOT NULL,
        close_utc    TIMESTAMPTZ NOT NULL,
        is_half_day  BOOLEAN NOT NULL,
        PRIMARY KEY (exchange, session_date)
    );
    """,
    # v2 — M1 canonical layer (SCD-2 columns on every canonical table)
    """
    CREATE TABLE IF NOT EXISTS bars_daily (
        ticker        VARCHAR NOT NULL,
        bar_date      DATE NOT NULL,
        open          DOUBLE,
        high          DOUBLE,
        low           DOUBLE,
        close         DOUBLE,          -- RAW price (post split-reversal)
        volume        DOUBLE,
        source_set    VARCHAR NOT NULL,
        grade         VARCHAR NOT NULL,          -- good | degraded | quarantined
        single_source BOOLEAN NOT NULL,
        payload_hash  VARCHAR NOT NULL,
        knowledge_time TIMESTAMPTZ NOT NULL,
        valid_from    TIMESTAMPTZ NOT NULL,
        valid_to      TIMESTAMPTZ,
        is_current    BOOLEAN NOT NULL,
        revision_seq  INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS corporate_actions (
        ticker        VARCHAR NOT NULL,
        action_type   VARCHAR NOT NULL,          -- split | dividend
        ex_date       DATE NOT NULL,
        ratio         DOUBLE,                    -- splits: to/from
        cash_amount   DOUBLE,                    -- dividends
        declared_date DATE,
        confidence    VARCHAR NOT NULL,          -- confirmed | single_source | inferred
        source_set    VARCHAR NOT NULL,
        payload_hash  VARCHAR NOT NULL,
        knowledge_time TIMESTAMPTZ NOT NULL,
        valid_from    TIMESTAMPTZ NOT NULL,
        valid_to      TIMESTAMPTZ,
        is_current    BOOLEAN NOT NULL,
        revision_seq  INTEGER NOT NULL
    );
    """,
    # v3 — M3 quality layer: the nightly vote audit trail (projection, rewritten each seal)
    """
    CREATE TABLE IF NOT EXISTS vote_results (
        ticker        VARCHAR NOT NULL,
        bar_date      DATE NOT NULL,
        verdict       VARCHAR NOT NULL,   -- confirmed | single_source | conflict
        n_sources     INTEGER NOT NULL,
        chosen_source VARCHAR,
        close_stooq    DOUBLE,
        close_yfinance DOUBLE,
        close_alpaca   DOUBLE,
        volume_agrees BOOLEAN,
        mad_flag      BOOLEAN NOT NULL DEFAULT FALSE,
        voted_at      TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (ticker, bar_date)
    );
    """,
    # v4 — M4 survivorship: snapshots are the immutable history; graveyard and
    # coverage are nightly projections over them (rebuilt deterministically)
    """
    CREATE TABLE IF NOT EXISTS universe_snapshots (
        source        VARCHAR NOT NULL,   -- nasdaqlisted | otherlisted
        snapshot_date DATE NOT NULL,
        ticker        VARCHAR NOT NULL,
        security_name VARCHAR,
        exchange      VARCHAR,
        is_etf        BOOLEAN,
        PRIMARY KEY (source, snapshot_date, ticker)
    );

    CREATE TABLE IF NOT EXISTS graveyard (
        ticker             VARCHAR NOT NULL,
        termination_date   DATE NOT NULL,
        termination_reason VARCHAR NOT NULL CHECK (termination_reason IN
                            ('merger', 'bankruptcy', 'acquisition', 'voluntary', 'unknown')),
        reason_confidence  VARCHAR NOT NULL,   -- pending | inferred | filing | confirmed
        reason_source      VARCHAR,
        terminal_return    DOUBLE,
        detection_source   VARCHAR NOT NULL,   -- symbol_dirs_diff | polygon
        first_seen         TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (ticker, termination_date)
    );

    CREATE TABLE IF NOT EXISTS coverage_metrics (
        audit_window VARCHAR PRIMARY KEY,
        window_start DATE,
        expected_n   INTEGER NOT NULL,
        covered_n    INTEGER NOT NULL,
        coverage     DOUBLE NOT NULL,
        computed_at  TIMESTAMPTZ NOT NULL
    );
    """,
    # v5 — M5 intraday: minute bars (yfinance consolidated), IEX BBO minute
    # buckets, and the incremental-processing marker for L0 payloads
    """
    CREATE TABLE IF NOT EXISTS bars_minute (
        ticker         VARCHAR NOT NULL,
        minute_ts      TIMESTAMPTZ NOT NULL,
        open           DOUBLE,
        high           DOUBLE,
        low            DOUBLE,
        close          DOUBLE,
        volume         DOUBLE,       -- consolidated (yfinance); NEVER IEX
        source         VARCHAR NOT NULL,
        knowledge_time TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (ticker, minute_ts)
    );

    CREATE TABLE IF NOT EXISTS quote_bars_1m (
        ticker         VARCHAR NOT NULL,
        minute_ts      TIMESTAMPTZ NOT NULL,
        bid_close      DOUBLE,
        ask_close      DOUBLE,
        bid_twm        DOUBLE,
        ask_twm        DOUBLE,
        n_quotes       INTEGER NOT NULL,
        knowledge_time TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (ticker, minute_ts)
    );

    CREATE TABLE IF NOT EXISTS intraday_processed (
        dataset      VARCHAR NOT NULL,
        request_key  VARCHAR NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (dataset, request_key)
    );

    CREATE TABLE IF NOT EXISTS serving_intraday (
        ticker     VARCHAR NOT NULL,
        minute_ts  TIMESTAMPTZ NOT NULL,
        bid        DOUBLE,
        ask        DOUBLE,
        volume     DOUBLE,
        derivation VARCHAR NOT NULL,   -- iex_bbo | corwin_schultz
        PRIMARY KEY (ticker, minute_ts)
    );
    """,
    # v6 — M6 trust layer: parity drift alarm, sectors, and the gap ledger
    """
    CREATE TABLE IF NOT EXISTS parity_scores (
        sample_date DATE NOT NULL,
        ticker      VARCHAR NOT NULL,
        bar_date    DATE NOT NULL,
        field       VARCHAR NOT NULL,   -- open | high | low | close | volume
        ours        DOUBLE,
        theirs      DOUBLE,
        rel_diff    DOUBLE,
        within_tol  BOOLEAN NOT NULL,
        PRIMARY KEY (sample_date, ticker, bar_date, field)
    );

    CREATE TABLE IF NOT EXISTS sectors (
        ticker     VARCHAR PRIMARY KEY,
        cik        VARCHAR,
        sic        VARCHAR,
        sector     VARCHAR,   -- sector ETF symbol (dashboard sector_map.yaml convention)
        industry   VARCHAR,   -- SIC description
        source     VARCHAR NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS gap_ledger (
        gap_key     VARCHAR PRIMARY KEY,
        description VARCHAR NOT NULL,
        metric      DOUBLE,
        unit        VARCHAR,
        severity    VARCHAR NOT NULL,   -- info | warn | blocker
        updated_at  TIMESTAMPTZ NOT NULL
    );
    """,
]

# Views are (re)created on every migrate() — idempotent, and they evolve without
# migration ceremony. The serving view IS the PIT guarantee: cum(D) multiplies
# only factors with ex_date <= D whose knowledge (in exchange-local time) had
# arrived by end of day D. exp(sum(ln)) stands in for a PRODUCT aggregate.
VIEWS: str = """
CREATE OR REPLACE VIEW vw_adjustment_factors AS
SELECT ticker, ex_date, 'split' AS factor_type, ratio AS factor, knowledge_time
FROM corporate_actions
WHERE is_current AND action_type = 'split' AND ratio IS NOT NULL AND ratio > 0
UNION ALL
SELECT ca.ticker, ca.ex_date, 'dividend' AS factor_type,
       prev.close / (prev.close - ca.cash_amount) AS factor,
       ca.knowledge_time
FROM corporate_actions ca
JOIN LATERAL (
    SELECT b.close
    FROM bars_daily b
    WHERE b.ticker = ca.ticker AND b.bar_date < ca.ex_date
      AND b.is_current AND b.grade <> 'quarantined'
    ORDER BY b.bar_date DESC
    LIMIT 1
) prev ON TRUE
WHERE ca.is_current AND ca.action_type = 'dividend'
  AND ca.cash_amount IS NOT NULL AND ca.cash_amount > 0
  AND prev.close > ca.cash_amount;

CREATE OR REPLACE VIEW vw_mad_daily_ohlcv AS
WITH b AS (
    SELECT * FROM bars_daily WHERE is_current AND grade <> 'quarantined'
),
c AS (
    SELECT b.ticker, b.bar_date,
        COALESCE(EXP(SUM(LN(f.factor)) FILTER (
            WHERE f.ex_date <= b.bar_date
              AND CAST(timezone('America/New_York', f.knowledge_time) AS DATE) <= b.bar_date
        )), 1.0) AS cum,
        COALESCE(EXP(SUM(LN(f.factor)) FILTER (
            WHERE f.factor_type = 'split' AND f.ex_date <= b.bar_date
              AND CAST(timezone('America/New_York', f.knowledge_time) AS DATE) <= b.bar_date
        )), 1.0) AS split_cum
    FROM b LEFT JOIN vw_adjustment_factors f ON f.ticker = b.ticker
    GROUP BY b.ticker, b.bar_date
)
SELECT
    CAST(b.ticker AS VARCHAR)               AS ticker,
    CAST(b.bar_date AS DATE)                AS effective_date,
    CAST(b.open * c.cum AS DOUBLE)          AS open,
    CAST(b.high * c.cum AS DOUBLE)          AS high,
    CAST(b.low * c.cum AS DOUBLE)           AS low,
    CAST(b.close * c.cum AS DOUBLE)         AS close,
    CAST(b.volume / c.split_cum AS DOUBLE)  AS volume
FROM b JOIN c ON b.ticker = c.ticker AND b.bar_date = c.bar_date;

CREATE OR REPLACE VIEW vw_mad_delisted AS
SELECT
    CAST(ticker AS VARCHAR)             AS ticker,
    CAST(termination_date AS DATE)      AS termination_date,
    CAST(termination_reason AS VARCHAR) AS termination_reason,
    CAST(terminal_return AS DOUBLE)     AS terminal_return
FROM graveyard;

CREATE OR REPLACE VIEW vw_mad_coverage AS
SELECT
    CAST(audit_window AS VARCHAR) AS audit_window,
    CAST(coverage AS DOUBLE)      AS coverage
FROM coverage_metrics;

CREATE OR REPLACE VIEW vw_mad_intraday AS
SELECT
    CAST(ticker AS VARCHAR)              AS ticker,
    timezone('UTC', minute_ts)           AS minute,   -- naive UTC TIMESTAMP (contract)
    CAST(bid AS DOUBLE)                  AS bid,
    CAST(ask AS DOUBLE)                  AS ask,
    CAST(volume AS DOUBLE)               AS volume,
    CAST(derivation AS VARCHAR)          AS derivation
FROM serving_intraday;

CREATE OR REPLACE VIEW vw_mad_sectors AS
SELECT
    CAST(ticker AS VARCHAR)   AS ticker,
    CAST(sector AS VARCHAR)   AS sector,
    CAST(industry AS VARCHAR) AS industry
FROM sectors
WHERE sector IS NOT NULL;
"""


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def migrate(conn: duckdb.DuckDBPyConnection) -> int:
    """Apply pending migrations; returns the schema version now in effect."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)"
    )
    row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
    current = int(row[0]) if row else 0
    for version, sql in enumerate(MIGRATIONS, start=1):
        if version > current:
            conn.execute(sql)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?, ?)",
                [version, utc_now()],
            )
    conn.execute(VIEWS)
    return len(MIGRATIONS)


def open_migrated(db_path: Path) -> duckdb.DuckDBPyConnection:
    conn = connect(db_path)
    migrate(conn)
    return conn
