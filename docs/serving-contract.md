# The serving contract

ARGUS publishes a **sealed, read-only DuckDB database** (`argus_serving.duckdb`) as its
product. The shapes in that database are a **frozen contract**: their schemas are fixed, they
are asserted both in CI and inside the publish job every night, and a schema regression fails
*inside* ARGUS and can never reach a consumer. Any evolution must be **additive-only** — never
rename or retype an existing column.

A consumer only ever reads `argus_serving.duckdb`. It never touches the build database
(`argus.duckdb`), so the single-writer DuckDB build process and any number of readers never
contend.

## The published shapes (`serving/contracts.py`)

| Object | Columns | Invariants enforced by the gate |
|---|---|---|
| `vw_mad_daily_ohlcv` | `ticker`, `effective_date`, `open`, `high`, `low`, `close`, `volume` | exact polars schema; `(ticker, effective_date)` unique; no null keys/prices; `high ≥ low`. |
| `vw_mad_delisted` | `ticker`, `termination_date`, `termination_reason`, `terminal_return` | exact schema; `(ticker, termination_date)` unique; `termination_reason` ∈ {merger, bankruptcy, acquisition, voluntary, unknown}. |
| `vw_mad_intraday` | `ticker`, `minute`, `bid`, `ask`, `volume`, `derivation` | exact schema (minute is naive UTC); `bid ≤ ask`; `(ticker, minute)` unique; `derivation` ∈ {iex_bbo, corwin_schultz}. |
| `vw_mad_sectors` | `ticker`, `sector`, `industry` | exact schema; tickers unique; no null sectors served. |
| `vw_mad_coverage` | `audit_window`, `coverage` | exact schema; `coverage` ∈ [0, 1]; at least one window present. |
| `gap_ledger` | `gap_key`, `description`, `metric`, `unit`, `severity`, `updated_at` | copied verbatim (see [Reliability](reliability.md#the-gap-ledger)). |
| `serving_meta` | `sealed_trade_date`, `published_at`, `argus_schema_version` | provenance of the snapshot. |

Prices in `vw_mad_daily_ohlcv` are **point-in-time corporate-action-adjusted** — see
[Point-in-time correctness](point-in-time.md). `vw_mad_intraday` is the hybrid frame: real IEX
BBO where available, a Corwin–Schultz synthetic bid/ask elsewhere, with every row tagged by
`derivation` so a consumer can keep the two paths separate.

### The intraday derivation (`derive/spreads.py`)

The intraday frame joins minute OHLCV (consolidated volume) with IEX BBO on `(ticker, minute)`.
Minutes with no quote — thin names, and every name during the 4–6 week baseline cold-start —
fall back to a synthetic bid/ask placed symmetrically around the minute close, with the
half-spread taken from the **Corwin–Schultz (2012)** daily high/low estimator. The share of
minutes still on the synthetic proxy is published in the gap ledger
(`intraday_iex_bbo_share`), so the consumer always knows how much of the frame is real.

## The publish job (`serving/publish.py`)

Publishing is **atomic and gated**:

1. **Materialize.** `ATTACH` a fresh temp file from the build connection and `CREATE OR
   REPLACE TABLE` each `vw_mad_*` shape into it (sorted), plus `gap_ledger` and a
   `serving_meta` provenance row. The serving objects become *tables* (a nightly snapshot),
   but keep the `vw_` names — the names are part of the contract.
2. **Gate.** Run every contract assertion against the sealed temp file *before* the swap. Any
   `ContractViolation` aborts the publish and **leaves yesterday's good copy live**.
3. **Atomic swap.** `os.replace` the temp file over `argus_serving.duckdb`. On Windows a reader
   may briefly hold the previous copy open; the swap retries a few times, then fails loudly.

A crashed publish leaves the previous good snapshot in place. A contract violation blocks the
swap. Either way, a consumer never sees a partial or invalid database.

## Consuming the serving database

Point a read-only DuckDB connection at the file and query the `vw_mad_*` tables:

```python
import duckdb

con = duckdb.connect("C:/argus-data/argus_serving.duckdb", read_only=True)

# PIT-adjusted daily OHLCV
df = con.execute("SELECT * FROM vw_mad_daily_ohlcv WHERE ticker = 'AAPL' ORDER BY effective_date").pl()

# What was sealed, and when
meta = con.execute("SELECT * FROM serving_meta").fetchone()

# What the free data couldn't buy
gaps = con.execute("SELECT gap_key, metric, unit, severity FROM gap_ledger ORDER BY severity").pl()
```

Because the schema is frozen and additive-only, a consumer can pin to these column names and
types and trust them across ARGUS versions.
