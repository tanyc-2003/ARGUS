# ARGUS

**A minimum-cost, point-in-time-correct market-data platform.**

ARGUS captures US equity market data from free sources, reconciles it across providers,
adjusts it for corporate actions without look-ahead, and publishes a sealed, contract-checked
DuckDB database that any downstream tool can read. It runs unattended on a single machine on
DuckDB + Parquet — no paid data feeds — and it **measures and discloses every data gap**
instead of papering over it.

## What it provides

- **PIT-correct adjusted OHLCV.** Daily open/high/low/close/volume, corporate-action-adjusted
  using only information that was knowable at the time. A served value for a past date can never
  change because of a later split or dividend, and ARGUS can explain any value factor by factor.
- **Cross-source–verified data.** Every daily bar is voted on across sources (yfinance, Alpaca
  IEX, Stooq) and graded `good` / `degraded` / `quarantined`. Nothing single-source is served
  without being tagged; conflicting bars are quarantined, never served.
- **Survivorship-bias-free universe.** Symbol-directory snapshots from day 1 build a forward
  graveyard of delisted tickers with termination reasons and coverage metrics.
- **Hybrid intraday frame.** Minute bars with real IEX best-bid/offer where available, and a
  Corwin–Schultz synthetic spread elsewhere — every row tagged with its derivation.
- **Sectors** mapped from SEC EDGAR (SIC → sector ETF).
- **A gap ledger.** A published table of exactly what free data cannot buy — single-source
  share, synthetic-spread share, coverage gaps, source outages — each with a severity.
- **A frozen serving contract.** The published shapes are schema-checked in CI and again inside
  the nightly publish; a regression fails inside ARGUS and can never reach a consumer.

## How it works, in one paragraph

Every night ARGUS pulls raw payloads from free sources and lands them append-only (**L0**, never
fetched twice). It normalizes them into an immutable Parquet event store (**L2**, the system of
record). It votes across sources to build a canonical, revision-tracked state in DuckDB (**L3**),
applies corporate-action factors as a point-in-time view, and materializes a **sealed serving
database** only after it passes a byte-for-byte contract gate. The DuckDB build file is
disposable — `argus rebuild` deterministically regenerates it by replaying the event store.

```
free sources ─▶ L0 landing ─▶ L2 events ─▶ vote + SCD-2 ─▶ canonical DuckDB
                (append-only)  (system of    (grade + revision   │
                               record)        history)           ▼
                                                        PIT serving views
                                                                 │
                                             publish (contract gate + atomic swap)
                                                                 ▼
                                                    argus_serving.duckdb  ◀─ consumers read this
```

## Quick start (Windows)

```powershell
# 1. configure (blank keys mean a job SKIPS, it does not fail)
copy .env.example .env    # then fill in your API keys

# 2. one-shot setup: pick where the data lives, create the venv, install, init the DB
.\scripts\setup_argus.bat            # prompts for a data root, e.g. D:\argus-data
.\scripts\setup_argus.bat D:\argus-data   # or pass it non-interactively

# 3. schedule it (daily 23:45 local + at logon; idempotent per trade date)
.\scripts\register_scheduled_tasks.ps1 -ArgusExe D:\argus-data\venv\Scripts\argus.exe
```

`setup_argus.bat` writes your chosen location into `.env` as `ARGUS_DATA_ROOT` (leaving your
keys untouched), creates the venv under it, installs the package with its `[dev]` extras, runs
`init-db`, and offers the one-off `argus bootstrap` if a Polygon key is present. It is safe to
re-run: an existing venv is reused, `init-db` only applies missing migrations, and it warns
before repointing `.env` at a different data root.

<details>
<summary>Manual setup, if you prefer</summary>

```powershell
py -3.14 -m venv D:\argus-data\venv          # OUTSIDE any cloud-synced folder
D:\argus-data\venv\Scripts\pip install -e ".[dev]"
copy .env.example .env                        # set ARGUS_DATA_ROOT=D:\argus-data
D:\argus-data\venv\Scripts\argus check
D:\argus-data\venv\Scripts\argus init-db
D:\argus-data\venv\Scripts\argus bootstrap    # one-off; requires the Polygon key
D:\argus-data\venv\Scripts\argus nightly
```
</details>

All data lands under `ARGUS_DATA_ROOT` (code default `C:\argus-data`; the setup script lets you
put it on any local disk) — startup **refuses** a OneDrive/Dropbox path, because Parquet + DuckDB
under a sync client risks corruption. The repo carries code only; `.gitignore` blocks `*.duckdb`.

## Choosing what it tracks

Edit **[`config/universe.yaml`](config/universe.yaml)** — the daily spine. It ships with **10
macro-factor ETFs + 102 S&P 100 constituents**, and it is the one file to change:

- **Add a ticker** → the next nightly detects it and pulls its **full deep history** once
  (`j02c_yf_backfill`). Existing tickers are never re-fetched or rewritten.
- **Remove a ticker** → it stops accruing; its history is kept.
- No re-bootstrap, no wipe. Editing before first setup works the same way.

**[`config/watchlist.yaml`](config/watchlist.yaml)** is a separate, deliberately small subset —
the names harvested *intraday* (minute bars + IEX quotes). It costs ~120–340 API calls **per
ticker per session**, so keep it curated; the universe costs ~1 call per ticker per night.

## Reading the data

Point a read-only DuckDB connection at the sealed serving database:

```python
import duckdb
con = duckdb.connect("D:/argus-data/argus_serving.duckdb", read_only=True)  # your ARGUS_DATA_ROOT
con.execute("SELECT * FROM vw_mad_daily_ohlcv WHERE ticker = 'AAPL' ORDER BY effective_date").pl()
con.execute("SELECT gap_key, metric, severity FROM gap_ledger").pl()   # what the free data can't buy
```

The published tables (`vw_mad_daily_ohlcv`, `vw_mad_intraday`, `vw_mad_delisted`,
`vw_mad_coverage`, `vw_mad_sectors`, `gap_ledger`, `serving_meta`) are a **frozen, additive-only
contract**. See [docs/serving-contract.md](docs/serving-contract.md).

## CLI

```
argus check        # env sanity: data root, keys, latest completed session
argus init-db      # create data root + build DB at current schema
argus bootstrap    # one-off deep-history spine (requires Polygon key)
argus nightly      # calendar gate -> capture -> build -> vote -> publish (idempotent)
argus rebuild --yes    # wipe canonical tables, deterministically replay from L2
argus verify-pit --ticker AAPL --date 2020-08-28   # show a served value's full audit trail
argus status       # recent job runs + DLQ depth
argus dlq-list     # open dead-letter entries
argus jobs-list    # nightly jobs in execution order
```

## Documentation

| Document | What it covers |
|---|---|
| [Architecture](docs/architecture.md) | Layered design, the two-clock model, core principles. |
| [Data flow & pipeline](docs/pipeline.md) | The nightly job registry, idempotency, bootstrap, rebuild. |
| [Sources & voting](docs/sources-and-voting.md) | The free sources and how disagreements are resolved. |
| [Point-in-time correctness](docs/point-in-time.md) | Two clocks, SCD-2 history, corporate-action adjustment. |
| [Data model](docs/data-model.md) | Every table and view; the L0/L2 Parquet layouts. |
| [Serving contract](docs/serving-contract.md) | The frozen published shapes, the gate, atomic publish. |
| [Reliability & operations](docs/reliability.md) | Budgets, circuit breakers, DLQ, chaos drills, gap ledger. |
| [Setup & runbook](docs/operations.md) | Install, configure, schedule, health checks, recovery. |

## Development

```powershell
pytest             # always offline: pytest-socket blocks the network in tests
ruff check .
mypy src
```

## Tech stack

Python 3.11+ · DuckDB · Polars · PyArrow · Pydantic · Typer · httpx · structlog ·
exchange-calendars. Sources: yfinance, Alpaca (IEX), Polygon, SEC EDGAR, Stooq, NASDAQ symbol
directories.
