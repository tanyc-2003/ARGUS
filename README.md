# ARGUS

Minimum-cost, PIT-correct market-data platform (Architecture v4, Tier 0) feeding the
Market Advisory Dashboard. DuckDB + Parquet on a single machine; free sources only;
every gap measured and disclosed instead of papered over.

**Design docs:** `../ARGUS Architecture v4.md` (the spec) and
`../ARGUS_Dashboard_Integration.md` (the consumer contract).

## Status

| Milestone | State |
|---|---|
| M0 — ops backbone + day-1 capture (symbol dirs, yfinance 1-min, Alpaca IEX quotes) | ✅ PR #1 |
| M1 — daily spine + factor layer + `vw_mad_daily_ohlcv` | ✅ PR #2 |
| M2 — nightly incrementals + revision detection (SCD-2) | ✅ PR #3 |
| M3 — cross-source voting + replay | ✅ PR #4 |
| M4 — survivorship (graveyard, reasons, coverage) | ✅ PR #5 |
| M5 — intraday serving (`vw_mad_intraday`) | pending |
| M6 — parity sampling, gap ledger, chaos drills | pending |

## Setup (Windows)

```powershell
# 1. venv OUTSIDE OneDrive (the repo is synced; runtime artifacts must not be)
py -3.14 -m venv C:\argus-data\venv
C:\argus-data\venv\Scripts\pip install -e ".[dev]"

# 2. configure
copy .env.example .env     # fill in Alpaca/Polygon keys; blank keys = jobs skip, not fail

# 3. sanity check + first run
C:\argus-data\venv\Scripts\argus check
C:\argus-data\venv\Scripts\argus init-db
C:\argus-data\venv\Scripts\argus bootstrap   # one-off daily spine (requires POLYGON key)
C:\argus-data\venv\Scripts\argus nightly

# inspect PIT correctness for any (ticker, date):
C:\argus-data\venv\Scripts\argus verify-pit --ticker AAPL --date 2020-08-28

# 4. schedule (fires daily 23:45 local + at logon; idempotent per trade date)
.\scripts\register_scheduled_tasks.ps1 -ArgusExe C:\argus-data\venv\Scripts\argus.exe
```

All data lands under `ARGUS_DATA_ROOT` (default `C:\argus-data`) — startup **refuses**
a OneDrive/Dropbox path. The repo carries code only; `.gitignore` blocks `*.duckdb`.

## Why capture starts on day 1

Three feeds compound with calendar time and cannot be backfilled later:

- **Symbol-directory snapshots** — the first snapshot is the forward graveyard's baseline;
  every delisting after go-live is caught by diffing (survivorship coverage ≈ 1.0 forward).
- **yfinance 1-minute bars** — Yahoo serves only ~30 days back; every day of delay is
  minute history permanently lost.
- **Alpaca IEX quotes** — friction baselines need 4–6 weeks of accrual before z-scores work.

M0 lands these raw payloads nightly (L0, append-only, never fetched twice); later
milestones build the processing and replay it over everything accrued since day 1.

## CLI

```
argus check        # env sanity: data root, keys, latest completed session
argus init-db      # create data root + build DB at current schema
argus nightly      # calendar gate -> capture jobs -> summary (idempotent)
argus job NAME     # run one job; argus jobs-list shows names
argus status       # recent job_runs + DLQ depth
argus dlq-list     # open dead-letter entries
```

## Development

```powershell
pytest             # offline always: pytest-socket blocks the network in tests
ruff check .
mypy src
```

Conventions the whole codebase holds:

- **Two clocks**: `knowledge_time` (when the world could know) vs `written_at` (when we
  wrote). Wall-clock reads only in `core/clocks.py` — a test enforces this.
- **Append-only L0**: `landing/store.py` refuses overwrites; never-fetch-twice via the
  manifest.
- **UTC everywhere**; exchange-local time exists only in `core/calendars.py`.
- **Every failure classified** (`ops/errors.py`) and dead-lettered; budget exhaustion is
  a normal terminal state, not an error.
