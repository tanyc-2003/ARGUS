# Setup & runbook

ARGUS targets Windows (the scheduler integration uses Windows Task Scheduler) and Python 3.11+.
It runs on a single machine with a local data volume.

## Install

```powershell
# 1. Configure first — setup reads .env to find (or set) your data root.
copy .env.example .env      # fill in keys; a blank key means the job SKIPS, it does not fail

# 2. One-shot setup.
.\scripts\setup_argus.bat              # prompts for the data root
.\scripts\setup_argus.bat D:\argus-data   # or pass it non-interactively
```

`setup_argus.bat` does the whole install: it takes the **data root** (where `argus.duckdb`,
Parquet, logs and the venv all live — pick any local disk, e.g. `D:\`), writes it into `.env` as
`ARGUS_DATA_ROOT` *without touching your keys*, creates the venv under it, `pip install -e
".[dev]"` (the `[dev]` extras matter — the verify loop runs out of this venv), runs `init-db`,
and offers the one-off `argus bootstrap` when a Polygon key is present.

It is **safe to re-run**: an existing venv is reused, `init-db` only applies missing migrations
(never wipes), and it warns before repointing `.env` at a *different* data root. It refuses a
OneDrive/Dropbox/GDrive path up front, mirroring the guard in `settings.py`.

<details>
<summary>Manual install, if you prefer</summary>

```powershell
py -3.14 -m venv D:\argus-data\venv       # OUTSIDE any cloud-synced folder
D:\argus-data\venv\Scripts\pip install -e ".[dev]"
copy .env.example .env                     # set ARGUS_DATA_ROOT=D:\argus-data
D:\argus-data\venv\Scripts\argus check
D:\argus-data\venv\Scripts\argus init-db
```
</details>

### Environment (`.env`, all vars prefixed `ARGUS_`)

| Variable | Needed for | Blank behavior |
|---|---|---|
| `ARGUS_DATA_ROOT` | Everything (code default `C:\argus-data`; `setup_argus.bat` sets it for you) | Must be **outside** OneDrive/Dropbox/GDrive — startup refuses a synced path unless `ARGUS_ALLOW_SYNCED_DATA_ROOT=1`. |
| `ARGUS_ALPACA_KEY_ID` / `ARGUS_ALPACA_SECRET_KEY` | Alpaca IEX daily bars + quotes (`j03`, `j05`) | Those jobs skip cleanly. |
| `ARGUS_POLYGON_API_KEY` | Corporate actions, delisted ref, parity, and **`bootstrap`** | Corporate-action jobs skip; `bootstrap` **refuses** to run. |
| `ARGUS_EDGAR_USER_AGENT` | SEC EDGAR (sectors, delisting reasons) | EDGAR job skips. Must be a descriptive UA with contact info (EDGAR fair-access policy). |

Nightly per-source call budgets are also configurable (`ARGUS_POLYGON_NIGHTLY_BUDGET`,
`ARGUS_YFINANCE_NIGHTLY_BUDGET`, `ARGUS_ALPACA_NIGHTLY_BUDGET`); defaults are in `settings.py`
and are sized in [Reliability](reliability.md#rate-discipline-buckets--budgets-opsratelimitpy).
An env var **overrides** the default, so a budget pinned in `.env` will silently keep an old
ceiling after an upgrade — check with `argus check` if a job exhausts unexpectedly.

## Choosing what ARGUS tracks

Edit **`config/universe.yaml`** (the daily spine; ships with 10 factor ETFs + 102 S&P 100
names). You can edit it before setup, or at any time afterwards:

- **Added** ticker → the next nightly pulls its **full deep history** once (`j02c_yf_backfill`).
  Existing tickers are never re-fetched or rewritten. No re-bootstrap needed.
- **Removed** ticker → stops accruing; its history is kept.

Use the canonical dotted form for share classes (`BRK.B`, not `BRK-B`) — adapters re-spell per
vendor. **`config/watchlist.yaml`** is the separate intraday subset; keep it small (it costs
120–340 API calls *per ticker per session*, versus ~1/night for the universe).

## First run

```powershell
# One-off deep-history spine (requires the Polygon key). ~8 minutes of rate-limited drip.
D:\argus-data\venv\Scripts\argus bootstrap

# A normal night.
D:\argus-data\venv\Scripts\argus nightly

# Explain how any served value was built, factor by factor.
D:\argus-data\venv\Scripts\argus verify-pit --ticker AAPL --date 2020-08-28
```

## Schedule it

```powershell
# Fires daily 23:45 local + at every logon; idempotent per trade date.
.\scripts\register_scheduled_tasks.ps1 -ArgusExe D:\argus-data\venv\Scripts\argus.exe
```

This registers two Windows scheduled tasks (both `StartWhenAvailable` + `WakeToRun`, so a
missed night runs as soon as the machine is next awake):

- **ARGUS Nightly** — daily at 23:45 local (comfortably after the US close year-round; the
  runner resolves the actual trade date from the exchange calendar).
- **ARGUS Catch-up** — at every logon; recovers nights lost to sleep/shutdown. Harmless if the
  night is already sealed.

The tasks set the **repo as the working directory** — this is required, because ARGUS resolves
`config/` relative to its working directory and Task Scheduler otherwise defaults to `System32`
(capture jobs would fail with `FileNotFoundError`). Double-firing is safe: jobs are idempotent
per trade date and a concurrent instance bows out on the DuckDB lock.

Both are registered as **per-user** tasks, so no elevated shell is needed. The logon trigger is
deliberately scoped to the current user: a bare `-AtLogOn` registers an *any-user* task, which
requires elevation and otherwise fails with `Access is denied` — leaving Nightly registered and
Catch-up silently missing.

Verify both exist:

```powershell
Get-ScheduledTask | Where-Object TaskName -like "ARGUS*" | Select-Object TaskName, State
```

## The CLI

| Command | Purpose |
|---|---|
| `argus check` | Env sanity: data root, which keys are set, latest completed session. |
| `argus init-db` | Create the data root and build the DB at the current schema. |
| `argus nightly [--only NAME ...] [--force]` | Run the nightly pipeline for the latest completed session. |
| `argus job NAME [--force]` | Run a single named job. |
| `argus jobs-list` | List the nightly jobs in execution order. |
| `argus bootstrap [--force]` | One-off deep-history spine (requires Polygon key). |
| `argus rebuild --yes` | Wipe canonical tables and deterministically replay from L2. |
| `argus verify-pit --ticker T --date YYYY-MM-DD` | Show the full PIT audit trail for a served value. |
| `argus status [--limit N]` | Recent `job_runs` + open DLQ depth. |
| `argus dlq-list [--limit N]` | Open dead-letter entries. |
| `argus dlq-resolve ID` | Mark a dead-letter entry resolved. For a `source_oversized` entry this also **re-arms the fetch** — the pair is skipped while the entry is open. |

## Daily health check (30 seconds)

```powershell
D:\argus-data\venv\Scripts\argus status      # last night's job statuses + DLQ depth
D:\argus-data\venv\Scripts\argus dlq-list     # anything open needs a look
```

A healthy night: every job is `ok`, `skipped_already_done`, `skipped_source_down` (keys not
configured), or `budget_exhausted` (resumes tomorrow). `failed` means there is a DLQ entry to
triage.

## DLQ triage

- **`source_schema_drift`** — a vendor changed shape. Inspect the landed payload at the path in
  the entry, fix the normalizer, re-run the build job (`argus job <name> --force`), then
  `argus dlq-resolve <id>`.
- **`vote_conflict`** — all sources disagree on a bar. Check `vote_results` for the per-source
  closes; the row is quarantined (never served) until sources agree.
- **`transport`** — network flake. The circuit breaker opens after 3 consecutive failures and
  self-heals after ~20h.

## Dead-source drill (run quarterly)

1. Rename a key in `.env` (e.g. `ARGUS_POLYGON_API_KEY` → `_DISABLED`).
2. Run `argus nightly --force`. Expected: that source's jobs record `skipped_source_down`,
   everything else completes, publish still seals, the gap ledger updates — zero unhandled
   exceptions.
3. Restore the key. The next run heals automatically (idempotent capture lookbacks).

## Disaster recovery (the DuckDB file is disposable)

The Parquet stores are the system of record (`{data_root}/landing`, `{data_root}/events`),
mirrored nightly to `{data_root}/backup` by `j15`. To restore:

```powershell
# If the data volume died, copy backup\landing + backup\events back first.
Remove-Item D:\argus-data\argus.duckdb
D:\argus-data\venv\Scripts\argus rebuild --yes    # deterministic replay + republish
```

## Key rotation / adding keys

Edit `.env`, then `argus check` — no restart needed, every run re-reads it. After adding the
Polygon key for the first time, run `argus bootstrap` once (the ~10y spine; refuses to run
without the split feed).

## Development

```powershell
pytest             # always offline: pytest-socket blocks the network in tests
ruff check .
mypy src
```

Conventions the whole codebase holds (each backed by a test):

- **Two clocks** — `knowledge_time` (when the world could know) vs `written_at` (when we
  wrote). The wall clock is read only in `core/clocks.py`.
- **Append-only L0** — `landing/store.py` refuses overwrites; never-fetch-twice via the
  manifest.
- **UTC everywhere** — exchange-local time exists only in `core/calendars.py` and the PIT
  factor logic.
- **Every failure classified** (`ops/errors.py`) and dead-lettered; budget exhaustion is a
  normal terminal state, not an error.
