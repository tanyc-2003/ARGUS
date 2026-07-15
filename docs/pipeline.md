# Data flow & the nightly pipeline

ARGUS does its work in a single nightly run: a **calendar gate** decides whether a session
just completed, then an **ordered list of jobs** runs, each with full bookkeeping, and the
night ends by publishing a sealed serving database. The whole run is idempotent per trade
date, so it is safe to fire twice (the scheduler plus a catch-up trigger) and safe to resume
after a crash.

Entry point: `argus nightly` → `orchestration/runner.py::run_nightly`.

## How a night runs

1. **Acquire the build database.** DuckDB is single-writer. If another ARGUS instance already
   holds the lock (the nightly and catch-up scheduler entries can fire concurrently), this
   instance logs and **bows out quietly** with exit code 0 — the other one is doing the work.
2. **Refresh the market calendar.** `calendars.refresh_market_sessions` populates
   `market_sessions` for a window around today (−60 … +370 days) from `exchange-calendars`.
3. **Calendar gate.** `latest_completed_session(now)` returns the trade date to process, or
   `None` if no US session has completed in the window (weekend/holiday) — in which case the
   night is a no-op.
4. **Run the registry in order.** Each job goes through `run_job`, which records a row in
   `job_runs` and returns a status. Jobs are skipped if already `ok` for the trade date
   (unless `--force` or the job is marked `always`).
5. **Summarize.** Exit code is `1` if any job recorded `failed`, else `0`. A non-zero exit is
   how the OS scheduler's "Last Run Result" surfaces a degraded night — silence is never
   treated as success.

## Job bookkeeping and statuses (`ops/jobs.py`)

The idempotency key is **`(job_name, trade_date)`**. A job that already has an `ok` row for
the trade date is skipped. Every run records exactly one of these statuses:

| Status | Meaning |
|---|---|
| `ok` | Completed; `rows_out` / `budget_used` / `detail` recorded. |
| `skipped_already_done` | An `ok` row already exists for this trade date (idempotent re-run). |
| `skipped_source_down` | Credentials missing or the circuit breaker is open. **Not a failure.** |
| `budget_exhausted` | The per-run call budget was spent. A **normal terminal state** — resumes tomorrow. |
| `failed` | An error was classified, recorded, and pushed to the dead-letter queue. |

`budget_exhausted` and `skipped_source_down` are deliberately *not* failures: running out of
patience against a free-tier rate limit, or a dead/keyless source, is expected operating
reality, not an incident. Only `failed` opens a dead letter and forces a non-zero exit.

Jobs marked `always=True` (the seals and publish) re-run even when an `ok` row exists,
because they are cheap projections that must refresh whenever any upstream capture in the
same trade date changed.

## The nightly registry (`orchestration/nightly.py::build_registry`)

Job names are **stable across versions** — they are half of the idempotency key, so they are
never renamed. Execution order is load-bearing: captures land raw data, builders normalize
and canonicalize it, seals project it, and publish seals the night.

| Job | Stage | What it does |
|---|---|---|
| `j01_symbol_dirs` | capture | NASDAQ/other listed symbol directory snapshots (survivorship baseline). |
| `j02_yf_daily` | capture | yfinance daily bars (primary consolidated spine). |
| `j02b_stooq_monthly` | capture (monthly gate) | Full-history Stooq re-pull every ~28 days; silent vendor rewrites surface as SCD-2 revisions. |
| `j02c_yf_backfill` | capture | Deep history for tickers newly added to `universe.yaml`; no-op once each has its spine. |
| `j03_alpaca_daily` | capture | Alpaca daily bars (IEX). |
| `j04_yf_minute` | capture | yfinance 1-minute bars (Yahoo serves only ~30 days back). |
| `j05_alpaca_quotes` | capture | Alpaca IEX quotes → minute BBO buckets (friction baselines). |
| `j06_polygon_ca` | capture | Polygon corporate actions (splits + dividends), rate-drip. |
| `j07_polygon_delisted` | capture | Polygon delisted-ticker reference. |
| `j07b_edgar` | capture | SEC EDGAR submissions (SIC → sector, delisting reasons). |
| `j08_build_actions` | build | Normalize + SCD-2 upsert corporate actions into `corporate_actions`. |
| `j09_build_daily` | build | Normalize daily incrementals → `bar_events`. |
| `j09b_build_stooq` | build | Process the monthly Stooq re-pull payloads (no-op on other nights). |
| `j10_vote_seal` | seal (always) | Cross-source vote over L2 → `bars_daily` + `vote_results`. |
| `j11_universe_seal` | seal (always) | Universe snapshots → graveyard + coverage metrics. |
| `j11b_intraday_seal` | seal (always) | Minute bars × IEX BBO (+ Corwin–Schultz fallback) → `serving_intraday`. |
| `j11c_sector_seal` | seal (always) | EDGAR SIC → sector ETF mapping → `sectors`. |
| `j11d_gap_ledger` | seal (always) | Recompute the gap ledger (what free data can't buy). |
| `j12_publish` | publish (always) | Materialize serving views, run the contract gate, atomically swap `argus_serving.duckdb`. |
| `j13_parity_sample` | audit (weekly gate) | ~25-bar spot check vs Polygon → `parity_scores` (drift alarm). |
| `j15_backup` | backup (always) | Mirror L0 + L2 Parquet into `backup/` (copy-if-absent). |

### Cadenced jobs

Some jobs should not run every night. They gate on the last genuinely-run success in
`job_runs`; a "not due" night records `ok` with a marker detail that the gate ignores:

- **`j02b_stooq_monthly`** — full-history re-pull every ~28 days. While Stooq is blocked (see
  [Sources](sources-and-voting.md#known-source-states)) it probes weekly with failure backoff
  so a blocked source never spams the DLQ nightly.
- **`j13_parity_sample`** — weekly ~25-bar comparison against Polygon EOD aggregates. Seeded
  by trade date, so a forced re-run compares the same bars. It is a **drift alarm**, not a
  parity target: sustained divergence *demotes a source in the voting priority*, it never
  rewrites ARGUS data.

## Growing the universe (`j02c_yf_backfill`)

`b01_yf_history` runs only under `argus bootstrap`, so a ticker **added to `universe.yaml`
afterwards** would otherwise accrue nothing but the rolling 12-day `j02` window — no deep
spine, ever, and no error to say so. `j02c_yf_backfill` closes that: every night it looks for
universe names with **no history payload** and pulls their full archive once.

- **Existing tickers are untouched** — their data is never re-fetched or rewritten.
- "Has history" is matched on the **ticker**, not the request_key: the key carries the trade
  date, so keying on it would re-pull the whole archive every night.
- It runs **before `j08`/`j09`**, so a new ticker's deep history is split-reversed against the
  corporate actions `j06` lands the same night.
- Costs 1 call per *new* ticker and nothing at all once the universe is stable.

So the supported workflow is simply: edit `config/universe.yaml`, and let the next night
converge. No re-bootstrap, no wipe.

## The bootstrap (`orchestration/nightly.py::bootstrap_registry`)

`argus bootstrap` is a one-off run that lays down the deep historical spine. It reuses the
same runner and bookkeeping (so a crashed bootstrap resumes where it stopped) with extra
one-off steps:

```
j06_polygon_ca  →  j08_build_actions  →  b01_yf_history  →  b02_build_daily
                →  j10_vote_seal  →  j11_universe_seal  →  j12_publish
```

Ordering is load-bearing: the split **reversal** at build time consumes the splits
canonicalized by `j08`, which consumes the payloads landed by `j06`. Bootstrap **refuses to
run without `ARGUS_POLYGON_API_KEY`** — without the split feed, the reversal cannot run and
the served prices would silently bake in look-ahead. The deep-history source is yfinance
`period=max` (Stooq's bulk endpoint is currently closed).

## Rebuild (`orchestration/rebuild.py`)

`argus rebuild --yes` wipes the canonical DuckDB tables (`bars_daily`, `corporate_actions`,
`vote_results`) and **replays them from the L2 Parquet event store** — re-voting and
republishing deterministically. The event store is untouched. This is the recovery path when
the DuckDB file is lost or corrupted, and the proof that the DuckDB file is genuinely
disposable. Because the vote runs over the *latest observation per (source, ticker, date)*
with deterministic tie-breaks, replay yields byte-identical canonical state every time.
