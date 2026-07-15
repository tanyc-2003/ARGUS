# ARGUS Runbook

## Daily health check (30 seconds)

```powershell
D:\argus-data\venv\Scripts\argus status      # last night's job statuses + DLQ depth
D:\argus-data\venv\Scripts\argus dlq-list    # anything open needs a look
```

A healthy night: every job `ok` / `skipped_already_done` / `skipped_source_down`
(keys not configured) / `budget_exhausted` (resumes tomorrow). `failed` means DLQ.

## Dead-source drill (run quarterly)

1. Rename a key in `.env` (e.g. `ARGUS_POLYGON_API_KEY` -> `_DISABLED`).
2. Run `argus nightly --force`. Expected: the source's jobs record
   `skipped_source_down`, everything else completes, publish still seals,
   gap ledger updates. Zero unhandled exceptions.
3. Restore the key. Next run heals automatically (idempotent capture lookbacks).

## DLQ triage

- `source_schema_drift`: a vendor changed shape. Inspect the landed payload at
  the path in the entry; fix the normalizer; re-run the build job with
  `argus job <name> --force`. Then `argus dlq-resolve <id>`.
- `source_oversized`: one (ticker, session) paginated past the page cap. NOT a
  vendor problem — the response is just bigger than we page. The pair is
  skipped while the entry is open, on purpose: retrying it costs a full page
  cap of calls *every night, forever*. If the volume is legitimate, raise
  `alpaca.MAX_PAGES_PER_SESSION` (check the budget clears it too), then
  `argus dlq-resolve <id>` to re-arm the fetch. Repeated entries for the same
  liquid names mean the cap is now under real volume — size it well above, not
  near, since a too-low cap hides the very sessions that prove it too low.
- `vote_conflict`: all sources disagree on a bar. Check `vote_results` for the
  per-source closes; the row is quarantined (never served) until sources agree.
- `transport`: network flake. The circuit breaker opens after 3 consecutive
  failures and self-heals after ~20h.

## Restore from scratch (the DuckDB file is disposable)

The Parquet stores are the system of record (`D:\argus-data\landing`, `\events`;
mirrored nightly to `D:\argus-data\backup` by j15). To restore:

```powershell
# if the data volume died: copy backup\landing + backup\events back first
Remove-Item D:\argus-data\argus.duckdb
D:\argus-data\venv\Scripts\argus rebuild --yes    # deterministic replay + republish
```

## Scheduler

- "ARGUS Nightly" (23:45 local, missed-run recovery, wake-to-run) MUST have the
  repo as its working directory — `scripts\register_scheduled_tasks.ps1` sets it.
- "ARGUS Catch-up" (at logon) registers as a **per-user** task; no elevated
  shell needed. Its trigger is scoped to the current user on purpose — a bare
  `-AtLogOn` is an *any-user* task, which needs elevation and otherwise fails
  with `Access is denied`, leaving Nightly registered and Catch-up missing.
- Double-fire is harmless: jobs are idempotent per trade date and a concurrent
  instance bows out on the DuckDB lock.
- Check both are actually there (only Nightly present = the failure above):

```powershell
Get-ScheduledTask | Where-Object TaskName -like "ARGUS*" | Select-Object TaskName, State
```

## Known source states

- **Stooq (2026-07): BLOCKED.** Both the per-symbol CSV endpoint (JS
  proof-of-work challenge) and the bulk file (401) are closed to plain HTTP
  clients. ARGUS respects the challenge rather than defeating it. The daily
  spine runs on yfinance (+ Alpaca once keys land); the bootstrap uses
  yfinance deep history (`b01_yf_history`). `j02b_stooq_monthly` probes weekly
  with failure backoff and heals automatically if Stooq reopens.

## Cadenced jobs

- `j02b_stooq_monthly`: full-history re-pull every ~28 days (weekly probe while
  blocked); silent vendor rewrites surface as SCD-2 revisions through the vote
  (watch revision counts).
- `j13_parity_sample`: ~25 bars vs Polygon weekly -> `parity_scores`; the gap
  ledger carries the worst divergence. Sustained breaches = demote the source
  in the voting priority (quality/voting.py `_PRIORITY`).

## Adding / removing tickers

Edit `config/universe.yaml`. Nothing else — no re-bootstrap, no wipe:

- **Added**: `j02c_yf_backfill` spots it on the next night and pulls its full
  deep history once (1 call). Tickers that already have history are never
  re-fetched or rewritten.
- **Removed**: stops accruing; history is kept.
- Use the dotted form for share classes (`BRK.B`) — adapters re-spell per
  vendor (Yahoo wants `BRK-B`, Alpaca rejects it).
- Confirm a new name landed: `argus status` (j02c `rows_out` = names added).

Do NOT paste the universe into `watchlist.yaml` — that drives intraday tick
capture at 120-340 calls per ticker per session (~1 GB/session for 110 names)
and will exhaust the Alpaca budget. Keep the watchlist a curated subset.

Budgets are sized against universe size (`polygon` 200, `edgar` 250 = 1 call
per ticker). Growing the universe past ~200 names means raising those too, or
`j06`/`j07b` will silently cover only the first N.

## Key rotation / adding keys

Edit `.env`, then `argus check`. No restart needed — every run re-reads it.
After adding the Polygon key for the first time: `argus bootstrap` (one-off
10y spine; refuses to run without the split feed, ~8 minutes of drip).

Note: an `ARGUS_*` env var in `.env` **overrides** the code default, so a
budget pinned there keeps its old ceiling after an upgrade. If a job exhausts
unexpectedly after a version bump, check `.env` first.
