# ARGUS Runbook

## Daily health check (30 seconds)

```powershell
C:\argus-data\venv\Scripts\argus status      # last night's job statuses + DLQ depth
C:\argus-data\venv\Scripts\argus dlq-list    # anything open needs a look
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
- `vote_conflict`: all sources disagree on a bar. Check `vote_results` for the
  per-source closes; the row is quarantined (never served) until sources agree.
- `transport`: network flake. The circuit breaker opens after 3 consecutive
  failures and self-heals after ~20h.

## Restore from scratch (the DuckDB file is disposable)

The Parquet stores are the system of record (`C:\argus-data\landing`, `\events`;
mirrored nightly to `C:\argus-data\backup` by j15). To restore:

```powershell
# if the data volume died: copy backup\landing + backup\events back first
Remove-Item C:\argus-data\argus.duckdb
C:\argus-data\venv\Scripts\argus rebuild --yes    # deterministic replay + republish
```

## Scheduler

- "ARGUS Nightly" (23:45 local, missed-run recovery, wake-to-run) MUST have the
  repo as its working directory — `scripts\register_scheduled_tasks.ps1` sets it.
- "ARGUS Catch-up" (at logon) needs an elevated shell to register.
- Double-fire is harmless: jobs are idempotent per trade date and a concurrent
  instance bows out on the DuckDB lock.

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

## Key rotation / adding keys

Edit `.env`, then `argus check`. No restart needed — every run re-reads it.
After adding the Polygon key for the first time: `argus bootstrap` (one-off
10y spine; refuses to run without the split feed, ~8 minutes of drip).
