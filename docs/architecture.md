# Architecture

ARGUS is a layered pipeline. Data moves in one direction — from raw vendor bytes to a
sealed serving database — and each layer has a single, testable responsibility. The design
goal is stated in one sentence: **serve the value the world could actually have known at
the time, from free data, and disclose every gap instead of papering over it.**

```
                    free vendors (yfinance, Alpaca IEX, Polygon, SEC EDGAR, Stooq, NASDAQ dirs)
                                                │
        ┌───────────────────────────────────────┼────────────────────────────────────────┐
        │  L0  LANDING   raw payloads, exactly as received; append-only; never fetched twice │
        │      {data_root}/landing/{dataset}/date=YYYY-MM-DD/source={source}/{hash}.{ext}    │
        └───────────────────────────────────────┼────────────────────────────────────────┘
                                                │ normalize (parse + reconcile to one shape)
        ┌───────────────────────────────────────┼────────────────────────────────────────┐
        │  L2  EVENT STORE   immutable observations, Parquet, THE SYSTEM OF RECORD           │
        │      bar_events / action_events — each row carries the two clocks + full lineage   │
        └───────────────────────────────────────┼────────────────────────────────────────┘
                                                │ vote (cross-source) + SCD-2 upsert
        ┌───────────────────────────────────────┼────────────────────────────────────────┐
        │  L3  CANONICAL   DuckDB, revision-tracked (SCD-2); disposable projection of L2     │
        │      bars_daily, corporate_actions, graveyard, sectors, bars_minute, quote_bars…   │
        └───────────────────────────────────────┼────────────────────────────────────────┘
                                                │ point-in-time factor view (no look-ahead)
        ┌───────────────────────────────────────┼────────────────────────────────────────┐
        │  SERVING VIEWS   vw_* — adjusted OHLCV, intraday, delisted, coverage, sectors      │
        └───────────────────────────────────────┼────────────────────────────────────────┘
                                                │ publish (materialize + contract gate + atomic swap)
        ┌───────────────────────────────────────┼────────────────────────────────────────┐
        │  SERVING DB   argus_serving.duckdb — sealed nightly snapshot, read-only for consumers│
        └────────────────────────────────────────────────────────────────────────────────┘
```

## The layers

### L0 — Landing zone (`landing/store.py`)

Raw vendor payloads written to disk **exactly as received**, partitioned by dataset, date,
and source. Two guarantees live here:

- **Append-only.** `write()` is the only writer and it refuses to overwrite. Immutability
  is enforced by construction, not by convention.
- **Never fetched twice.** Before any wire call, a job calls `ensure(dataset, source,
  request_key)`; a hit in the `landing_manifest` table means the payload is already on disk
  and the network call is skipped. This is what makes re-runs cheap and idempotent.

Because the payloads are the original bytes, any downstream bug can be fixed and replayed
without re-hitting a vendor (and re-spending a rate budget).

### L2 — Event store (`events/store.py`, `events/schemas.py`)

The **system of record**. Normalizers parse L0 payloads into a small set of immutable event
schemas (`bar_events`, `action_events`) and append them as new Parquet part files. There is
no update or delete API — `append` writes a part, `scan` reads them all. Every event row
carries the two clocks (below), a `payload_hash`, and a `landing_key` back to the L0 payload
that produced it.

The canonical DuckDB layer downstream is a **disposable projection** of this store. That is
the central architectural bet: if the DuckDB file is lost or a build bug is found, `argus
rebuild` replays L2 deterministically and reproduces the exact same canonical state.

### L3 — Canonical layer (`canonical/`, DuckDB tables)

The reconciled, queryable state. Cross-source **voting** (`quality/voting.py`) decides what
enters canonical and with what quality grade, and the generic **SCD-2 upsert**
(`canonical/scd2.py`) is the *only* mutation path into canonical tables. SCD-2 means values
are never updated in place — a correction closes the current row and opens a new version
stamped at the time the correction became knowable, so as-of queries can time-travel.

### Serving views + publish (`serving/`)

The serving views (`vw_*`) apply corporate-action factors as a **point-in-time computation**
and expose only the frozen consumer shapes. The `publish` job materializes those views into a
fresh file, runs the contract gate against it, and atomically swaps it into place. A consumer
never reads the build database and never sees a half-built or contract-violating snapshot.

## The two clocks

The heart of PIT-correctness (`core/clocks.py`). Every fact has two timestamps:

- **`knowledge_time`** — when the *world* could first know the fact.
- **`written_at`** — when *ARGUS* actually recorded it.

**Only `knowledge_time` participates in as-of logic.** How it is stamped depends on the fact:

| Fact kind | `knowledge_time` | Rationale |
|---|---|---|
| Fresh nightly observation | the pull moment | We learned it when we pulled it. |
| Backfilled world fact (bootstrap bar, historical split/dividend) | the fact's own date (a bar at its close; a corporate action at its ex-date) | We could not have known a 2016 bar before 2016 ended. This is what makes historical as-of reconstruction possible. |
| Correction / revision | the detection time | An as-of query *before* the correction was detected must still return the pre-correction value. |

A source-tree test enforces that **nothing outside `core/clocks.py` calls
`datetime.now()`/`utcnow()` directly** — every "now" flows through this one file, so the
clock discipline can't quietly erode.

Everything is **UTC everywhere**. Exchange-local time exists only in `core/calendars.py`
(session boundaries) and in the point-in-time factor logic (a corporate action is "known" by
exchange-local end of day).

## Core principles

These hold across the whole codebase and are each backed by tests:

1. **Free sources only, every gap disclosed.** No paid feeds. What money cannot buy is
   *measured* (the [gap ledger](reliability.md#the-gap-ledger)), not hidden.
2. **Immutability at the edges.** L0 and L2 are append-only; the DuckDB file is a rebuildable
   projection.
3. **Two clocks, and only `knowledge_time` drives as-of logic.** No look-ahead is possible
   in a served value.
4. **Nothing single-source enters canonical unconfirmed without being tagged.** Cross-source
   voting grades every bar (`good` / `degraded` / `quarantined`).
5. **Every failure is classified and dead-lettered.** Budget exhaustion and a dead source are
   *normal terminal states*, not error loops.
6. **Idempotency per trade date.** `(job_name, trade_date)` is the idempotency key; a night
   can be double-fired or resumed with no harm.
7. **The serving shape is a frozen contract.** A schema regression fails inside ARGUS and can
   never reach a consumer.

## The data root

All runtime artifacts (Parquet, DuckDB, logs, backups) live under a single `ARGUS_DATA_ROOT`
(default `C:\argus-data`). Startup **refuses** a path inside a cloud-synced folder
(OneDrive/Dropbox/Google Drive): Parquet append churn plus DuckDB WAL files under a sync
client is a corruption generator. The code repository carries code only; `.gitignore` blocks
`*.duckdb`. See `settings.py`.
