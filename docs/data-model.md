# Data model

ARGUS keeps its data in three physical stores under `ARGUS_DATA_ROOT`:

- **L0 landing** — raw payloads, Parquet/JSON/CSV on disk, partitioned.
- **L2 event store** — immutable Parquet event tables (the system of record).
- **Build DuckDB** (`argus.duckdb`) — the canonical + operational + serving state; a
  disposable projection rebuilt from L2.
- **Serving DuckDB** (`argus_serving.duckdb`) — the sealed nightly snapshot consumers read.

## On-disk layouts

### L0 landing (`landing/store.py`)

```
{data_root}/landing/{dataset}/date=YYYY-MM-DD/source={source}/{slug}.{hash}.{ext}
```

Every landed payload is registered in the `landing_manifest` table (below), which enforces
never-fetch-twice. Datasets include `yf_daily`, `yf_minute`, `alpaca_daily`, `alpaca_quotes`,
`polygon_splits`, `polygon_dividends`, `polygon_delisted`, `polygon_parity`, `edgar_submissions`,
`symbol_dirs`, and the Stooq history pulls.

### L2 event store (`events/schemas.py`)

Append-only Parquet part files under `{data_root}/events/{event_type}/part-*.parquet`. Two
event types, each carrying the two clocks, a `payload_hash`, and a `landing_key` back to L0:

**`bar_events`** — one row per observed bar per source:

| Column | Type | Notes |
|---|---|---|
| `event_id` | Utf8 | |
| `source` | Utf8 | `yfinance` / `stooq` / `alpaca_iex` |
| `ticker`, `interval`, `bar_date` | Utf8 / Utf8 / Date | `interval` = `1d` (minute events also flow here) |
| `open`,`high`,`low`,`close`,`volume` | Float64 | `close` is **raw** (post split-reversal) |
| `vendor_adjusted` | Boolean | True if the vendor served split-adjusted prices |
| `reversal_factor` | Float64 | factor applied at L1 to reconstruct raw (1.0 = none) |
| `knowledge_time`, `written_at` | Datetime(UTC) | the two clocks |
| `payload_hash`, `landing_key` | Utf8 | content hash + lineage to L0 |

**`action_events`** — one row per corporate action: `action_type` (`split`/`dividend`),
`ex_date`, `ratio` (splits), `cash_amount` (dividends), `declared_date`, plus the same clock +
lineage columns.

## Build database (`db.py`)

Schema is applied by an **ordered, append-only migration list** — entries are never edited
after shipping, only appended; the version is the 1-based index. Views are recreated on every
migrate (they evolve without migration ceremony). Migrations are versioned by the milestone
that introduced them.

### Operational tables (v1)

| Table | Purpose |
|---|---|
| `landing_manifest` | Registry of every landed L0 payload; PK `(dataset, source, request_key)` enforces never-fetch-twice. |
| `job_runs` | One row per job execution: status, timings, `rows_out`, `budget_used`, `error_class`. |
| `dead_letter` | Classified failures needing human attention (see [Reliability](reliability.md)). |
| `source_health` | Per-source circuit-breaker state (`closed`/`open`, consecutive failures, cooldown). |
| `market_sessions` | Exchange session boundaries from `exchange-calendars`. |
| `schema_migrations` | Applied migration versions. |

### Canonical tables (SCD-2, v2)

**`bars_daily`** — the canonical daily spine. `close` is **raw** price. Carries the vote
outputs (`source_set`, `grade`, `single_source`) and the SCD-2 columns (`valid_from`,
`valid_to`, `is_current`, `revision_seq`). Grades: `good` / `degraded` / `quarantined`.

**`corporate_actions`** — splits and dividends with `ex_date`, `ratio`/`cash_amount`,
`confidence` (`confirmed`/`single_source`/`inferred`), and the same SCD-2 columns.

### Quality, survivorship, intraday, trust (v3–v6)

| Table | Milestone | Purpose |
|---|---|---|
| `vote_results` | v3 | Per-bar vote audit: verdict, `n_sources`, chosen source, per-source closes, `volume_agrees`, `mad_flag`. Projection, rewritten each seal. |
| `universe_snapshots` | v4 | Immutable symbol-directory snapshots (`nasdaqlisted`/`otherlisted`). |
| `graveyard` | v4 | Delisted tickers: `termination_date`, `termination_reason` (enum: merger/bankruptcy/acquisition/voluntary/unknown), `reason_confidence`, `terminal_return`, `detection_source`. |
| `coverage_metrics` | v4 | Survivorship coverage per audit window (`10y`, etc.), in `[0,1]`. |
| `bars_minute` | v5 | Minute OHLCV; consolidated (yfinance) volume, **never IEX**. |
| `quote_bars_1m` | v5 | IEX BBO aggregated to minute buckets (bid/ask close + time-weighted mean). |
| `intraday_processed` | v5 | Incremental-processing marker for L0 minute payloads. |
| `serving_intraday` | v5 | The hybrid minute frame served: bid/ask/volume + `derivation` (`iex_bbo`/`corwin_schultz`). |
| `parity_scores` | v6 | Weekly Polygon spot-check: per-field `ours`/`theirs`/`rel_diff`/`within_tol`. |
| `sectors` | v6 | Ticker → SIC → sector ETF + industry description (from EDGAR). |
| `gap_ledger` | v6 | What free data cannot buy, measured: `metric`, `unit`, `severity` (`info`/`warn`/`blocker`). |

## Views (`db.py::VIEWS`)

Recreated on every migrate; idempotent.

- **`vw_adjustment_factors`** — the single source of truth for corporate-action factors, a
  UNION of split factors (`ratio`) and dividend factors (`prev_close / (prev_close − cash)`)
  over current `corporate_actions`. See [Point-in-time](point-in-time.md).
- **`vw_mad_daily_ohlcv`** — PIT-adjusted daily OHLCV. Multiplies raw prices by the cumulative
  factor of only those factors knowable by end of each bar date; divides volume by the split
  factor. **This view is the PIT guarantee.**
- **`vw_mad_delisted`** — the graveyard as the served delisted shape.
- **`vw_mad_coverage`** — survivorship coverage per audit window.
- **`vw_mad_intraday`** — `serving_intraday` with the minute cast to naive UTC.
- **`vw_mad_sectors`** — non-null sector mappings.

The `vw_mad_*` views (and their materialized tables in the serving DB) are the frozen
[serving contract](serving-contract.md). The `vw_` prefix is preserved on the materialized
serving *tables* deliberately — the names are part of the contract.

## Repo-side config (`config/`, `config_files.py`)

YAML loaded from the working directory (or `ARGUS_CONFIG_DIR`):

- **`universe.yaml`** — the tickers ARGUS tracks (`ticker`, `role`); the **R1 daily spine** and
  the file users are expected to edit. Ships with 10 macro-factor ETFs (`role: factor_etf`, the
  dashboard's fixed proxies) + 102 S&P 100 names (`role: sp100`). `role` is metadata only —
  nothing branches on it. Costs ~1 call per ticker per night per source. Add a ticker and the
  next nightly backfills its full deep history once (`j02c_yf_backfill`, see
  [pipeline](pipeline.md#growing-the-universe-j02c_yf_backfill)); remove one and it stops
  accruing while keeping its history.
- **`watchlist.yaml`** — the **R2 intraday** subset (minute bars + IEX quotes), a plain ticker
  list. Deliberately small and *not* the universe: quote capture costs **120–340 calls per
  ticker per session** (~1 GB/session at 110 names), versus ~1 call/ticker/night for the
  universe. Keep it curated.
- **`sic_sector_map.yaml`** — `(lo, hi, sector_etf)` SIC ranges, first match wins, for the
  sector mapping.

### Ticker spelling

Tickers are stored in the **canonical dotted form** for share classes (`BRK.B`). Vendors
disagree on this and no single spelling works everywhere: Alpaca and Polygon serve `BRK.B` and
reject `BRK-B`; Yahoo is the exact opposite. Adapters re-spell for their own vendor
(`sources/_yf.py::yahoo_symbol`), so config stays vendor-neutral.
