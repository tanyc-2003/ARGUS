# Reliability & operations model

ARGUS runs unattended on a single machine against free, rate-limited, sometimes-dead sources.
Its reliability model is built on one idea: **degrade honestly.** A missing source, an
exhausted rate budget, or a dead network must never corrupt data, never spam, and never fail
silently — the night completes on whatever it has, the gap is disclosed, and the failure is
visible.

## Rate discipline: buckets + budgets (`ops/ratelimit.py`)

Two independent limits govern every source:

- **Token bucket** — bounds the *instantaneous* rate. Per-source steady-state rates encode the
  free-tier limits: Polygon 5/min, yfinance ~0.5/s, Alpaca 3/s, EDGAR 8/s.
- **Run budget** — bounds *total calls per nightly run*. When spent, the job raises
  `BudgetExhausted`, which is recorded as `budget_exhausted` and **resumes tomorrow**. Patience
  is the currency; running out of it is a normal terminal state, not an error.

Each job gets its **own** `RunBudget`, so these are per-job ceilings, not a shared pool:

| Source | Budget | Sized against (measured 2026-07) |
|---|---|---|
| Polygon | 400 (`ARGUS_POLYGON_NIGHTLY_BUDGET`) | `j06` costs **`len(KINDS)` calls per ticker** — splits *and* dividends → 112 × 2 = **224 every night**; ~45 min at 5/min |
| yfinance | 600 (`ARGUS_YFINANCE_NIGHTLY_BUDGET`) | `j02` 1/universe ticker; `j04` watchlist × ~16 sessions |
| Alpaca | 15,000 (`ARGUS_ALPACA_NIGHTLY_BUDGET`) | quote ticks paginate: **120–340 calls per ticker per session**. A full 5-session backfill of 15 watchlist names measured **6,747 calls / 48 min**; a steady night ~1.4k |
| EDGAR | 250 (constant) | 1 call per universe ticker still missing a sector |

These are **ceilings, not targets** — a normal night spends a fraction. Size them against a
*full* pass with headroom: see "never start a fetch you cannot finish" below for why a budget
that cannot cover one complete sweep fails unevenly rather than gracefully.

Every wire call goes through one instrumented HTTP client (`ops/http.py::FetchClient`):
bucket → budget → bounded retry with backoff → bytes. Retryable statuses (429, 5xx) back off
(honoring `Retry-After`); a non-retryable 4xx raises `TransportFailure` immediately. Only
yfinance bypasses the client (its library manages Yahoo auth) and is wrapped by the same bucket
and budget at the job layer.

## Circuit breaker (`ops/health.py`)

A source that keeps failing is *skipped*, not allowed to fail the night. After **3 consecutive
failures** the circuit **opens** and the source's jobs record `skipped_source_down`; voting
proceeds with the remaining sources and the gap is disclosed. The circuit re-closes after a
**20-hour cooldown** (shorter than one nightly cadence), so a transient outage self-heals on the
next run — the first attempt after cooldown probes half-open.

## Error taxonomy & the dead-letter queue (`ops/errors.py`, `ops/dlq.py`)

Every failure that reaches the DLQ carries a classification:

| `ErrorClass` | Meaning | Terminal state? |
|---|---|---|
| `rate_limit_exhausted` | Run budget spent | **Normal** — resume tomorrow, no DLQ, no error loop |
| `source_down` | Circuit open or credentials missing | **Normal** — `skipped_source_down`, no DLQ |
| `source_schema_drift` | Vendor silently changed payload shape | Failure → DLQ |
| `source_oversized` | One request's paginated payload exceeds the page cap | Item skipped → DLQ, **suppressed thereafter** |
| `vote_conflict` | All sources disagree on a bar | Row quarantined → DLQ |
| `transport` | Network/HTTP failure after retries | Failure → DLQ |
| `unknown` | Anything unclassified | Failure → DLQ |

`budget_exhausted` and `skipped_source_down` are deliberately not failures. Only genuine
failures open a **dead letter** — an entry needing human attention — and force a non-zero exit
code so the OS scheduler flags the night. Triage guidance is in the [runbook](operations.md).

`source_oversized` is deliberately distinct from `source_schema_drift`: the vendor's shape is
fine, the response is simply larger than we are willing to page. Conflating the two hides real
drift behind a volume problem. It is also the one class that **suppresses future work**: the
pair is recorded once and skipped on later runs (`dlq.has_open`), because retrying a request
that cannot fit costs a full page cap of calls *every night, forever*. `argus dlq-resolve <id>`
clears the entry and re-arms the fetch once the cap is raised.

### Paged captures: never start a fetch you cannot finish

Alpaca quote ticks land **atomically** — one payload per (ticker, session), assembled from up to
hundreds of pages. A fetch interrupted mid-pagination therefore spends every call it made and
writes **nothing**. So `alpaca.capture` reserves the worst case before starting: if the run
budget has less than `MAX_PAGES_PER_SESSION` left, it stops cleanly with `BudgetExhausted`
rather than half-fetching.

Two sizing rules follow, both learned the hard way (see the 2026-07 starvation below):

- **The page cap is a runaway guard, not a fit.** It must sit far above real volume — sizing it
  against *observed* volume is circular, because a cap that is too low hides the very sessions
  that would prove it too low (they never land). Measured peak has climbed 171 → 295 → 339 pages
  as the cap rose; the cap is 800.
- **Budgets must clear a full pass with room to grow.** A budget that cannot cover one complete
  sweep of the watchlist/universe does not degrade evenly — it starves whatever the loop reaches
  last, deterministically, every night.
- **Count the calls per item, not the items.** The cost of a job is rarely "1 per ticker": `j06`
  is one call per (ticker × KIND), quote capture is one per *page*. Both budgets have been set
  wrong by assuming otherwise. `tests/unit/test_budget_sizing.py` pins each budget against the
  **shipped** config so an undersized ceiling fails CI instead of silently truncating a night.

For `j06_polygon_ca` an exhausted budget is a **correctness** failure, not just a coverage one:
a ticker whose corporate actions never land has no canonicalized splits, so the reversal cannot
run and its prices would be served split-adjusted-as-raw — the exact risk `bootstrap` refuses to
take when the Polygon key is missing.

### Fair ordering under a short budget

Any loop over (name × session) must iterate **session-major, newest first**. Ticker-major order
means an early, expensive name drains the budget and the tail of the list is starved *every
night, deterministically* — never sampled, never recovered. Session-major means a short budget
drops the **oldest backfill** instead, and every name still gets the latest session.

> **2026-07 incident.** `j05_alpaca_quotes` landed data for only the first 5 of 15 watchlist
> tickers, nightly. Five defects compounded: a page cap below real volume (so busy sessions were
> permanently unfetchable), those cap hits misfiled as `schema_drift`, per-item containment
> copied from a source where a drift costs 1 call rather than 200, failures recorded *nowhere*
> (so the same doomed pairs burned ~1.2k calls every night forever), and ticker-major ordering
> that starved the same 10 names permanently. Fixing only the budget would have left four of
> them live.

## Idempotency (`ops/jobs.py`)

`(job_name, trade_date)` is the idempotency key. A job with an existing `ok` row for the trade
date is skipped. This makes the two scheduler entries (nightly + catch-up) safe to double-fire,
and makes a crashed night safe to resume — it picks up where it left off. Concurrent instances
are handled at the DuckDB lock: the second one bows out quietly.

## Immutability & disposable state

- **L0 landing** and **L2 events** are append-only Parquet — the system of record.
- **The build DuckDB file is disposable.** `argus rebuild` deterministically regenerates it by
  replaying L2. This is why a corrupted or lost DuckDB file is a non-event.
- **`j15_backup`** mirrors L0 + L2 into `backup/` with copy-if-absent (correct precisely because
  the files are immutable). The DuckDB file is excluded from backup on purpose — it is
  rebuildable.

## Chaos drills (`tests/chaos/test_degradation.py`)

The reliability model is not aspirational — it is a test. The chaos drill runs the **real
nightly registry with the network blocked and no keys**, and asserts that the night must:

- skip keyless/keyed-off sources cleanly (`skipped_source_down`),
- fail network-dependent sources *loudly* (not silently),
- still run every local seal and the publish on whatever state exists,
- file at least one dead letter and exit non-zero (silence is not success),
- still hand the consumer a **sealed, contract-valid (even if empty)** serving database.

A second identical dead night must be no worse than the first — the DLQ grows at most linearly
with failing jobs, never explodes. There is also a **dead-source drill** to run quarterly by
hand (see the [runbook](operations.md)).

## The gap ledger (`orchestration/trust_jobs.py::gap_ledger_seal`)

The philosophical core of ARGUS: *what $0 cannot buy is measured and served, never discovered
by surprise.* Every night `j11d_gap_ledger` recomputes a small table of honesty metrics, each
with a severity (`info`/`warn`/`blocker`):

| `gap_key` | Measures |
|---|---|
| `intraday_iex_bbo_share` | Share of served intraday minutes with real IEX BBO (rest are Corwin–Schultz proxy). |
| `daily_single_source_share` | Share of current daily bars confirmed by only one source. |
| `daily_quarantined_count` | Current daily bars quarantined by vote conflict / MAD screen. |
| `delisted_coverage_10y` | Delisted names with price history over the 10y window (`blocker` below 0.95). |
| `delisted_reasons_unknown_share` | Graveyard rows still `reason='unknown'` (EDGAR classification pending). |
| `sectors_missing_count` | Universe tickers without a mapped sector (ETFs have no SIC). |
| `parity_worst_rel_diff` | Worst field divergence vs Polygon in the latest weekly sample. |
| `sources_circuit_open` | Sources currently circuit-open (dead or blocked). |

The gap ledger is published in the serving database, so a consumer always knows the current
quality envelope of the data it is reading.
