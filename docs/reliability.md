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
| `vote_conflict` | All sources disagree on a bar | Row quarantined → DLQ |
| `transport` | Network/HTTP failure after retries | Failure → DLQ |
| `unknown` | Anything unclassified | Failure → DLQ |

`budget_exhausted` and `skipped_source_down` are deliberately not failures. Only genuine
failures open a **dead letter** — an entry needing human attention — and force a non-zero exit
code so the OS scheduler flags the night. Triage guidance is in the [runbook](operations.md).

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
