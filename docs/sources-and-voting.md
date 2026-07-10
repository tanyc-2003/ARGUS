# Sources & cross-source voting

ARGUS uses only free data sources, and treats no single free source as trustworthy on its
own. Every daily bar is reconciled across the sources that saw it, graded, and tagged. This
is the "free-data defense": nothing enters the canonical layer unconfirmed without being
marked as such.

## The sources

| Source | Feeds | Role | Notes |
|---|---|---|---|
| **yfinance** (Yahoo) | daily bars, 1-minute bars | Primary consolidated spine | ~30 days of minute history only; deep daily history via `period=max`. Bypasses the shared HTTP client (its library manages Yahoo auth) but is wrapped by the same rate bucket + budget. |
| **Alpaca** (IEX feed) | daily bars, quotes → minute BBO | Confirmation + intraday friction | Free account. IEX **volume is structurally excluded** from serving (a single-venue print is not consolidated volume). Needs an API key; blank key = job skips. |
| **Polygon** (free tier) | corporate actions (splits/dividends), delisted reference, parity samples | Corporate-action truth + drift alarm | 5 calls/min — rate-dripped. Corporate actions are load-bearing (splits drive the reversal); required for `bootstrap`. |
| **SEC EDGAR** | company submissions | Sector (SIC) mapping, delisting reasons | Fair-access policy requires a descriptive `User-Agent` with contact info. |
| **Stooq** | full daily history | Third-source confirmation | **Currently blocked** (see below); the vote degrades gracefully to two sources. |
| **NASDAQ symbol directories** | `nasdaqlisted` / `otherlisted` snapshots | Survivorship baseline | The first snapshot is the forward graveyard's baseline; every later delisting is caught by diffing. |

### Why capture must start on day 1

Three feeds compound with calendar time and **cannot be backfilled later**:

- **Symbol-directory snapshots** — the first snapshot is the survivorship baseline; forward
  coverage of delistings only approaches 1.0 because diffing starts on day 1.
- **yfinance 1-minute bars** — Yahoo serves only ~30 days back; every day of delay is minute
  history permanently lost.
- **Alpaca IEX quotes** — friction baselines need 4–6 weeks of accrual before z-scores mean
  anything.

The capture jobs land these raw payloads nightly from the very first run; later processing
replays over everything accrued since day 1.

### Known source states

- **Stooq (as of 2026-07): BLOCKED.** Both the per-symbol CSV endpoint (a JavaScript
  proof-of-work challenge) and the bulk file (HTTP 401) are closed to plain HTTP clients.
  ARGUS **respects the challenge rather than defeating it.** The daily spine runs on yfinance
  (plus Alpaca once keys are configured); `j02b_stooq_monthly` probes weekly with failure
  backoff and heals automatically if Stooq reopens.

## Cross-source voting (`quality/voting.py`)

The vote runs over the **latest observation per `(source, ticker, bar_date)`** in the L2
event store and produces the full canonical candidate state. Because it is a pure function of
L2 with deterministic tie-breaks (`knowledge_time`, then `written_at`, then `payload_hash`),
it **doubles as the replay function**: `argus rebuild` is literally "re-run the vote over L2".

### Agreement rules

- **Close agreement:** relative difference ≤ **0.1%** (using the midpoint as denominator).
- **Volume agreement:** ≤ **5%** — and **IEX volume is structurally excluded**, so an
  `alpaca_iex` row can never contribute a served volume figure.
- Source priority when several agree (consolidated-volume vendors first):
  `yfinance → stooq → alpaca_iex`.

### Verdicts and grades

| Verdict | Condition | Grade | Served? |
|---|---|---|---|
| `confirmed` | ≥ 2 sources with an agreeing close pair | `good` (or `degraded` if the volume vote failed) | Yes |
| `single_source` | Exactly one source saw the bar | `degraded` | Yes, **tagged** |
| `alpaca_only_skipped` | Only IEX saw the bar | — | **No** — an IEX-only print with IEX-only volume would poison downstream volume features |
| `conflict` | ≥ 2 sources, but no agreeing pair | `quarantined` | **No** — kept for audit, dead-lettered, never served |

A failed *volume* vote degrades a row's grade without quarantining its prices — the close is
still trustworthy even when volumes disagree. The vote also records a `source_set` audit trail
(which sources were present) and the per-source closes, written to the `vote_results` table so
any served value can be explained after the fact.

### The result

The vote's output is upserted into `bars_daily` through the [SCD-2 path](point-in-time.md), so
a bar that changes on a later night becomes a revision rather than an overwrite. The
`vote_results` table is a projection, rewritten each seal, that captures the full per-bar audit
trail (verdict, chosen source, per-source closes, whether volume agreed).
