# Point-in-time correctness

The single most important property ARGUS guarantees is that a served value reflects **only
what the world could have known at the time** — no look-ahead. This document explains the
three mechanisms that deliver it: the two clocks, SCD-2 revision history, and point-in-time
corporate-action adjustment.

## 1. The two clocks

Every fact carries two timestamps (`core/clocks.py`):

- **`knowledge_time`** — when the world could first know the fact.
- **`written_at`** — when ARGUS recorded it.

Only `knowledge_time` drives as-of logic. It is stamped differently by fact kind:

- **Fresh nightly observation** → the pull moment (`pull_knowledge_time`).
- **Backfilled world fact** (bootstrap bar, historical split/dividend) → the fact's own date,
  stamped at exchange-local end of day (`asof_knowledge_time`). A daily bar is knowable at its
  own close; a corporate action by its ex-date. This is what makes historical as-of
  reconstruction *possible* — we honestly did not know a 2016 bar before 2016 ended.
- **Correction / revision** → the detection time. An as-of query before the correction was
  detected still returns the pre-correction value.

A test enforces that **only `core/clocks.py` reads the wall clock** — every other module goes
through it, so the discipline can't silently erode.

## 2. SCD-2 revision history (`canonical/scd2.py`)

Canonical tables (`bars_daily`, `corporate_actions`) are never updated in place. The generic
SCD-2 upsert is the **only** mutation path into them, and it works per natural key:

| Situation | Action |
|---|---|
| No history for the key | Insert version 1 with the row's **own** `knowledge_time` (a backfilled fact keeps its historical stamp). |
| Current row, same `payload_hash` | No-op — idempotent re-runs cost nothing. |
| Current row, different value | **Revision:** close the current version (`valid_to`, `is_current = FALSE`) and open the next one stamped at the **detection time**, not the fact's own date. |

Every canonical row therefore carries `valid_from` / `valid_to` / `is_current` / `revision_seq`.
Because a correction only becomes knowable when detected, stamping a revision at the bar's own
date would rewrite the past — SCD-2 prevents exactly that, so as-of queries can time-travel
through the true revision history.

This is also how silent vendor rewrites are caught: the monthly Stooq re-pull flows through the
same build+vote+upsert path, so a changed historical bar surfaces as an SCD-2 revision. The
revision count *is* the diff alarm.

## 3. Point-in-time corporate-action adjustment

Served prices are corporate-action-adjusted using a **forward, total-return** convention. The
adjustment factors are a **view** over `corporate_actions` (`vw_adjustment_factors` in `db.py`),
so there is exactly one source of truth with full SCD-2 history:

- **Splits:** factor = `split_to / split_from`.
- **Dividends:** factor = `prev_close / (prev_close − cash_amount)`, where `prev_close` is the
  most recent non-quarantined close *before* the ex-date.

The served value for `(ticker, D)` multiplies the raw price by the cumulative factor:

```
adj(D) = raw(D) × cum(D)
cum(D) = ∏ { f.factor : f.ex_date ≤ D  AND  f is knowable (exchange-local) by end of day D }
```

The knowability clause is the no-look-ahead guarantee: a factor is applied to bar `D` only if
its `ex_date ≤ D` **and** its `knowledge_time`, converted to America/New_York, lands on or
before `D`. A split announced *after* day `D` can never change the served value for day `D`.
In the serving view (`vw_mad_daily_ohlcv`), `EXP(SUM(LN(factor)) FILTER (…))` stands in for the
filtered product; splits are additionally accumulated separately so volume can be divided by
the split factor while prices are multiplied.

### Split reversal at ingest

Some vendors serve prices already split-adjusted. Those are **reversed back to raw** at
normalization time (L1), and each `bar_event` records `vendor_adjusted` and the
`reversal_factor` applied. Canonical `bars_daily.close` is always **raw** price; all adjustment
happens in the serving view. This keeps the adjustment logic in exactly one place and keeps the
canonical store reconstructable.

## Showing your work: `argus verify-pit`

Because the whole chain is deterministic and auditable, ARGUS can explain any served value
factor by factor:

```
argus verify-pit --ticker AAPL --date 2020-08-28
```

`factors/adjustment.py::pit_report` reconstructs, for `(ticker, date)`: the raw close, every
adjustment factor with its ex-date and knowledge time, whether each was applied, the cumulative
factor, and the adjusted close. It also computes `no_lookahead` — `True` iff every applied
factor was knowable (exchange-local) by end of the bar date. The CLI exits non-zero if that
check ever fails, so it can be wired into a smoke test.
