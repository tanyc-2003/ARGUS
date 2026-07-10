# ARGUS Documentation

ARGUS is a minimum-cost, point-in-time-correct (PIT) market-data platform. It captures
US equity market data from free sources, reconciles it across providers, adjusts it for
corporate actions without look-ahead, and publishes a sealed, contract-checked DuckDB
database that any downstream consumer can read. Everything runs on a single machine on
DuckDB + Parquet, with no paid data feeds.

These documents describe how the system is built and how to operate it.

## Start here

| Document | What it covers |
|---|---|
| [Architecture](architecture.md) | The layered design (L0→serving), the two-clock model, and the core principles that everything else follows from. |
| [Data flow & the nightly pipeline](pipeline.md) | The ordered job registry, how a night runs, idempotency, and the bootstrap sequence. |
| [Sources & cross-source voting](sources-and-voting.md) | The free data sources, how disagreements are resolved, and how quality grades are assigned. |
| [Point-in-time correctness](point-in-time.md) | The two clocks, SCD-2 revision history, corporate-action adjustment, and how look-ahead is prevented. |
| [Data model](data-model.md) | Every table and view in the build database, plus the L0/L2 Parquet layouts. |
| [The serving contract](serving-contract.md) | The frozen shapes ARGUS publishes, the contract gate, and the atomic publish. |
| [Reliability & operations](reliability.md) | Rate budgets, circuit breakers, the dead-letter queue, chaos drills, and the gap ledger. |
| [Setup & runbook](operations.md) | Installing, configuring, scheduling, daily health checks, and disaster recovery. |

## The one-paragraph version

Every night ARGUS pulls raw payloads from free sources and lands them append-only (**L0**,
never fetched twice). It normalizes them into an immutable event store (**L2**, the system
of record in Parquet). It votes across sources to build a canonical, revision-tracked state
in DuckDB (**L3**), applies corporate-action factors as a point-in-time view (**L4/serving**),
and materializes a sealed serving database (**publish**) only after it passes a byte-for-byte
contract gate. The DuckDB build file is disposable — `argus rebuild` deterministically
regenerates it by replaying the Parquet event store. What free data cannot buy (single-source
bars, synthetic spreads, coverage gaps) is measured and published in a **gap ledger** rather
than hidden.
