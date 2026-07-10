"""P6 — replay determinism: the DuckDB canonical state is a disposable
projection of the L2 event store. Wipe it, rebuild it, get the same state.
"""

from argus.orchestration.build_jobs import build_actions, build_daily_bars, vote_and_seal
from argus.orchestration.rebuild import canonical_state_fingerprint, rebuild_canonical
from argus.orchestration.universe_jobs import universe_seal
from argus.serving import contracts
from argus.serving.publish import publish
from tests.integration.test_daily_slice import (
    EMPTY_CA,
    SPLITS_AAPL,
    STOOQ_AAPL,
    STOOQ_SPY,
    _land,
)


def _build_normal_path(ctx) -> str:  # type: ignore[no-untyped-def]
    _land(ctx, "polygon_splits", "AAPL", SPLITS_AAPL, "json")
    _land(ctx, "polygon_dividends", "AAPL", EMPTY_CA, "json")
    _land(ctx, "polygon_splits", "SPY", EMPTY_CA, "json")
    _land(ctx, "polygon_dividends", "SPY", EMPTY_CA, "json")
    _land(ctx, "stooq_daily", "AAPL", STOOQ_AAPL.encode(), "csv")
    _land(ctx, "stooq_daily", "SPY", STOOQ_SPY.encode(), "csv")
    build_actions(ctx)
    build_daily_bars(ctx)
    vote_and_seal(ctx)
    universe_seal(ctx)  # publish gates on coverage being served
    publish(ctx)
    return canonical_state_fingerprint(ctx.conn)


def test_rebuild_reproduces_the_same_state(ctx) -> None:
    fp_live = _build_normal_path(ctx)

    summary = rebuild_canonical(ctx.settings, ctx.conn, ctx.trade_date)
    fp_rebuilt = canonical_state_fingerprint(ctx.conn)
    assert fp_rebuilt == fp_live
    assert summary["bars_current"] == 5
    assert summary["actions_replayed"] == 1

    # and the rebuild is itself deterministic
    rebuild_canonical(ctx.settings, ctx.conn, ctx.trade_date)
    assert canonical_state_fingerprint(ctx.conn) == fp_rebuilt

    # the published serving copy still honors the frozen contract post-rebuild
    assert contracts.assert_daily_ohlcv(ctx.settings.serving_db_path) == 5


def test_rebuild_survives_duplicate_events(ctx) -> None:
    """Force re-processing duplicates events in L2; latest-per-source dedup
    must keep the rebuilt state identical."""
    fp_live = _build_normal_path(ctx)
    build_daily_bars(ctx)  # re-append the same observations
    build_actions(ctx)
    rebuild_canonical(ctx.settings, ctx.conn, ctx.trade_date)
    assert canonical_state_fingerprint(ctx.conn) == fp_live
