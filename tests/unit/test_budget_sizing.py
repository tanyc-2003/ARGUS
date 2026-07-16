"""The nightly budgets must clear the SHIPPED config, not a test fixture.

A budget that cannot cover one full pass does not degrade evenly -- it starves
whatever the loop reaches last, deterministically, every night. For j06 that is
also a correctness problem, not just coverage: a ticker whose corporate actions
never land has no canonicalized splits, so the reversal cannot run and its
prices would be served split-adjusted-as-raw.

These tests exist because the sizing was got wrong twice: the alpaca budget was
set against a per-ticker cost that ignored pagination (2026-07 starvation), and
the polygon budget was set against "1 call per ticker" when j06 actually costs
one call per (ticker x KIND).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from argus.settings import Settings
from argus.sources.polygon_ref import KINDS

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config"


def _shipped(name: str) -> list[dict]:
    data = yaml.safe_load((REPO_CONFIG / name).read_text(encoding="utf-8"))
    return data["tickers"]


@pytest.fixture()
def defaults() -> Settings:
    return Settings(_env_file=None)  # type: ignore[call-arg]


def test_polygon_budget_covers_universe_times_kinds(defaults: Settings) -> None:
    """j06 iterates universe x KINDS (splits AND dividends) and re-fetches every
    night, since the request_key carries the trade date. Nothing is skipped."""
    need = len(_shipped("universe.yaml")) * len(KINDS)
    assert defaults.polygon_nightly_budget >= need, (
        f"polygon budget {defaults.polygon_nightly_budget} < {need} "
        f"({len(_shipped('universe.yaml'))} tickers x {len(KINDS)} kinds). "
        "j06 would exhaust and the uncovered tickers would silently lose their "
        "corporate actions -- their splits never reverse."
    )


def test_yfinance_budget_covers_the_universe(defaults: Settings) -> None:
    """j02 costs 1 call per universe ticker."""
    assert defaults.yfinance_nightly_budget >= len(_shipped("universe.yaml"))


def test_watchlist_stays_a_curated_subset() -> None:
    """Quote capture costs 120-340 calls per ticker per session, so the
    watchlist is NOT the universe. Pasting the universe in here would blow the
    alpaca budget and re-create the 2026-07 starvation."""
    watchlist = yaml.safe_load((REPO_CONFIG / "watchlist.yaml").read_text(encoding="utf-8"))
    assert len(watchlist["tickers"]) <= 30, (
        "watchlist has grown past a curated subset; re-check the alpaca budget "
        "against ~120-340 calls per ticker per session before raising this."
    )


def test_factor_etfs_are_all_present() -> None:
    """role: factor_etf entries are the dashboard's fixed macro proxies."""
    etfs = {r["ticker"] for r in _shipped("universe.yaml") if r.get("role") == "factor_etf"}
    assert etfs == {"SPY", "TLT", "HYG", "QQQ", "IWD", "IWM", "UUP", "DBC", "SVXY", "SMH"}


def test_universe_tickers_use_the_canonical_dotted_form() -> None:
    """Alpaca/Polygon want BRK.B; Yahoo wants BRK-B and the adapter re-spells it.
    A dash here would fetch fine from Yahoo and fail everywhere else."""
    dashed = [r["ticker"] for r in _shipped("universe.yaml") if "-" in r["ticker"]]
    assert not dashed, f"use the dotted canonical form for share classes: {dashed}"
