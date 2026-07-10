"""Shared fixtures. All tests run offline (pytest-socket via addopts) against
tmp data roots; real ARGUS_* env vars and any .env file are neutralized so a
developer's local configuration can never leak into test behavior."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest
import structlog

from argus import db as db_module
from argus.ops.jobs import JobContext
from argus.settings import Settings


@pytest.fixture(autouse=True)
def _clean_argus_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("ARGUS_"):
            monkeypatch.delenv(key)


@pytest.fixture()
def test_config_dir(tmp_path: Path) -> Path:
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "watchlist.yaml").write_text("tickers:\n  - SPY\n  - AAPL\n", encoding="utf-8")
    (cfg / "universe.yaml").write_text(
        "tickers:\n  - {ticker: SPY, role: factor_etf}\n  - {ticker: AAPL, role: seed}\n",
        encoding="utf-8",
    )
    (cfg / "sic_sector_map.yaml").write_text(
        "ranges:\n"
        "  - {lo: 3570, hi: 3579, sector: XLK}\n"
        "  - {lo: 6000, hi: 6499, sector: XLF}\n",
        encoding="utf-8",
    )
    return cfg


@pytest.fixture()
def settings(tmp_path: Path, test_config_dir: Path) -> Settings:
    return Settings(  # type: ignore[call-arg]
        data_root=tmp_path / "argus-data",
        config_dir=test_config_dir,
        _env_file=None,
    )


@pytest.fixture()
def conn(settings: Settings):
    c = db_module.open_migrated(settings.db_path)
    yield c
    c.close()


@pytest.fixture()
def ctx(settings: Settings, conn) -> JobContext:  # type: ignore[no-untyped-def]
    return JobContext(
        settings=settings,
        conn=conn,
        trade_date=date(2026, 7, 7),  # a regular Tuesday session
        log=structlog.get_logger("argus-test"),
    )
