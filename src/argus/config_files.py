"""Loaders for the repo-side YAML config (universe, watchlist)."""

from __future__ import annotations

from pathlib import Path

import yaml

from argus.settings import Settings


def _config_path(settings: Settings, name: str) -> Path:
    base = settings.config_dir
    if not base.is_absolute():
        base = Path.cwd() / base
    return base / name


def load_watchlist(settings: Settings) -> list[str]:
    path = _config_path(settings, "watchlist.yaml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    tickers = [str(t).upper() for t in data.get("tickers", [])]
    if not tickers:
        raise ValueError(f"watchlist at {path} is empty")
    return tickers


def load_universe(settings: Settings) -> list[dict[str, str]]:
    path = _config_path(settings, "universe.yaml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    rows = [
        {"ticker": str(r["ticker"]).upper(), "role": str(r.get("role", "member"))}
        for r in data.get("tickers", [])
    ]
    if not rows:
        raise ValueError(f"universe at {path} is empty")
    return rows
