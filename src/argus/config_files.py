"""Loaders for the repo-side YAML config (universe, watchlist)."""

from __future__ import annotations

from pathlib import Path

import yaml

from argus.settings import Settings


def _config_path(settings: Settings, name: str) -> Path:
    base = settings.config_dir
    if not base.is_absolute():
        base = Path.cwd() / base
    path = base / name
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. The config dir resolves against the working directory "
            f"(cwd={Path.cwd()}); run from the repo root, set ARGUS_CONFIG_DIR to an "
            "absolute path, or register the scheduled task with -RepoRoot."
        )
    return path


def load_watchlist(settings: Settings) -> list[str]:
    path = _config_path(settings, "watchlist.yaml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    tickers = [str(t).upper() for t in data.get("tickers", [])]
    if not tickers:
        raise ValueError(f"watchlist at {path} is empty")
    return tickers


def load_sic_map(settings: Settings) -> list[tuple[int, int, str]]:
    """(lo, hi, sector_etf) ranges, first match wins — see config/sic_sector_map.yaml."""
    path = _config_path(settings, "sic_sector_map.yaml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    ranges = [(int(r["lo"]), int(r["hi"]), str(r["sector"])) for r in data.get("ranges", [])]
    if not ranges:
        raise ValueError(f"sic map at {path} is empty")
    return ranges


def sic_to_sector(sic: str | int | None, ranges: list[tuple[int, int, str]]) -> str | None:
    if sic is None or sic == "":
        return None
    try:
        code = int(sic)
    except (TypeError, ValueError):
        return None
    for lo, hi, sector in ranges:
        if lo <= code <= hi:
            return sector
    return None


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
