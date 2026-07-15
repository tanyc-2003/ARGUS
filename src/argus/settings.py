"""Application settings (pydantic-settings, `.env`, all vars prefixed ARGUS_).

The data root holds every Parquet/DuckDB artifact and MUST live outside any
cloud-synced tree: Parquet append churn plus DuckDB WAL files under a sync
client (OneDrive/Dropbox/GDrive) is a corruption generator. Startup refuses a
synced-looking path unless explicitly overridden.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_SYNC_MARKERS = ("onedrive", "dropbox", "google drive", "googledrive")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ARGUS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # defined BEFORE data_root on purpose: pydantic validates fields in definition
    # order, and the data_root validator reads this flag from info.data.
    allow_synced_data_root: bool = False
    data_root: Path = Path(r"C:\argus-data")

    # repo-relative config directory (universe/watchlist/tolerances yaml)
    config_dir: Path = Path("config")

    alpaca_key_id: str = ""
    alpaca_secret_key: str = ""
    polygon_api_key: str = ""
    edgar_user_agent: str = ""

    # nightly per-source call budgets (calls per run); buckets enforce the per-second rate
    polygon_nightly_budget: int = 120
    yfinance_nightly_budget: int = 600
    # Alpaca quote ticks paginate at 10k/call, so one liquid name costs 230-300
    # calls for a busy session (measured 2026-07: QQQ 2.95M quotes = 295 calls,
    # SPY 2.27M = 228). One session across the 15-name watchlist is ~2-2.5k; the
    # 5-session backfill is ~10-12k. The old 2000 could not even cover a single
    # session, so the job died a third of the way through the watchlist EVERY
    # night and the tail never got data. A CEILING, not a target: a steady-state
    # night spends ~2.5k (~14 min at 3 calls/s); only a full backfill approaches
    # this, and capture() stops cleanly rather than half-fetching when it binds.
    alpaca_nightly_budget: int = 15_000

    @field_validator("data_root")
    @classmethod
    def _refuse_synced_data_root(cls, v: Path, info) -> Path:  # noqa: ANN001
        resolved = str(v.expanduser().resolve()).lower()
        if any(marker in resolved for marker in _SYNC_MARKERS):
            # validators run in field-definition order, so the override flag may not be
            # parsed yet; read it straight from the raw input instead.
            raw_override = info.data.get("allow_synced_data_root")
            if not raw_override:
                raise ValueError(
                    f"ARGUS_DATA_ROOT resolves inside a cloud-synced folder: {v}. "
                    "Parquet/DuckDB under a sync client risks corruption. Point it at a "
                    "local path (e.g. C:\\argus-data) or set ARGUS_ALLOW_SYNCED_DATA_ROOT=1 "
                    "if you really mean it."
                )
        return v

    # ---- derived paths (all under data_root) ------------------------------------
    @property
    def landing_dir(self) -> Path:
        return self.data_root / "landing"

    @property
    def events_dir(self) -> Path:
        return self.data_root / "events"

    @property
    def db_path(self) -> Path:
        return self.data_root / "argus.duckdb"

    @property
    def serving_db_path(self) -> Path:
        return self.data_root / "argus_serving.duckdb"

    @property
    def log_dir(self) -> Path:
        return self.data_root / "logs"

    def ensure_dirs(self) -> None:
        for p in (self.data_root, self.landing_dir, self.events_dir, self.log_dir):
            p.mkdir(parents=True, exist_ok=True)


def load_settings(**overrides: object) -> Settings:
    """Build Settings; keyword overrides win over env/.env (used heavily by tests)."""
    return Settings(**overrides)  # type: ignore[arg-type]
