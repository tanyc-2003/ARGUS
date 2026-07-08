from pathlib import Path

import pytest
from pydantic import ValidationError

from argus.settings import Settings


def _mk(**kw: object) -> Settings:
    return Settings(_env_file=None, **kw)  # type: ignore[call-arg]


def test_onedrive_data_root_refused(tmp_path: Path) -> None:
    bad = tmp_path / "OneDrive - University of Southampton" / "data"
    with pytest.raises(ValidationError, match="cloud-synced"):
        _mk(data_root=bad)


def test_dropbox_data_root_refused(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        _mk(data_root=tmp_path / "Dropbox" / "argus")


def test_synced_root_allowed_with_override(tmp_path: Path) -> None:
    bad = tmp_path / "OneDrive" / "data"
    s = _mk(allow_synced_data_root=True, data_root=bad)
    assert s.data_root == bad


def test_plain_local_root_ok(tmp_path: Path) -> None:
    s = _mk(data_root=tmp_path / "argus-data")
    assert s.landing_dir == s.data_root / "landing"
    assert s.db_path.name == "argus.duckdb"
    assert s.serving_db_path.name == "argus_serving.duckdb"


def test_ensure_dirs_creates_tree(tmp_path: Path) -> None:
    s = _mk(data_root=tmp_path / "argus-data")
    s.ensure_dirs()
    assert s.landing_dir.is_dir()
    assert s.events_dir.is_dir()
    assert s.log_dir.is_dir()
