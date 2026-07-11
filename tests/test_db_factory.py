import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LIB_DIR = Path(os.environ.get("SSH_LIBRARY_PATH", ROOT.parents[3] / "lib" / "ssh_library"))
sys.path.append(str(ROOT))
if (LIB_DIR / "ssh_library").exists():
    sys.path.append(str(LIB_DIR))
elif (LIB_DIR / "__init__.py").exists():
    sys.path.append(str(LIB_DIR.parent))


def test_get_db_default_uses_sec_reports_manager(monkeypatch):
    monkeypatch.delenv("DB_BACKEND", raising=False)

    from models.SecReportsManager import SecReportsManager
    from models.db_factory import get_db

    assert isinstance(get_db(), SecReportsManager)


def test_get_db_ssh_library_backend(monkeypatch):
    if not ((LIB_DIR / "ssh_library").exists() or (LIB_DIR / "__init__.py").exists()):
        pytest.skip("ssh-library checkout is not available")

    monkeypatch.setenv("DB_BACKEND", "ssh_library")

    from models.db_factory import get_db
    from ssh_library import SecReportsManager

    assert isinstance(get_db(), SecReportsManager)


def test_get_db_postgres_backend_uses_scraper_sec_reports_manager(monkeypatch):
    monkeypatch.setenv("DB_BACKEND", "postgres")

    from models.SecReportsManager import SecReportsManager
    from models.db_factory import get_db

    assert isinstance(get_db(), SecReportsManager)
