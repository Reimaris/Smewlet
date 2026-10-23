"""Guardrails for the Smewlet rebrand."""

from datetime import datetime
from pathlib import Path

import pytest

from app.config import DEFAULT_USER_AGENT, Settings
from app.services.db_export import sanitized_export_filename
from app.services.watchlists.plex import PLEX_PRODUCT
from app.services.watchlists.simkl import SIMKL_APP_NAME
from app.version import APP_NAME, APP_VERSION

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCANNED_DIRS: tuple[str, ...] = ("app", "templates", ".github")
SCANNED_SUFFIXES: frozenset[str] = frozenset(
    {".py", ".html", ".js", ".css", ".svg", ".yml", ".yaml", ".json", ".toml", ".md"}
)
OLD_BRAND = "nzboxer"


def find_violations(root: Path) -> list[str]:
    """Return ``<relative path>:<line>`` for every mention of the old brand."""
    hits: list[str] = []
    for directory in SCANNED_DIRS:
        base = root / directory
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
                continue
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for number, line in enumerate(text.splitlines(), start=1):
                if OLD_BRAND in line.lower():
                    hits.append(f"{path.relative_to(root)}:{number}")
    return hits


def test_brand_constants() -> None:
    assert APP_NAME == "Smewlet"
    assert DEFAULT_USER_AGENT == f"Smewlet/{APP_VERSION} (Linux; x64)"
    assert PLEX_PRODUCT == "Smewlet"
    assert SIMKL_APP_NAME == "smewlet"
    assert (
        sanitized_export_filename(datetime(2026, 10, 16, 14, 25, 30))
        == "smewlet_sanitized_20261016_142530.db"
    )


def test_repository_has_no_old_brand() -> None:
    violations = find_violations(PROJECT_ROOT)
    assert violations == [], "Old brand name found at: " + ", ".join(violations)


def test_scanner_detects_violation(tmp_path: Path) -> None:
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "x.py").write_text('x = "NZBoxer"\n')
    assert find_violations(tmp_path) == ["app/x.py:1"]


def test_default_database_url_constant(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import DEFAULT_DATABASE_URL

    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert DEFAULT_DATABASE_URL == "sqlite+aiosqlite:///./config/smewlet.db"
    assert Settings().database_url == DEFAULT_DATABASE_URL
