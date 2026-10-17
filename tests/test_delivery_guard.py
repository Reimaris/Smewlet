"""Delivery storage guard (m78)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import NamedTuple

import pytest

from app.services.delivery.guard import (
    InsufficientStorage,
    check_free_space,
    same_filesystem,
)


class _Usage(NamedTuple):
    total: int
    used: int
    free: int


@pytest.fixture
def fake_free(monkeypatch: pytest.MonkeyPatch):
    seen: list[Path] = []

    def _set(free: int) -> list[Path]:
        def usage(path: Path) -> _Usage:
            seen.append(Path(path))
            return _Usage(0, 0, free)

        monkeypatch.setattr(shutil, "disk_usage", usage)
        return seen

    return _set


@pytest.mark.parametrize(
    ("free", "needed", "buffer", "ok"),
    [
        (100, 60, 40, True),  # exact fit with buffer
        (99, 60, 40, False),  # one byte short
        (60, 60, 40, False),  # file fits but buffer violated
        (0, 10**12, 0, True),  # buffer 0 disables the guard
    ],
)
def test_check_free_space(fake_free, tmp_path, free, needed, buffer, ok) -> None:
    fake_free(free)
    if ok:
        check_free_space(tmp_path, needed, buffer)
    else:
        with pytest.raises(InsufficientStorage):
            check_free_space(tmp_path, needed, buffer)


def test_uses_nearest_existing_parent(fake_free, tmp_path) -> None:
    seen = fake_free(10**9)
    check_free_space(tmp_path / "new" / "deeper", 1, 1)
    assert seen == [tmp_path]


def test_same_filesystem(tmp_path) -> None:
    assert same_filesystem(tmp_path, tmp_path / "missing" / "dir")
