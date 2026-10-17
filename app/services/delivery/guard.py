"""Storage guard: verify the destination volume has room before writing."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


class InsufficientStorage(Exception):
    """The destination volume cannot hold the file plus the free-space buffer."""


def _nearest_existing(path: Path) -> Path:
    path = path.absolute()
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def check_free_space(
    dest_dir: Path, needed_bytes: int, min_free_buffer_bytes: int
) -> None:
    """Raise ``InsufficientStorage`` unless free space >= needed + buffer (no-op for buffer 0)."""
    if min_free_buffer_bytes == 0:
        return
    free = shutil.disk_usage(_nearest_existing(dest_dir)).free
    required = needed_bytes + min_free_buffer_bytes
    if free < required:
        raise InsufficientStorage(
            f"{free} bytes free, {required} required (file + buffer)"
        )


def same_filesystem(a: Path, b: Path) -> bool:
    """True when the nearest existing parents of ``a`` and ``b`` share a device."""
    return os.stat(_nearest_existing(a)).st_dev == os.stat(_nearest_existing(b)).st_dev
