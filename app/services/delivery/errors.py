"""Readable delivery error messages."""

from __future__ import annotations

import errno
import os
from pathlib import Path


def describe_os_error(exc: OSError, path: Path | str) -> str:
    """Turn an ``OSError`` into a message with the path and a hint; never raises."""
    try:
        code = exc.errno
        if code in (errno.EACCES, errno.EPERM):
            return f"Permission denied: {path} (check folder permissions for the Smewlet user)"
        if code == errno.EROFS:
            return f"Read-only file system: {path} (the mount is read-only)"
        if code == errno.ENOSPC:
            return f"No space left on device: {path}"
        if code == errno.ENOENT:
            return f"Path not found: {path} (is the folder mounted?)"
        if code == errno.EXDEV:
            return f"Cannot hardlink across filesystems: {path}"
        if code in (errno.EIO, errno.ESTALE):
            return f"Storage error on {path} (the share may be offline or remounted)"
        if code == errno.ENAMETOOLONG:
            return f"File name too long: {path}"
        if code:
            return f"{os.strerror(code)}: {path}"
        return f"OS error: {path}"
    except Exception:
        return "OS error"
