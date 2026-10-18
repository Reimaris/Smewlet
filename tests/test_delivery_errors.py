from __future__ import annotations

import errno

import pytest

from app.services.delivery.errors import describe_os_error

P = "/media/series/Show"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (
            errno.EACCES,
            f"Permission denied: {P} (check folder permissions for the Smewlet user)",
        ),
        (
            errno.EPERM,
            f"Permission denied: {P} (check folder permissions for the Smewlet user)",
        ),
        (errno.EROFS, f"Read-only file system: {P} (the mount is read-only)"),
        (errno.ENOSPC, f"No space left on device: {P}"),
        (errno.ENOENT, f"Path not found: {P} (is the folder mounted?)"),
        (errno.EXDEV, f"Cannot hardlink across filesystems: {P}"),
        (
            errno.EIO,
            f"Storage error on {P} (the share may be offline or remounted)",
        ),
        (
            errno.ESTALE,
            f"Storage error on {P} (the share may be offline or remounted)",
        ),
        (errno.ENAMETOOLONG, f"File name too long: {P}"),
        (errno.EBUSY, "Device or resource busy: " + P),
    ],
)
def test_describe_os_error_table(code: int, expected: str) -> None:
    assert describe_os_error(OSError(code, "x"), P) == expected


def test_describe_os_error_without_errno() -> None:
    assert describe_os_error(OSError("boom"), P) == f"OS error: {P}"
