"""Storage-offline detection: bounded root probe, global flag and backoff watcher."""

from __future__ import annotations

import asyncio
import errno
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.rate_limit_queue import clock_label

logger = logging.getLogger(__name__)

OFFLINE_ERRNOS: frozenset[int] = frozenset(
    {
        errno.ENOENT,
        errno.EIO,
        errno.ESTALE,
        errno.ETIMEDOUT,
        errno.EHOSTDOWN,
        errno.EHOSTUNREACH,
        errno.ENOTCONN,
        errno.ENXIO,
    }
)
PROBE_TIMEOUT_SECONDS: float = 10.0
BACKOFF_SECONDS: tuple[int, ...] = (60, 120, 240, 480, 960, 1800)

_UNWRITABLE_ERRNOS = frozenset({errno.EROFS, errno.EACCES, errno.EPERM})


class StorageOffline(Exception):
    """Raised when a library root fails the reachability probe."""

    def __init__(self, root: Path) -> None:
        super().__init__(f"Storage offline: {root}")
        self.root = root


_inflight: dict[Path, asyncio.Future[bool]] = {}
_roots: list[Path] = []
_offline = False
_attempt = 0
_next_check: datetime | None = None
_watcher: asyncio.Task[None] | None = None
_on_recovered: Callable[[], Awaitable[None]] | None = None


def _probe_sync(root: Path) -> bool:
    """Blocking probe: list the first entry, then create and delete a marker file."""
    try:
        with os.scandir(root) as it:
            next(it, None)
    except OSError as exc:
        return exc.errno not in OFFLINE_ERRNOS
    marker = root / f".smewlet_probe_{os.getpid()}"
    try:
        marker.write_bytes(b"")
        marker.unlink()
    except OSError as exc:
        # Reachable but unwritable: the delivery itself reports the readable error.
        return exc.errno not in OFFLINE_ERRNOS or isinstance(exc, PermissionError)
    return True


def _swallow(fut: asyncio.Future[bool]) -> None:
    if not fut.cancelled():
        fut.exception()


async def probe_root(root: Path) -> bool:
    """True when ``root`` is reachable; False when offline, hung or already being probed."""
    pending = _inflight.get(root)
    if pending is not None and not pending.done():
        return False
    fut: asyncio.Future[bool] = asyncio.ensure_future(
        asyncio.to_thread(_probe_sync, root)
    )
    fut.add_done_callback(_swallow)
    fut.add_done_callback(
        lambda f: _inflight.pop(root, None) if _inflight.get(root) is f else None
    )
    _inflight[root] = fut
    try:
        return await asyncio.wait_for(asyncio.shield(fut), PROBE_TIMEOUT_SECONDS)
    except TimeoutError:
        return False
    except OSError:
        return False


def is_offline() -> bool:
    return _offline


def next_check_at() -> datetime | None:
    return _next_check if _offline else None


def set_recovery_callback(callback: Callable[[], Awaitable[None]] | None) -> None:
    global _on_recovered
    _on_recovered = callback


async def mark_offline(root: Path) -> None:
    """Set the offline flag and start the backoff watcher once."""
    global _offline, _watcher, _next_check
    if not _offline:
        _next_check = datetime.now(UTC) + timedelta(seconds=BACKOFF_SECONDS[0])
    _offline = True
    if root not in _roots:
        _roots.append(root)
    if _watcher is None or _watcher.done():
        _watcher = asyncio.create_task(_watch())


async def _watch() -> None:
    global _offline, _attempt, _next_check
    while _offline:
        delay = BACKOFF_SECONDS[min(_attempt, len(BACKOFF_SECONDS) - 1)]
        _next_check = datetime.now(UTC) + timedelta(seconds=delay)
        await asyncio.sleep(delay)
        results = [await probe_root(r) for r in list(_roots)]
        if all(results):
            _offline = False
            _attempt = 0
            _next_check = None
            _roots.clear()
            logger.info("Delivery storage is reachable again")
            if _on_recovered is not None:
                try:
                    await _on_recovered()
                except Exception:  # noqa: BLE001
                    logger.exception("Delivery recovery callback failed")
            return
        _attempt += 1


async def stop_watcher() -> None:
    """Cancel the watcher and reset all offline state."""
    global _offline, _attempt, _next_check, _watcher
    task, _watcher = _watcher, None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _offline = False
    _attempt = 0
    _next_check = None
    _roots.clear()


def offline_banner_context() -> dict[str, Any] | None:
    if not _offline:
        return None
    nxt = _next_check
    return {
        "roots": [str(r) for r in _roots],
        "next_check_label": clock_label(nxt) if nxt else "soon",
        "attempt": _attempt,
    }
