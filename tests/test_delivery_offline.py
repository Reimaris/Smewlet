"""Storage-offline probe, flag and backoff watcher."""

import asyncio
import errno
import time
from pathlib import Path

import pytest

from app.services.delivery import offline


@pytest.fixture(autouse=True)
async def _reset_offline():
    await offline.stop_watcher()
    offline.set_recovery_callback(None)
    yield
    await offline.stop_watcher()
    offline.set_recovery_callback(None)


async def test_probe_success(tmp_path: Path):
    assert await offline.probe_root(tmp_path) is True
    assert list(tmp_path.iterdir()) == []


async def test_probe_stale_handle_is_offline(tmp_path: Path, monkeypatch):
    def boom(_root):
        raise OSError(errno.ESTALE, "Stale file handle")

    monkeypatch.setattr(offline.os, "scandir", boom)
    assert await offline.probe_root(tmp_path) is False


async def test_probe_read_only_write_is_reachable(tmp_path: Path, monkeypatch):
    def boom(self, *a, **k):
        raise OSError(errno.EROFS, "Read-only")

    monkeypatch.setattr(Path, "write_bytes", boom)
    assert await offline.probe_root(tmp_path) is True


async def test_probe_hang_times_out_and_blocks_second_thread(
    tmp_path: Path, monkeypatch
):
    calls: list[Path] = []

    def hang(root):
        calls.append(root)
        time.sleep(0.3)
        return True

    monkeypatch.setattr(offline, "_probe_sync", hang)
    monkeypatch.setattr(offline, "PROBE_TIMEOUT_SECONDS", 0.05)
    assert await offline.probe_root(tmp_path) is False
    assert await offline.probe_root(tmp_path) is False
    assert len(calls) == 1
    await asyncio.sleep(0.4)
    monkeypatch.setattr(offline, "PROBE_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(offline, "_probe_sync", lambda root: calls.append(root) or True)
    assert await offline.probe_root(tmp_path) is True
    assert len(calls) == 2


async def test_backoff_sequence_then_recovery(tmp_path: Path, monkeypatch):
    sleeps: list[float] = []
    results = iter([False] * 6 + [True])
    recovered: list[bool] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    async def fake_probe(_root):
        return next(results)

    async def on_recovered():
        recovered.append(offline.is_offline())

    monkeypatch.setattr(offline.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(offline, "probe_root", fake_probe)
    offline.set_recovery_callback(on_recovered)

    await offline.mark_offline(tmp_path)
    assert offline.is_offline()
    assert offline.offline_banner_context()["roots"] == [str(tmp_path)]
    assert offline._watcher is not None
    await offline._watcher

    assert sleeps == [60, 120, 240, 480, 960, 1800, 1800]
    assert recovered == [False]
    assert not offline.is_offline()
    assert offline._attempt == 0
    assert offline.next_check_at() is None
    assert offline.offline_banner_context() is None


async def test_banner_context_label(tmp_path: Path):
    await offline.mark_offline(tmp_path)
    ctx = offline.offline_banner_context()
    assert ctx["attempt"] == 0
    assert len(ctx["next_check_label"]) == 5 and ctx["next_check_label"][2] == ":"
