"""A share that dies mid-copy must pause or fail the delivery, never hang it."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.services.delivery import engines, offline, worker
from tests.conftest import (
    PAYLOAD,
    FakeDownloaderClient,
    fake_urlopen,
    make_delivery_provider,
    make_pending_delivery,
)


@pytest.fixture(autouse=True)
async def _fast_watchdog(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(engines, "DELIVERY_WRITE_STALL_SECONDS", 0.2)
    monkeypatch.setattr(engines, "DELIVERY_ABANDON_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(engines, "WATCHDOG_POLL_SECONDS", 0.02)
    await offline.stop_watcher()
    yield
    await offline.stop_watcher()


@pytest.fixture
def release() -> Iterator[threading.Event]:
    """Event that blocked 'writes' wait on; released on teardown."""
    event = threading.Event()
    yield event
    event.set()


@pytest.fixture
async def history_id(db_session, downloader, tmp_path: Path) -> int:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    downloader(FakeDownloaderClient())
    return (await make_pending_delivery(db_session)).id


def _hanging_fetch(release: threading.Event, started: list[Path]) -> Callable[..., int]:
    def fetch(url, part, size, history_id, stop, on_bytes) -> int:
        part.write_bytes(b"x")
        started.append(part)
        release.wait()
        return 0

    return fetch


async def _ticker(ticks: list[int]) -> None:
    while True:
        ticks.append(1)
        await asyncio.sleep(0.01)


async def _row(db_session, history_id: int):
    from app.db.models import DownloadHistory

    row = await db_session.get(DownloadHistory, history_id)
    await db_session.refresh(row)
    return row


async def test_stall_with_unreachable_root_pauses_as_storage_offline(
    db_session, history_id, monkeypatch, release, tmp_path
) -> None:
    started: list[Path] = []
    monkeypatch.setattr(engines, "_fetch_to_file", _hanging_fetch(release, started))
    monkeypatch.setattr(engines, "probe_root", AsyncMock(return_value=False))
    ticks: list[int] = []
    task = asyncio.create_task(_ticker(ticks))
    began = time.monotonic()
    try:
        await asyncio.wait_for(worker.process_delivery(db_session, history_id), 10)
    finally:
        task.cancel()

    row = await _row(db_session, history_id)
    assert (row.delivery_status, row.delivery_error) == ("paused", "STORAGE_OFFLINE")
    assert offline.is_offline()
    assert time.monotonic() - began < 5
    assert len(ticks) > 5  # the event loop kept turning
    assert not started[0].exists()  # .part removed


async def test_stall_with_reachable_root_fails_as_download_stalled(
    db_session, history_id, monkeypatch, release
) -> None:
    started: list[Path] = []
    monkeypatch.setattr(engines, "_fetch_to_file", _hanging_fetch(release, started))
    monkeypatch.setattr(engines, "probe_root", AsyncMock(return_value=True))

    await asyncio.wait_for(worker.process_delivery(db_session, history_id), 10)

    row = await _row(db_session, history_id)
    assert (row.delivery_status, row.delivery_error) == ("failed", "DOWNLOAD_STALLED")
    assert not offline.is_offline()
    assert not started[0].exists()


async def test_moving_progress_is_not_a_stall(
    db_session, history_id, monkeypatch
) -> None:
    def slow_but_moving(url, part, size, hid, stop, on_bytes) -> int:
        part.write_bytes(PAYLOAD)
        for done in range(100, size + 1, 100):  # runs 3x longer than the stall limit
            on_bytes(done)
            time.sleep(0.02)
        return size

    monkeypatch.setattr(engines, "_fetch_to_file", slow_but_moving)
    monkeypatch.setattr(engines, "DELIVERY_WRITE_STALL_SECONDS", 0.2)

    await asyncio.wait_for(worker.process_delivery(db_session, history_id), 10)

    row = await _row(db_session, history_id)
    assert (row.delivery_status, row.delivery_error) == ("delivered", None)


async def test_cancel_during_hung_write_returns_within_grace(
    db_session, history_id, monkeypatch, release
) -> None:
    started: list[Path] = []
    monkeypatch.setattr(engines, "_fetch_to_file", _hanging_fetch(release, started))
    monkeypatch.setattr(engines, "DELIVERY_WRITE_STALL_SECONDS", 60)
    task = asyncio.create_task(worker.process_delivery(db_session, history_id))
    while not started:
        await asyncio.sleep(0.01)

    began = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)

    assert time.monotonic() - began < 2
    assert not started[0].exists()


async def test_run_daemon_runs_on_a_daemon_thread_and_propagates() -> None:
    thread = await engines._run_daemon(threading.current_thread)
    assert thread.daemon is True
    assert thread.name == "delivery-download"
    assert await engines._run_daemon(lambda a, b: a + b, 1, 2) == 3

    def boom() -> None:
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        await engines._run_daemon(boom)


def test_run_daemon_result_after_loop_closed_is_dropped(
    release: threading.Event, monkeypatch: pytest.MonkeyPatch
) -> None:
    errors: list[Any] = []
    monkeypatch.setattr(threading, "excepthook", errors.append)
    loop = asyncio.new_event_loop()

    async def start() -> asyncio.Future[Any]:
        return engines._run_daemon(release.wait)

    loop.run_until_complete(start())
    loop.close()
    release.set()
    time.sleep(0.2)
    assert errors == []


async def test_wait_bounded_reports_unfinished_job(release: threading.Event) -> None:
    job = engines._run_daemon(release.wait)
    assert await engines._wait_bounded(job, 0.05) is False
    release.set()
    assert await engines._wait_bounded(job, 1.0) is True


async def test_range_workers_are_daemon_threads(monkeypatch, tmp_path: Path) -> None:
    seen: list[threading.Thread] = []
    real = engines._pwrite_all

    def spy(fd: int, data: memoryview, offset: int) -> None:
        seen.append(threading.current_thread())
        real(fd, data, offset)

    monkeypatch.setattr(engines, "_pwrite_all", spy)
    monkeypatch.setattr(engines, "_urlopen", fake_urlopen())
    monkeypatch.setattr(engines, "RANGE_BYTES", 1000)

    part = tmp_path / "f.part"
    written = await asyncio.to_thread(
        engines._fetch_ranged,
        "https://example.com/f.mkv",
        part,
        len(PAYLOAD),
        1,
        threading.Event(),
        lambda _n: None,
    )

    assert written == len(PAYLOAD)
    assert part.read_bytes() == PAYLOAD
    assert seen and all(t.daemon for t in seen)
    assert all(t.name.startswith("delivery-range") for t in seen)


@pytest.mark.parametrize("fetch_name", ["_fetch_single", "_fetch_ranged"])
async def test_every_attempt_writes_a_fresh_part_file(
    fetch_name: str, monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(engines, "_urlopen", fake_urlopen())
    monkeypatch.setattr(engines, "RANGE_BYTES", 1000)
    part = tmp_path / "f.part"
    part.write_bytes(b"old")
    old_inode = part.stat().st_ino
    args: tuple[Any, ...] = ("https://example.com/f.mkv", part)
    if fetch_name == "_fetch_ranged":
        args += (len(PAYLOAD),)
    args += (1, threading.Event(), lambda _n: None)

    await asyncio.to_thread(getattr(engines, fetch_name), *args)

    assert part.read_bytes() == PAYLOAD
    assert part.stat().st_ino != old_inode  # unlinked first, not truncated in place


async def test_late_progress_from_abandoned_thread_is_ignored(
    monkeypatch, release, tmp_path: Path
) -> None:
    from app.services.delivery.config import DeliveryConfig
    from app.services.downloaders.base import DownloadLink

    reports: list[int] = []

    async def progress(done: int, _total: int) -> None:
        reports.append(done)

    def stalled_then_late(url, part, size, hid, stop, on_bytes) -> int:
        release.wait()
        on_bytes(size)  # wakes after the stall was declared
        return 0

    monkeypatch.setattr(engines, "_fetch_to_file", stalled_then_late)
    monkeypatch.setattr(engines, "probe_root", AsyncMock(return_value=False))
    engine = engines.CloudStreamEngine(progress)
    cfg = DeliveryConfig(shows_path=str(tmp_path), min_free_buffer_bytes=0)
    link = DownloadLink(name="a.mkv", url="https://example.com/a", size_bytes=3000)

    with pytest.raises(offline.StorageOffline):
        await engine._stream(link, tmp_path / "a.mkv", cfg, 1)
    release.set()
    await asyncio.sleep(0.2)

    assert reports == []
