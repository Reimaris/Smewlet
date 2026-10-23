"""A hung library mount (dead NFS server) must pause delivery, never freeze the app."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from app.db.models import DeliveredFile
from app.services.delivery import engines, offline, worker
from tests.conftest import FakeDownloaderClient, make_delivery_provider, make_history


@pytest.fixture(autouse=True)
async def _fast_fs_timeout(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(offline, "FS_TIMEOUT_SECONDS", 0.2)
    await offline.stop_watcher()
    yield
    await offline.stop_watcher()


@pytest.fixture
def hang() -> Iterator[threading.Event]:
    """Event that blocked 'filesystem' calls wait on; released on teardown."""
    release = threading.Event()
    yield release
    release.set()


async def _ticker(counter: list[int]) -> None:
    while True:
        counter.append(1)
        await asyncio.sleep(0.01)


async def test_run_fs_returns_result_and_passes_arguments(tmp_path: Path) -> None:
    assert await offline.run_fs(tmp_path, lambda a, b=0: a + b, 1, b=2) == 3


async def test_run_fs_hang_raises_storage_offline_and_loop_keeps_running(
    tmp_path: Path, hang: threading.Event
) -> None:
    ticks: list[int] = []
    task = asyncio.create_task(_ticker(ticks))
    try:
        with pytest.raises(offline.StorageOffline) as exc:
            await offline.run_fs(tmp_path, hang.wait)
    finally:
        task.cancel()
    assert exc.value.root == tmp_path
    assert len(ticks) > 5  # the event loop kept turning while the call hung


async def test_stuck_calls_do_not_exhaust_the_default_executor(
    tmp_path: Path, hang: threading.Event
) -> None:
    for _ in range(offline.FS_WORKERS):
        with pytest.raises(offline.StorageOffline):
            await offline.run_fs(tmp_path, hang.wait)
    assert await asyncio.to_thread(lambda: "alive") == "alive"


async def test_hung_mount_pauses_delivery_as_storage_offline(
    db_session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, hang: threading.Event
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await make_history(
        db_session,
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
        deliver_enabled=True,
        delivery_status="pending",
    )
    await db_session.commit()

    async def get(_s: Any, _t: str) -> FakeDownloaderClient:
        return FakeDownloaderClient()

    monkeypatch.setattr(engines, "get_downloader_client_for_type", get)
    monkeypatch.setattr(worker, "get_downloader_client_for_type", get)
    monkeypatch.setattr(engines, "_existing_target", lambda *_a: hang.wait())

    ticks: list[int] = []
    task = asyncio.create_task(_ticker(ticks))
    try:
        await asyncio.wait_for(worker.process_delivery(db_session, h.id), 10)
    finally:
        task.cancel()

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == ("paused", "STORAGE_OFFLINE")
    assert offline.is_offline()
    assert len(ticks) > 5


async def test_recovery_marks_offline_instead_of_hanging(
    db_session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, hang: threading.Event
) -> None:
    await make_delivery_provider(db_session, shows_path=str(tmp_path))
    h = await make_history(
        db_session,
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
        deliver_enabled=True,
        delivery_status="delivering",
    )
    db_session.add(
        DeliveredFile(
            history_id=h.id,
            source_name="a",
            kind="video",
            status="pending",
            dest_path=str(tmp_path / "Show" / "ep.mkv"),
        )
    )
    await db_session.commit()
    monkeypatch.setattr(worker, "_remove_stale_part", lambda *_a: hang.wait())

    await asyncio.wait_for(worker.recover_interrupted_deliveries(db_session), 10)

    assert offline.is_offline()
