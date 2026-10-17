"""Delivery after-effects: upgrade replace, cloud purge, notifications (m78)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DeliveredFile, DownloadHistory, SystemSettings
from app.services.delivery import worker
from tests.conftest import (
    PAYLOAD,
    FakeDownloaderClient,
    make_delivery_provider,
    make_history,
    make_series,
)

NEW_DEST = "Test Series (2024)/Season 01/Test Series - S01E01.mkv"


class _PurgeClient(FakeDownloaderClient):
    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.deleted: list[str] = []

    async def delete_transfer(self, tid: str, delete_files: bool = True) -> bool:
        self.deleted.append(tid)
        return True


async def _season_pack_row(session: AsyncSession, **kw: Any) -> DownloadHistory:
    item, seasons, episodes = await make_series(
        session, seasons=1, episodes_per_season=1
    )
    fields: dict[str, Any] = dict(
        item=item,
        season=seasons[0],
        episode=episodes[0],
        deliver_enabled=True,
        delivery_status="pending",
    )
    fields.update(kw)
    h = await make_history(session, **fields)
    await session.commit()
    return h


async def _seed_old(
    session: AsyncSession, root: Path, new: DownloadHistory, rel: str
) -> Path:
    old = await make_history(
        session,
        item=None,
        nzb_guid="g-old",
        downloader_transfer_id="tb-old",
        delivery_status="delivered",
        episode_id=new.episode_id,
        season_id=new.season_id,
    )
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"old-release")
    session.add(
        DeliveredFile(
            history_id=old.id,
            source_name="old.mkv",
            kind="video",
            status="delivered",
            dest_path=str(path),
            episode_id=new.episode_id,
            season_id=new.season_id,
        )
    )
    new.is_upgrade = True
    new.replaces_history_id = old.id
    await session.commit()
    return path


async def test_upgrade_removes_only_recorded_old_files(
    db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    new = await _season_pack_row(db_session)
    old_file = await _seed_old(
        db_session, tmp_path, new, "Test Series (2024)/Season 01/Old Name S01E01.mkv"
    )
    stray = tmp_path / "Test Series (2024)/Season 01/notes.txt"
    stray.write_text("keep")
    lonely = await _seed_old_dir_file(db_session, tmp_path, new)
    downloader(FakeDownloaderClient())

    await worker.process_delivery(db_session, new.id)

    await db_session.refresh(new)
    assert new.delivery_status == "delivered"
    assert (tmp_path / NEW_DEST).read_bytes() == PAYLOAD
    assert not old_file.exists()
    assert stray.read_text() == "keep"  # unrecorded file untouched
    assert not lonely.parent.exists()  # emptied folder pruned
    assert tmp_path.exists()  # library root never removed


async def _seed_old_dir_file(
    session: AsyncSession, root: Path, new: DownloadHistory
) -> Path:
    path = root / "Old Folder/Old Name S01E01.mkv"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"old")
    session.add(
        DeliveredFile(
            history_id=new.replaces_history_id or 0,
            source_name="old2.mkv",
            kind="video",
            status="delivered",
            dest_path=str(path),
            episode_id=new.episode_id,
            season_id=new.season_id,
        )
    )
    await session.commit()
    return path


async def test_upgrade_replaces_file_at_identical_destination(
    db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    new = await _season_pack_row(db_session)
    old_file = await _seed_old(db_session, tmp_path, new, NEW_DEST)
    downloader(FakeDownloaderClient())

    await worker.process_delivery(db_session, new.id)

    await db_session.refresh(new)
    assert new.delivery_status == "delivered"
    assert old_file.read_bytes() == PAYLOAD


async def test_failed_upgrade_keeps_old_files(db_session, downloader, tmp_path) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    new = await _season_pack_row(db_session)
    old_file = await _seed_old(
        db_session, tmp_path, new, "Test Series (2024)/Season 01/Old Name S01E01.mkv"
    )
    downloader(FakeDownloaderClient(size=9999))  # SIZE_MISMATCH

    await worker.process_delivery(db_session, new.id)

    await db_session.refresh(new)
    assert new.delivery_status == "failed"
    assert old_file.read_bytes() == b"old-release"


async def test_manual_delete_transfer_leaves_library_files(
    db_session, tmp_path
) -> None:
    from app.core.transfer_poller import manual_delete_transfer

    h = await _season_pack_row(db_session, delivery_status="delivered")
    lib = tmp_path / "Show/ep.mkv"
    lib.parent.mkdir()
    lib.write_bytes(b"x")
    db_session.add(
        DeliveredFile(
            history_id=h.id,
            source_name="a",
            kind="video",
            status="delivered",
            dest_path=str(lib),
        )
    )
    await db_session.commit()

    await manual_delete_transfer(db_session, history_id=h.id)

    assert lib.read_bytes() == b"x"


@pytest.mark.parametrize(
    ("flag", "dtype", "size", "purged"),
    [
        (True, "torbox", 3000, True),
        (False, "torbox", 3000, False),
        (True, "torbox", 9999, False),  # delivery failed: nothing purged
    ],
)
async def test_cloud_purge_after_delivery(
    flag, dtype, size, purged, db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(
        db_session,
        shows_path=str(tmp_path),
        min_free_buffer_bytes=0,
        delete_cloud_after_delivery=flag,
    )
    h = await _season_pack_row(db_session, downloader_type=dtype)
    client = _PurgeClient(size=size)
    downloader(client)

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert bool(client.deleted) is purged
    assert h.status_detail == ("deleted" if purged else "completed")
    if purged:
        assert h.delivery_status == "delivered"


async def test_sabnzbd_is_never_purged(db_session, downloader, tmp_path) -> None:
    src = tmp_path / "sab"
    src.mkdir()
    (src / "Example.Show.S01E01.1080p.WEB-DL.mkv").write_bytes(PAYLOAD)
    await make_delivery_provider(
        db_session,
        shows_path=str(tmp_path / "lib"),
        delete_cloud_after_delivery=True,
    )
    h = await _season_pack_row(db_session, downloader_type="sabnzbd")
    client = _PurgeClient(folder=str(src))
    downloader(client)

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert h.delivery_status == "delivered"
    assert client.deleted == []


@pytest.mark.parametrize(
    ("failed_on", "delivered_on", "size", "expected_flag"),
    [
        (True, False, 9999, "notify_on_delivery_failed"),
        (False, False, 9999, None),
        (False, True, 3000, "notify_on_delivered"),
        (False, False, 3000, None),
    ],
)
async def test_notification_toggles_gate_messages(
    failed_on,
    delivered_on,
    size,
    expected_flag,
    db_session,
    downloader,
    tmp_path,
    monkeypatch,
) -> None:
    sent: list[str] = []
    real = SystemSettings

    async def fake_dispatch(session: Any, flag: str, *_a: Any, **_k: Any) -> dict:
        row = (await session.execute(select(real).where(real.id == 1))).scalars().one()
        if getattr(row, flag):
            sent.append(flag)
        return {}

    monkeypatch.setattr(
        "app.services.notifications.dispatch_alert_message", fake_dispatch
    )
    db_session.add(
        SystemSettings(
            id=1,
            notify_on_delivery_failed=failed_on,
            notify_on_delivered=delivered_on,
        )
    )
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await _season_pack_row(db_session)
    downloader(FakeDownloaderClient(size=size))

    await worker.process_delivery(db_session, h.id)

    assert sent == ([expected_flag] if expected_flag else [])


async def test_insufficient_storage_pause_notifies_as_failure(
    db_session, downloader, tmp_path, monkeypatch
) -> None:
    flags: list[str] = []

    async def fake_dispatch(_s: Any, flag: str, *_a: Any, **_k: Any) -> dict:
        flags.append(flag)
        return {}

    monkeypatch.setattr(
        "app.services.notifications.dispatch_alert_message", fake_dispatch
    )
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=10**18
    )
    h = await _season_pack_row(db_session)
    downloader(FakeDownloaderClient())

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == ("paused", "INSUFFICIENT_STORAGE")
    assert flags == ["notify_on_delivery_failed"]
