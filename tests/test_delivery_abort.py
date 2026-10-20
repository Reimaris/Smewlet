"""Abort of queued and running cloud deliveries (m80)."""

from __future__ import annotations

import io
from typing import Any

import pytest
from sqlalchemy import select

from app.core.transfer_poller import _delivery_fields
from app.db.models import DeliveredFile
from app.services.delivery import engines, worker
from tests.conftest import (
    PAYLOAD,
    FakeDownloaderClient,
    make_delivery_provider,
    make_history,
)


async def _row(session: Any, **kw: Any):
    fields: dict[str, Any] = dict(
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
        deliver_enabled=True,
        delivery_status="pending",
    )
    fields.update(kw)
    h = await make_history(session, **fields)
    await session.commit()
    return h


async def test_pending_cloud_row_is_aborted_and_then_ignored(
    db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(db_session, shows_path=str(tmp_path))
    h = await _row(db_session)
    downloader(FakeDownloaderClient())

    assert await worker.abort_delivery(db_session, h.id) is True

    await db_session.refresh(h)
    assert (h.delivery_status, h.deliver_enabled, h.delivery_error) == (
        "none",
        False,
        None,
    )
    await worker.process_delivery(db_session, h.id)
    await db_session.refresh(h)
    assert h.delivery_status == "none"
    assert list(tmp_path.iterdir()) == []


async def test_running_abort_removes_part_and_keeps_delivered_rows(
    db_session, downloader, monkeypatch, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await _row(db_session)
    downloader(FakeDownloaderClient())
    monkeypatch.setattr(engines, "CHUNK_BYTES", 1000)  # 3000 bytes -> 3 chunks
    kept = DeliveredFile(
        history_id=h.id,
        source_name="earlier.mkv",
        dest_path=str(tmp_path / "earlier.mkv"),
        kind="video",
        status="delivered",
    )
    db_session.add(kept)
    await db_session.commit()
    notified: list[str] = []

    async def no_notify(*_a: Any, **_k: Any) -> None:
        notified.append("sent")

    monkeypatch.setattr(worker, "_notify", no_notify)
    opened: list[io.BytesIO] = []

    class AbortingStream(io.BytesIO):
        """The user aborts while the second chunk is still being fetched."""

        def readinto(self, b: Any) -> int:
            if self.tell():
                engines.request_abort(
                    h.id
                )  # what abort_delivery does for a running row
            n = super().readinto(b)
            opened.append(self)
            return n

    monkeypatch.setattr(
        engines, "_urlopen", lambda url, timeout=None: AbortingStream(PAYLOAD)
    )
    h.delivery_status = "delivering"
    await db_session.commit()

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.deliver_enabled) == ("none", False)
    assert len(opened) >= 2
    assert not list(tmp_path.rglob("*.part"))
    assert notified == []
    assert not engines.is_abort_requested(h.id)
    rows = (await db_session.execute(select(DeliveredFile))).scalars().all()
    assert [r.source_name for r in rows] == ["earlier.mkv"]


async def test_sabnzbd_and_settled_rows_cannot_be_aborted(db_session) -> None:
    sab = await _row(db_session, downloader_type="sabnzbd", nzb_guid="sab")
    done = await _row(db_session, delivery_status="delivered", nzb_guid="done")

    assert await worker.abort_delivery(db_session, sab.id) is False
    assert await worker.abort_delivery(db_session, done.id) is False
    assert await worker.abort_delivery(db_session, 99999) is False
    assert _delivery_fields(sab, {}, True)["can_abort_delivery"] is False
    assert not engines.is_abort_requested(sab.id)


@pytest.mark.parametrize(
    ("status", "downloader_type", "expected"),
    [
        ("pending", "torbox", True),
        ("delivering", "premiumize", True),
        ("failed", "torbox", False),
        ("none", "torbox", False),
        ("pending", "sabnzbd", False),
    ],
)
async def test_can_abort_delivery_flag(
    db_session, status, downloader_type, expected
) -> None:
    h = await _row(db_session, delivery_status=status, downloader_type=downloader_type)
    assert _delivery_fields(h, {}, True)["can_abort_delivery"] is expected


async def test_abort_route_and_deliver_now_again(async_client, db_session) -> None:
    from datetime import datetime, timezone

    sab = await _row(db_session, downloader_type="sabnzbd", nzb_guid="sab")
    assert (
        await async_client.post(f"/api/history/{sab.id}/abort-delivery")
    ).status_code == 409

    h = await _row(db_session, completed_at=datetime.now(timezone.utc), nzb_guid="tb")
    resp = await async_client.post(f"/api/history/{h.id}/abort-delivery")
    assert resp.status_code == 200
    assert resp.headers["HX-Trigger"] == "refreshDashboard"
    await db_session.refresh(h)
    assert h.delivery_status == "none"
    assert worker.is_deliver_now_allowed(h) is True
