"""Delivery worker: state machine, retry/deliver-now rules, recovery (m78)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DeliveredFile, DownloadHistory
from app.services.delivery import engines, worker
from app.services.downloaders.base import (
    DownloaderFile,
    DownloaderTransfer,
    DownloadLink,
    TransferState,
)
from tests.conftest import make_delivery_provider, make_history

NAME = "Example.Show.S01E01.1080p.WEB-DL.mkv"
PAYLOAD = b"x" * 3000
URL = "https://example.com/dl/file.mkv"


class _Client:
    def __init__(self, size: int = len(PAYLOAD), folder: str | None = None) -> None:
        self.size, self.folder = size, folder

    async def get_transfer_files(self, _tid: str) -> list[DownloaderFile]:
        return [DownloaderFile(name=NAME, size_bytes=300 * 1024 * 1024)]

    async def get_download_links(self, _tid: str) -> list[DownloadLink]:
        return [DownloadLink(name=NAME, url=URL, size_bytes=self.size)]

    async def get_transfers(self) -> list[DownloaderTransfer]:
        return [
            DownloaderTransfer(
                transfer_id="tb-9000",
                name="x",
                state=TransferState.COMPLETED,
                progress_pct=100.0,
                folder_id=self.folder,
            )
        ]


@pytest.fixture(autouse=True)
def fresh_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "_queue", None)


@pytest.fixture
def downloader(monkeypatch: pytest.MonkeyPatch):
    def _set(client: _Client) -> None:
        async def get(_s: Any, _t: str) -> _Client:
            return client

        for mod in (worker, engines):
            monkeypatch.setattr(mod, "get_downloader_client_for_type", get)
        monkeypatch.setattr(
            engines,
            "_client_factory",
            lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(200, content=PAYLOAD)
                )
            ),
        )

    return _set


async def _pending(session: AsyncSession, **kw: Any) -> DownloadHistory:
    fields: dict[str, Any] = dict(
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
        deliver_enabled=True,
        delivery_status="pending",
        progress_pct=55.0,
    )
    fields.update(kw)
    h = await make_history(session, **fields)
    await session.commit()
    return h


async def test_cloud_delivery_marks_delivered_and_resets_progress(
    db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await _pending(db_session)
    downloader(_Client())

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == ("delivered", None)
    assert h.delivered_at is not None
    assert (h.progress_pct, h.download_speed_bytes, h.eta_seconds) == (0.0, 0, None)
    (row,) = (await db_session.execute(select(DeliveredFile))).scalars()
    assert Path(row.dest_path or "").read_bytes() == PAYLOAD


@pytest.mark.parametrize(
    ("config", "size", "status", "error"),
    [
        ({"min_free_buffer_bytes": 0}, 9999, "failed", "SIZE_MISMATCH"),
        (
            {"shows_path": "", "min_free_buffer_bytes": 0},
            3000,
            "paused",
            "PATH_NOT_CONFIGURED",
        ),
        ({"min_free_buffer_bytes": 10**18}, 3000, "paused", "INSUFFICIENT_STORAGE"),
    ],
)
async def test_aggregate_failure_rules(
    config, size, status, error, db_session, downloader, tmp_path
) -> None:
    config = {"shows_path": str(tmp_path), **config}
    await make_delivery_provider(db_session, **config)
    h = await _pending(db_session)
    downloader(_Client(size=size))

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == (status, error)
    assert h.delivered_at is None


async def test_inactive_provider_pauses(db_session, downloader) -> None:
    provider = await make_delivery_provider(db_session)
    provider.is_active = False
    h = await _pending(db_session)
    downloader(_Client())

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == ("paused", "PATH_NOT_CONFIGURED")


async def test_sabnzbd_rows_use_local_link_engine(
    db_session, downloader, tmp_path
) -> None:
    src = tmp_path / "sab"
    src.mkdir()
    (src / NAME).write_bytes(PAYLOAD)
    lib = tmp_path / "lib"
    await make_delivery_provider(db_session, shows_path=str(lib))
    h = await _pending(db_session, downloader_type="sabnzbd")
    downloader(_Client(folder=str(src)))

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert h.delivery_status == "delivered"
    (row,) = (await db_session.execute(select(DeliveredFile))).scalars()
    assert os.path.samefile(src / NAME, row.dest_path or "")


async def test_only_pending_rows_are_processed(db_session, downloader) -> None:
    await make_delivery_provider(db_session)
    h = await _pending(db_session, delivery_status="none")
    downloader(_Client())

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert h.delivery_status == "none"


@pytest.mark.parametrize(
    ("start", "allowed"),
    [
        ("failed", True),
        ("paused", True),
        ("none", False),
        ("delivered", False),
        ("pending", False),
    ],
)
async def test_retry_delivery(start, allowed, db_session) -> None:
    h = await _pending(db_session, delivery_status=start, delivery_error="X")

    assert await worker.retry_delivery(db_session, h.id) is allowed

    await db_session.refresh(h)
    assert h.delivery_status == ("pending" if allowed else start)
    if allowed:
        assert h.delivery_error is None
        assert worker._get_queue().get_nowait() == h.id


@pytest.mark.parametrize(
    ("overrides", "allowed"),
    [
        ({"delivery_status": "none"}, True),
        ({"delivery_status": "failed"}, True),
        ({"delivery_status": "paused"}, True),
        ({"delivery_status": "delivered"}, False),
        ({"delivery_status": "delivering"}, False),
        ({"status_detail": "deleted"}, False),
        ({"status_detail": "replaced"}, False),
        ({"downloader_type": "sabnzbd"}, False),
        ({"completed_at": None}, False),
    ],
)
async def test_deliver_now(overrides, allowed, db_session) -> None:
    fields: dict[str, Any] = dict(
        delivery_status="none",
        deliver_enabled=False,
        completed_at=datetime.now(timezone.utc),
        status_detail="completed",
    )
    fields.update(overrides)
    h = await _pending(db_session, **fields)

    assert await worker.deliver_now(db_session, h.id) is allowed

    await db_session.refresh(h)
    assert h.deliver_enabled is allowed
    assert (h.delivery_status == "pending") is allowed


async def test_recovery_resets_delivering_and_deletes_part_files(
    db_session, tmp_path
) -> None:
    await make_delivery_provider(db_session, shows_path=str(tmp_path))
    h = await _pending(db_session, delivery_status="delivering")
    waiting = await _pending(db_session, nzb_guid="g-wait")
    done = await _pending(db_session, nzb_guid="g-done", delivery_status="delivered")
    orphan = tmp_path / "Show" / "ep.mkv.part"
    finished = tmp_path / "Show" / "done.mkv"
    outside = tmp_path.parent / f"{tmp_path.name}-outside.mkv.part"
    orphan.parent.mkdir()
    for f in (orphan, outside):
        f.write_bytes(b"x")
    finished.write_bytes(b"x")
    db_session.add_all(
        [
            DeliveredFile(
                history_id=h.id,
                source_name="a",
                kind="video",
                status="pending",
                dest_path=str(tmp_path / "Show" / "ep.mkv"),
            ),
            DeliveredFile(
                history_id=h.id,
                source_name="b",
                kind="video",
                status="failed",
                dest_path=str(outside)[: -len(".part")],
            ),
            DeliveredFile(
                history_id=done.id,
                source_name="c",
                kind="video",
                status="delivered",
                dest_path=str(finished),
            ),
        ]
    )
    await db_session.commit()

    assert await worker.recover_interrupted_deliveries(db_session) == 2

    for row in (h, waiting, done):
        await db_session.refresh(row)
    assert (h.delivery_status, waiting.delivery_status) == ("pending", "pending")
    assert done.delivery_status == "delivered"
    assert not orphan.exists()
    assert finished.exists()
    assert outside.exists()  # outside configured roots: never touched
    queue = worker._get_queue()
    assert sorted([queue.get_nowait(), queue.get_nowait()]) == sorted(
        [h.id, waiting.id]
    )


async def test_worker_loop_processes_enqueued_rows(
    db_session, downloader, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await _pending(db_session)
    downloader(_Client())

    await worker.start_delivery_worker()
    try:
        await worker.enqueue_delivery(h.id)
        await worker._get_queue().join()
    finally:
        await worker.stop_delivery_worker()

    db_session.expire_all()
    await db_session.refresh(h)
    assert h.delivery_status == "delivered"


# ---------------------------------------------------------------------------
# Completion hooks and dry-run (m78-10)
# ---------------------------------------------------------------------------


async def _complete_via_poller(
    session: AsyncSession, *, deliver_enabled: bool, provider_active: bool
) -> DownloadHistory:
    from app.core.transfer_poller import run_transfer_poller_tick
    from app.db.models import MediaStatus
    from tests.conftest import make_movie

    provider = await make_delivery_provider(session, movies_path="/lib/movies")
    provider.is_active = provider_active
    movie = await make_movie(
        session, title="Example Film", year=2020, status=MediaStatus.DOWNLOADING
    )
    history = await make_history(
        session,
        item=movie,
        downloader_transfer_id="99001",
        nzb_guid="guid-complete",
        status_detail="downloading",
        is_dismissed=False,
        deliver_enabled=deliver_enabled,
    )
    await session.commit()
    snapshot = [
        {
            "id": 99001,
            "name": "Example.Film.2020.1080p.WEB-DL-GRP",
            "download_state": "completed",
            "download_finished": True,
            "progress": 1.0,
            "download_speed": 0,
            "eta": 0,
            "files": [
                {"name": "Example.Film.2020.1080p.WEB-DL-GRP.mkv", "size": 4 * 1024**3}
            ],
        }
    ]
    await run_transfer_poller_tick(session, pre_fetched_downloads=snapshot)
    await session.refresh(history)
    return history


@pytest.mark.parametrize(
    ("deliver_enabled", "provider_active", "queued"),
    [(True, True, True), (False, True, False), (True, False, False)],
)
async def test_poller_completion_enqueues_only_when_enabled_and_active(
    deliver_enabled, provider_active, queued, db_session
) -> None:
    history = await _complete_via_poller(
        db_session, deliver_enabled=deliver_enabled, provider_active=provider_active
    )

    assert history.status_detail == "completed"
    assert history.delivery_status == ("pending" if queued else "none")
    queue = worker._get_queue()
    assert queue.qsize() == (1 if queued else 0)
    if queued:
        assert queue.get_nowait() == history.id


@pytest.mark.parametrize(
    ("active", "explicit", "expected"),
    [
        (True, None, True),
        (False, None, False),
        (True, False, False),
        (False, True, True),
    ],
)
async def test_resolve_deliver_enabled(active, explicit, expected, db_session) -> None:
    from app.services.delivery.hooks import resolve_deliver_enabled

    provider = await make_delivery_provider(db_session)
    provider.is_active = active
    await db_session.flush()

    assert await resolve_deliver_enabled(db_session, explicit) is expected


async def test_dry_run_plan_is_logged_without_writing(
    db_session, caplog, tmp_path
) -> None:
    from app.services.delivery.hooks import log_dry_run_delivery_plan

    await make_delivery_provider(db_session, shows_path=str(tmp_path))
    h = await _pending(
        db_session,
        delivery_status="none",
        nzb_title="Example.Show.S01E02.1080p.WEB-DL-GRP",
    )

    with caplog.at_level("INFO"):
        await log_dry_run_delivery_plan(db_session, h)

    assert "[DELIVERY] [DRY RUN]" in caplog.text
    assert "Example Show - S01E02.mkv" in caplog.text
    assert list(tmp_path.iterdir()) == []
    await db_session.refresh(h)
    assert h.delivery_status == "none"
    assert (await db_session.execute(select(DeliveredFile))).first() is None
