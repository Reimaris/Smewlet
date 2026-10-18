"""Delivery worker: state machine, retry/deliver-now rules, recovery (m78)."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DeliveredFile, DownloadHistory
from app.services.delivery import refresh, worker
from tests.conftest import (
    NAME,
    PAYLOAD,
    FakeDownloaderClient,
    make_delivery_provider,
    make_history,
)


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
    downloader(FakeDownloaderClient())

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
    downloader(FakeDownloaderClient(size=size))

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == (status, error)
    assert h.delivered_at is None


async def test_inactive_provider_pauses(db_session, downloader) -> None:
    provider = await make_delivery_provider(db_session)
    provider.is_active = False
    h = await _pending(db_session)
    downloader(FakeDownloaderClient())

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
    downloader(FakeDownloaderClient(folder=str(src)))

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert h.delivery_status == "delivered"
    (row,) = (await db_session.execute(select(DeliveredFile))).scalars()
    assert os.path.samefile(src / NAME, row.dest_path or "")


async def test_only_pending_rows_are_processed(db_session, downloader) -> None:
    await make_delivery_provider(db_session)
    h = await _pending(db_session, delivery_status="none")
    downloader(FakeDownloaderClient())

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
    downloader(FakeDownloaderClient())

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


# ---------------------------------------------------------------------------
# Ad-hoc completion detection (m78-11b)
# ---------------------------------------------------------------------------


def _adhoc_snapshot(state: str = "completed", size: int = 4 * 1024**3) -> list[dict]:
    return [
        {
            "id": 99101,
            "name": "Example.Film.2020.1080p.WEB-DL-GRP",
            "download_state": state,
            "download_finished": state == "completed",
            "progress": 1.0 if state == "completed" else 0.4,
            "download_speed": 1000,
            "eta": 30,
            "files": [{"name": "Example.Film.2020.1080p.WEB-DL-GRP.mkv", "size": size}],
        }
    ]


async def _adhoc_tick(
    session: AsyncSession,
    snapshot: list[dict],
    *,
    deliver_enabled: bool = True,
    provider_active: bool = True,
) -> DownloadHistory:
    from app.core.transfer_poller import run_transfer_poller_tick

    provider = await make_delivery_provider(session, movies_path="/lib/movies")
    provider.is_active = provider_active
    history = await make_history(
        session,
        is_ad_hoc=True,
        media_type_label="movie",
        media_title="Example Film",
        nzb_guid="guid-adhoc",
        downloader_transfer_id="99101",
        status_detail="downloading",
        is_dismissed=False,
        deliver_enabled=deliver_enabled,
    )
    await session.commit()
    await run_transfer_poller_tick(session, pre_fetched_downloads=snapshot)
    await session.refresh(history)
    return history


@pytest.mark.parametrize(
    ("deliver_enabled", "provider_active", "queued"),
    [(True, True, True), (False, True, False), (True, False, False)],
)
async def test_adhoc_completion_enqueues_only_when_enabled_and_active(
    deliver_enabled, provider_active, queued, db_session
) -> None:
    history = await _adhoc_tick(
        db_session,
        _adhoc_snapshot(),
        deliver_enabled=deliver_enabled,
        provider_active=provider_active,
    )

    assert history.status_detail == "completed"
    assert history.completed_at is not None
    assert history.progress_pct == 100.0
    assert history.delivery_status == ("pending" if queued else "none")
    assert worker._get_queue().qsize() == (1 if queued else 0)


async def test_adhoc_progress_updates_while_downloading(db_session) -> None:
    history = await _adhoc_tick(db_session, _adhoc_snapshot("downloading"))

    assert history.status_detail == "downloading"
    assert history.progress_pct == pytest.approx(40.0)
    assert history.delivery_status == "none"
    assert worker._get_queue().qsize() == 0


async def test_adhoc_fake_payload_fails_without_delivery(db_session) -> None:
    history = await _adhoc_tick(db_session, _adhoc_snapshot(size=500 * 1024))

    assert history.status_detail.startswith("failed: Layer 3 Fake Detection")
    assert history.failed_at is not None
    assert history.delivery_status == "none"
    assert worker._get_queue().qsize() == 0


async def test_adhoc_terminal_failure_state(db_session) -> None:
    history = await _adhoc_tick(db_session, _adhoc_snapshot("failed"))

    assert history.status_detail.startswith("failed:")
    assert worker._get_queue().qsize() == 0


async def test_active_adhoc_rows_keep_poller_awake(db_session) -> None:
    from app.core.transfer_poller import count_downloading_entities

    await make_history(
        db_session,
        is_ad_hoc=True,
        nzb_guid="g-a",
        downloader_transfer_id="1",
        status_detail="downloading",
    )
    await make_history(
        db_session,
        is_ad_hoc=True,
        is_dry_run=True,
        nzb_guid="g-b",
        downloader_transfer_id="2",
        status_detail="downloading",
    )
    await db_session.commit()

    assert await count_downloading_entities(db_session) == 1


# ---------------------------------------------------------------------------
# Media server refresh after a burst (m79-03)
# ---------------------------------------------------------------------------

PLEX_CFG = {"scan_plex_url": "http://plex.example.com", "scan_plex_token": "test-tok"}


@pytest.fixture(autouse=True)
def _clear_refresh_folders():
    worker._refresh_folders.clear()
    yield
    worker._refresh_folders.clear()


@pytest.fixture
def refresh_calls(monkeypatch):
    """Record refresh calls and alerts; ``results`` is what the fake refresh returns."""
    state: dict[str, Any] = {"refresh": [], "alerts": [], "results": []}

    async def fake_refresh(cfg, folders):
        state["refresh"].append(set(folders))
        return state["results"]

    async def fake_alert(session, flag, head, body):
        state["alerts"].append((flag, head, body))

    monkeypatch.setattr(worker, "refresh_media_servers", fake_refresh)
    monkeypatch.setattr("app.services.notifications.dispatch_alert_message", fake_alert)
    return state


async def _deliver_two(db_session, downloader, tmp_path, **cfg) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0, **cfg
    )
    downloader(FakeDownloaderClient())
    for title in ("Example Show", "Other Show"):
        h = await _pending(db_session, media_title=title)
        await worker.process_delivery(db_session, h.id)


async def test_burst_refreshes_once_with_all_title_folders(
    db_session, downloader, tmp_path, refresh_calls
) -> None:
    await _deliver_two(db_session, downloader, tmp_path, **PLEX_CFG)
    assert len(worker._refresh_folders) == 2

    await worker._flush_refresh()

    assert refresh_calls["refresh"] == [
        {tmp_path / "Example Show (2018)", tmp_path / "Other Show (2018)"}
    ]
    assert worker._refresh_folders == set()
    await worker._flush_refresh()
    assert len(refresh_calls["refresh"]) == 1


async def test_no_refresh_without_configured_server(
    db_session, downloader, tmp_path, refresh_calls
) -> None:
    await _deliver_two(db_session, downloader, tmp_path)
    await worker._flush_refresh()
    assert refresh_calls["refresh"] == []


async def test_no_refresh_when_nothing_delivered(refresh_calls, db_session) -> None:
    await make_delivery_provider(db_session, **PLEX_CFG)
    await db_session.commit()
    await worker._flush_refresh()
    assert refresh_calls["refresh"] == []


async def test_failed_refresh_keeps_delivered_and_alerts_once(
    db_session, downloader, tmp_path, refresh_calls
) -> None:
    refresh_calls["results"] = [
        refresh.RefreshResult("plex", False, "full", "HTTP 401 Unauthorized"),
        refresh.RefreshResult("jellyfin", False, "full", "timed out"),
    ]
    await _deliver_two(db_session, downloader, tmp_path, **PLEX_CFG)
    refresh_calls["alerts"].clear()

    await worker._flush_refresh()

    alerts = [a for a in refresh_calls["alerts"] if a[1] == "Library refresh failed"]
    assert len(alerts) == 1
    flag, _, body = alerts[0]
    assert flag == "notify_on_delivery_failed"
    assert "Plex refresh failed: HTTP 401 Unauthorized" in body
    assert "Jellyfin refresh failed: timed out" in body
    rows = (await db_session.execute(select(DownloadHistory))).scalars()
    assert {r.delivery_status for r in rows} == {"delivered"}


async def test_flush_never_raises(
    db_session, downloader, tmp_path, refresh_calls, monkeypatch
) -> None:
    async def boom(cfg, folders):
        raise RuntimeError("boom")

    monkeypatch.setattr(worker, "refresh_media_servers", boom)
    await _deliver_two(db_session, downloader, tmp_path, **PLEX_CFG)
    await worker._flush_refresh()


async def test_worker_loop_refreshes_after_retry_success(
    db_session, downloader, tmp_path, refresh_calls
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0, **PLEX_CFG
    )
    h = await _pending(db_session, delivery_status="failed")
    downloader(FakeDownloaderClient())

    await worker.start_delivery_worker()
    try:
        assert await worker.retry_delivery(db_session, h.id)
        for _ in range(100):
            if refresh_calls["refresh"]:
                break
            await asyncio.sleep(0.05)
    finally:
        await worker.stop_delivery_worker()

    assert refresh_calls["refresh"] == [{tmp_path / "Example Show (2018)"}]
