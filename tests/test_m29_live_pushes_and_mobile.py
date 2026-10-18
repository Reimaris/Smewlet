"""
Tests for Milestone m29 (v3.2.0): Live Active Pushes & Mobile Dashboard
=======================================================================
Ticket 01: Completed Transfer Retention & TorBox 6s Cache Cadence
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.transfer_poller import (
    get_active_pushes,
    get_push_history_ledger,
    run_transfer_poller_tick,
)
from app.db.models import (
    MediaStatus,
)
from app.services.downloaders import torbox
from tests.conftest import make_history, make_movie, make_series, seed_ready_setup


@pytest.mark.anyio
async def test_torbox_cache_ttl_and_force_refresh(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify TORBOX_USENET_CACHE_TTL == 6.0, _poll_limiter interval == 6.0, and ?force=1 clears cache."""
    assert getattr(torbox, "TORBOX_USENET_CACHE_TTL", None) == 6.0
    assert torbox._poll_limiter.min_interval == 6.0

    # Seed a downloading movie so GET /api/pushes/active polls TorBox
    movie = await make_movie(db_session, status=MediaStatus.DOWNLOADING)
    await make_history(
        db_session,
        item=movie,
        downloader_transfer_id="901",
        status_detail="downloading",
        is_dismissed=False,
    )
    await db_session.commit()

    # Populate TorBox cache manually
    torbox._cached_usenet_downloads = [
        {"id": 901, "download_state": "downloading", "progress": 0.25}
    ]
    torbox._cached_usenet_downloads_at = 999999999.0

    with (
        patch.object(
            torbox,
            "clear_usenet_cache",
            wraps=torbox.clear_usenet_cache,
        ) as mock_clear,
        patch.object(
            torbox,
            "get_usenet_downloads",
            new=AsyncMock(
                return_value=[
                    {"id": 901, "download_state": "downloading", "progress": 0.5}
                ]
            ),
        ) as mock_get,
    ):
        # Without force=1 -> does not call clear_usenet_cache
        resp1 = await async_client.get("/api/pushes/active")
        assert resp1.status_code == 200
        mock_clear.assert_not_called()
        mock_get.assert_awaited_once_with(
            bypass_cache=False,
            session=pytest.approx(db_session, abs=1)
            if False
            else mock_get.call_args.kwargs["session"],
        )

        # With force=1 -> calls clear_usenet_cache before polling
        resp2 = await async_client.get("/api/pushes/active?force=1")
        assert resp2.status_code == 200
        mock_clear.assert_called_once()


@pytest.mark.anyio
async def test_completed_retention_in_active_pushes_and_poller_tick(
    db_session: AsyncSession,
) -> None:
    """Verify completed transfers set completed_at on Layer 3 verification, stay in Card 5 for 300s, and expire after 300s while remaining in Card 4."""
    # Check DB column exists via PRAGMA
    cols = [
        r[1]
        for r in (
            await db_session.execute(text("PRAGMA table_info(download_history)"))
        ).fetchall()
    ]
    assert "completed_at" in cols

    movie = await make_movie(
        db_session, title="Dune Part Two", year=2024, status=MediaStatus.DOWNLOADING
    )
    hist = await make_history(
        db_session,
        item=movie,
        downloader_transfer_id="777",
        status_detail="downloading",
        is_dismissed=False,
        completed_at=None,
    )
    await db_session.commit()

    # Run transfer poller tick with pre-fetched completed playable video transfer
    tb_payload = [
        {
            "id": 777,
            "download_state": "completed",
            "download_finished": True,
            "progress": 1.0,
            "files": [
                {"name": "Dune.Part.Two.2024.2160p.WEB-DL.mkv", "size": 15000000000}
            ],
        }
    ]
    with patch(
        "app.services.discord.dispatch_batch_notification_event",
        new=AsyncMock(),
    ):
        await run_transfer_poller_tick(db_session, pre_fetched_downloads=tb_payload)

    await db_session.refresh(hist)
    await db_session.refresh(movie)
    assert movie.status == MediaStatus.COMPLETED
    assert hist.status_detail == "completed"
    assert hist.completed_at is not None

    # Within 300s -> returned in get_active_pushes with Ready on TorBox and is_completed=True
    pushes = await get_active_pushes(db_session)
    assert len(pushes) == 1
    row = pushes[0]
    assert row["history_id"] == hist.id
    assert row["is_completed"] is True
    assert row["status"] == "Ready on TorBox"
    assert row["progress_pct"] == 100.0
    assert row["speed_and_eta"] == "100% • Ready"
    assert row["completed_at"] is not None

    # Aged past 300s (e.g., 305s ago) -> excluded from get_active_pushes, still in get_push_history_ledger
    hist.completed_at = datetime.now(timezone.utc) - timedelta(seconds=305)
    await db_session.commit()

    pushes_after = await get_active_pushes(db_session)
    assert pushes_after == []

    ledger = await get_push_history_ledger(db_session)
    assert len(ledger) == 1
    assert ledger[0]["id"] == hist.id
    assert ledger[0]["downloader_status"] == "READY"


@pytest.mark.anyio
async def test_failed_pushes_retention_in_card5_removed_after_5_minutes(
    db_session: AsyncSession,
) -> None:
    """Verify failed pushes stay in Card 5 (Tab 5) for 5 minutes and are removed afterwards."""
    now_utc = datetime.now(timezone.utc)
    movie = await make_movie(
        db_session,
        title="Failed Push Retention Movie",
        simkl_id=505,
        status=MediaStatus.FAILED,
    )
    failed_hist = await make_history(
        db_session,
        item=movie,
        nzb_title="Failed.Movie.2024.1080p-GRP",
        nzb_guid="guid-failed-505",
        status_detail="failed: TorBox download failed: error",
        push_mode="auto",
        is_dismissed=False,
    )
    # Stamped within 5 minutes (e.g., 60 seconds ago)
    failed_hist.created_at = now_utc - timedelta(seconds=60)
    await db_session.commit()

    # Within 300s -> returned in get_active_pushes with is_failed=True
    pushes = await get_active_pushes(db_session)
    failed_rows = [p for p in pushes if p["history_id"] == failed_hist.id]
    assert len(failed_rows) == 1
    assert failed_rows[0]["is_failed"] is True
    assert failed_rows[0]["status"] == "Failed (Auto-Push)"

    # Aged past 300s (e.g., 305 seconds ago) -> removed from get_active_pushes
    failed_hist.created_at = now_utc - timedelta(seconds=305)
    await db_session.commit()

    pushes_expired = await get_active_pushes(db_session)
    failed_rows_expired = [
        p for p in pushes_expired if p["history_id"] == failed_hist.id
    ]
    assert failed_rows_expired == []


@pytest.mark.anyio
async def test_completed_card5_actions_dismiss_delete_and_blacklist(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify Dismiss, Delete from TorBox, and Blacklist Release on completed rows in Card 5."""
    now_utc = datetime.now(timezone.utc)

    # 1. Dismiss on a completed row: hides from Card 5, keeps Movie COMPLETED and Card 4 READY ON TORBOX
    m1 = await make_movie(
        db_session, title="Movie Dismiss", simkl_id=101, status=MediaStatus.COMPLETED
    )
    h1 = await make_history(
        db_session,
        item=m1,
        downloader_transfer_id="1001",
        status_detail="completed",
        is_dismissed=False,
        completed_at=now_utc - timedelta(seconds=30),
    )

    # 2. Delete from TorBox on a completed row: deletes from TorBox, marks deleted/dismissed, resets Movie to SEARCHING
    m2 = await make_movie(
        db_session, title="Movie Delete", simkl_id=102, status=MediaStatus.COMPLETED
    )
    h2 = await make_history(
        db_session,
        item=m2,
        downloader_transfer_id="1002",
        status_detail="completed",
        is_dismissed=False,
        completed_at=now_utc - timedelta(seconds=30),
    )

    # 3. Blacklist on a completed row: creates BlacklistedRelease, deletes from TorBox, marks deleted, resets Movie to SEARCHING
    m3 = await make_movie(
        db_session, title="Movie Blacklist", simkl_id=103, status=MediaStatus.COMPLETED
    )
    h3 = await make_history(
        db_session,
        item=m3,
        nzb_title="Movie.Blacklist.2024.1080p.WEB-DL-BAD",
        nzb_guid="guid-bl-1003",
        downloader_transfer_id="1003",
        status_detail="completed",
        is_dismissed=False,
        completed_at=now_utc - timedelta(seconds=30),
    )
    await db_session.commit()

    # Action 1: Dismiss h1
    resp_dismiss = await async_client.post(f"/api/pushes/{h1.id}/dismiss")
    assert resp_dismiss.status_code == 200
    await db_session.refresh(h1)
    await db_session.refresh(m1)
    assert h1.is_dismissed is True
    assert h1.status_detail == "completed"
    assert m1.status == MediaStatus.COMPLETED

    ledger = await get_push_history_ledger(db_session)
    h1_ledger = next(r for r in ledger if r["id"] == h1.id)
    assert h1_ledger["downloader_status"] == "READY"

    # Action 2: Delete from TorBox h2
    with patch.object(
        torbox, "delete_usenet_download", new=AsyncMock(return_value={"success": True})
    ) as mock_del_tb:
        resp_del = await async_client.post(f"/api/pushes/{h2.id}/delete-transfer")
        assert resp_del.status_code == 200
        hx_trig = json.loads(resp_del.headers["HX-Trigger"])
        assert "smewlet:poller-state" in hx_trig
        assert hx_trig.get("refreshDashboard") is True
        mock_del_tb.assert_awaited_once_with(
            1002, session=mock_del_tb.call_args.kwargs["session"]
        )

    await db_session.refresh(h2)
    await db_session.refresh(m2)
    assert h2.status_detail == "deleted"
    assert h2.is_dismissed is True
    assert m2.status == MediaStatus.SEARCHING

    # Action 3: Blacklist h3 from Active Pushes
    with patch.object(
        torbox, "delete_usenet_download", new=AsyncMock(return_value={"success": True})
    ) as mock_del_bl:
        resp_bl = await async_client.post(f"/api/pushes/{h3.id}/blacklist")
        assert resp_bl.status_code == 200
        mock_del_bl.assert_awaited_once_with(
            1003, session=mock_del_bl.call_args.kwargs["session"]
        )

    await db_session.refresh(h3)
    await db_session.refresh(m3)
    assert h3.status_detail == "deleted"
    assert h3.is_dismissed is True
    assert m3.status == MediaStatus.SEARCHING


@pytest.mark.anyio
async def test_dismiss_all_card5_transfers(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify Dismiss All clears completed, failed, and dry-run rows from Card 5 while preserving active downloading rows."""
    from app.core.transfer_poller import register_dry_run_push

    now_utc = datetime.now(timezone.utc)

    # 1. Completed movie
    m1 = await make_movie(
        db_session, title="Completed Movie", simkl_id=201, status=MediaStatus.COMPLETED
    )
    h1 = await make_history(
        db_session,
        item=m1,
        downloader_transfer_id="2001",
        status_detail="completed",
        is_dismissed=False,
        completed_at=now_utc - timedelta(seconds=20),
    )

    # 2. Failed movie
    m2 = await make_movie(
        db_session, title="Failed Movie", simkl_id=202, status=MediaStatus.FAILED
    )
    h2 = await make_history(
        db_session,
        item=m2,
        status_detail="failed: indexer timeout",
        push_mode="auto",
        is_dismissed=False,
    )
    h2.failed_at = now_utc - timedelta(seconds=20)

    # 3. Active downloading movie (must NOT be dismissed!)
    m3 = await make_movie(
        db_session,
        title="Downloading Movie",
        simkl_id=203,
        status=MediaStatus.DOWNLOADING,
    )
    h3 = await make_history(
        db_session,
        item=m3,
        downloader_transfer_id="2003",
        status_detail="downloading",
        is_dismissed=False,
    )

    # 4. In-memory synthetic dry-run record
    register_dry_run_push(
        media_item_id=999,
        media_title="Dry Run Movie",
        target_badge="Movie",
        release_title="Dry.Run.Movie.2024.1080p",
        resolution="1080p",
        source="web-dl",
        language="de",
        feature_pills=[],
        push_mode="auto",
    )
    await db_session.commit()

    # Before dismiss-all: Card 5 has 4 rows
    pushes_before = await get_active_pushes(db_session)
    assert len(pushes_before) == 4

    # Trigger POST /api/pushes/dismiss-all
    resp = await async_client.post(
        "/api/pushes/dismiss-all", headers={"HX-Request": "true"}
    )
    assert resp.status_code == 200

    await db_session.refresh(h1)
    await db_session.refresh(m1)
    await db_session.refresh(h2)
    await db_session.refresh(m2)
    await db_session.refresh(h3)
    await db_session.refresh(m3)

    # Completed movie is dismissed but remains COMPLETED in library
    assert h1.is_dismissed is True
    assert m1.status == MediaStatus.COMPLETED

    # Failed movie is dismissed and reverted to SEARCHING
    assert h2.is_dismissed is True
    assert m2.status == MediaStatus.SEARCHING

    # Active downloading movie is untouched!
    assert h3.is_dismissed is False
    assert m3.status == MediaStatus.DOWNLOADING

    # After dismiss-all: Card 5 only has the active downloading row
    pushes_after = await get_active_pushes(db_session)
    assert len(pushes_after) == 1
    assert pushes_after[0]["history_id"] == h3.id


@pytest.mark.anyio
async def test_card5_live_polling_and_completed_row_ui(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify Card 5 Alpine 3s polling trigger, force=1 refresh button, Ready on TorBox completed row UI, and mobile stacked classes."""
    now_utc = datetime.now(timezone.utc)
    movie = await make_movie(
        db_session, title="Completed UI Movie", status=MediaStatus.COMPLETED
    )
    hist = await make_history(
        db_session,
        item=movie,
        downloader_transfer_id="2001",
        status_detail="completed",
        is_dismissed=False,
        completed_at=now_utc - timedelta(seconds=15),
    )
    await db_session.commit()

    # 1. Verify dashboard.html uses hx-trigger="load, refreshActivePushes", 3000ms interval, and force=1 refresh button
    dash_resp = await async_client.get("/")
    assert dash_resp.status_code == 200
    dash_html = dash_resp.text
    assert 'hx-trigger="load, refreshActivePushes"' in dash_html
    assert "refreshActivePushes" in dash_html
    assert "3000" in dash_html
    assert 'hx-get="/api/pushes/active?force=1"' in dash_html
    assert "every 8s[activeCard ===" not in dash_html

    # 2. Verify GET /api/pushes/active with HX-Request: true renders completed row badge, actions, and mobile classes
    partial_resp = await async_client.get(
        "/api/pushes/active", headers={"HX-Request": "true"}
    )
    assert partial_resp.status_code == 200
    partial_html = partial_resp.text
    assert "Ready on TorBox" in partial_html
    assert "bg-emerald-600 text-white" in partial_html
    assert f"/api/pushes/{hist.id}/dismiss" in partial_html
    assert "Dismiss from Active Pushes (keeps in History)" in partial_html
    assert f"/api/pushes/{hist.id}/delete-transfer" in partial_html
    assert "overflow-x-hidden md:overflow-x-auto" in partial_html
    assert (
        "block md:table w-full md:min-w-full text-left border-collapse table-fixed md:table-auto"
        in partial_html
    )
    assert (
        "block md:table-row-group divide-y divide-[#3f3f46] text-sm w-full"
        in partial_html
    )
    assert "hidden md:table-header-group" in partial_html
    assert "block md:table-row p-3 md:p-0 hover:bg-[#2a2a32]" in partial_html
    assert (
        "block md:table-cell md:px-6 md:py-4 w-full md:max-w-[260px] break-words"
        in partial_html
    )
    assert (
        "hidden md:table-cell md:px-6 md:py-4 md:w-[320px] md:min-w-[320px] md:max-w-[320px] align-top break-words"
        in partial_html
    )
    assert (
        "hidden md:table-cell md:px-6 md:py-4 whitespace-nowrap md:min-w-[180px]"
        in partial_html
    )
    assert (
        "hidden md:table-cell md:px-6 md:py-4 whitespace-nowrap text-xs font-mono text-gray-300"
        in partial_html
    )
    assert "hidden md:table-cell md:px-6 md:py-4 whitespace-nowrap" in partial_html
    assert (
        "pt-2.5 md:pt-4 border-t border-[#3f3f46]/40 md:border-t-0 md:px-6 md:py-4 text-right md:text-left whitespace-nowrap md:w-1 w-full"
        in partial_html
    )


@pytest.mark.anyio
async def test_mobile_responsive_dashboard_navbar_and_push_modal(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify Ticket 03 mobile-responsive classes across base.html, dashboard.html (Cards 1-4 & Top Deck), and push_modal.html."""
    movie = await make_movie(
        db_session, title="Mobile Test Movie", status=MediaStatus.SEARCHING
    )
    series, seasons, episodes = await make_series(
        db_session,
        title="Mobile Test Series",
        release_date=datetime.now(timezone.utc) - timedelta(days=100),
        seasons=1,
        episodes_per_season=3,
    )
    await make_history(
        db_session,
        item=movie,
        downloader_transfer_id="3001",
        status_detail="completed",
        is_dismissed=True,
    )
    await db_session.commit()

    await seed_ready_setup(db_session)
    await db_session.commit()
    # 1. GET / — verify base.html navbar and dashboard.html Top Deck + Cards 1-4
    resp = await async_client.get("/")
    assert resp.status_code == 200
    html = resp.text

    # base.html compact navbar on < sm
    assert "w-9 h-9 sm:w-11 sm:h-11" in html
    assert "hidden sm:inline-block" in html
    assert (
        '<span class="hidden sm:inline">Sync Watchlist</span><span class="sm:hidden">Sync</span>'
        in html
    )
    assert '<span class="hidden sm:inline">Dashboard</span>' in html
    assert '<span class="hidden sm:inline">Settings</span>' in html

    # dashboard.html 6-Card Top Deck
    assert "grid grid-cols-3 lg:grid-cols-6 gap-2 sm:gap-4 mb-5 sm:mb-8" in html
    assert "md:translate-y-2 ring-2" in html
    assert "px-2.5 py-2 sm:px-3.5 sm:py-2.5" in html
    assert (
        "text-[11px] sm:text-sm font-bold tracking-wider uppercase leading-tight break-words sm:truncate"
        in html
    )
    assert "hidden md:grid grid-cols-3" in html
    assert "hidden md:grid grid-cols-2" in html
    assert "hidden md:flex flex-col" in html

    # dashboard.html Tab 5 Dismiss All button
    assert 'hx-post="/api/pushes/dismiss-all"' in html
    assert "Dismiss All" in html

    # dashboard.html Cards 1-3 & Card 4 (History) mobile stacked row cards
    assert "hidden md:table-header-group bg-[#1e1e24]" in html
    assert "block md:table-row p-3 md:p-0 hover:bg-[#2a2a32]" in html
    assert (
        "block md:table-cell md:px-6 md:py-4 w-full md:min-w-[260px] break-words"
        in html
    )
    assert "hidden md:table-cell md:px-6 md:py-4 md:w-px md:whitespace-nowrap" in html
    assert (
        "md:hidden mt-2.5 px-2.5 py-1.5 bg-[#1e1e24]/70 rounded border border-[#3f3f46]/40 flex items-center justify-between"
        in html
    )
    assert "text-purple-300 border border-purple-500/30" in html
    assert "overflow-x-hidden md:overflow-x-auto" in html
    assert (
        "block md:table w-full md:min-w-full divide-y divide-[#3f3f46] table-fixed md:table-auto"
        in html
    )
    assert (
        "block md:table-row-group divide-y divide-[#3f3f46] bg-[#24242b] w-full" in html
    )
    assert (
        "block md:table-cell md:px-4 md:py-3.5 w-full md:w-auto md:min-w-[260px] break-words max-w-full min-w-0"
        in html
    )
    assert "hidden md:table-cell md:px-4 md:py-3.5 md:w-px md:whitespace-nowrap" in html
    assert (
        "border-t border-[#3f3f46]/40 mt-3 pt-2.5 md:mt-0 md:pt-3.5 md:border-t-0 md:px-4 md:py-3.5 text-right md:w-px md:whitespace-nowrap w-full"
        in html
    )

    # 2. GET /api/items/{id}/push-modal — verify Push Modal mobile padding, season header wrapping, and episode grid
    modal_resp = await async_client.get(f"/api/items/{series.id}/push-modal")
    assert modal_resp.status_code == 200
    modal_html = modal_resp.text

    assert "p-0 sm:p-4" in modal_html
    assert "p-3 sm:p-5" in modal_html
    assert "p-3 sm:p-6" in modal_html
    assert (
        "flex flex-wrap sm:flex-nowrap items-center justify-between gap-2" in modal_html
    )
    assert "grid-cols-2 sm:grid-cols-3 gap-2" in modal_html
