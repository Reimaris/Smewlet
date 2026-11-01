"""Tests for Ticket 02: Transfer Poller Stalled Watchdog & Settings UI Modernization."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.transfer_poller import run_transfer_poller_tick
from app.db.models import (
    BlacklistedRelease,
    DownloadHistory,
    MediaItem,
    MediaStatus,
    MediaType,
    Provider,
    SystemSettings,
)


@pytest.fixture
async def stalled_watchdog_db(
    db_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
):
    provider = Provider(name="Simkl", type="simkl", category="watchlist")
    indexer = Provider(
        category="indexer",
        type="newznab",
        name="TestIndexer",
        api_url="https://api.testindexer.com",
        api_key="test-key",
        priority=1,
        is_active=True,
    )
    sys_settings = SystemSettings(
        id=1,
        download_timeout_minutes=60,
        sh_max_retries=3,
        discord_enabled=False,
    )
    db_session.add_all([provider, indexer, sys_settings])
    await db_session.commit()
    return db_session, session_factory, provider, indexer


@pytest.mark.asyncio
async def test_transfer_poller_stalled_watchdog_triggers_failure_and_autoreplace(
    stalled_watchdog_db,
):
    """Verify that a transfer whose elapsed time exceeds download_timeout_minutes * 60 is flagged
    as is_stalled, triggers _handle_transfer_failure, adds the release to the blacklist,
    deletes the transfer from TorBox, increments fail_count, and initiates auto-replacement.
    """
    session, _, provider, _ = stalled_watchdog_db
    now_utc = datetime.now(timezone.utc)

    # Movie actively downloading with torbox_sent_at set 65 minutes ago (timeout is 60m)
    movie = MediaItem(
        provider_id=provider.id,
        title="Stalled Sci-Fi",
        media_type=MediaType.MOVIE,
        status=MediaStatus.DOWNLOADING,
        year=2024,
        simkl_id=98765,
        fail_count=0,
    )
    session.add(movie)
    await session.flush()

    hist = DownloadHistory(
        media_item_id=movie.id,
        nzb_title="Stalled.Sci-Fi.2024.1080p.WEB-DL",
        nzb_guid="guid-stalled-001",
        score=8000.0,
        size_bytes=4 * (1024**3),
        resolution="1080p",
        source="web-dl",
        downloader_transfer_id="tb-stalled-1",
        dispatched_at=now_utc - timedelta(minutes=65),
        push_mode="auto",
        grabbed_language="en",
        status_detail="downloading",
    )
    session.add(hist)
    await session.commit()

    # TorBox snapshot reports stalled download state ("downloading", progress 10%)
    stalled_snapshot = [
        {
            "id": "tb-stalled-1",
            "name": hist.nzb_title,
            "download_state": "downloading",
            "progress": 0.10,
            "download_speed": 0,
            "eta": 99999,
            "files": [],
        }
    ]

    candidate_results = [
        {
            "title": "Stalled.Sci-Fi.2024.1080p.BluRay",
            "guid": "guid-stalled-replacement-002",
            "link": "https://api.testindexer.com/get/2",
            "size": 5 * (1024**3),
            "indexer_id": 2,
            "indexer_name": "TestIndexer",
            "pub_date": "2024-05-01T00:00:00Z",
        }
    ]

    from tests.conftest import VALID_NZB_BYTES

    with (
        patch(
            "app.services.downloaders.torbox.delete_usenet_download",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock_del_tb,
        patch(
            "app.services.indexers.newznab.search_movie",
            new_callable=AsyncMock,
            return_value=candidate_results,
        ) as mock_search,
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new_callable=AsyncMock,
            return_value=(VALID_NZB_BYTES, "replacement.nzb"),
        ),
        patch(
            "app.services.downloaders.torbox.send_nzb_file",
            new_callable=AsyncMock,
            return_value={"id": "tb-stalled-2", "hash": "hash-stalled-2"},
        ) as mock_send_tb,
    ):
        res = await run_transfer_poller_tick(
            session, pre_fetched_downloads=stalled_snapshot
        )

        assert res["polled"] is True
        mock_del_tb.assert_awaited_once_with("tb-stalled-1", session=session)
        mock_search.assert_awaited_once()
        mock_send_tb.assert_awaited_once()

    await session.refresh(hist)
    await session.refresh(movie)

    # History replaced and dismissed
    assert hist.status_detail == "replaced"
    assert hist.is_dismissed is True

    # Movie incremented fail_count and remains DOWNLOADING for the replacement
    assert movie.status == MediaStatus.DOWNLOADING
    assert movie.fail_count == 1

    # Release added to blacklist with timeout reason
    bl = (
        await session.execute(
            select(BlacklistedRelease).where(
                BlacklistedRelease.media_item_id == movie.id,
                BlacklistedRelease.nzb_guid == "guid-stalled-001",
            )
        )
    ).scalar_one_or_none()
    assert bl is not None
    assert "Stalled download exceeded timeout (60m)" in bl.reason


@pytest.mark.asyncio
async def test_transfer_poller_under_timeout_is_not_flagged_as_stalled(
    stalled_watchdog_db,
):
    """Verify that a transfer under the timeout threshold is not flagged as stalled."""
    session, _, provider, _ = stalled_watchdog_db
    now_utc = datetime.now(timezone.utc)

    # Movie actively downloading with torbox_sent_at set 25 minutes ago (timeout is 60m)
    movie = MediaItem(
        provider_id=provider.id,
        title="Active Moving Movie",
        media_type=MediaType.MOVIE,
        status=MediaStatus.DOWNLOADING,
        year=2024,
        simkl_id=98766,
        fail_count=0,
    )
    session.add(movie)
    await session.flush()

    hist = DownloadHistory(
        media_item_id=movie.id,
        nzb_title="Active.Moving.Movie.2024.1080p.WEB-DL",
        nzb_guid="guid-active-001",
        score=8200.0,
        size_bytes=3 * (1024**3),
        resolution="1080p",
        source="web-dl",
        downloader_transfer_id="tb-active-1",
        dispatched_at=now_utc - timedelta(minutes=25),
        push_mode="auto",
        grabbed_language="en",
        status_detail="downloading",
    )
    session.add(hist)
    await session.commit()

    active_snapshot = [
        {
            "id": "tb-active-1",
            "name": hist.nzb_title,
            "download_state": "downloading",
            "progress": 0.40,
            "download_speed": 10485760,
            "eta": 300,
            "files": [],
        }
    ]

    with patch(
        "app.services.downloaders.torbox.delete_usenet_download",
        new_callable=AsyncMock,
    ) as mock_del_tb:
        res = await run_transfer_poller_tick(
            session, pre_fetched_downloads=active_snapshot
        )
        mock_del_tb.assert_not_called()

    assert res["polled"] is True
    await session.refresh(hist)
    await session.refresh(movie)

    # Still downloading, not failed or replaced
    assert movie.status == MediaStatus.DOWNLOADING
    assert movie.fail_count == 0
    assert hist.status_detail == "downloading"
    assert hist.progress_pct == 40.0

    bl = (
        await session.execute(
            select(BlacklistedRelease).where(
                BlacklistedRelease.media_item_id == movie.id
            )
        )
    ).scalar_one_or_none()
    assert bl is None


@pytest.mark.asyncio
async def test_transfer_poller_processing_at_100pct_fails_after_unpack_grace(
    stalled_watchdog_db,
):
    """A transfer stuck in 'processing' at 100% fails after the unpack grace, not the full timeout."""
    session, _, provider, _ = stalled_watchdog_db
    now_utc = datetime.now(timezone.utc)

    movie = MediaItem(
        provider_id=provider.id,
        title="Stuck Processing Movie",
        media_type=MediaType.MOVIE,
        status=MediaStatus.DOWNLOADING,
        year=2024,
        simkl_id=98767,
        fail_count=0,
    )
    session.add(movie)
    await session.flush()

    def _hist(guid: str, tid: str, stuck_minutes: int) -> DownloadHistory:
        return DownloadHistory(
            media_item_id=movie.id,
            nzb_title="Stuck.Processing.Movie.2024.1080p.WEB-DL",
            nzb_guid=guid,
            score=8200.0,
            size_bytes=3 * (1024**3),
            resolution="1080p",
            source="web-dl",
            downloader_transfer_id=tid,
            dispatched_at=now_utc - timedelta(minutes=16),
            unpack_started_at=now_utc - timedelta(minutes=stuck_minutes),
            push_mode="manual",
            grabbed_language="en",
            status_detail="processing",
        )

    fresh = _hist("guid-proc-fresh", "tb-proc-fresh", 2)
    stuck = _hist("guid-proc-stuck", "tb-proc-stuck", 12)
    session.add_all([fresh, stuck])
    await session.commit()

    snapshot = [
        {
            "id": tid,
            "name": "Stuck.Processing.Movie.2024.1080p.WEB-DL",
            "download_state": "processing",
            "progress": 1.0,
            "files": [],
        }
        for tid in ("tb-proc-fresh", "tb-proc-stuck")
    ]
    with patch(
        "app.services.downloaders.torbox.delete_usenet_download",
        new_callable=AsyncMock,
    ):
        await run_transfer_poller_tick(session, pre_fetched_downloads=snapshot)

    await session.refresh(fresh)
    await session.refresh(stuck)
    assert not (fresh.status_detail or "").startswith("failed")
    assert (stuck.status_detail or "").startswith("failed")
    assert "stuck in processing" in (stuck.status_detail or "")
