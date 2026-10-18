"""UI and endpoint tests for Ticket 08: 6-Card Interactive Dashboard, 3-Tier Status Tables & Active Pushes UI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    DownloadHistory,
    Episode,
    EpisodeStatus,
    MediaItem,
    MediaStatus,
    MediaType,
    Provider,
    Season,
    SeasonStatus,
    SystemSettings,
)
from tests.conftest import seed_core_prerequisites


@pytest.fixture
async def v3_ui_db(db_session: AsyncSession):
    """Seed the shared in-memory SQLite database with Movies, Series, Anime, History, and Active Pushes."""
    now = datetime.now(timezone.utc)
    recent_sync = (now - timedelta(minutes=2)).isoformat()

    session = db_session
    if True:
        sys_settings = SystemSettings(id=1)
        provider = Provider(
            name="Simkl",
            type="simkl",
            category="watchlist",
            is_active=True,
            client_id="cid",
            access_token="tok",
            config_json=f'{{"sync_interval_minutes": 60, "sync_movies": true, "sync_series": true, "sync_anime": true, "last_synced_at": "{recent_sync}"}}',
        )
        session.add_all([sys_settings, provider])
        await seed_core_prerequisites(session)
        await session.flush()

        # 1. Movie - In Progress (DOWNLOADING)
        movie_in_progress = MediaItem(
            provider_id=provider.id,
            simkl_id=1001,
            title="Dune: Part Two",
            year=2024,
            release_date=now - timedelta(days=90),
            media_type=MediaType.MOVIE,
            overview="Paul Atreides unites with Chani and the Fremen while seeking revenge.",
            status=MediaStatus.DOWNLOADING,
        )
        # 2. Movie - Ready to Push (Released & Wanted/Searching)
        movie_ready = MediaItem(
            provider_id=provider.id,
            simkl_id=1002,
            title="Oppenheimer",
            year=2023,
            release_date=now - timedelta(days=300),
            media_type=MediaType.MOVIE,
            status=MediaStatus.SEARCHING,
        )
        # 3. Movie - Upcoming / Future (TBA: release_date is None)
        movie_tba = MediaItem(
            provider_id=provider.id,
            simkl_id=1003,
            title="Blade Runner 2099 Movie",
            year=now.year + 1,
            release_date=None,
            media_type=MediaType.MOVIE,
            status=MediaStatus.FUTURE,
        )
        # 4. Movie - Completed (History)
        movie_history = MediaItem(
            provider_id=provider.id,
            simkl_id=1004,
            tmdb_id=603,
            imdb_id="tt0133093",
            title="The Matrix",
            alt_title="Matrix Reloaded Origins",
            year=1999,
            release_date=now - timedelta(days=5000),
            media_type=MediaType.MOVIE,
            status=MediaStatus.COMPLETED,
            updated_at=now - timedelta(hours=2),
        )
        anime_movie_history = MediaItem(
            provider_id=provider.id,
            simkl_id=1005,
            anilist_id=21519,
            title="Your Name.",
            alt_title="Kimi no Na wa.",
            year=2016,
            release_date=now - timedelta(days=2500),
            media_type=MediaType.MOVIE,
            is_anime_movie=True,
            status=MediaStatus.COMPLETED,
            updated_at=now - timedelta(hours=1),
        )
        series_history = MediaItem(
            provider_id=provider.id,
            simkl_id=1006,
            tmdb_id=1396,
            imdb_id="tt0903747",
            title="Breaking Bad",
            year=2008,
            release_date=now - timedelta(days=4000),
            media_type=MediaType.SHOW,
            status=MediaStatus.COMPLETED,
            updated_at=now - timedelta(minutes=30),
        )

        # 5. Anime with chronological Seasons + Movies (In Progress: 1/2 Seasons, 1/2 Movies)
        anime_franchise = MediaItem(
            provider_id=provider.id,
            simkl_id=2001,
            anilist_id=101922,
            title="Demon Slayer",
            year=2019,
            release_date=now - timedelta(days=1000),
            media_type=MediaType.ANIME,
            status=MediaStatus.SEARCHING,
        )
        session.add_all(
            [
                movie_in_progress,
                movie_ready,
                movie_tba,
                movie_history,
                anime_movie_history,
                series_history,
                anime_franchise,
            ]
        )
        await session.flush()

        s1 = Season(
            media_item_id=anime_franchise.id,
            season_number=1,
            watch_order=1,
            type_number=1,
            entry_type="season",
            title="Unwavering Resolve Arc",
            monitored=True,
            status=SeasonStatus.COMPLETED,
            air_date=now - timedelta(days=900),
        )
        m1 = Season(
            media_item_id=anime_franchise.id,
            season_number=2,
            watch_order=2,
            type_number=1,
            entry_type="movie",
            title="Mugen Train",
            monitored=True,
            status=SeasonStatus.COMPLETED,
            air_date=now - timedelta(days=700),
        )
        s2 = Season(
            media_item_id=anime_franchise.id,
            season_number=3,
            watch_order=3,
            type_number=2,
            entry_type="season",
            title="Entertainment District Arc",
            monitored=True,
            status=SeasonStatus.SEARCHING,
            air_date=now - timedelta(days=500),
        )
        m2 = Season(
            media_item_id=anime_franchise.id,
            season_number=4,
            watch_order=4,
            type_number=2,
            entry_type="movie",
            title="Infinity Castle",
            monitored=True,
            status=SeasonStatus.FUTURE,
            air_date=None,
        )
        session.add_all([s1, m1, s2, m2])
        await session.flush()

        # 6. Series with active episode push and failed manual pick for Active Pushes table
        series_active = MediaItem(
            provider_id=provider.id,
            simkl_id=3001,
            title="Star Wars: The Clone Wars",
            year=2008,
            release_date=now - timedelta(days=2000),
            media_type=MediaType.SHOW,
            status=MediaStatus.SEARCHING,
        )
        session.add(series_active)
        await session.flush()

        cw_s1 = Season(
            media_item_id=series_active.id,
            season_number=1,
            watch_order=1,
            type_number=1,
            entry_type="season",
            monitored=True,
            status=SeasonStatus.SEARCHING,
            air_date=now - timedelta(days=1900),
        )
        session.add(cw_s1)
        await session.flush()

        cw_ep5 = Episode(
            season_id=cw_s1.id,
            episode_number=5,
            monitored=True,
            status=EpisodeStatus.DOWNLOADING,
            air_date=now - timedelta(days=1890),
        )
        cw_ep6_failed = Episode(
            season_id=cw_s1.id,
            episode_number=6,
            monitored=True,
            status=EpisodeStatus.FAILED,
            air_date=now - timedelta(days=1880),
        )
        session.add_all([cw_ep5, cw_ep6_failed])
        await session.flush()

        # Active downloading movie history
        dl_movie = DownloadHistory(
            media_item_id=movie_in_progress.id,
            nzb_title="Dune.Part.Two.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.HEVC-GROUP",
            nzb_guid="guid-dune-2",
            resolution="2160p",
            source="web-dl",
            grabbed_language="en",
            downloader_transfer_id="tb-dune-2",
            push_mode="auto",
            progress_pct=42.5,
            download_speed_bytes=25 * 1024 * 1024,
            eta_seconds=310,
            status_detail="downloading",
            is_dismissed=False,
        )
        # Active downloading episode history
        dl_ep5 = DownloadHistory(
            media_item_id=series_active.id,
            season_id=cw_s1.id,
            episode_id=cw_ep5.id,
            nzb_title="Star.Wars.The.Clone.Wars.S01E05.1080p.BluRay.x264-GROUP",
            nzb_guid="guid-cw-s01e05",
            resolution="1080p",
            source="bluray",
            grabbed_language="en",
            downloader_transfer_id="tb-cw-e05",
            push_mode="auto",
            progress_pct=78.0,
            download_speed_bytes=12 * 1024 * 1024,
            eta_seconds=45,
            status_detail="unpacking",
            is_dismissed=False,
        )
        # Failed manual pick episode history
        dl_ep6_failed = DownloadHistory(
            media_item_id=series_active.id,
            season_id=cw_s1.id,
            episode_id=cw_ep6_failed.id,
            nzb_title="Star.Wars.The.Clone.Wars.S01E06.1080p.WEBRip-BAD",
            nzb_guid="guid-cw-s01e06-bad",
            resolution="1080p",
            source="webrip",
            grabbed_language="de",
            downloader_transfer_id=None,
            push_mode="manual",
            progress_pct=0.0,
            download_speed_bytes=0,
            eta_seconds=None,
            status_detail="failed: repair failed",
            is_dismissed=False,
        )
        dl_matrix_done = DownloadHistory(
            media_item_id=movie_history.id,
            nzb_title="The.Matrix.1999.2160p.UHD.BluRay.x265-GROUP",
            nzb_guid="guid-matrix-done",
            resolution="2160p",
            source="bluray",
            grabbed_language="en",
            downloader_transfer_id="tb-matrix-done",
            dispatched_at=now - timedelta(hours=2),
            status_detail="completed",
            is_dismissed=False,
            notification_sent=True,
        )
        dl_your_name_done = DownloadHistory(
            media_item_id=anime_movie_history.id,
            nzb_title="Your.Name.2016.1080p.BluRay.x264-GROUP",
            nzb_guid="guid-your-name-done",
            resolution="1080p",
            source="bluray",
            grabbed_language="ja",
            downloader_transfer_id="tb-your-name-done",
            dispatched_at=now - timedelta(hours=1),
            status_detail="completed",
            is_dismissed=False,
            notification_sent=True,
        )
        dl_bb_done = DownloadHistory(
            media_item_id=series_history.id,
            nzb_title="Breaking.Bad.S01.1080p.BluRay.x264-GROUP",
            nzb_guid="guid-bb-done",
            resolution="1080p",
            source="bluray",
            grabbed_language="en",
            downloader_transfer_id="tb-bb-done",
            dispatched_at=now - timedelta(minutes=30),
            status_detail="completed",
            is_dismissed=False,
            notification_sent=True,
        )
        session.add_all(
            [
                dl_movie,
                dl_ep5,
                dl_ep6_failed,
                dl_matrix_done,
                dl_your_name_done,
                dl_bb_done,
            ]
        )
        await session.commit()

        yield session


@pytest.mark.asyncio
async def test_clean_top_navbar_removes_legacy_pills_and_dropdown(
    v3_ui_db,
    async_client: AsyncClient,
):
    """Verify the top navbar renders Sync Watchlist, Dashboard, and Settings without legacy schedule pills or state dropdown."""
    client = async_client
    if True:
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        assert "Sync Watchlist" in html
        assert 'hx-post="/sync"' in html
        assert 'href="/settings"' in html
        # Legacy schedule indicator & automation state dropdown must be removed from top navbar
        assert 'id="automation-nav-status"' not in html
        assert 'hx-post="/api/automation/state"' not in html
        assert "Upgrades Only" not in html


@pytest.mark.asyncio
async def test_six_card_interactive_top_deck_and_downward_animation(
    v3_ui_db,
    async_client: AsyncClient,
):
    """Verify the 6 interactive top cards exist and apply translate-y-2 + glow animation on activeCard."""
    client = async_client
    if True:
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        # All 6 cards present in top deck
        for card_key, card_label in [
            ("movie", "Movies"),
            ("show", "Series"),
            ("anime", "Anime"),
            ("history", "History"),
            ("active_pushes", "Active Pushes"),
            ("manual_search", "Manual Search"),
        ]:
            assert card_label in html
            assert f"'{card_key}'" in html

        # Active card downward shift animation class present
        assert "translate-y-2" in html
        assert "activeCard" in html


@pytest.mark.asyncio
async def test_three_tier_status_sections_and_five_column_layout_with_dual_anime_pills(
    v3_ui_db,
    async_client: AsyncClient,
):
    """Verify 3-Tier horizontal partitioning (In Progress, Ready to Push, Upcoming / Future),
    5-column table headers, TBA badge, and Dual Progress Pills on Anime rows.
    """
    client = async_client
    if True:
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        # 3-Tier section headers
        assert "In Progress" in html
        assert "Ready to Push" in html
        assert "Upcoming / Future" in html

        # 5-Column headers in active category tables
        assert "Title" in html
        assert "Profile" in html
        assert "Release Date" in html
        assert "Status & Progress" in html or "Status &amp; Progress" in html
        assert "Actions" in html
        assert "Last Checked" not in html
        assert "Awaiting Upgrades" not in html
        assert "Not Yet Downloaded" not in html

        # Verify items placed in expected tier tbody sections
        assert 'data-tier="in_progress"' in html
        assert 'data-tier="ready_to_push"' in html
        assert 'data-tier="upcoming"' in html

        # TBA movie renders TBA badge and still has Search / Push enabled
        assert "Blade Runner 2099 Movie" in html
        assert "TBA" in html
        assert "Search / Push" in html

        # Anime row renders Dual Progress Pills: 1/2 Seasons and 1/2 Movies
        assert "Demon Slayer" in html
        assert "1/2 Seasons" in html
        assert "1/2 Movies" in html

        # Mobile compact card layout assertions (Ticket 01)
        assert "block md:table-row p-3 md:p-0 hover:bg-[#2a2a32]" in html
        assert (
            "hidden md:table-cell md:px-6 md:py-4 md:w-px md:whitespace-nowrap" in html
        )
        assert "pt-2.5 md:pt-4 border-t border-[#3f3f46]/40 md:border-t-0" in html


@pytest.mark.asyncio
async def test_single_deck_navigation_unified_six_column_history_and_fluid_columns(
    v3_ui_db,
    async_client: AsyncClient,
):
    """Verify single-deck navigation, unified 6-column History table (Title | Type | Profile | Archived At | Status | Actions)
    with color-coded Type badges (MOVIE/SERIES/ANIME), rich Title cell parity, and fluid Title column widths.
    """
    client = async_client
    if True:
        # Check on main dashboard (Card 4)
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        # 1. Single-Deck Navigation: no secondary category pill switcher or History sub-tab buttons
        assert "🎬 Movies" not in html
        assert "📺 Series" not in html
        assert "😊 Anime" not in html
        assert "historyTab" not in html
        assert "Active Media Watchlist" not in html
        assert "Archive History" not in html
        assert "Filter watchlist..." in html
        assert "Search push history ledger..." in html

        # 2. Fluid Title column widths and no restrictive max-w-[280px] truncation cap
        assert "max-w-[280px]" not in html
        assert "w-full min-w-[260px]" in html
        assert "w-px whitespace-nowrap" in html

        # 3. Unified 5-column Push History Ledger table with sortable columns and default Pushed At desc sort
        assert "historySortCol: 'pushed-at'" in html
        assert "historySortDir: 'desc'" in html
        assert "sortHistoryBy('title')" in html
        assert "sortHistoryBy('release')" in html
        assert "sortHistoryBy('pushed-at')" in html
        assert "sortHistoryBy('status')" in html

        # 4. Color-coded Type badges (MOVIE blue, SERIES purple, ANIME pink including anime-movie)
        assert "bg-blue-500/10 text-blue-400 border border-blue-500/20" in html
        assert "bg-purple-500/10 text-purple-400 border border-purple-500/20" in html
        assert "bg-pink-500/10 text-pink-400 border border-pink-500/20" in html

        # 5. Rich History Title cell parity: clickable title -> push-modal, alt_title, external ID badges
        assert "The Matrix" in html
        assert "Matrix Reloaded Origins" in html
        assert "Your Name." in html
        assert "Kimi no Na wa." in html
        assert "Breaking Bad" in html
        assert "https://simkl.com/movies/1004" in html
        assert "https://themoviedb.org/movie/603" in html
        assert "https://imdb.com/title/tt0133093" in html
        assert "https://anilist.co/anime/21519" in html
        assert "Pushed At" in html
        assert "Downloader Status" in html
        assert "READY ON TORBOX" in html
        assert "Delete from TorBox" in html
        assert "Re-Push" in html

        # 6. Mobile overflow-free Card 4 layout assertions (Ticket 02)
        assert "block md:table-row p-3 md:p-0 transition-colors" in html
        assert (
            "block md:table-cell md:px-4 md:py-3.5 w-full md:w-auto md:min-w-[260px] break-words"
            in html
        )
        assert "truncate max-w-full" in html
        assert "hidden md:table-cell md:px-4 md:py-3.5 md:min-w-[220px]" in html
        assert (
            "hidden md:table-cell md:px-4 md:py-3.5 text-gray-400 text-xs font-mono md:w-px md:whitespace-nowrap"
            in html
        )
        assert (
            "hidden md:table-cell md:px-4 md:py-3.5 md:w-px md:whitespace-nowrap"
            in html
        )
        assert (
            "border-t border-[#3f3f46]/40 mt-3 pt-2.5 md:mt-0 md:pt-3.5 md:border-t-0 md:px-4 md:py-3.5"
            in html
        )


@pytest.mark.asyncio
async def test_active_pushes_card_six_column_live_table_and_actions(
    v3_ui_db,
    async_client: AsyncClient,
):
    """Verify Active Pushes (Card 5 & /api/pushes/active partial) renders the 6-column flat live transfer table
    with progress bars, speed/ETA, Cancel & Delete for active transfers, and Re-Search & Pick / Dismiss for failed manual picks.
    """
    client = async_client
    if True:
        # 1. Main dashboard contains Active Pushes view
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        assert "Title & Target" in html or "Title &amp; Target" in html
        assert "Release Name" in html
        assert "Progress" in html
        assert "Speed & ETA" in html or "Speed &amp; ETA" in html
        assert "Star Wars: The Clone Wars — S01E05" in html
        assert "42.5%" in html
        assert "78.0%" in html
        assert "Cancel & Delete" in html or "Cancel &amp; Delete" in html
        assert "Re-Search & Pick" in html or "Re-Search &amp; Pick" in html
        assert "Dismiss" in html
        # Completed movie ("The Matrix") must NOT appear in Active Pushes
        assert "tb-matrix" not in html

        # 2. HTMX partial endpoint GET /api/pushes/active with HX-Request: true returns HTML table partial
        hx_resp = await client.get("/api/pushes/active", headers={"HX-Request": "true"})
        assert hx_resp.status_code == 200
        hx_html = hx_resp.text
        assert "Star Wars: The Clone Wars — S01E05" in hx_html
        assert "Dune.Part.Two.2024.2160p" in hx_html
        assert "Failed (Manual Pick)" in hx_html
        assert "Re-Search & Pick" in hx_html or "Re-Search &amp; Pick" in hx_html
        assert "Dismiss" in hx_html


@pytest.mark.asyncio
async def test_preset_backed_full_filter_adhoc_manual_search_and_saved_defaults(
    v3_ui_db,
    async_client: AsyncClient,
    monkeypatch,
):
    """Verify Card 6 (Manual Search) renders the Search Preset quick-fill selector, External ID inputs,
    Two-Tier Language dropdowns, Video/Audio Requirements, Results Limit, and Save as Default checkbox,
    and executes ID-only searches and persists saved defaults across dashboard loads.
    """
    from app.db.models import SearchPreset
    from app.services.indexers import newznab as newznab_client

    # Add a custom SearchPreset to verify preset quick-fill payload on GET /
    custom_preset = SearchPreset(
        name="4K German Remux",
        is_default=False,
        primary_language="de",
        fallback_language="en",
        video_quality_mode="custom",
        audio_quality_mode="custom",
        custom_config_json='{"resolutions": ["2160p"], "sources": ["remux"], "video_codecs": ["hevc"], "hdr_formats": ["dv"], "audio_formats": ["lossless"], "audio_channels": ["7.1"]}',
    )
    v3_ui_db.add(custom_preset)
    await v3_ui_db.commit()

    captured_calls: list[dict[str, object]] = []

    async def fake_search_movie(
        imdb_id=None,
        tmdb_id=None,
        title=None,
        category=None,
        api_url=None,
        api_key=None,
        session=None,
    ):
        captured_calls.append(
            {
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "title": title,
                "category": category,
            }
        )
        return [
            {
                "title": "The.Matrix.1999.2160p.UHD.BluRay.REMUX.DV.HEVC.TrueHD.7.1.German-GROUP",
                "guid": "guid-matrix-4k",
                "link": "https://example.com/matrix.nzb",
                "size": 55 * 1024 * 1024 * 1024,
                "pubDate": "2026-09-01T12:00:00Z",
            }
        ]

    monkeypatch.setattr(newznab_client, "search_movie", fake_search_movie)

    client = async_client
    if True:
        # 1. Verify GET / renders Preset Quick-Fill dropdown and full filter controls in Card 6
        resp = await client.get("/")
        assert resp.status_code == 200
        html = resp.text

        assert "search-presets-payload" in html
        assert "4K German Remux" in html
        assert "applySearchPreset" in html
        assert 'name="imdb_id"' in html
        assert 'name="tmdb_id"' in html
        assert 'name="tvdb_id"' in html
        assert 'name="primary_language"' in html
        assert 'name="fallback_language"' in html
        assert 'name="resolution"' in html
        assert 'name="source"' in html
        assert 'name="video_codec"' in html
        assert 'name="hdr"' in html
        assert 'name="audio_tier"' in html
        assert 'name="audio_channels"' in html
        assert 'name="limit"' in html
        assert 'name="save_defaults"' in html
        assert "Save as Default" in html

        # 2. Execute an ID-only search (query="", imdb_id="tt0133093") with save_defaults=True
        # Tier 1 persists the sticky defaults (empty input only yields the error).
        save_resp = await client.post(
            "/api/search/manual",
            data={
                "query": "",
                "category": "movie",
                "primary_language": "de",
                "fallback_language": "en",
                "resolution": "2160p",
                "source": "remux",
                "video_codec": "hevc",
                "hdr": "dv",
                "audio_tier": "lossless",
                "audio_channels": "7.1",
                "limit": "25",
                "save_defaults": "true",
            },
        )
        assert "Please enter a title or ID." in save_resp.text
        search_resp = await client.post(
            "/api/search/manual/releases",
            data={
                "scope": "item",
                "kind": "movie",
                "title": "The Matrix",
                "drawer_id": "drawer-tmdb-603",
                "imdb_id": "tt0133093",
                "tmdb_id": "603",
                "primary_language": "de",
                "fallback_language": "en",
                "resolution": "2160p",
                "source": "remux",
                "video_codec": "hevc",
                "hdr": "dv",
                "audio_tier": "lossless",
                "audio_channels": "7.1",
                "limit": "25",
            },
        )
        assert search_resp.status_code == 200
        assert (
            "The.Matrix.1999.2160p.UHD.BluRay.REMUX.DV.HEVC.TrueHD.7.1.German-GROUP"
            in search_resp.text
        )
        assert len(captured_calls) == 1
        assert captured_calls[0]["imdb_id"] == "tt0133093"
        assert captured_calls[0]["tmdb_id"] == 603

        # 3. Subsequent GET / pre-populates saved defaults from SystemSettings.scoring_settings["manual_search_defaults"]
        resp2 = await client.get("/")
        assert resp2.status_code == 200
        html2 = resp2.text
        assert 'manualPrimaryLanguage: "de"' in html2
        assert 'manualFallbackLanguage: "en"' in html2
        assert 'manualResolution: "2160p"' in html2
        assert 'manualSource: "remux"' in html2
        assert 'manualVideoCodec: "hevc"' in html2
        assert 'manualHdr: "dv"' in html2
        assert 'manualAudioTier: "lossless"' in html2
        assert 'manualAudioChannels: "7.1"' in html2
        assert "manualLimit: 25" in html2 or 'manualLimit: "25"' in html2


@pytest.mark.asyncio
async def test_active_pushes_in_flight_banner_and_gated_poller_state_header(
    db_session: AsyncSession,
    async_client: AsyncClient,
):
    """Verify:
    1. Card 5 (`#active-pushes-container`) uses `hx-trigger="load, refreshActivePushes"`.
    2. When `transfer_poller.register_in_flight_push(...)` is active and there are no active downloads yet,
       `GET /api/pushes/active` (with `HX-Request: true`) renders the pulsing cyan status banner
       and the tailored message (`Searching indexers for best releases — episodes will appear here momentarily...`),
       and emits `HX-Trigger: {"smewlet:poller-state": {"pollerAwake": true, ...}}`.
    """
    import json

    from app.core.transfer_poller import transfer_poller

    resp = await async_client.get("/")
    assert resp.status_code == 200
    assert 'hx-trigger="load, refreshActivePushes"' in resp.text

    transfer_poller.register_in_flight_push(777, "Attack on Titan")
    try:
        hx_resp = await async_client.get(
            "/api/pushes/active", headers={"HX-Request": "true"}
        )
        assert hx_resp.status_code == 200
        hx_html = hx_resp.text
        assert "Searching indexers &amp; dispatching to downloader:" in hx_html or (
            "Searching indexers & dispatching to downloader:" in hx_html
        )
        assert "Attack on Titan" in hx_html
        assert (
            "Searching indexers for best releases — episodes will appear here momentarily..."
            in hx_html
        )
        hx_trigger_raw = hx_resp.headers.get("HX-Trigger")
        assert hx_trigger_raw is not None
        hx_trigger = json.loads(hx_trigger_raw)
        assert "smewlet:poller-state" in hx_trigger
        poller_state = hx_trigger["smewlet:poller-state"]
        assert poller_state["pollerAwake"] is True
        assert poller_state["inFlightCount"] == 1
    finally:
        transfer_poller.unregister_in_flight_push(777)


@pytest.mark.asyncio
async def test_push_modal_mobile_responsive_layout(
    db_session: AsyncSession,
    async_client: AsyncClient,
    v3_ui_db,
):
    """Verify push modal renders responsive mobile-first classes, compact header, and 2-row footer layout."""
    from app.db.models import MediaItem

    res = await db_session.execute(
        select(MediaItem).where(MediaItem.title == "Dune: Part Two")
    )
    movie = res.scalar_one()

    resp = await async_client.get(
        f"/api/items/{movie.id}/push-modal",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    html = resp.text

    # Backdrop & Dialog Container
    assert "nzb-modal-backdrop" in html
    assert "p-0 sm:p-4" in html
    assert "h-[100dvh] sm:h-auto sm:max-h-[90vh]" in html
    assert "rounded-none sm:rounded-xl" in html
    assert "width: 100vw; height: 100vh;" not in html
    assert "max-height: 90vh;" not in html

    # Compact Header Zone
    assert "w-12 h-16 sm:w-20 sm:h-28" in html
    assert "hidden sm:block" in html
    assert "text-base sm:text-xl font-bold text-white truncate" in html

    # Footer Action Bar
    assert "pb-[max(0.625rem,env(safe-area-inset-bottom))]" in html
    assert (
        "grid grid-cols-2 sm:flex sm:items-center gap-2 sm:gap-3 w-full sm:w-auto"
        in html
    )
    assert "Search &amp; Pick Manually" in html or "Search & Pick Manually" in html
    assert "Auto-Push Best" in html

    # Dry-run check: verify Simulate Push retains responsive classes
    from app.config import settings

    orig_dry_run = settings.dry_run
    try:
        settings.dry_run = True
        dry_resp = await async_client.get(
            f"/api/items/{movie.id}/push-modal",
            headers={"HX-Request": "true"},
        )
        assert dry_resp.status_code == 200
        dry_html = dry_resp.text
        assert "Simulate Push (Dry Run)" in dry_html
        assert "bg-amber-600 hover:bg-amber-500" in dry_html
        assert "w-full sm:w-auto" in dry_html
    finally:
        settings.dry_run = orig_dry_run
