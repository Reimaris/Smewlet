from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.automation import (
    enrich_anime_metadata,
    reset_anime_metadata,
    sync_all_providers,
)
from app.db.models import (
    MediaItem,
    MediaStatus,
    MediaType,
    Provider,
    Season,
    SeasonStatus,
)
from app.services import anilist


@pytest.mark.anyio
async def test_anilist_hierarchy_chronological_tv_and_movies_with_ova_bridge() -> None:
    """Verify get_anime_root_and_hierarchy retains TV/ONA seasons and canonical MOVIE nodes
    in chronological order while bridging across OVA/SPECIAL nodes, populating entry_type,
    watch_order, and type_number.
    """
    # Demon Slayer franchise graph:
    # 101922: Season 1 (TV, 2019-04-06) -> SEQUEL 112151 (Movie 1: Mugen Train, 2020-10-16)
    # 112151: Movie 1 (MOVIE, 2020-10-16) -> SEQUEL 999999 (Special recap OVA, 2021-10-10)
    # 999999: Special OVA (OVA, 2021-10-10) -> SEQUEL 142329 (Season 2: Entertainment District, 2021-12-05)
    # 142329: Season 2 (TV, 2021-12-05) -> SEQUEL 166240 (Movie 2: Infinity Castle, TBA / no startDate)
    mock_nodes: dict[int, dict[str, Any]] = {
        101922: {
            "id": 101922,
            "type": "ANIME",
            "format": "TV",
            "episodes": 26,
            "title": {
                "romaji": "Kimetsu no Yaiba",
                "english": "Demon Slayer: Kimetsu no Yaiba",
                "native": "鬼滅の刃",
            },
            "status": "FINISHED",
            "startDate": {"year": 2019, "month": 4, "day": 6},
            "idMal": 38000,
            "relations": {
                "edges": [
                    {
                        "relationType": "SEQUEL",
                        "node": {
                            "id": 112151,
                            "type": "ANIME",
                            "format": "MOVIE",
                            "episodes": 1,
                            "title": {
                                "romaji": "Kimetsu no Yaiba: Mugen Ressha-hen",
                                "english": "Demon Slayer -Kimetsu no Yaiba- The Movie: Mugen Train",
                                "native": "劇場版「鬼滅の刃」無限列車編",
                            },
                            "status": "FINISHED",
                            "startDate": {"year": 2020, "month": 10, "day": 16},
                            "idMal": 40456,
                        },
                    }
                ]
            },
        },
        112151: {
            "id": 112151,
            "type": "ANIME",
            "format": "MOVIE",
            "episodes": 1,
            "title": {
                "romaji": "Kimetsu no Yaiba: Mugen Ressha-hen",
                "english": "Demon Slayer -Kimetsu no Yaiba- The Movie: Mugen Train",
                "native": "劇場版「鬼滅の刃」無限列車編",
            },
            "status": "FINISHED",
            "startDate": {"year": 2020, "month": 10, "day": 16},
            "idMal": 40456,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {
                            "id": 101922,
                            "type": "ANIME",
                            "format": "TV",
                        },
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {
                            "id": 999999,
                            "type": "ANIME",
                            "format": "OVA",
                            "episodes": 1,
                            "title": {"english": "Demon Slayer Special OVA"},
                            "status": "FINISHED",
                            "startDate": {"year": 2021, "month": 10, "day": 10},
                        },
                    },
                ]
            },
        },
        999999: {
            "id": 999999,
            "type": "ANIME",
            "format": "OVA",
            "episodes": 1,
            "title": {"english": "Demon Slayer Special OVA"},
            "status": "FINISHED",
            "startDate": {"year": 2021, "month": 10, "day": 10},
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 112151, "type": "ANIME", "format": "MOVIE"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {
                            "id": 142329,
                            "type": "ANIME",
                            "format": "TV",
                            "episodes": 11,
                            "title": {
                                "english": "Demon Slayer: Entertainment District Arc",
                                "romaji": "Kimetsu no Yaiba: Yuukaku-hen",
                            },
                            "status": "FINISHED",
                            "startDate": {"year": 2021, "month": 12, "day": 5},
                            "idMal": 47778,
                        },
                    },
                ]
            },
        },
        142329: {
            "id": 142329,
            "type": "ANIME",
            "format": "TV",
            "episodes": 11,
            "title": {
                "english": "Demon Slayer: Entertainment District Arc",
                "romaji": "Kimetsu no Yaiba: Yuukaku-hen",
            },
            "status": "FINISHED",
            "startDate": {"year": 2021, "month": 12, "day": 5},
            "idMal": 47778,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 999999, "type": "ANIME", "format": "OVA"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {
                            "id": 166240,
                            "type": "ANIME",
                            "format": "MOVIE",
                            "episodes": 1,
                            "title": {
                                "english": "Demon Slayer: Infinity Castle",
                                "romaji": "Kimetsu no Yaiba: Mugenjou-hen",
                            },
                            "status": "NOT_YET_RELEASED",
                            "startDate": {"year": None, "month": None, "day": None},
                        },
                    },
                ]
            },
        },
        166240: {
            "id": 166240,
            "type": "ANIME",
            "format": "MOVIE",
            "episodes": 1,
            "title": {
                "english": "Demon Slayer: Infinity Castle",
                "romaji": "Kimetsu no Yaiba: Mugenjou-hen",
            },
            "status": "NOT_YET_RELEASED",
            "startDate": {"year": None, "month": None, "day": None},
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 142329, "type": "ANIME", "format": "TV"},
                    }
                ]
            },
        },
    }

    class FakeResp:
        status_code = 200
        headers: dict[str, str] = {}

        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, Any]:
            return self._payload

    async def fake_post_with_retry(
        _client: Any, json_data: dict[str, Any], max_retries: int = 3
    ) -> FakeResp:
        mid = json_data["variables"]["id"]
        return FakeResp({"data": {"Media": mock_nodes.get(mid)}})

    with patch(
        "app.services.anilist._post_with_retry", side_effect=fake_post_with_retry
    ):
        # Start from the middle (Season 2 ID 142329) to verify upward prequel traversal through OVA & Movie
        result = await anilist.get_anime_root_and_hierarchy(142329)

    assert result is not None
    assert result["root"]["id"] == 101922
    hierarchy = result["hierarchy"]

    # Should contain 4 canonical entries (Season 1, Movie 1, Season 2, Movie 2), excluding the OVA
    assert [n["id"] for n in hierarchy] == [101922, 112151, 142329, 166240]
    assert [n["entry_type"] for n in hierarchy] == [
        "season",
        "movie",
        "season",
        "movie",
    ]
    assert [n["watch_order"] for n in hierarchy] == [1, 2, 3, 4]
    assert [n["type_number"] for n in hierarchy] == [1, 1, 2, 2]


@pytest.mark.anyio
async def test_enrich_and_reset_anime_metadata_populates_entries_and_dual_progress_pills(
    db_session: AsyncSession,
) -> None:
    """Verify enrich_anime_metadata and reset_anime_metadata persist entry_type, watch_order,
    type_number, TBA-to-FUTURE status on unannounced entries, and accurate dual progress pill counts.
    """
    provider = Provider(name="Simkl", type="simkl", category="watchlist")
    db_session.add(provider)
    await db_session.flush()

    item = MediaItem(
        provider_id=provider.id,
        simkl_id=5001,
        anilist_id=101922,
        title="Demon Slayer: Kimetsu no Yaiba",
        year=2019,
        media_type=MediaType.ANIME,
        status=MediaStatus.PENDING,
    )
    db_session.add(item)
    await db_session.flush()

    fake_hierarchy = {
        "root": {
            "id": 101922,
            "format": "TV",
            "episodes": 26,
            "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
            "startDate": {"year": 2019, "month": 4, "day": 6},
            "idMal": 38000,
        },
        "hierarchy": [
            {
                "id": 101922,
                "format": "TV",
                "episodes": 26,
                "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
                "startDate": {"year": 2019, "month": 4, "day": 6},
                "entry_type": "season",
                "watch_order": 1,
                "type_number": 1,
            },
            {
                "id": 112151,
                "format": "MOVIE",
                "episodes": 1,
                "title": {"english": "Demon Slayer: Mugen Train"},
                "startDate": {"year": 2020, "month": 10, "day": 16},
                "entry_type": "movie",
                "watch_order": 2,
                "type_number": 1,
            },
            {
                "id": 142329,
                "format": "TV",
                "episodes": 11,
                "title": {"english": "Demon Slayer: Entertainment District Arc"},
                "startDate": {"year": 2021, "month": 12, "day": 5},
                "entry_type": "season",
                "watch_order": 3,
                "type_number": 2,
            },
            {
                "id": 166240,
                "format": "MOVIE",
                "episodes": 1,
                "title": {"english": "Demon Slayer: Infinity Castle"},
                "startDate": {"year": None, "month": None, "day": None},
                "entry_type": "movie",
                "watch_order": 4,
                "type_number": 2,
            },
        ],
    }

    with patch(
        "app.services.anilist.get_anime_root_and_hierarchy",
        new=AsyncMock(return_value=fake_hierarchy),
    ):
        await enrich_anime_metadata(db_session, item)

    stmt = (
        select(MediaItem)
        .where(MediaItem.id == item.id)
        .options(selectinload(MediaItem.seasons))
    )
    loaded = (await db_session.execute(stmt)).scalars().first()
    assert loaded is not None
    assert loaded.status == MediaStatus.SEARCHING

    entries = sorted(loaded.seasons, key=lambda s: s.watch_order)
    assert len(entries) == 4

    assert [(e.watch_order, e.entry_type, e.type_number) for e in entries] == [
        (1, "season", 1),
        (2, "movie", 1),
        (3, "season", 2),
        (4, "movie", 2),
    ]
    # Entry 4 has no startDate -> TBA -> must be FUTURE
    assert entries[0].status == SeasonStatus.SEARCHING
    assert entries[1].status == SeasonStatus.SEARCHING
    assert entries[2].status == SeasonStatus.SEARCHING
    assert entries[3].status == SeasonStatus.FUTURE
    assert entries[3].is_tba is True

    # Verify Dual Progress Counts & Pills (initially 0/2 Seasons, 0/2 Movies)
    assert loaded.total_seasons == 2
    assert loaded.downloaded_seasons == 0
    assert loaded.total_movies == 2
    assert loaded.downloaded_movies == 0
    assert loaded.progress_pills == ["0/2 Seasons", "0/2 Movies"]

    # Mark Season 1 and Movie 1 as COMPLETED
    entries[0].status = SeasonStatus.COMPLETED
    entries[1].status = SeasonStatus.DOWNLOADED
    await db_session.commit()
    await db_session.refresh(loaded, ["seasons"])

    assert loaded.downloaded_seasons == 1
    assert loaded.total_seasons == 2
    assert loaded.downloaded_movies == 1
    assert loaded.total_movies == 2
    assert loaded.progress_pills == ["1/2 Seasons", "1/2 Movies"]

    # Verify reset_anime_metadata preserves downloaded state and entry_type/watch_order/type_number
    loaded_id = loaded.id
    with patch(
        "app.services.anilist.get_anime_root_and_hierarchy",
        new=AsyncMock(return_value=fake_hierarchy),
    ):
        reset_item = await reset_anime_metadata(db_session, loaded_id)

    assert reset_item is not None
    stmt_after = (
        select(MediaItem)
        .where(MediaItem.id == loaded_id)
        .options(selectinload(MediaItem.seasons))
    )
    after_reset = (await db_session.execute(stmt_after)).scalars().first()
    assert after_reset is not None
    entries_after = sorted(after_reset.seasons, key=lambda s: s.watch_order)
    assert [(e.watch_order, e.entry_type, e.type_number) for e in entries_after] == [
        (1, "season", 1),
        (2, "movie", 1),
        (3, "season", 2),
        (4, "movie", 2),
    ]
    assert entries_after[3].status == SeasonStatus.FUTURE


@pytest.mark.anyio
async def test_tba_to_future_classification_rule_on_sync(
    db_session: AsyncSession,
) -> None:
    """Verify watchlist items and seasons with release_date is None or year > current_year
    are classified as FUTURE ('TBA') rather than WANTED / SEARCHING.
    """
    from tests.conftest import ingest_simkl_payloads

    provider = Provider(name="Simkl", type="simkl", category="watchlist")
    db_session.add(provider)
    await db_session.flush()

    next_year = datetime.now(timezone.utc).year + 2
    simkl_movies = [
        {
            "movie": {
                "title": "Released Movie With Date",
                "year": 2024,
                "ids": {"simkl": 1001, "tmdb": "2001"},
            }
        },
        {
            "movie": {
                "title": "Unannounced TBA Movie Without Release Date",
                "year": 2025,
                "ids": {"simkl": 1002, "tmdb": "2002"},
            }
        },
        {
            "movie": {
                "title": "Future Year Movie",
                "year": next_year,
                "ids": {"simkl": 1003, "tmdb": "2003"},
            }
        },
    ]

    simkl_shows = [
        {
            "show": {
                "title": "Show With TBA Season 2",
                "year": 2023,
                "ids": {"simkl": 3001, "tmdb": "4001"},
            }
        },
        {
            "show": {
                "title": "Completely Unannounced TBA Show",
                "year": None,
                "ids": {"simkl": 3002, "tmdb": "4002"},
            }
        },
    ]

    async def fake_movie_details(tmdb_id: int) -> dict[str, Any]:
        if tmdb_id == 2001:
            return {"title": "Released Movie With Date", "release_date": "2024-03-01"}
        if tmdb_id == 2002:
            # Missing release_date -> TBA
            return {
                "title": "Unannounced TBA Movie Without Release Date",
                "release_date": None,
            }
        if tmdb_id == 2003:
            return {"title": "Future Year Movie", "release_date": None}
        return {}

    async def fake_digital_release(tmdb_id: int) -> datetime | None:
        if tmdb_id == 2001:
            return datetime(2024, 3, 1, tzinfo=timezone.utc)
        return None

    async def fake_show_details(tmdb_id: int) -> dict[str, Any]:
        if tmdb_id == 4001:
            return {
                "name": "Show With TBA Season 2",
                "first_air_date": "2023-05-10",
                "seasons": [
                    {"season_number": 1, "episode_count": 8, "air_date": "2023-05-10"},
                    {
                        "season_number": 2,
                        "episode_count": 8,
                        "air_date": None,
                    },  # TBA Season!
                ],
            }
        if tmdb_id == 4002:
            return {
                "name": "Completely Unannounced TBA Show",
                "first_air_date": None,
                "seasons": [
                    {"season_number": 1, "episode_count": 10, "air_date": None},
                ],
            }
        return {}

    with (
        patch("app.services.tmdb.get_movie_details", side_effect=fake_movie_details),
        patch(
            "app.services.tmdb.get_digital_release_date",
            side_effect=fake_digital_release,
        ),
        patch("app.services.tmdb.get_show_details", side_effect=fake_show_details),
        patch(
            "app.services.tmdb.get_season_details",
            new=AsyncMock(return_value={"episodes": []}),
        ),
    ):
        await ingest_simkl_payloads(db_session, provider, simkl_movies, MediaType.MOVIE)
        await ingest_simkl_payloads(db_session, provider, simkl_shows, MediaType.SHOW)
        await db_session.commit()

    items_res = await db_session.execute(
        select(MediaItem).options(selectinload(MediaItem.seasons))
    )
    by_tmdb = {i.tmdb_id: i for i in items_res.scalars().all()}

    # 1001: Has past release_date -> SEARCHING
    assert by_tmdb[2001].status == MediaStatus.SEARCHING
    assert by_tmdb[2001].is_tba is False

    # 1002: release_date is None -> TBA -> FUTURE
    assert by_tmdb[2002].status == MediaStatus.FUTURE
    assert by_tmdb[2002].is_tba is True

    # 1003: year > current_year -> FUTURE
    assert by_tmdb[2003].status == MediaStatus.FUTURE
    assert by_tmdb[2003].is_tba is True

    # 3001: Show has first_air_date in 2023 -> SEARCHING, Season 1 -> SEARCHING, Season 2 (air_date=None) -> FUTURE
    show_3001 = by_tmdb[4001]
    assert show_3001.status == MediaStatus.SEARCHING
    seasons_3001 = {s.season_number: s for s in show_3001.seasons}
    assert seasons_3001[1].status == SeasonStatus.SEARCHING
    assert seasons_3001[2].status == SeasonStatus.FUTURE
    assert seasons_3001[2].is_tba is True

    # 3002: Show has first_air_date=None -> FUTURE, Season 1 -> FUTURE
    show_3002 = by_tmdb[4002]
    assert show_3002.status == MediaStatus.FUTURE
    assert show_3002.seasons[0].status == SeasonStatus.FUTURE


@pytest.mark.anyio
async def test_selective_simkl_sync_and_15m_cooldown_and_periodic_interval(
    db_session: AsyncSession,
) -> None:
    """Verify Simkl sync respects sync_movies/sync_series/sync_anime toggles, updates last_synced_at,
    triggers non-blockingly on dashboard load only when >15 minutes have elapsed, and evaluates
    periodic interval eligibility.
    """
    from app.core.automation import is_watchlist_periodic_sync_due

    provider = Provider(
        name="Simkl Selective",
        type="simkl",
        category="watchlist",
        client_id="cid_123",
        access_token="tok_456",
        is_active=True,
        config_json=json.dumps(
            {
                "sync_interval_minutes": 60,
                "sync_movies": True,
                "sync_series": False,
                "sync_anime": False,
                "last_synced_at": None,
            }
        ),
    )
    db_session.add(provider)
    await db_session.commit()

    # Initially last_synced_at is None -> periodic sync is due
    assert is_watchlist_periodic_sync_due(provider) is True

    called_categories: list[str] = []

    async def fake_get_watchlist(
        media_type: str, client_id: str, access_token: str
    ) -> list[dict[str, Any]]:
        called_categories.append(media_type)
        return []

    with patch(
        "app.services.watchlists.simkl.get_watchlist", side_effect=fake_get_watchlist
    ):
        await sync_all_providers()

    # Only 'movies' should have been fetched because sync_series=False and sync_anime=False
    assert called_categories == ["movies"]

    await db_session.refresh(provider)
    last_synced_str = provider.watchlist_config.get("last_synced_at")
    assert last_synced_str is not None

    # Immediately after sync: the 60m periodic sync is not due
    now = datetime.now(timezone.utc)
    assert is_watchlist_periodic_sync_due(provider, now=now) is False

    # 16 minutes later: the 60m periodic interval has not elapsed
    after_16m = now + timedelta(minutes=16)
    assert is_watchlist_periodic_sync_due(provider, now=after_16m) is False

    # 61 minutes later: 60m periodic interval is due
    after_61m = now + timedelta(minutes=61)
    assert is_watchlist_periodic_sync_due(provider, now=after_61m) is True

    # If sync_interval_minutes == 0 (manual/on-load only), periodic sync is never due
    cfg = provider.watchlist_config
    cfg["sync_interval_minutes"] = 0
    provider.config_json = cfg
    assert is_watchlist_periodic_sync_due(provider, now=after_61m) is False


@pytest.mark.anyio
async def test_dashboard_page_load_does_not_sync_and_renders_navbar_button(
    db_session: AsyncSession,
    async_client: AsyncClient,
) -> None:
    """Verify GET / never starts a sync (only the orchestrator tick or the manual button do)
    and still renders the 'Sync Watchlist' navbar button.
    """
    twenty_mins_ago = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    provider = Provider(
        name="Simkl Main",
        type="simkl",
        category="watchlist",
        client_id="cid_123",
        access_token="tok_456",
        is_active=True,
        config_json=json.dumps(
            {
                "sync_interval_minutes": 60,
                "sync_movies": True,
                "sync_series": True,
                "sync_anime": True,
                "last_synced_at": twenty_mins_ago,
            }
        ),
    )
    db_session.add(provider)
    await db_session.commit()

    with patch("app.core.automation.sync_all_providers", new=AsyncMock()) as mock_sync:
        resp = await async_client.get("/")
        assert resp.status_code == 200
        assert "Sync Watchlist" in resp.text
        assert mock_sync.await_count == 0


@pytest.mark.anyio
async def test_simkl_anime_sync_attaches_existing_seasons_via_mal_id_and_skips_redundant_anilist_enrichment(
    db_session: AsyncSession,
) -> None:
    """Verify that once an anime franchise is enriched, adding sequels/movies/cours from Simkl
    (which only provide simkl_id + mal_id) attaches them to existing Seasons/MediaItem with zero
    redundant AniList hierarchy queries and preserves the franchise during Simkl pruning.
    """
    provider = Provider(
        name="Simkl Anime Sync",
        type="simkl",
        category="watchlist",
        client_id="cid_123",
        access_token="tok_456",
        is_active=True,
        config_json=json.dumps(
            {
                "sync_interval_minutes": 60,
                "sync_movies": False,
                "sync_series": False,
                "sync_anime": True,
            }
        ),
    )
    db_session.add(provider)
    await db_session.commit()

    fake_hierarchy = {
        "root": {
            "id": 101922,
            "idMal": 38000,
            "format": "TV",
            "episodes": 26,
            "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
            "startDate": {"year": 2019, "month": 4, "day": 6},
        },
        "hierarchy": [
            {
                "id": 101922,
                "idMal": 38000,
                "format": "TV",
                "episodes": 26,
                "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
                "startDate": {"year": 2019, "month": 4, "day": 6},
                "entry_type": "season",
                "watch_order": 1,
                "type_number": 1,
            },
            {
                "id": 112151,
                "idMal": 40456,
                "format": "MOVIE",
                "episodes": 1,
                "title": {"english": "Demon Slayer: Mugen Train"},
                "startDate": {"year": 2020, "month": 10, "day": 16},
                "entry_type": "movie",
                "watch_order": 2,
                "type_number": 1,
            },
            {
                "id": 142329,
                "idMal": 47778,
                "format": "TV",
                "episodes": 11,
                "title": {"english": "Demon Slayer: Entertainment District Arc"},
                "startDate": {"year": 2021, "month": 12, "day": 5},
                "entry_type": "season",
                "watch_order": 3,
                "type_number": 2,
            },
        ],
    }

    # Sync 1: User adds Season 1 on Simkl (Simkl payload has simkl + mal, no anilist)
    watchlist_sync_1 = [
        {
            "status": "watching",
            "anime": {
                "title": "Demon Slayer: Kimetsu no Yaiba",
                "year": 2019,
                "ids": {"simkl": 101, "mal": "38000"},
            },
        }
    ]

    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist_sync_1),
        ),
        patch(
            "app.services.anilist.get_anime_ids_by_mal_ids",
            new=AsyncMock(return_value={38000: 101922}),
        ) as mock_batch_mal,
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=fake_hierarchy),
        ) as mock_hierarchy,
        patch(
            "app.services.anilist.search_anime_id_by_title",
            new=AsyncMock(return_value=None),
        ) as mock_title_search,
    ):
        await sync_all_providers()
        assert mock_batch_mal.await_count == 1
        assert mock_hierarchy.await_count == 1
        assert mock_title_search.await_count == 0

    stmt = select(MediaItem).options(selectinload(MediaItem.seasons))
    items = (await db_session.execute(stmt)).scalars().all()
    assert len(items) == 1
    franchise = items[0]
    assert franchise.last_metadata_refreshed_at is not None
    seasons_by_order = {s.watch_order: s for s in franchise.seasons}
    assert seasons_by_order[1].mal_id == 38000
    assert seasons_by_order[2].mal_id == 40456
    assert seasons_by_order[3].mal_id == 47778

    # Sync 2: User adds Movie 1 (mal=40456) and Season 2 (mal=47778) on Simkl.
    # Because Season.mal_id already has 40456 and 47778, ZERO AniList calls should be made!
    watchlist_sync_2 = [
        {
            "status": "watching",
            "anime": {
                "title": "Demon Slayer: Kimetsu no Yaiba",
                "year": 2019,
                "ids": {"simkl": 101, "mal": "38000"},
            },
        },
        {
            "status": "plantowatch",
            "anime": {
                "title": "Demon Slayer: Mugen Train",
                "year": 2020,
                "ids": {"simkl": 102, "mal": "40456"},
            },
        },
        {
            "status": "plantowatch",
            "anime": {
                "title": "Demon Slayer: Entertainment District Arc",
                "year": 2021,
                "ids": {"simkl": 103, "mal": "47778"},
            },
        },
    ]

    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist_sync_2),
        ),
        patch(
            "app.services.anilist.get_anime_ids_by_mal_ids",
            new=AsyncMock(return_value={}),
        ) as mock_batch_mal_2,
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=fake_hierarchy),
        ) as mock_hierarchy_2,
        patch(
            "app.services.anilist.search_anime_id_by_title",
            new=AsyncMock(return_value=None),
        ) as mock_title_search_2,
    ):
        await sync_all_providers()
        assert mock_batch_mal_2.await_count == 0
        assert mock_hierarchy_2.await_count == 0
        assert mock_title_search_2.await_count == 0

    db_session.expire_all()
    items = (await db_session.execute(stmt)).scalars().all()
    assert len(items) == 1
    franchise = items[0]
    seasons_by_order = {s.watch_order: s for s in franchise.seasons}

    # Sync 3: User marks Season 1 (101) and Movie 1 (102) as completed on Simkl; only Season 2 (103) remains active.
    # Franchise must NOT be pruned, and still makes 0 AniList calls!
    watchlist_sync_3 = [
        {
            "status": "watching",
            "anime": {
                "title": "Demon Slayer: Entertainment District Arc",
                "year": 2021,
                "ids": {"simkl": 103, "mal": "47778"},
            },
        },
    ]
    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist_sync_3),
        ),
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=fake_hierarchy),
        ) as mock_hierarchy_3,
    ):
        await sync_all_providers()
        assert mock_hierarchy_3.await_count == 0

    db_session.expire_all()
    items = (await db_session.execute(stmt)).scalars().all()
    assert len(items) == 1


@pytest.mark.anyio
async def test_other_root_consolidation_reconciles_new_seasons_and_persists_simkl_ids(
    db_session: AsyncSession,
) -> None:
    """Verify that when a newly added Simkl sequel/cour resolves via AniList to an existing
    other_root MediaItem, enrich_anime_metadata reconciles any new seasons onto other_root and
    permanently associates the new simkl_id so subsequent syncs make 0 AniList calls.
    """
    provider = Provider(
        name="Simkl Root Merge",
        type="simkl",
        category="watchlist",
        client_id="cid_123",
        access_token="tok_456",
        is_active=True,
        config_json=json.dumps(
            {
                "sync_interval_minutes": 60,
                "sync_movies": False,
                "sync_series": False,
                "sync_anime": True,
            }
        ),
    )
    db_session.add(provider)
    await db_session.flush()

    # Existing root MediaItem originally only had Season 1 in DB
    root_item = MediaItem(
        provider_id=provider.id,
        simkl_id=201,
        mal_id=38000,
        anilist_id=101922,
        title="Demon Slayer: Kimetsu no Yaiba",
        year=2019,
        media_type=MediaType.ANIME,
        status=MediaStatus.SEARCHING,
        last_metadata_refreshed_at=datetime.now(timezone.utc),
    )
    db_session.add(root_item)
    await db_session.flush()
    s1 = Season(
        media_item_id=root_item.id,
        season_number=1,
        watch_order=1,
        type_number=1,
        entry_type="season",
        simkl_id=201,
        anilist_id=101922,
        mal_id=38000,
        title="Demon Slayer: Kimetsu no Yaiba",
        monitored=True,
        episode_count=26,
        status=SeasonStatus.SEARCHING,
    )
    db_session.add(s1)
    await db_session.commit()

    # Now user adds newly announced Season 2 (simkl_id=202, mal_id=47778, anilist_id=142329) to Simkl
    watchlist = [
        {
            "status": "watching",
            "anime": {
                "title": "Demon Slayer: Entertainment District Arc",
                "year": 2021,
                "ids": {"simkl": 202, "mal": "47778"},
            },
        }
    ]

    expanded_hierarchy = {
        "root": {
            "id": 101922,
            "idMal": 38000,
            "format": "TV",
            "episodes": 26,
            "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
            "startDate": {"year": 2019, "month": 4, "day": 6},
        },
        "hierarchy": [
            {
                "id": 101922,
                "idMal": 38000,
                "format": "TV",
                "episodes": 26,
                "title": {"english": "Demon Slayer: Kimetsu no Yaiba"},
                "startDate": {"year": 2019, "month": 4, "day": 6},
                "entry_type": "season",
                "watch_order": 1,
                "type_number": 1,
            },
            {
                "id": 142329,
                "idMal": 47778,
                "format": "TV",
                "episodes": 11,
                "title": {"english": "Demon Slayer: Entertainment District Arc"},
                "startDate": {"year": 2021, "month": 12, "day": 5},
                "entry_type": "season",
                "watch_order": 2,
                "type_number": 2,
            },
        ],
    }

    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist),
        ),
        patch(
            "app.services.anilist.get_anime_ids_by_mal_ids",
            new=AsyncMock(return_value={47778: 142329}),
        ),
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=expanded_hierarchy),
        ) as mock_hier,
    ):
        await sync_all_providers()
        assert mock_hier.await_count == 1

    db_session.expire_all()
    stmt = select(MediaItem).options(selectinload(MediaItem.seasons))
    items = (await db_session.execute(stmt)).scalars().all()
    assert len(items) == 1
    merged_root = items[0]
    assert merged_root.id == root_item.id
    assert len(merged_root.seasons) == 2
    s2 = next(s for s in merged_root.seasons if s.watch_order == 2)
    assert s2.mal_id == 47778
    assert s2.anilist_id == 142329

    # Next sync with the exact same watchlist must make ZERO AniList calls!
    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist),
        ),
        patch(
            "app.services.anilist.get_anime_ids_by_mal_ids",
            new=AsyncMock(return_value={}),
        ) as mock_batch_2,
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=expanded_hierarchy),
        ) as mock_hier_2,
    ):
        await sync_all_providers()
        assert mock_batch_2.await_count == 0
        assert mock_hier_2.await_count == 0


@pytest.mark.anyio
async def test_tba_format_null_fallback_and_24h_batched_future_season_refresh(
    db_session: AsyncSession,
) -> None:
    """Verify TBA anime with format=None fall back to a Season 1 entry, are not re-queried
    within 24h, and after 24h are refreshed via a single batched id_in query rather than
    full hierarchy traversal.
    """
    # 1. Verify get_anime_root_and_hierarchy fallback when format is None (TBA)
    tba_node = {
        "id": 199999,
        "idMal": 59999,
        "type": "ANIME",
        "format": None,
        "episodes": None,
        "title": {"romaji": "Mirai no Anime", "english": "Future Anime TBA"},
        "status": "NOT_YET_RELEASED",
        "startDate": {"year": None, "month": None, "day": None},
        "relations": {"edges": []},
    }

    class FakeResp:
        status_code = 200
        headers: dict[str, str] = {}

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, Any]:
            return {"data": {"Media": tba_node}}

    with patch(
        "app.services.anilist._post_with_retry",
        new=AsyncMock(return_value=FakeResp()),
    ):
        res = await anilist.get_anime_root_and_hierarchy(199999)

    assert res is not None
    assert len(res["hierarchy"]) == 1
    assert res["hierarchy"][0]["id"] == 199999
    assert res["hierarchy"][0]["entry_type"] == "season"
    assert res["hierarchy"][0]["watch_order"] == 1
    assert res["hierarchy"][0]["type_number"] == 1

    # 2. Ingest TBA anime via sync_all_providers
    provider = Provider(
        name="Simkl TBA Sync",
        type="simkl",
        category="watchlist",
        client_id="cid_123",
        access_token="tok_456",
        is_active=True,
        config_json=json.dumps(
            {
                "sync_interval_minutes": 60,
                "sync_movies": False,
                "sync_series": False,
                "sync_anime": True,
            }
        ),
    )
    db_session.add(provider)
    await db_session.commit()

    watchlist = [
        {
            "status": "plantowatch",
            "anime": {
                "title": "Future Anime TBA",
                "year": None,
                "ids": {"simkl": 301, "mal": "59999"},
            },
        }
    ]

    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist),
        ),
        patch(
            "app.services.anilist.get_anime_ids_by_mal_ids",
            new=AsyncMock(return_value={59999: 199999}),
        ),
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=res),
        ) as mock_hier,
        patch(
            "app.services.anilist.get_anime_details_batch_by_ids",
            new=AsyncMock(return_value={}),
        ) as mock_batch_refresh,
    ):
        await sync_all_providers()
        assert mock_hier.await_count == 1
        assert mock_batch_refresh.await_count == 0

    stmt = (
        select(MediaItem)
        .where(MediaItem.mal_id == 59999)
        .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
    )
    item = (await db_session.execute(stmt)).scalars().first()
    assert item is not None
    assert item.status == MediaStatus.FUTURE
    assert len(item.seasons) == 1
    assert item.seasons[0].status == SeasonStatus.FUTURE
    assert item.seasons[0].episode_count is None
    assert item.last_metadata_refreshed_at is not None

    # Immediate re-sync (< 24h): 0 hierarchy calls and 0 batch refresh calls
    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist),
        ),
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=res),
        ) as mock_hier_2,
        patch(
            "app.services.anilist.get_anime_details_batch_by_ids",
            new=AsyncMock(return_value={}),
        ) as mock_batch_refresh_2,
    ):
        await sync_all_providers()
        assert mock_hier_2.await_count == 0
        assert mock_batch_refresh_2.await_count == 0

    # Age last_metadata_refreshed_at beyond 24h -> batch refresh runs (still 0 hierarchy calls!)
    item.last_metadata_refreshed_at = datetime.now(timezone.utc) - timedelta(hours=25)
    await db_session.commit()

    batch_updated_payload = {
        199999: {
            "id": 199999,
            "idMal": 59999,
            "format": "TV",
            "episodes": 12,
            "status": "RELEASING",
            "startDate": {"year": 2025, "month": 1, "day": 15},
            "title": {"english": "Future Anime Released"},
        }
    }

    with (
        patch(
            "app.services.watchlists.simkl.get_watchlist",
            new=AsyncMock(return_value=watchlist),
        ),
        patch(
            "app.services.anilist.get_anime_root_and_hierarchy",
            new=AsyncMock(return_value=res),
        ) as mock_hier_3,
        patch(
            "app.services.anilist.get_anime_details_batch_by_ids",
            new=AsyncMock(return_value=batch_updated_payload),
        ) as mock_batch_refresh_3,
        patch(
            "app.core.episode_growth.refresh_episode_growth",
            new=AsyncMock(return_value=0),
        ),
    ):
        await sync_all_providers()
        assert mock_hier_3.await_count == 0
        assert mock_batch_refresh_3.await_count == 1

    db_session.expire_all()
    item_after = (await db_session.execute(stmt)).scalars().first()
    assert item_after is not None
    assert item_after.status == MediaStatus.SEARCHING
    assert item_after.seasons[0].status == SeasonStatus.SEARCHING
    assert item_after.seasons[0].episode_count == 12
    assert len(item_after.seasons[0].episodes) == 12
