"""Episode growth probe (m86, ADR-188)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core import episode_growth
from app.core.episode_growth import refresh_episode_growth
from app.core.phase_stats import EpisodeGrowthStats
from app.db.models import (
    EpisodeStatus,
    MediaItem,
    MediaType,
    Season,
    SeasonStatus,
)
from app.services import anilist, tmdb
from tests.conftest import make_history, make_series

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
SHOW_RETURNING = {
    "status": "Returning Series",
    "seasons": [{"season_number": 2, "episode_count": 6}],
}
SEASON_PAYLOAD = {
    "episodes": [
        {"episode_number": 4, "air_date": "2026-10-20"},
        {"episode_number": 5, "air_date": "2026-11-06"},
        {"episode_number": 6, "air_date": None},
    ]
}


class Calls:
    def __init__(self) -> None:
        self.show = 0
        self.season = 0
        self.batch = 0


@pytest.fixture
def tmdb_mock(monkeypatch):
    calls = Calls()
    state = {"show": SHOW_RETURNING, "season": SEASON_PAYLOAD}

    async def show(tmdb_id, api_key=None, session=None):
        calls.show += 1
        return state["show"]

    async def season(tmdb_id, season_number, api_key=None, session=None):
        calls.season += 1
        return state["season"]

    monkeypatch.setattr(tmdb, "get_show_details", show)
    monkeypatch.setattr(tmdb, "get_season_details", season)
    return calls, state


async def _marshals(session, **item_overrides):
    item, seasons, eps = await make_series(
        session,
        seasons=[
            {"season_number": 2, "episode_count": 3, "status": SeasonStatus.COMPLETED}
        ],
        episodes_per_season=3,
        episode_statuses=[EpisodeStatus.DOWNLOADED],
        title="Marshals",
        tmdb_id=290856,
        **item_overrides,
    )
    for ep in eps:
        await make_history(session, item=item, season=seasons[0], episode=ep)
    await session.commit()
    return item.id


async def _load(session, item_id):
    session.expire_all()
    return (
        await session.execute(
            select(MediaItem)
            .where(MediaItem.id == item_id)
            .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_season_grows_from_three_to_six(db_session, tmdb_mock):
    calls, _ = tmdb_mock
    item_id = await _marshals(db_session)
    stats = EpisodeGrowthStats()

    added = await refresh_episode_growth(db_session, NOW, stats=stats)

    item = await _load(db_session, item_id)
    season = item.seasons[0]
    by_num = {e.episode_number: e for e in season.episodes}
    assert added == 3 and stats.episodes_added == 3 and stats.seasons_grown == 1
    assert season.episode_count == 6
    assert [by_num[n].status for n in (1, 2, 3)] == [EpisodeStatus.DOWNLOADED] * 3
    assert by_num[4].status == EpisodeStatus.SEARCHING  # aired 2026-10-20
    assert by_num[5].status == EpisodeStatus.FUTURE
    assert by_num[6].status == EpisodeStatus.FUTURE  # no date yet
    assert season.status == SeasonStatus.SEARCHING
    assert (calls.show, calls.season) == (1, 1)
    assert item.last_episode_growth_check_at is not None


@pytest.mark.asyncio
async def test_unchanged_count_fetches_no_season_and_writes_no_rows(
    db_session, tmdb_mock
):
    calls, state = tmdb_mock
    state["show"] = {
        "status": "Returning Series",
        "seasons": [{"season_number": 2, "episode_count": 3}],
    }
    item_id = await _marshals(db_session)

    assert await refresh_episode_growth(db_session, NOW) == 0

    item = await _load(db_session, item_id)
    assert len(item.seasons[0].episodes) == 3
    assert calls.season == 0
    assert item.last_episode_growth_check_at is not None


@pytest.mark.asyncio
async def test_pacing_blocks_second_probe_within_24h(db_session, tmdb_mock):
    calls, state = tmdb_mock
    state["show"] = {
        "status": "Returning Series",
        "seasons": [{"season_number": 2, "episode_count": 3}],
    }
    await _marshals(db_session)
    await refresh_episode_growth(db_session, NOW)
    await refresh_episode_growth(db_session, NOW + timedelta(hours=23))
    assert calls.show == 1
    await refresh_episode_growth(db_session, NOW + timedelta(hours=25))
    assert calls.show == 2


@pytest.mark.asyncio
async def test_ended_show_without_growth_is_settled_for_90_days(db_session, tmdb_mock):
    calls, state = tmdb_mock
    state["show"] = {
        "status": "Ended",
        "seasons": [{"season_number": 2, "episode_count": 3}],
    }
    await _marshals(db_session)
    await refresh_episode_growth(db_session, NOW)
    await refresh_episode_growth(db_session, NOW + timedelta(days=60))
    assert calls.show == 1
    await refresh_episode_growth(db_session, NOW + timedelta(days=91))
    assert calls.show == 2


@pytest.mark.asyncio
async def test_only_latest_non_special_season_is_probed(db_session, tmdb_mock):
    calls, state = tmdb_mock
    state["show"] = {
        "status": "Returning Series",
        "seasons": [
            {"season_number": 1, "episode_count": 9},
            {"season_number": 2, "episode_count": 3},
        ],
    }
    item, seasons, _ = await make_series(
        db_session,
        seasons=[{"season_number": 0}, {"season_number": 1}, {"season_number": 2}],
        episodes_per_season=3,
        episode_statuses=[EpisodeStatus.DOWNLOADED],
        tmdb_id=1,
    )
    await db_session.commit()
    await refresh_episode_growth(db_session, NOW)
    assert calls.season == 0  # S2 matches, S1 growth ignored


@pytest.mark.asyncio
async def test_season_with_future_episode_is_skipped(db_session, tmdb_mock):
    calls, _ = tmdb_mock
    await make_series(
        db_session,
        seasons=[{"season_number": 2}],
        episodes_per_season=3,
        episode_statuses=[EpisodeStatus.DOWNLOADED, EpisodeStatus.FUTURE],
        tmdb_id=1,
    )
    await db_session.commit()
    assert await refresh_episode_growth(db_session, NOW) == 0
    assert calls.show == 0


@pytest.mark.asyncio
async def test_tmdb_failure_changes_nothing_but_stamps(db_session, tmdb_mock):
    calls, state = tmdb_mock
    state["show"] = None
    item_id = await _marshals(db_session)

    assert await refresh_episode_growth(db_session, NOW) == 0

    item = await _load(db_session, item_id)
    assert len(item.seasons[0].episodes) == 3
    assert item.last_episode_growth_check_at is not None


@pytest.mark.asyncio
async def test_anime_null_episodes_uses_next_airing_episode(db_session, monkeypatch):
    async def batch(ids):
        return {
            111: {
                "id": 111,
                "episodes": None,
                "status": "RELEASING",
                "nextAiringEpisode": {"episode": 14},
            }
        }

    async def schedule(anilist_id):
        return [(14, NOW + timedelta(days=2))]

    monkeypatch.setattr(anilist, "get_anime_details_batch_by_ids", batch)
    monkeypatch.setattr(anilist, "get_anime_airing_schedule", schedule)
    item, _, _ = await make_series(
        db_session,
        seasons=[
            {"season_number": 1, "anilist_id": 111, "status": SeasonStatus.SEARCHING}
        ],
        episodes_per_season=13,
        episode_statuses=[EpisodeStatus.DOWNLOADED],
        media_type=MediaType.ANIME,
        anilist_id=111,
    )
    item_id = item.id
    await db_session.commit()

    assert await refresh_episode_growth(db_session, NOW) == 1

    season = (await _load(db_session, item_id)).seasons[0]
    ep14 = next(e for e in season.episodes if e.episode_number == 14)
    assert ep14.status == EpisodeStatus.FUTURE
    assert ep14.air_date.replace(tzinfo=timezone.utc) == NOW + timedelta(days=2)
    assert season.episode_count == 14


@pytest.mark.asyncio
async def test_ensure_episode_rows_aired_through_marks_undated_rows_searching(
    db_session,
):
    season = Season(season_number=1, episode_count=1)
    season.episodes = []
    added = episode_growth.ensure_episode_rows(
        db_session, season, 3, {}, NOW, aired_through=2
    )
    states = {e.episode_number: e.status for e in season.episodes}
    assert added == 3
    assert states == {
        1: EpisodeStatus.SEARCHING,
        2: EpisodeStatus.SEARCHING,
        3: EpisodeStatus.FUTURE,
    }


@pytest.mark.asyncio
async def test_anilist_batch_query_requests_next_airing_episode(monkeypatch):
    seen = {}

    async def fake_post(client, payload):
        seen["query"] = payload["query"]
        raise RuntimeError("stop")

    monkeypatch.setattr(anilist, "_post_with_retry", fake_post)
    await anilist.get_anime_details_batch_by_ids([1])
    assert "nextAiringEpisode" in seen["query"]


@pytest.mark.asyncio
async def test_library_maintenance_runs_growth_phase(db_session, monkeypatch, caplog):
    from app.services.library_maintenance import run_library_maintenance

    async def fake_growth(session, now=None, *, stats=None):
        return 4

    monkeypatch.setattr(episode_growth, "refresh_episode_growth", fake_growth)
    with caplog.at_level("INFO", logger="app.core.automation"):
        stats = await run_library_maintenance(db_session, include_anime=False)
    assert stats.episodes_grown == 4
    assert "[SYNC] [MAINT] [growth] 4 new episodes added" in caplog.text


@pytest.mark.asyncio
async def test_library_maintenance_survives_growth_probe_failure(
    db_session, monkeypatch
):
    from app.services.library_maintenance import run_library_maintenance

    async def boom(session, now=None, *, stats=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(episode_growth, "refresh_episode_growth", boom)
    stats = await run_library_maintenance(db_session, include_anime=False)
    assert stats.episodes_grown == 0
