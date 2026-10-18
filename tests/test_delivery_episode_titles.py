from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.db.models import MediaType
from app.services import tmdb
from app.services.delivery import episode_titles
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.episode_titles import tmdb_episode_title
from app.services.delivery.planner import build_delivery_plan
from app.services.downloaders.base import DownloaderFile
from tests.conftest import make_history, make_series

PAYLOAD = {
    "episodes": [
        {"episode_number": 1, "name": "Chapter One"},
        {"episode_number": 2, "name": "Episode 2"},
    ]
}
CFG = DeliveryConfig(shows_path="/lib/shows", anime_path="/lib/anime")


@pytest.fixture
def season_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    episode_titles._season_titles.clear()
    calls: list[tuple[int, int]] = []

    async def fake(tmdb_id: int, season: int, **_kw: Any) -> dict[str, Any]:
        calls.append((tmdb_id, season))
        return PAYLOAD

    monkeypatch.setattr(tmdb, "get_season_details", fake)
    return calls


async def test_title_placeholder_and_cache(db_session, season_calls) -> None:
    assert await tmdb_episode_title(db_session, 1, 1, 1) == "Chapter One"
    assert await tmdb_episode_title(db_session, 1, 1, 2) is None
    assert await tmdb_episode_title(db_session, 1, 1, 9) is None
    assert len(season_calls) == 1


async def test_failure_returns_none_and_is_not_cached(db_session, monkeypatch) -> None:
    episode_titles._season_titles.clear()

    async def boom(*_a: Any, **_k: Any) -> None:
        raise httpx.ConnectError("down")

    monkeypatch.setattr(tmdb, "get_season_details", boom)
    assert await tmdb_episode_title(db_session, 2, 1, 1) is None

    async def none(*_a: Any, **_k: Any) -> None:
        return None

    monkeypatch.setattr(tmdb, "get_season_details", none)
    assert await tmdb_episode_title(db_session, 2, 1, 1) is None

    async def ok(*_a: Any, **_k: Any) -> dict[str, Any]:
        return PAYLOAD

    monkeypatch.setattr(tmdb, "get_season_details", ok)
    assert await tmdb_episode_title(db_session, 2, 1, 1) == "Chapter One"


async def test_planner_names_show_episodes_with_title(db_session, season_calls) -> None:
    item, seasons, _ = await make_series(
        db_session, seasons=1, episodes_per_season=2, title="Show", tmdb_id=42
    )
    history = await make_history(db_session, item=item, season=seasons[0])
    big = 100 * 1024 * 1024
    files = [
        DownloaderFile(name=f"Show.S01E0{n}.1080p.mkv", size_bytes=big) for n in (1, 2)
    ]

    plan = await build_delivery_plan(db_session, history, files, CFG)

    assert [p.dest.relative.name for p in plan] == [
        "Show - S01E01 - Chapter One.mkv",
        "Show - S01E02.mkv",
    ]


async def test_anime_plans_never_look_up_titles(db_session, season_calls) -> None:
    item, seasons, _ = await make_series(
        db_session,
        seasons=[dict(season_number=1, anilist_id=7)],
        episodes_per_season=1,
        media_type=MediaType.ANIME,
        title="Show",
        tmdb_id=42,
        anilist_id=7,
    )
    history = await make_history(db_session, item=item, season=seasons[0])
    files = [DownloaderFile(name="Show.S01E01.1080p.mkv", size_bytes=100 * 1024 * 1024)]

    await build_delivery_plan(db_session, history, files, CFG)

    assert season_calls == []
