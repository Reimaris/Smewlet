from __future__ import annotations

from app.db.models import DeliveredFile, MediaType, SystemSettings
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.folder_pin import (
    backfill_delivery_folders,
    pin_delivery_folders,
)
from app.services.delivery.planner import build_delivery_plan
from app.services.downloaders.base import DownloaderFile
from tests.conftest import make_delivery_provider, make_history, make_series

CFG = DeliveryConfig(shows_path="/lib/shows", anime_path="/lib/anime")
BIG = 100 * 1024 * 1024


async def _anime(db_session, **season_extra):
    item, seasons, _ = await make_series(
        db_session,
        seasons=[dict(season_number=1, anilist_id=77, **season_extra)],
        episodes_per_season=1,
        media_type=MediaType.ANIME,
        title="Show",
        title_english="Show English",
        title_romaji="Show Romaji",
        year=2024,
        anilist_id=77,
    )
    history = await make_history(db_session, item=item, season=seasons[0])
    return item, seasons[0], history


async def _plan(db_session, history):
    files = [DownloaderFile(name="Show.S01E01.1080p.mkv", size_bytes=BIG)]
    (planned,) = await build_delivery_plan(db_session, history, files, CFG)
    return planned


async def _set_language(db_session, language: str) -> None:
    settings = await db_session.get(SystemSettings, 1)
    if settings is None:
        settings = SystemSettings(id=1)
        db_session.add(settings)
    settings.anime_title_language = language
    await db_session.flush()


async def test_pinned_folder_wins_over_language(db_session) -> None:
    _, season, history = await _anime(db_session, delivery_folder="Show Romaji (2024)")
    await _set_language(db_session, "english")

    planned = await _plan(db_session, history)

    assert planned.dest.relative.parts[0] == "Show Romaji (2024)"


async def test_first_delivery_pins_and_survives_language_switch(db_session) -> None:
    _, season, history = await _anime(db_session)
    await _set_language(db_session, "romaji")
    planned = await _plan(db_session, history)
    first = planned.dest.relative.parts[0]
    row = DeliveredFile(
        history_id=history.id,
        season_id=season.id,
        source_name="x.mkv",
        dest_path=str(planned.dest.full),
        kind="video",
        status="delivered",
        anilist_id=77,
    )
    db_session.add(row)
    await db_session.flush()

    await pin_delivery_folders(db_session, [row], CFG)
    assert season.delivery_folder == first

    await _set_language(db_session, "english")
    again = await _plan(db_session, history)
    assert again.dest.relative.parts[0] == first


async def test_backfill_pins_delivered_anime_only(db_session) -> None:
    await make_delivery_provider(
        db_session, anime_path="/lib/anime", shows_path="/lib/shows"
    )
    _, season, history = await _anime(db_session)
    show, (show_season,), _ = await make_series(db_session, simkl_id=555, seasons=1)
    show_history = await make_history(
        db_session, item=show, season=show_season, nzb_guid="g2"
    )
    for hist, sn, path in (
        (history, season, "/lib/anime/Show Romaji (2024)/Season 01/e01.mkv"),
        (show_history, show_season, "/lib/shows/Plain (2020)/Season 01/e01.mkv"),
    ):
        db_session.add(
            DeliveredFile(
                history_id=hist.id,
                season_id=sn.id,
                source_name="x.mkv",
                dest_path=path,
                kind="video",
                status="delivered",
            )
        )
    await db_session.flush()

    assert await backfill_delivery_folders(db_session) == 1
    assert season.delivery_folder == "Show Romaji (2024)"
    assert show_season.delivery_folder is None
    assert await backfill_delivery_folders(db_session) == 0
