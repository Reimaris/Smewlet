"""Delivery planner: season pack matching, unmatched files, ad-hoc packs (m78)."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.delivery.config import DeliveryConfig
from app.services.delivery.planner import build_delivery_plan
from app.services.downloaders.base import DownloaderFile
from tests.conftest import make_history, make_series

CFG = DeliveryConfig(shows_path="/lib/shows", anime_path="/lib/anime")
BIG = 100 * 1024 * 1024


def _f(name: str, size: int = BIG) -> DownloaderFile:
    return DownloaderFile(name=name, size_bytes=size)


async def test_season_pack_matches_episodes_and_skips_unmatched(
    db_session: AsyncSession,
) -> None:
    item, seasons, episodes = await make_series(
        db_session, seasons=1, episodes_per_season=3, title="Example Show", year=2018
    )
    history = await make_history(db_session, item=item, season=seasons[0])
    files = [_f(f"Example.Show.S01E0{n}.1080p.WEB-DL.mkv") for n in (1, 2, 3)] + [
        _f("Extras.mkv")
    ]

    plan = await build_delivery_plan(db_session, history, files, CFG)

    matched = [p for p in plan if p.status == "pending"]
    assert [p.episode_id for p in matched] == [e.id for e in episodes]
    assert all(p.season_id == seasons[0].id for p in matched)
    assert matched[0].dest.full == Path(
        "/lib/shows/Example Show (2018)/Season 01/Example Show - S01E01.mkv"
    )
    (skipped,) = [p for p in plan if p.status == "skipped"]
    assert skipped.source.name == "Extras.mkv"
    assert skipped.error == "UNMATCHED_EPISODE"
    assert skipped.episode_id is None


async def test_subtitle_follows_video_destination(db_session: AsyncSession) -> None:
    item, seasons, episodes = await make_series(
        db_session, seasons=1, episodes_per_season=2, title="Example Show", year=2018
    )
    history = await make_history(db_session, item=item, season=seasons[0])
    files = [
        _f("Example.Show.S01E01.mkv"),
        _f("Example.Show.S01E02.mkv"),
        _f("Subs/Example.Show.S01E01.de.srt", 100),
        _f("Subs/Example.Show.S01E09.de.srt", 100),
    ]

    plan = await build_delivery_plan(db_session, history, files, CFG)

    (sub,) = [p for p in plan if p.kind == "subtitle" and p.status == "pending"]
    assert sub.episode_id == episodes[0].id
    assert sub.dest.relative.name == "Example Show - S01E01.de.srt"
    orphan = [p for p in plan if p.kind == "subtitle" and p.status == "skipped"]
    assert len(orphan) == 1


async def test_ad_hoc_pack_uses_guessit_only(db_session: AsyncSession) -> None:
    history = await make_history(
        db_session,
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
    )
    files = [
        _f("Example.Show.S02E05.mkv"),
        _f("Example.Show.S02E05.en.srt", 100),
    ]

    plan = await build_delivery_plan(db_session, history, files, CFG)

    video, sub = plan
    assert video.episode_id is None and video.status == "pending"
    assert video.dest.relative == Path(
        "Example Show (2018)/Season 02/Example Show - S02E05.mkv"
    )
    assert sub.dest.relative.name == "Example Show - S02E05.en.srt"


async def test_unconfigured_root_marks_file_failed(db_session: AsyncSession) -> None:
    history = await make_history(
        db_session, is_ad_hoc=True, media_type_label="series", media_title="X"
    )
    plan = await build_delivery_plan(
        db_session, history, [_f("X.S01E01.mkv")], DeliveryConfig()
    )
    assert plan[0].status == "failed"
    assert plan[0].error == "PATH_NOT_CONFIGURED"
