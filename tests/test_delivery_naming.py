"""Template rendering and destination routing for local directory delivery (m78)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.parser import parse_release_name
from app.db.models import (
    DeliveredFile,
    DownloadHistory,
    Episode,
    MediaItem,
    MediaType,
    Season,
)
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.naming import (
    anime_folder_suffix,
    render_template,
    resolve_destination,
    sanitize_component,
)
from tests.conftest import make_history, make_movie

CFG = DeliveryConfig(
    movies_path="/lib/movies",
    shows_path="/lib/shows",
    anime_path="/lib/anime",
    anime_movies_path="/lib/anime-movies",
)
PARSED = parse_release_name("Example.Show.S01E03.1080p.WEB-DL.x265-GRP")


def _history(**kw: object) -> DownloadHistory:
    fields: dict[str, object] = {
        "nzb_title": "x",
        "resolution": "1080p",
        "source": "WEB-DL",
        "is_ad_hoc": False,
    }
    fields.update(kw)
    return DownloadHistory(**fields)


@pytest.mark.parametrize(
    ("raw", "clean"),
    [
        ("A/B:C?", "A_B_C_"),
        ("..", ""),
        ("../etc/passwd", "_etc_passwd"),
        ("  Name.  ", "Name"),
        ("a\x00b\tc", "a_b_c"),
        ("Two   Spaces", "Two Spaces"),
    ],
)
def test_sanitize_component(raw: str, clean: str) -> None:
    assert sanitize_component(raw) == clean


@pytest.mark.parametrize(
    ("template", "values", "expected"),
    [
        (
            "{title} ({year})/{title}.{ext}",
            {"title": "Film", "year": 2020, "ext": "mkv"},
            "Film (2020)/Film.mkv",
        ),
        (
            "{title} ({year})/{title}.{ext}",
            {"title": "Film", "year": None, "ext": "mkv"},
            "Film/Film.mkv",
        ),
        (
            "{title} - S{season:02d}E{episode:02d} - {episode_title}.{ext}",
            {
                "title": "Show",
                "season": 1,
                "episode": 3,
                "episode_title": "",
                "ext": "mkv",
            },
            "Show - S01E03.mkv",
        ),
        (
            "{title} [{resolution} {source}].{ext}",
            {"title": "F", "resolution": "1080p", "source": None, "ext": "mkv"},
            "F [1080p].mkv",
        ),
        (
            "{title} [{resolution} {source}].{ext}",
            {"title": "F", "resolution": None, "source": "", "ext": "mkv"},
            "F.mkv",
        ),
        (
            "{title}/{title}.{ext}",
            {"title": "A/B:C", "ext": "mkv"},
            "A_B_C/A_B_C.mkv",
        ),
    ],
)
def test_render_template(
    template: str, values: dict[str, object], expected: str
) -> None:
    assert render_template(template, values) == expected


def test_component_capped_at_255_bytes_keeping_extension() -> None:
    out = render_template("{title}.{ext}", {"title": "é" * 300, "ext": "mkv"})
    assert len(out.encode()) <= 255
    assert out.endswith(".mkv")


def _anime(movie: bool = False) -> tuple[MediaItem, Season, Episode]:
    item = MediaItem(
        title="Example Anime", media_type=MediaType.ANIME, year=2019, anilist_id=5
    )
    season = Season(
        season_number=1,
        entry_type="movie" if movie else "season",
        title="Example Anime",
        air_date=datetime(2020, 4, 1, tzinfo=timezone.utc),
        anilist_id=100001,
    )
    return item, season, Episode(season_id=1, episode_number=3)


def test_anime_tv_entry_routing() -> None:
    item, season, episode = _anime()
    dest = resolve_destination(
        CFG, _history(), item, season, episode, PARSED, "romaji", ".mkv"
    )
    assert dest is not None
    assert dest.root == Path("/lib/anime")
    assert dest.relative == Path(
        "Example Anime (2020)/Season 01/Example Anime - S01E03.mkv"
    )
    assert dest.anilist_id == 100001
    assert dest.full == Path("/lib/anime") / dest.relative


def test_anime_movie_slot_routing() -> None:
    item, season, _ = _anime(movie=True)
    dest = resolve_destination(
        CFG, _history(), item, season, None, PARSED, "romaji", "mkv"
    )
    assert dest is not None
    assert dest.root == Path("/lib/anime-movies")
    assert dest.relative == Path(
        "Example Anime (2020)/Example Anime (2020) [1080p WEB-DL].mkv"
    )


def test_unconfigured_root_returns_none() -> None:
    item, season, episode = _anime()
    cfg = DeliveryConfig(movies_path="/lib/movies")
    assert (
        resolve_destination(
            cfg, _history(), item, season, episode, PARSED, "romaji", "mkv"
        )
        is None
    )


def test_live_action_show_and_movie() -> None:
    show = MediaItem(title="Example Show", media_type=MediaType.SHOW, year=2018)
    season = Season(season_number=2, entry_type="season")
    episode = Episode(season_id=1, episode_number=4)
    dest = resolve_destination(
        CFG, _history(), show, season, episode, PARSED, "romaji", "mkv"
    )
    assert dest is not None
    assert dest.root == Path("/lib/shows")
    assert dest.relative == Path(
        "Example Show (2018)/Season 02/Example Show - S02E04.mkv"
    )
    assert dest.anilist_id is None

    movie = MediaItem(title="Example Film", media_type=MediaType.MOVIE, year=None)
    dest = resolve_destination(
        CFG, _history(), movie, None, None, PARSED, "romaji", "mkv"
    )
    assert dest is not None
    assert dest.root == Path("/lib/movies")
    assert dest.relative == Path("Example Film/Example Film [1080p WEB-DL].mkv")


@pytest.mark.parametrize(
    ("label", "root", "has_season"),
    [
        ("anime", "/lib/anime", True),
        ("anime-movie", "/lib/anime-movies", False),
        ("movie", "/lib/movies", False),
        ("series", "/lib/shows", True),
    ],
)
def test_ad_hoc_routing_uses_history_snapshot(
    label: str, root: str, has_season: bool
) -> None:
    history = _history(
        is_ad_hoc=True,
        media_title="Snapshot Title",
        media_year=2021,
        media_type_label=label,
        anilist_id=42 if label.startswith("anime") else None,
    )
    dest = resolve_destination(CFG, history, None, None, None, PARSED, "romaji", "mkv")
    assert dest is not None
    assert dest.root == Path(root)
    assert dest.relative.parts[0] == "Snapshot Title (2021)"
    assert ("Season 01" in dest.relative.parts) == has_season
    assert dest.anilist_id == (42 if label.startswith("anime") else None)


async def test_anime_folder_suffix_on_collision(db_session: AsyncSession) -> None:
    item = await make_movie(db_session)
    history = await make_history(db_session, item=item)
    root = Path("/lib/anime")
    db_session.add(
        DeliveredFile(
            history_id=history.id,
            source_name="a.mkv",
            kind="video",
            dest_path=str(root / "Example Anime (2020)" / "Season 01" / "a.mkv"),
            anilist_id=100001,
        )
    )
    await db_session.commit()
    folder = "Example Anime (2020)"
    assert await anime_folder_suffix(db_session, root, folder, 100001) == ""
    assert (
        await anime_folder_suffix(db_session, root, folder, 100002)
        == " [anilist-100002]"
    )
    assert await anime_folder_suffix(db_session, root, folder, None) == ""
    assert await anime_folder_suffix(db_session, root, "Other (2020)", 100002) == ""
    assert (
        await anime_folder_suffix(db_session, Path("/lib/elsewhere"), folder, 100002)
        == ""
    )


@pytest.mark.parametrize(
    ("title", "name"),
    [
        ("Chapter One", "Example Show - S01E04 - Chapter One.mkv"),
        (None, "Example Show - S01E04.mkv"),
    ],
)
def test_episode_title_in_show_file_name(title: str | None, name: str) -> None:
    show = MediaItem(title="Example Show", media_type=MediaType.SHOW, year=2018)
    season = Season(season_number=1, entry_type="season")
    episode = Episode(season_id=1, episode_number=4)
    dest = resolve_destination(
        CFG, _history(), show, season, episode, PARSED, "romaji", "mkv", title
    )
    assert dest is not None
    assert dest.relative.name == name
