"""Template rendering, name sanitizing and destination routing for delivery."""

from __future__ import annotations

import posixpath
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from app.core.anime_titles import (
    anime_season_title_for,
    anime_title_for,
    is_anime_scope,
)
from app.db.models import DeliveredFile, MediaType
from app.services.delivery.config import DeliveryConfig

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.core.parser import ParsedRelease
    from app.db.models import DownloadHistory, Episode, MediaItem, Season

MAX_COMPONENT_BYTES = 255

_FORBIDDEN_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f\x7f]')
_NUMERIC_KEYS = ("season", "episode")


def sanitize_component(value: str) -> str:
    """Make ``value`` safe as one path component (no separators, no ``..``)."""
    cleaned = _FORBIDDEN_RE.sub("_", str(value))
    cleaned = cleaned.replace("..", "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" .")


def _cap_bytes(component: str, limit: int = MAX_COMPONENT_BYTES) -> str:
    if len(component.encode("utf-8")) <= limit:
        return component
    stem, ext = posixpath.splitext(component)
    if len(ext.encode("utf-8")) >= limit:  # not a real extension
        stem, ext = component, ""
    budget = limit - len(ext.encode("utf-8"))
    stem = stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore").rstrip(" .")
    return stem + ext


def _present(values: Mapping[str, object], key: str) -> bool:
    return values.get(key) not in (None, "", 0)


class _Values(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return ""


def _drop_optional_blocks(template: str, values: Mapping[str, object]) -> str:
    if not _present(values, "year"):
        template = template.replace(" ({year})", "")
    if not _present(values, "episode_title"):
        template = template.replace(" - {episode_title}", "")
    quality = "[{resolution} {source}]"
    if quality in template:
        keys = [k for k in ("resolution", "source") if _present(values, k)]
        block = f"[{' '.join('{' + k + '}' for k in keys)}]" if keys else ""
        template = template.replace(f" {quality}", f" {block}" if block else "")
        template = template.replace(quality, block)
    return template


def render_template(template: str, values: Mapping[str, object]) -> str:
    """Render a ``/``-separated template into a sanitized relative path string."""
    template = _drop_optional_blocks(template, values)
    safe = _Values()
    for key, value in values.items():
        if key in _NUMERIC_KEYS:
            safe[key] = value if isinstance(value, int) else 0
        elif key == "year":
            safe[key] = value if value else ""
        else:
            safe[key] = sanitize_component("" if value is None else str(value))
    for key in _NUMERIC_KEYS:
        safe.setdefault(key, 0)
    components: list[str] = []
    for part in template.split("/"):
        rendered = sanitize_component(part.format_map(safe))
        if rendered:
            components.append(_cap_bytes(rendered))
    return "/".join(components)


@dataclass(frozen=True)
class Destination:
    root: Path  # one of the configured *_path values
    relative: Path
    anilist_id: int | None

    @property
    def full(self) -> Path:
        return self.root / self.relative


def resolve_destination(
    cfg: DeliveryConfig,
    history: DownloadHistory,
    item: MediaItem | None,
    season: Season | None,
    episode: Episode | None,
    parsed: ParsedRelease,
    title_language: str,
    ext: str,
    episode_title: str | None = None,
) -> Destination | None:
    """Route a file to its root and template; ``None`` when the root is unconfigured."""
    label = history.media_type_label or ""
    ad_hoc = bool(history.is_ad_hoc) or item is None
    anime = (item is not None and is_anime_scope(item)) or label in {
        "anime",
        "anime-movie",
    }
    ext = ext.lstrip(".")
    ep_number = episode.episode_number if episode is not None else parsed.episode
    values: dict[str, object] = {
        "resolution": history.resolution or parsed.resolution,
        "source": history.source or parsed.source,
        "ext": ext,
        "episode": ep_number or 0,
        "episode_title": episode_title,
    }
    anilist_id: int | None = None

    if anime:
        is_movie = (
            (season is not None and season.entry_type == "movie")
            or (item is not None and bool(item.is_anime_movie))
            or label == "anime-movie"
        )
        if ad_hoc:
            entry_title = history.media_title or parsed.title
            year = history.media_year
            anilist_id = history.anilist_id
        else:
            assert item is not None
            entry_title = (
                anime_season_title_for(item, season, title_language)
                if season is not None
                else anime_title_for(item, title_language)
            )
            year = (
                season.air_date.year
                if season is not None and season.air_date is not None
                else item.year or history.media_year
            )
            anilist_id = (
                (season.anilist_id if season is not None else None)
                or item.anilist_id
                or history.anilist_id
            )
        values.update(entry_title=entry_title, year=year)
        root, template = (
            (cfg.anime_movies_path, cfg.anime_movie_template)
            if is_movie
            else (cfg.anime_path, cfg.anime_template)
        )
    else:
        if ad_hoc:
            title, year = history.media_title or parsed.title, history.media_year
        else:
            assert item is not None
            title, year = item.title, item.year or history.media_year
        values.update(title=title, year=year or parsed.year)
        is_movie = (
            item.media_type == MediaType.MOVIE if item is not None else label == "movie"
        )
        if is_movie:
            root, template = cfg.movies_path, cfg.movie_template
        else:
            values["season"] = (
                season.season_number if season is not None else parsed.season or 0
            )
            root, template = cfg.shows_path, cfg.show_template

    if not root:
        return None
    relative = render_template(template, values)
    root_path = Path(root)
    dest = Destination(root_path, Path(relative), anilist_id)
    if not dest.full.resolve().is_relative_to(root_path.resolve()):
        raise ValueError("Resolved delivery path escapes the library root")
    return dest


async def anime_folder_suffix(
    session: AsyncSession, root: Path, folder: str, anilist_id: int | None
) -> str:
    """``" [anilist-<id>]"`` when ``folder`` already holds a different AniList entry."""
    if anilist_id is None:
        return ""
    prefix = f"{root / folder}/"
    stmt = (
        select(DeliveredFile.id)
        .where(
            DeliveredFile.dest_path.startswith(prefix, autoescape=True),
            DeliveredFile.anilist_id.is_not(None),
            DeliveredFile.anilist_id != anilist_id,
        )
        .limit(1)
    )
    if (await session.execute(stmt)).first() is None:
        return ""
    return f" [anilist-{anilist_id}]"
