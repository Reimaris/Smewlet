"""Turn the files of a finished release into an ordered delivery plan."""

from __future__ import annotations

import posixpath
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.anime_titles import normalize_anime_title_language
from app.core.fake_detector import MIN_VIDEO_EPISODE_BYTES, MIN_VIDEO_MOVIE_BYTES
from app.core.parser import ParsedRelease, parse_release_name
from app.db.models import (
    DownloadHistory,
    Episode,
    MediaItem,
    MediaType,
    Season,
    SystemSettings,
)
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.filtering import ClassifiedFile, classify_files
from app.services.delivery.naming import (
    Destination,
    anime_folder_suffix,
    resolve_destination,
)
from app.services.downloaders.base import DownloaderFile

UNMATCHED_EPISODE = "UNMATCHED_EPISODE"
PATH_NOT_CONFIGURED = "PATH_NOT_CONFIGURED"


@dataclass(frozen=True)
class PlannedFile:
    source: DownloaderFile
    dest: Destination
    kind: str  # "video" | "subtitle"
    episode_id: int | None
    season_id: int | None
    anilist_id: int | None
    status: str = "pending"  # pending | skipped | failed
    error: str | None = None


def _basename(name: str) -> str:
    return posixpath.basename(name.replace("\\", "/"))


def _is_movie(
    history: DownloadHistory, item: MediaItem | None, season: Season | None
) -> bool:
    if (history.media_type_label or "") in {"movie", "anime-movie"}:
        return True
    if season is not None and season.entry_type == "movie":
        return True
    return item is not None and (
        item.media_type == MediaType.MOVIE or bool(item.is_anime_movie)
    )


def _placeholder(name: str) -> Destination:
    return Destination(Path(), Path(_basename(name)), None)


async def build_delivery_plan(
    session: AsyncSession,
    history: DownloadHistory,
    files: Sequence[DownloaderFile],
    cfg: DeliveryConfig,
) -> list[PlannedFile]:
    """Plan videos first (in file order), each followed by its subtitles."""
    ad_hoc = bool(history.is_ad_hoc)
    item = (
        await session.get(MediaItem, history.media_item_id)
        if history.media_item_id is not None and not ad_hoc
        else None
    )
    season = (
        await session.get(Season, history.season_id)
        if history.season_id is not None and not ad_hoc
        else None
    )
    is_movie = _is_movie(history, item, season)
    settings = await session.get(SystemSettings, 1)
    language = normalize_anime_title_language(
        settings.anime_title_language if settings is not None else None
    )

    classified = classify_files(
        files,
        is_movie=is_movie,
        min_video_bytes=MIN_VIDEO_MOVIE_BYTES if is_movie else MIN_VIDEO_EPISODE_BYTES,
    )

    # Episode lookup for packs: (season_number, episode_number) -> (Episode, Season)
    lookup: dict[tuple[int | None, int], tuple[Episode, Season]] = {}
    pinned: tuple[Episode, Season] | None = None
    if item is not None and not is_movie:
        stmt = (
            select(Episode, Season)
            .join(Season, Episode.season_id == Season.id)
            .where(Season.media_item_id == item.id)
        )
        if season is not None:
            stmt = stmt.where(Season.id == season.id)
        for ep, sn in (await session.execute(stmt)).all():
            key = (None if season is not None else sn.season_number, ep.episode_number)
            lookup[key] = (ep, sn)
            if history.episode_id == ep.id:
                pinned = (ep, sn)

    def parse(cf: ClassifiedFile) -> ParsedRelease:
        return parse_release_name(_basename(cf.file.name))

    async def route(
        cf: ClassifiedFile,
        match: tuple[Episode, Season] | None,
        parsed: ParsedRelease,
        stem_of: Destination | None,
    ) -> PlannedFile:
        ext = posixpath.splitext(_basename(cf.file.name))[1]
        episode, ep_season = match if match is not None else (None, season)
        season_id = ep_season.id if ep_season is not None else history.season_id
        dest = resolve_destination(
            cfg, history, item, ep_season, episode, parsed, language, ext
        )
        if dest is None:
            return PlannedFile(
                cf.file,
                _placeholder(cf.file.name),
                cf.kind,
                episode.id if episode else None,
                season_id,
                None,
                "failed",
                PATH_NOT_CONFIGURED,
            )
        if stem_of is not None:  # subtitle: <video stem>[.<lang>]<ext>
            stem = stem_of.relative.with_suffix("").name
            lang = f".{cf.language}" if cf.language else ""
            dest = Destination(
                dest.root,
                stem_of.relative.with_name(f"{stem}{lang}{ext.lower()}"),
                stem_of.anilist_id,
            )
        elif dest.anilist_id is not None and len(dest.relative.parts) > 1:
            folder = dest.relative.parts[0]
            pinned_folder = ep_season.delivery_folder if ep_season else None
            if pinned_folder:
                dest = Destination(
                    dest.root,
                    Path(pinned_folder, *dest.relative.parts[1:]),
                    dest.anilist_id,
                )
                suffix = ""
            else:
                suffix = await anime_folder_suffix(
                    session, dest.root, folder, dest.anilist_id
                )
            if suffix:
                dest = Destination(
                    dest.root,
                    Path(f"{folder}{suffix}", *dest.relative.parts[1:]),
                    dest.anilist_id,
                )
        return PlannedFile(
            cf.file,
            dest,
            cf.kind,
            episode.id if episode else None,
            season_id,
            dest.anilist_id,
        )

    def find_match(parsed: ParsedRelease) -> tuple[Episode, Season] | None:
        if parsed.episode is None:
            return None
        key = (None if season is not None else parsed.season, parsed.episode)
        found = lookup.get(key)
        if found is None and season is None and parsed.season is None:
            found = lookup.get((1, parsed.episode))
        return found

    plan: list[PlannedFile] = []
    video_plans: dict[tuple[int | None, int | None], PlannedFile] = {}
    needs_match = item is not None and not is_movie

    for cf in classified:
        if cf.kind != "video":
            continue
        parsed = parse(cf)
        match = pinned if pinned is not None else None
        if match is None and needs_match:
            match = find_match(parsed)
            if match is None:
                plan.append(
                    PlannedFile(
                        cf.file,
                        _placeholder(cf.file.name),
                        "video",
                        None,
                        history.season_id,
                        None,
                        "skipped",
                        UNMATCHED_EPISODE,
                    )
                )
                continue
        planned = await route(cf, match, parsed, None)
        plan.append(planned)
        video_plans.setdefault(
            (planned.episode_id, parsed.episode if ad_hoc or not needs_match else None),
            planned,
        )

    for cf in classified:
        if cf.kind != "subtitle":
            continue
        parsed = parse(cf)
        if is_movie:
            host = next(iter(video_plans.values()), None)
            match = None
        else:
            match = pinned if pinned is not None else None
            if match is None and needs_match:
                match = find_match(parsed)
            host = (
                video_plans.get((match[0].id, None))
                if match is not None
                else video_plans.get((None, parsed.episode))
            )
        if host is None or host.status != "pending":
            plan.append(
                PlannedFile(
                    cf.file,
                    _placeholder(cf.file.name),
                    "subtitle",
                    None,
                    history.season_id,
                    None,
                    "skipped",
                    UNMATCHED_EPISODE,
                )
            )
            continue
        plan.append(await route(cf, match, parsed, host.dest))
    return plan
