"""Episode growth probe (m86, ADR-188).

Detects episodes a provider announces after a season's rows were created (e.g. a
season growing from 3 to 6 episodes) and adds only the missing ``Episode`` rows.
Cheap probe first (TMDB show-level ``episode_count`` / AniList ``episodes`` +
``nextAiringEpisode``); season detail is fetched only when the count grows.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.phase_stats import EpisodeGrowthStats
from app.db.models import (
    Episode,
    EpisodeStatus,
    MediaItem,
    MediaStatus,
    MediaType,
    Season,
    SeasonStatus,
)
from app.services import anilist, tmdb

logger = logging.getLogger("app.core.automation")

EPISODE_GROWTH_INTERVAL = timedelta(hours=24)
EPISODE_GROWTH_SETTLED_AFTER = timedelta(days=90)

_TMDB_FINAL_STATUSES = frozenset({"Ended", "Canceled"})
_ANILIST_FINAL_STATUSES = frozenset({"FINISHED", "CANCELLED"})


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def ensure_episode_rows(
    session: AsyncSession,
    season: Season,
    target_count: int,
    air_dates: Mapping[int, datetime | None],
    now: datetime,
    aired_through: int = 0,
) -> int:
    """Add the missing ``Episode`` rows ``1..target_count`` to *season*; return how many.

    An undated row is ``FUTURE`` unless its number is <= ``aired_through``. Existing
    rows are never modified; ``Season.episode_count`` is only ever raised.
    """
    existing = {ep.episode_number for ep in season.episodes or []}
    added = 0
    for number in range(1, target_count + 1):
        if number in existing:
            continue
        air_date = air_dates.get(number)
        if air_date is None:
            released = number <= aired_through
        else:
            released = _as_utc(air_date) <= now
        # Assigning ``season`` keeps season.episodes current; add() persists the row.
        session.add(
            Episode(
                season=season,
                episode_number=number,
                monitored=True,
                air_date=air_date,
                status=EpisodeStatus.SEARCHING if released else EpisodeStatus.FUTURE,
            )
        )
        added += 1
    if added:
        season.episode_count = max(season.episode_count or 0, target_count)
    return added


def _candidate_season(item: MediaItem) -> Season | None:
    """Latest regular season whose known episodes have all been released."""
    seasons = [
        s
        for s in item.seasons or []
        if s.season_number > 0 and s.status != SeasonStatus.IGNORED
    ]
    if not seasons:
        return None
    latest = max(seasons, key=lambda s: s.season_number)
    episodes = latest.episodes or []
    if not episodes or any(ep.status == EpisodeStatus.FUTURE for ep in episodes):
        return None
    return latest


def is_episode_growth_candidate(item: MediaItem, now: datetime) -> bool:
    """True when *item* is due for a growth probe. Needs seasons/episodes loaded."""
    if item.media_type not in (MediaType.SHOW, MediaType.ANIME):
        return False
    if item.status == MediaStatus.IGNORED:
        return False
    stamp = item.last_episode_growth_check_at
    if stamp is not None and _as_utc(now) < _as_utc(stamp) + EPISODE_GROWTH_INTERVAL:
        return False
    return _candidate_season(item) is not None


def _local_count(season: Season) -> int:
    return max(season.episode_count or 0, len(season.episodes or []))


def _stamp(item: MediaItem, now: datetime, *, settled: bool) -> None:
    """Record a probe. A settled item's stamp is pushed ahead so it is next due in 90 days."""
    item.last_episode_growth_check_at = (
        now + (EPISODE_GROWTH_SETTLED_AFTER - EPISODE_GROWTH_INTERVAL)
        if settled
        else now
    )


def _tmdb_air_dates(
    season_details: dict[str, Any],
) -> tuple[dict[int, datetime | None], int]:
    from app.core.automation import parse_broadcast_air_date

    dates: dict[int, datetime | None] = {}
    highest = 0
    for ep in season_details.get("episodes") or []:
        number = ep.get("episode_number")
        if not isinstance(number, int) or number <= 0:
            continue
        highest = max(highest, number)
        dates[number] = parse_broadcast_air_date(ep.get("air_date"))
    return dates, highest


async def _probe_show(
    session: AsyncSession,
    item: MediaItem,
    season: Season,
    now: datetime,
    show_cache: dict[int, dict[str, Any] | None],
) -> int:
    """Probe one TMDB show; return the number of episodes added."""
    if not item.tmdb_id:
        return 0
    tmdb_id = int(item.tmdb_id)
    if tmdb_id not in show_cache:
        try:
            show_cache[tmdb_id] = await tmdb.get_show_details(tmdb_id, session=session)
        except Exception as e:  # noqa: BLE001
            logger.warning("Episode growth: TMDB show %d failed: %s", tmdb_id, e)
            show_cache[tmdb_id] = None
    show = show_cache[tmdb_id]
    if not show:
        _stamp(item, now, settled=False)
        return 0

    entry = next(
        (
            s
            for s in show.get("seasons") or []
            if s.get("season_number") == season.season_number
        ),
        None,
    )
    upstream = int((entry or {}).get("episode_count") or 0)
    added = 0
    if upstream > _local_count(season):
        details = await tmdb.get_season_details(
            tmdb_id, season.season_number, session=session
        )
        if details:
            air_dates, highest = _tmdb_air_dates(details)
            added = ensure_episode_rows(
                session, season, max(upstream, highest), air_dates, now
            )
        else:
            _stamp(item, now, settled=False)
            return 0
    _stamp(
        item,
        now,
        settled=added == 0 and show.get("status") in _TMDB_FINAL_STATUSES,
    )
    return added


async def _probe_anime(
    session: AsyncSession,
    items: list[tuple[MediaItem, Season, int]],
    now: datetime,
    stats: EpisodeGrowthStats,
    grown: list[tuple[MediaItem, Season]],
) -> int:
    """Probe anime seasons with one batched AniList query; return episodes added."""
    try:
        nodes = await anilist.get_anime_details_batch_by_ids(
            sorted({anilist_id for _, _, anilist_id in items})
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Episode growth: AniList batch failed: %s", e)
        nodes = {}

    total = 0
    for item, season, anilist_id in items:
        node = nodes.get(anilist_id)
        if not node:
            _stamp(item, now, settled=False)
            continue
        next_ep = int((node.get("nextAiringEpisode") or {}).get("episode") or 0)
        target = max(int(node.get("episodes") or 0), next_ep)
        added = 0
        if target > _local_count(season):
            try:
                schedule = await anilist.get_anime_airing_schedule(anilist_id)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Episode growth: AniList schedule %d failed: %s", anilist_id, e
                )
                _stamp(item, now, settled=False)
                continue
            finished = node.get("status") == "FINISHED"
            aired_through = next_ep - 1 if next_ep else (target if finished else 0)
            added = ensure_episode_rows(
                session, season, target, dict(schedule), now, aired_through
            )
        if added:
            stats.seasons_grown += 1
            grown.append((item, season))
        _stamp(
            item,
            now,
            settled=added == 0 and node.get("status") in _ANILIST_FINAL_STATUSES,
        )
        total += added
    return total


async def refresh_episode_growth(
    session: AsyncSession,
    now: datetime | None = None,
    *,
    stats: EpisodeGrowthStats | None = None,
) -> int:
    """Probe due items for newly announced episodes; return the episodes added."""
    from app.core.transfer_poller import (
        _recalculate_parent_status,
        _sync_season_status_from_episodes,
    )

    now = datetime.now(timezone.utc) if now is None else _as_utc(now)
    stats = stats if stats is not None else EpisodeGrowthStats()

    stmt = (
        select(MediaItem)
        .where(
            MediaItem.media_type.in_((MediaType.SHOW, MediaType.ANIME)),
            MediaItem.status != MediaStatus.IGNORED,
        )
        .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
    )
    items = (await session.execute(stmt)).scalars().unique().all()
    due = [i for i in items if is_episode_growth_candidate(i, now)]
    if not due:
        return 0

    added_total = 0
    grown: list[tuple[MediaItem, Season]] = []
    show_cache: dict[int, dict[str, Any] | None] = {}
    anime_work: list[tuple[MediaItem, Season, int]] = []

    for item in due:
        season = _candidate_season(item)
        if season is None:
            continue
        stats.probed += 1
        if item.media_type == MediaType.ANIME:
            anilist_id = season.anilist_id or (
                item.anilist_id if len(item.seasons or []) == 1 else None
            )
            if anilist_id:
                anime_work.append((item, season, int(anilist_id)))
            else:
                _stamp(item, now, settled=False)
            continue
        added = await _probe_show(session, item, season, now, show_cache)
        if added:
            stats.seasons_grown += 1
            grown.append((item, season))
            added_total += added

    if anime_work:
        added_total += await _probe_anime(session, anime_work, now, stats, grown)

    stats.episodes_added = added_total
    for _, season in grown:
        _sync_season_status_from_episodes(season)
    await session.flush()
    for item in {i.id: i for i, _ in grown}.values():
        await _recalculate_parent_status(session, item)
    await session.commit()
    return added_total
