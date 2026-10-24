"""Ad-hoc episode batch push (m85, ADR-186).

Pushes every aired episode of one season, or of all seasons, as individual
``scope=episode`` ad-hoc pushes so no season pack is ever chosen.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.adhoc_search import (
    AdHocFilters,
    _target_label,
    push_adhoc_release,
    search_and_score_releases,
)
from app.core.transfer_poller import ACTIVE_POLLABLE_STATUS_DETAILS
from app.db.database import async_session_factory
from app.db.models import DownloadHistory
from app.services.media_search import EpisodeSummary, list_episodes, list_seasons

logger = logging.getLogger(__name__)


@dataclass
class BatchSummary:
    pushed: list[str] = field(default_factory=list)
    queued: list[str] = field(default_factory=list)
    no_release: list[str] = field(default_factory=list)
    skipped_in_flight: list[str] = field(default_factory=list)
    skipped_unaired: int = 0
    aborted_reason: str | None = None


def select_batch_episodes(
    episodes: list[EpisodeSummary], today: date
) -> tuple[list[EpisodeSummary], int]:
    """Return (aired episodes in episode order, count of unaired episodes skipped).

    An episode with no air date counts as aired (AniList episodes carry none).
    """
    aired: list[EpisodeSummary] = []
    unaired = 0
    for ep in sorted(episodes, key=lambda e: e["episode_number"]):
        raw = ep.get("air_date")
        try:
            air = date.fromisoformat(raw[:10]) if raw else None
        except ValueError:
            air = None
        if air is not None and air > today:
            unaired += 1
        else:
            aired.append(ep)
    return aired, unaired


async def has_in_flight_adhoc_push(
    session: AsyncSession,
    *,
    tmdb_id: int | None,
    anilist_id: int | None,
    media_title: str,
    target_label: str,
) -> bool:
    """True if a non-dismissed ad-hoc push of this episode is still downloading or delivering."""
    if tmdb_id is not None:
        identity = DownloadHistory.tmdb_id == tmdb_id
    elif anilist_id is not None:
        identity = DownloadHistory.anilist_id == anilist_id
    else:
        identity = DownloadHistory.media_title == media_title
    in_flight = or_(
        func.lower(func.coalesce(DownloadHistory.status_detail, "")).in_(
            ACTIVE_POLLABLE_STATUS_DETAILS
        ),
        and_(
            DownloadHistory.deliver_enabled.is_(True),
            DownloadHistory.delivery_status.in_(("pending", "delivering")),
        ),
    )
    stmt = (
        select(DownloadHistory.id)
        .where(
            DownloadHistory.is_ad_hoc.is_(True),
            DownloadHistory.is_dismissed.is_(False),
            DownloadHistory.is_dry_run.is_(False),
            DownloadHistory.target_label == target_label,
            identity,
            in_flight,
        )
        .limit(1)
    )
    return (await session.execute(stmt)).first() is not None


async def run_episode_batch(
    *,
    provider: str,
    ext_id: int,
    kind: str,
    batch_scope: str,
    season: int | None,
    titles: list[str],
    year: int | None,
    imdb_id: str | None,
    tmdb_id: int | None,
    tvdb_id: int | None,
    anilist_id: int | None,
    poster_url: str | None,
    media_title: str,
    filters: AdHocFilters,
    limit: int,
    deliver: bool | None,
) -> BatchSummary:
    """Search and push each aired episode in order; never raises."""
    summary = BatchSummary()
    try:
        async with async_session_factory() as session:
            if batch_scope == "season" and season is not None:
                season_numbers = [season]
            else:
                season_numbers = sorted(
                    s["season_number"]
                    for s in await list_seasons(provider, ext_id, kind, session)  # type: ignore[arg-type]
                )
        today = datetime.now(timezone.utc).date()
        for season_n in season_numbers:
            async with async_session_factory() as session:
                episodes = await list_episodes(
                    provider,
                    ext_id,
                    kind,  # type: ignore[arg-type]
                    season_n,
                    session,
                )
            aired, unaired = select_batch_episodes(episodes, today)
            summary.skipped_unaired += unaired
            for ep in aired:
                if await _push_one(
                    summary,
                    season_n,
                    ep["episode_number"],
                    kind=kind,
                    titles=titles,
                    year=year,
                    imdb_id=imdb_id,
                    tmdb_id=tmdb_id,
                    tvdb_id=tvdb_id,
                    anilist_id=anilist_id,
                    poster_url=poster_url,
                    media_title=media_title,
                    filters=filters,
                    limit=limit,
                    deliver=deliver,
                ):
                    break
            if summary.aborted_reason:
                break
    except Exception as exc:
        logger.exception("[ADHOC-BATCH] %s failed: %s", media_title, exc)
        summary.aborted_reason = summary.aborted_reason or "Metadata lookup failed"
    logger.info(
        "[ADHOC-BATCH] %s: pushed %d, queued %d, no release %d, in flight %d, unaired %d%s",
        media_title,
        len(summary.pushed),
        len(summary.queued),
        len(summary.no_release),
        len(summary.skipped_in_flight),
        summary.skipped_unaired,
        f", aborted: {summary.aborted_reason}" if summary.aborted_reason else "",
    )
    return summary


async def _push_one(
    summary: BatchSummary,
    season_n: int,
    episode_n: int,
    *,
    kind: str,
    titles: list[str],
    year: int | None,
    imdb_id: str | None,
    tmdb_id: int | None,
    tvdb_id: int | None,
    anilist_id: int | None,
    poster_url: str | None,
    media_title: str,
    filters: AdHocFilters,
    limit: int,
    deliver: bool | None,
) -> bool:
    """Handle one episode. Returns True when the batch must stop."""
    label = _target_label(season_n, episode_n) or ""
    async with async_session_factory() as session:
        if await has_in_flight_adhoc_push(
            session,
            tmdb_id=tmdb_id,
            anilist_id=anilist_id,
            media_title=media_title,
            target_label=label,
        ):
            summary.skipped_in_flight.append(label)
            return False
    result = await search_and_score_releases(
        kind=kind,
        scope="episode",
        titles=titles,
        year=year,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        season=season_n,
        episode=episode_n,
        filters=filters,
        limit=limit,
    )
    candidates = result.primary_results or result.fallback_results
    if result.error_message or not candidates:
        summary.no_release.append(label)
        return False
    best = candidates[0]
    async with async_session_factory() as session:
        outcome = await push_adhoc_release(
            session,
            release_link=str(best.get("link") or best.get("guid") or ""),
            nzb_title=best["title"],
            nzb_guid=best.get("guid") or None,
            score=best.get("score"),
            size_bytes=int(best.get("size") or 0),
            is_fallback=bool(best.get("is_fallback")),
            matched_language=best.get("matched_language"),
            media_title=media_title,
            media_year=year,
            media_kind=kind or None,
            poster_url=poster_url,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            anilist_id=anilist_id,
            season=season_n,
            episode=episode_n,
            deliver=deliver,
        )
    if outcome.ok:
        (summary.queued if outcome.queued else summary.pushed).append(label)
        return False
    if outcome.hard_failure:
        summary.aborted_reason = outcome.message
        return True
    summary.no_release.append(label)
    return False
