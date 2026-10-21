"""Air-date auto-push and episode upgrades.

The evaluation helpers are pure (no DB access, no network); callers supply
``now_utc``. ``run_initial_air_push`` is the DB-backed pipeline built on them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.core.automation import is_date_only_air_date, is_on_hold
from app.core.outage import has_active_outage
from app.core.parser import parse_release_name
from app.core.phase_stats import AirPushStats
from app.core.prerequisites import setup_blocks
from app.core.push_engine import (
    _dispatch_candidate_list_to_downloader,
    _load_blacklisted_sets,
    _query_show_across_indexers,
    _score_and_partition_candidates,
    execute_auto_push,
    resolve_effective_search_config,
)
from app.core.rate_limit_queue import get_downloader_cooldown_remaining
from app.core.search_lock import item_lock_manager
from app.db.database import async_session_factory
from app.db.models import (
    DownloadHistory,
    Episode,
    EpisodeStatus,
    MediaItem,
    Season,
)
from app.services.indexers import newznab as newznab_client

logger = logging.getLogger(__name__)


_RESOLUTION_RANKS = {"480p": 1, "720p": 2, "1080p": 3, "2160p": 4}
_SOURCE_RANKS = {
    "hdtv": 1,
    "web-dl": 2,
    "webrip": 2,
    "bluray": 3,
    "uhd bluray": 3,
    "bdrip": 3,
    "bluray remux": 4,
}
_LOSSLESS_AUDIO = ("truehd", "dts-hd ma", "flac", "atmos")
_NON_HDR_VALUES = {None, "", "10-bit"}


def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def is_episode_air_due(
    ep: Episode,
    delay_hours: int,
    now_utc: datetime,
    max_age_hours: int | None = None,
) -> bool:
    """True when a SEARCHING episode's air date plus delay has been reached.

    Date-only air dates (no broadcast time) only become due once that day has
    fully ended (+24h) plus the delay. With ``max_age_hours`` set, episodes older
    than ``due + max_age`` are excluded (backlog guard).
    """
    if ep.air_date is None or ep.status != EpisodeStatus.SEARCHING:
        return False
    air_date = _as_utc(ep.air_date)
    if is_date_only_air_date(air_date):
        air_date = air_date.replace(microsecond=0) + timedelta(hours=24)
    due = air_date + timedelta(hours=delay_hours)
    if now_utc < due:
        return False
    return max_age_hours is None or now_utc <= due + timedelta(hours=max_age_hours)


def is_episode_in_upgrade_window(
    ep: Episode, window_hours: int, now_utc: datetime
) -> bool:
    """True while ``now_utc`` lies within [first_pushed_at, first_pushed_at + window]."""
    if ep.first_pushed_at is None:
        return False
    start = _as_utc(ep.first_pushed_at)
    return start <= now_utc <= start + timedelta(hours=window_hours)


@dataclass(frozen=True)
class QualityTier:
    resolution_rank: int
    source_rank: int
    has_hdr: bool
    has_lossless_audio: bool


def quality_tier_from_release_name(name: str) -> QualityTier:
    """Derive a comparable quality tier from a release name."""
    parsed = parse_release_name(name)
    audio = (parsed.audio_codec or "").lower()
    return QualityTier(
        resolution_rank=_RESOLUTION_RANKS.get((parsed.resolution or "").lower(), 0),
        source_rank=_SOURCE_RANKS.get((parsed.source or "").lower(), 0),
        has_hdr=(parsed.hdr or "").lower() not in _NON_HDR_VALUES,
        has_lossless_audio=any(k in audio for k in _LOSSLESS_AUDIO),
    )


def is_candidate_valid_upgrade(
    candidate_score: float,
    candidate_is_fallback: bool,
    candidate_tier: QualityTier,
    current_score: float,
    current_is_fallback: bool,
    current_tier: QualityTier,
) -> tuple[bool, str]:
    """Strict upgrade evaluation (spec §4.2)."""
    if current_is_fallback and not candidate_is_fallback:
        return True, "Primary language upgrade over fallback release"
    if candidate_is_fallback and not current_is_fallback:
        return False, "Candidate is fallback while current is primary"
    if candidate_score <= current_score:
        return (
            False,
            f"Score {candidate_score:.1f} does not beat current {current_score:.1f}",
        )
    tier_improved = (
        candidate_tier.resolution_rank > current_tier.resolution_rank
        or candidate_tier.source_rank > current_tier.source_rank
        or (not current_tier.has_hdr and candidate_tier.has_hdr)
        or (not current_tier.has_lossless_audio and candidate_tier.has_lossless_audio)
    )
    if not tier_improved:
        return False, "Numerical score higher but quality tier has not improved"
    return (
        True,
        f"Quality tier and score improved ({candidate_score:.1f} > {current_score:.1f})",
    )


async def run_initial_air_push(
    session: AsyncSession, now_utc: datetime, *, stats: AirPushStats | None = None
) -> int:
    """Auto-push due SEARCHING episodes of opted-in series; returns episodes dispatched."""
    from app.core.transfer_poller import transfer_poller

    stmt = (
        select(MediaItem)
        .where(MediaItem.auto_push_episodes.is_(True))
        .options(
            selectinload(MediaItem.provider),
            selectinload(MediaItem.watchlist_sources),
            selectinload(MediaItem.seasons).selectinload(Season.episodes),
        )
    )
    items = list((await session.execute(stmt)).scalars().all())

    # Snapshot work up front: execute_auto_push commits, which expires loaded ORM rows.
    work: list[tuple[int, str, list[int]]] = []
    for item in items:
        if is_on_hold(item):
            continue
        due_ids = [
            ep.id
            for season in item.seasons
            for ep in season.episodes
            if is_episode_air_due(
                ep,
                settings.air_delay_hours,
                now_utc,
                max_age_hours=settings.upgrade_window_hours,
            )
            and ep.fail_count <= settings.sh_max_retries
        ]
        if due_ids:
            work.append((item.id, item.title, due_ids))

    if stats is not None:
        stats.eligible = sum(len(due_ids) for _, _, due_ids in work)
    dispatched = 0
    for item_id, title, due_ids in work:
        if not item_lock_manager.try_acquire(item_id, owner="air_push"):
            logger.debug("[AIR-PUSH] '%s' is busy; retrying next tick.", title)
            continue
        try:
            transfer_poller.register_in_flight_push(item_id, title)
            res = await execute_auto_push(session, item_id, {"episode_ids": due_ids})
            dispatched += min(int(res.get("dispatched_count") or 0), len(due_ids))
        except Exception as exc:  # noqa: BLE001
            logger.exception("[AIR-PUSH] Auto-push failed for '%s': %s", title, exc)
            await session.rollback()
        finally:
            transfer_poller.unregister_in_flight_push(item_id)
            item_lock_manager.release(item_id)
    if stats is not None:
        stats.dispatched = dispatched
    return dispatched


def _is_season_watched(item: MediaItem, season: Season) -> bool:
    """Watchlist watch signal (season granularity, as used by auto-advance)."""
    if item.watched_completed:
        return True
    watched_order = int(item.last_watched_order or 0)
    return watched_order > 0 and season.watch_order <= watched_order


def _upgrade_rate_limited() -> bool:
    return (
        get_downloader_cooldown_remaining() > 0
        or newznab_client.get_all_indexer_cooldowns_remaining() > 0
    )


async def _evaluate_episode_upgrade(
    session: AsyncSession, item_id: int, episode_id: int, now_utc: datetime
) -> bool:
    """Evaluate one episode; returns True when an upgrade was dispatched."""
    item = (
        await session.execute(
            select(MediaItem)
            .where(MediaItem.id == item_id)
            .options(
                selectinload(MediaItem.provider),
                selectinload(MediaItem.seasons).selectinload(Season.episodes),
            )
        )
    ).scalar_one()
    season = next(s for s in item.seasons for e in s.episodes if e.id == episode_id)
    ep = next(e for e in season.episodes if e.id == episode_id)

    current = (
        (
            await session.execute(
                select(DownloadHistory)
                .where(
                    DownloadHistory.episode_id == ep.id,
                    DownloadHistory.is_dismissed.is_(False),
                    DownloadHistory.status_detail == "completed",
                )
                .order_by(DownloadHistory.id.desc())
            )
        )
        .scalars()
        .first()
    )
    if current is None:
        return False

    effective_cfg = await resolve_effective_search_config(session, item)
    blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
        session, item.id
    )
    raw = await _query_show_across_indexers(
        session,
        item,
        season.season_number,
        ep.episode_number,
        effective_cfg=effective_cfg,
    )
    partitioned = await _score_and_partition_candidates(
        raw_results=raw,
        blacklisted_guids=blacklisted_guids,
        blacklisted_titles=blacklisted_titles,
        expected_title=item.title,
        expected_year=None,
        expected_alt_title=item.alt_title,
        expected_season=season.season_number,
        expected_episode=ep.episode_number,
        expected_season_title=(
            season.title if season.title and season.title != item.title else None
        ),
        runtime_minutes=item.runtime_minutes,
        effective_cfg=effective_cfg,
        media_type=(
            item.media_type.value
            if hasattr(item.media_type, "value")
            else str(item.media_type or "")
        ),
        season_episode_count=season.episode_count or len(season.episodes),
        session=session,
        item_id=item.id,
    )

    current_score = current.score or 0.0
    current_tier = quality_tier_from_release_name(current.nzb_title)
    approved: list[dict] = []
    for cand in partitioned["all_valid"]:
        ok, reason = is_candidate_valid_upgrade(
            float(cand.get("score") or 0.0),
            bool(cand.get("is_fallback", False)),
            quality_tier_from_release_name(cand["title"]),
            current_score,
            bool(current.is_fallback),
            current_tier,
        )
        if ok:
            logger.debug("[UPGRADE] '%s': %s -> %s", item.title, cand["title"], reason)
            approved.append(cand)

    dispatched = False
    if approved and not _upgrade_rate_limited():
        new_hist = await _dispatch_candidate_list_to_downloader(
            session,
            [max(approved, key=lambda c: float(c.get("score") or 0.0))],
            item=item,
            season=season,
            episode=ep,
            push_mode="auto",
            reset_fail_count=False,
            is_upgrade=True,
            replaces_history_id=current.id,
        )
        if isinstance(new_hist, DownloadHistory):
            dispatched = True
            ep.upgrades_applied += 1

    if _upgrade_rate_limited():
        await session.commit()
        return dispatched  # rate-limited: evaluation not stamped
    ep.last_upgrade_checked_at = now_utc
    await session.commit()
    return dispatched


async def run_episode_upgrades(
    session: AsyncSession, now_utc: datetime, *, stats: AirPushStats | None = None
) -> int:
    """Re-search completed in-window episodes of opted-in series; returns upgrades dispatched."""
    if settings.dry_run or _upgrade_rate_limited():
        return 0

    items = list(
        (
            await session.execute(
                select(MediaItem)
                .where(MediaItem.auto_push_episodes.is_(True))
                .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
            )
        )
        .scalars()
        .all()
    )
    # Snapshot ids: commits expire loaded ORM rows.
    work: list[tuple[int, str, list[int]]] = []
    for item in items:
        ids = [
            ep.id
            for season in item.seasons
            if not _is_season_watched(item, season)
            for ep in season.episodes
            if ep.status == EpisodeStatus.COMPLETED
            and is_episode_in_upgrade_window(ep, settings.upgrade_window_hours, now_utc)
            and ep.upgrades_applied < settings.max_upgrade_checks
        ]
        if ids:
            work.append((item.id, item.title, ids))

    if stats is not None:
        stats.eligible = sum(len(ids) for _, _, ids in work)
    upgraded = 0
    for item_id, title, ep_ids in work:
        if not item_lock_manager.try_acquire(item_id, owner="air_upgrade"):
            logger.debug("[UPGRADE] '%s' is busy; retrying next tick.", title)
            continue
        try:
            for ep_id in ep_ids:
                if _upgrade_rate_limited():
                    return upgraded
                if await _evaluate_episode_upgrade(session, item_id, ep_id, now_utc):
                    upgraded += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("[UPGRADE] Upgrade check failed for '%s': %s", title, exc)
            await session.rollback()
        finally:
            item_lock_manager.release(item_id)
    return upgraded


async def run_air_date_push_and_upgrade_tick(now_utc: datetime | None = None) -> None:
    """Hourly job: initial air pushes, then upgrades; each step's failure is isolated."""
    if await setup_blocks("air-date push"):
        return
    if has_active_outage():
        logger.info("[AIR-PUSH] Skipped: outage active")
        return
    now = now_utc or datetime.now(timezone.utc)
    async with async_session_factory() as session:
        push_stats = AirPushStats()
        try:
            await run_initial_air_push(session, now, stats=push_stats)
            logger.info(
                "[AIR-PUSH] [1/2] Air-date auto-push: %d eligible episodes found across active series",
                push_stats.eligible,
            )
        except Exception as exc:
            logger.error(
                "[AIR-PUSH] Initial air push step failed: %s", exc, exc_info=True
            )
            logger.info("[AIR-PUSH] [1/2] Air-date auto-push: failed (see error above)")
        upgrade_stats = AirPushStats()
        suffix = ""
        if settings.dry_run:
            suffix = " (dry-run)"
        elif _upgrade_rate_limited():
            suffix = " (rate-limited)"
        try:
            await run_episode_upgrades(session, now, stats=upgrade_stats)
            logger.info(
                "[AIR-PUSH] [2/2] Quality upgrade pass: %d eligible upgrade candidates found%s",
                upgrade_stats.eligible,
                suffix,
            )
        except Exception as exc:
            logger.error(
                "[UPGRADE] Episode upgrade step failed: %s", exc, exc_info=True
            )
            logger.info(
                "[AIR-PUSH] [2/2] Quality upgrade pass: failed (see error above)"
            )
    logger.info("[AIR-PUSH] Hourly pass complete")
