"""
Auto-Wake / Auto-Sleep Transfer Poller & Split Failure Recovery (v3.0.0)
========================================================================
Monitors active downloader transfers on a 20s cadence ONLY when at least one entity
is in `DOWNLOADING` status, returning to a zero-API sleep state once all active
transfers complete or fail.

Responsibilities:
1. Auto-Wake / Auto-Sleep lifecycle (`transfer_poller.wake()` / `transfer_poller.sleep()`).
2. Live progress tracking (`progress_pct`, `download_speed_bytes`, `eta_seconds`, `status_detail`)
   on active `DownloadHistory` rows.
3. Layer 3 Playable Video Verification (`is_transfer_filelist_fake`) with 10-minute
   unpack grace period for archives (`status_detail = "unpacking"`), immediate
   Tick-1 rejection for non-archive fake payloads (samples, executables),
   transitioning verified transfers to `COMPLETED` and dispatching `Ready on <downloader>` notifications.
4. Split Failure Recovery:
   - `push_mode == "auto"`: Deletes broken transfer from the downloader, blacklists the release,
     and automatically searches & pushes the next-best non-blacklisted candidate.
   - `push_mode == "manual"`: Deletes broken transfer from the downloader, blacklists the release,
     and transitions the entity to `FAILED` (preserving the row in Active Pushes until
     the user clicks `[Re-Search & Pick]` or `[Dismiss]`).
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import CursorResult, delete, exists, func, select
from sqlalchemy.orm import selectinload

from app.core.anime_titles import anime_title_for
from app.core.failure_logger import log_failure
from app.core.fake_detector import is_transfer_filelist_fake
from app.core.outage import is_queued_detail
from app.core.parser import build_release_feature_pills, parse_release_name
from app.core.prerequisites import setup_blocks
from app.core.rate_limit_queue import (
    downloader_rate_limit_detail,
    get_downloader_cooldown_remaining,
)
from app.core.time_utils import to_local_datetime
from app.db.database import async_session_factory
from app.db.models import (
    BlacklistedRelease,
    DeliveredFile,
    DeliveryStatus,
    DownloadHistory,
    Episode,
    EpisodeStatus,
    MediaItem,
    MediaStatus,
    MediaType,
    Season,
    SeasonStatus,
    SystemSettings,
)
from app.services import discord
from app.services.availability_notifier import schedule_availability_notification
from app.services.downloaders.base import (
    DOWNLOADER_DISPLAY_NAMES,
    BaseDownloader,
    DownloaderNetworkError,
    DownloaderTransfer,
    TransferState,
)
from app.services.downloaders.factory import (
    downloader_class,
    get_downloader_client_for_type,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

LOCAL_POLL_INTERVAL_SECONDS: int = 5  # SABnzbd
CLOUD_POLL_INTERVAL_SECONDS: int = 20  # cloud downloaders
RATE_LIMIT_COOLDOWN_SECONDS: int = 900

NOT_FOUND_GRACE_PERIOD_SECONDS = 900
COMPLETED_RETENTION_SECONDS = 300
FAILED_RETENTION_SECONDS = 300
UNPACK_GRACE_PERIOD_SECONDS = 600

_poller_lock = asyncio.Lock()


@dataclass
class DryRunPushRecord:
    """In-memory tracking record for a simulated dispatch during Dry-Run Mode."""

    id: str
    timestamp: datetime
    media_item_id: int | None
    media_title: str
    release_title: str
    target_badge: str
    feature_pills: list[dict[str, Any]] = field(default_factory=list)
    push_mode: str = "auto"
    score: float | None = None
    size_bytes: int | None = None
    resolution: str | None = None
    source: str | None = None
    language: str | None = None
    season_id: int | None = None
    episode_id: int | None = None
    is_upgrade: bool = False


_dry_run_registry: dict[str, DryRunPushRecord] = {}


def register_dry_run_push(
    *,
    media_item_id: int | None,
    media_title: str,
    release_title: str,
    target_badge: str,
    feature_pills: list[dict[str, Any]] | None = None,
    push_mode: str = "auto",
    score: float | None = None,
    size_bytes: int | None = None,
    resolution: str | None = None,
    source: str | None = None,
    language: str | None = None,
    season_id: int | None = None,
    episode_id: int | None = None,
) -> DryRunPushRecord:
    """Register a simulated dispatch in the in-memory dry-run registry."""
    rec_id = f"dry-run-{uuid.uuid4().hex[:8]}"
    record = DryRunPushRecord(
        id=rec_id,
        timestamp=datetime.now(timezone.utc),
        media_item_id=media_item_id,
        media_title=media_title,
        release_title=release_title,
        target_badge=target_badge,
        feature_pills=feature_pills or [],
        push_mode=push_mode,
        score=score,
        size_bytes=size_bytes,
        resolution=resolution,
        source=source,
        language=language,
        season_id=season_id,
        episode_id=episode_id,
    )
    _dry_run_registry[rec_id] = record
    return record


def dismiss_dry_run_push(push_id: str) -> bool:
    """Dismiss a synthetic dry-run record from memory by ID."""
    if push_id in _dry_run_registry:
        del _dry_run_registry[push_id]
        return True
    return False


def get_dry_run_records() -> list[DryRunPushRecord]:
    """Return all active synthetic dry-run records, expiring entries older than 5m (300s)."""
    now_utc = datetime.now(timezone.utc)
    expired = [
        rid
        for rid, rec in _dry_run_registry.items()
        if (
            now_utc
            - (
                rec.timestamp
                if rec.timestamp.tzinfo
                else rec.timestamp.replace(tzinfo=timezone.utc)
            )
        ).total_seconds()
        >= COMPLETED_RETENTION_SECONDS
    ]
    for rid in expired:
        _dry_run_registry.pop(rid, None)
    return list(_dry_run_registry.values())


def clear_dry_run_pushes() -> None:
    """Clear all in-memory dry-run records (testing/reset helper)."""
    _dry_run_registry.clear()


class TransferPollerState:
    """In-memory runtime state for the Auto-Wake / Auto-Sleep Transfer Poller."""

    def __init__(self) -> None:
        self.is_awake: bool = False
        self.last_polled_at: datetime | None = None
        self.active_count: int = 0
        self._scheduler: Any | None = None
        self.interval_seconds: int = CLOUD_POLL_INTERVAL_SECONDS
        # Maps item_id -> human-readable item title currently being searched/dispatched in background
        self.in_flight_pushes: dict[int, str] = {}

    @property
    def last_tick_at(self) -> datetime | None:
        return self.last_polled_at

    @last_tick_at.setter
    def last_tick_at(self, val: datetime | None) -> None:
        self.last_polled_at = val

    def set_scheduler(self, scheduler: Any) -> None:
        self._scheduler = scheduler

    def register_in_flight_push(self, item_id: int, title: str) -> None:
        self.in_flight_pushes[item_id] = title
        self.wake()

    def unregister_in_flight_push(self, item_id: int) -> None:
        self.in_flight_pushes.pop(item_id, None)

    @property
    def has_in_flight_pushes(self) -> bool:
        return len(self.in_flight_pushes) > 0

    @property
    def in_flight_titles(self) -> list[str]:
        return list(self.in_flight_pushes.values())

    def wake(self, active_types: set[str] | None = None) -> None:
        """Wake the poller; with ``active_types`` also pick the effective cadence.

        5 s iff every active row is SABnzbd (all-local), else 20 s.
        """
        was_awake = self.is_awake
        self.is_awake = True
        if self._scheduler is not None:
            try:
                if active_types is not None:
                    interval = (
                        LOCAL_POLL_INTERVAL_SECONDS
                        if active_types and active_types <= {"sabnzbd"}
                        else CLOUD_POLL_INTERVAL_SECONDS
                    )
                    if interval != self.interval_seconds:
                        from apscheduler.triggers.interval import IntervalTrigger

                        self._scheduler.reschedule_job(
                            "transfer_poller_job",
                            trigger=IntervalTrigger(seconds=interval),
                        )
                        self.interval_seconds = interval
                        logger.info("Transfer Poller cadence set to %ds.", interval)
                job = self._scheduler.get_job("transfer_poller_job")
                if job is not None and job.next_run_time is None:
                    self._scheduler.resume_job("transfer_poller_job")
                    logger.info("⚡ Transfer Poller resumed in APScheduler.")
            except Exception as exc:
                logger.warning("Could not resume transfer_poller_job: %s", exc)
        if not was_awake:
            logger.info(
                "⚡ Transfer Poller woken up — active downloads or in-flight pushes detected."
            )

    def sleep(self, active_count: int = 0) -> None:
        if self.has_in_flight_pushes or active_count > 0:
            # Never sleep while a background Auto-Push is still querying indexers / uploading NZBs
            self.is_awake = True
            self.active_count = active_count
            return
        if self.is_awake:
            logger.info(
                "💤 Transfer Poller going to sleep — 0 active downloads remaining."
            )
        self.is_awake = False
        self.active_count = active_count
        if self._scheduler is not None:
            try:
                job = self._scheduler.get_job("transfer_poller_job")
                if job is not None and job.next_run_time is not None:
                    self._scheduler.pause_job("transfer_poller_job")
                    logger.info(
                        "💤 Transfer Poller paused in APScheduler (zero idle CPU/DB usage)."
                    )
            except Exception as exc:
                logger.warning("Could not pause transfer_poller_job: %s", exc)


transfer_poller = TransferPollerState()


def wake_transfer_poller() -> None:
    """Wake the transfer poller when a new push is dispatched to the downloader."""
    transfer_poller.wake()


async def count_downloading_entities(session: AsyncSession) -> int:
    """Count all active downloading push targets across Movies, Season Packs, and Episodes.

    Season rows in DOWNLOADING state are only counted when they have zero child Episode
    rows in DOWNLOADING state (true season-pack downloads), ensuring a single downloading
    episode reports DOWNLOADING: 1 rather than 2.
    """
    movies_c = (
        await session.execute(
            select(func.count(MediaItem.id)).where(
                MediaItem.status == MediaStatus.DOWNLOADING,
                MediaItem.media_type == MediaType.MOVIE,
            )
        )
    ).scalar() or 0

    seasons_c = (
        await session.execute(
            select(func.count(Season.id)).where(
                Season.status == SeasonStatus.DOWNLOADING,
                ~exists(
                    select(Episode.id).where(
                        Episode.season_id == Season.id,
                        Episode.status == EpisodeStatus.DOWNLOADING,
                    )
                ),
            )
        )
    ).scalar() or 0

    episodes_c = (
        await session.execute(
            select(func.count(Episode.id)).where(
                Episode.status == EpisodeStatus.DOWNLOADING
            )
        )
    ).scalar() or 0

    adhoc_c = (
        await session.execute(
            select(func.count(DownloadHistory.id)).where(
                DownloadHistory.is_ad_hoc.is_(True),
                DownloadHistory.is_dry_run.is_(False),
                DownloadHistory.downloader_transfer_id.is_not(None),
                func.lower(func.coalesce(DownloadHistory.status_detail, "")).in_(
                    ACTIVE_POLLABLE_STATUS_DETAILS
                ),
            )
        )
    ).scalar() or 0

    return int(movies_c + seasons_c + episodes_c + adhoc_c)


def _normalize_progress_pct(raw_progress: Any, status_lower: str) -> float:
    """Normalize downloader progress (either 0.0..1.0 or 0.0..100.0) into 0.0..100.0 percentage."""
    if status_lower in ("completed", "cached"):
        return 100.0
    try:
        val = float(raw_progress or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if val < 0.0:
        return 0.0
    if 0.0 < val <= 1.0:
        return round(val * 100.0, 1)
    return round(min(val, 100.0), 1)


def _is_terminal_failure_state(status_lower: str) -> bool:
    """Check if a downloader status string represents a permanent terminal failure."""
    if not status_lower:
        return False
    return (
        status_lower.startswith("failed")
        or status_lower.startswith("error")
        or "aborted" in status_lower
        or "repair failed" in status_lower
        or "not-complete" in status_lower
        or status_lower
        in (
            "cannot be re-completed",
            "not enough repair blocks",
            "not_found",
        )
    )


async def _delete_and_purge_transfer(
    session: AsyncSession,
    transfer_id: str | None,
    tb_item: dict[str, Any] | None = None,
    downloader_type: str | None = "torbox",
) -> None:
    """Delete a failed/canceled transfer from the row's own downloader (best-effort)."""
    if not transfer_id:
        return
    dtype = downloader_type or "torbox"
    if dtype != "torbox":
        try:
            client = await get_downloader_client_for_type(session, dtype)
            if client is None:
                logger.warning(
                    "Cannot delete %s transfer %s: downloader provider removed.",
                    dtype,
                    transfer_id,
                )
                return
            if not await client.delete_transfer(str(transfer_id), delete_files=True):
                logger.warning(
                    "%s refused delete of transfer %s.",
                    client.display_name,
                    transfer_id,
                )
        except Exception as exc:
            logger.warning(
                "Failed to delete %s transfer %s: %s", dtype, transfer_id, exc
            )
        return
    tb_type = tb_item.get("_type", "usenet") if tb_item else "usenet"
    try:
        await downloader_class("torbox").delete_raw_transfer(
            transfer_id, kind=tb_type, session=session
        )
    except Exception as exc:
        logger.warning(
            "Failed to delete %s %s from downloader: %s", tb_type, transfer_id, exc
        )


async def _purge_for_row(
    session: AsyncSession,
    transfer_id: str | None,
    downloader_type: str | None,
    tb_item: dict[str, Any] | None = None,
) -> None:
    """Delete via the row's own downloader."""
    await _delete_and_purge_transfer(
        session, transfer_id, tb_item, downloader_type=downloader_type
    )


async def _finalize_upgrade_replacement(
    session: AsyncSession, new_history: DownloadHistory
) -> None:
    """On upgrade completion, purge the superseded release and mark its history replaced."""
    if new_history.replaces_history_id is None:
        return
    old = (
        await session.execute(
            select(DownloadHistory).where(
                DownloadHistory.id == new_history.replaces_history_id
            )
        )
    ).scalar_one_or_none()
    if old is None or old.status_detail == "replaced":
        return
    await _purge_for_row(session, old.downloader_transfer_id, old.downloader_type)
    old.status_detail = "replaced"
    old.is_dismissed = True
    old.notification_sent = True


async def _has_live_replaced_release(
    session: AsyncSession, history: DownloadHistory
) -> bool:
    """True iff `history` is an upgrade whose superseded release still exists and is not yet replaced."""
    if not history.is_upgrade or history.replaces_history_id is None:
        return False
    old = (
        await session.execute(
            select(DownloadHistory).where(
                DownloadHistory.id == history.replaces_history_id
            )
        )
    ).scalar_one_or_none()
    return old is not None and old.status_detail != "replaced"


async def _revert_failed_upgrade(
    session: AsyncSession,
    history: DownloadHistory,
    episode: Episode | None,
    season: Season | None,
    reason: str,
) -> bool:
    """Return the episode to its previous release when an upgrade download fails."""
    if episode is None or not await _has_live_replaced_release(session, history):
        return False
    episode.status = EpisodeStatus.COMPLETED
    history.status_detail = f"failed: {reason}"
    history.is_dismissed = True
    history.notification_sent = True
    history.download_speed_bytes = 0
    history.eta_seconds = None
    _sync_season_status_from_episodes(season)
    return True


async def _auto_replace_failed_push(
    session: AsyncSession,
    item: MediaItem,
    season: Season | None = None,
    episode: Episode | None = None,
) -> DownloadHistory | None:
    """Search and push the next-best non-blacklisted release candidate for a failed Auto-Push."""
    from sqlalchemy import inspect as sa_inspect

    from app.core.push_engine import (
        _dispatch_candidate_list_to_downloader,
        _load_blacklisted_sets,
        _push_single_season_or_movie_entry,
        _query_movie_across_indexers,
        _query_show_across_indexers,
        _score_and_partition_candidates,
        resolve_effective_search_config,
    )

    if get_downloader_cooldown_remaining() > 0:
        logger.warning(
            "Skipping auto-replacement search for '%s' — downloader cooldown active.",
            item.title,
        )
        return None

    effective_cfg = await resolve_effective_search_config(session, item)
    blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
        session, item.id
    )

    item_media_type = (
        item.media_type.value
        if hasattr(item.media_type, "value")
        else str(item.media_type or "")
    )

    if episode is not None:
        if season is None:
            if (
                "season" not in sa_inspect(episode).unloaded
                and episode.season is not None
            ):
                season = episode.season
            elif episode.season_id is not None:
                season = await session.get(Season, episode.season_id)
        if season is None:
            return None
        effective_s_num = int(season.type_number or season.season_number)
        raw_results = await _query_show_across_indexers(
            session,
            item,
            season_number=effective_s_num,
            episode_number=episode.episode_number,
        )
        partitioned = await _score_and_partition_candidates(
            raw_results=raw_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=item.title,
            expected_year=None,
            expected_alt_title=item.alt_title,
            expected_season=effective_s_num,
            expected_episode=episode.episode_number,
            expected_season_title=(
                season.title if season.title and season.title != item.title else None
            ),
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type=item_media_type,
            season_episode_count=season.episode_count,
            session=session,
            item_id=item.id,
        )
        res = await _dispatch_candidate_list_to_downloader(
            session,
            partitioned["all_valid"],
            item=item,
            season=season,
            episode=episode,
            push_mode="auto",
            event_type="push_initiated",
            reset_fail_count=False,
        )
        return res if isinstance(res, DownloadHistory) else None

    if season is not None:
        entry_type = getattr(season, "entry_type", "season") or "season"
        if entry_type != "movie":
            if "episodes" in sa_inspect(season).unloaded:
                await session.refresh(season, ["episodes"])
            has_completed_or_downloaded_eps = any(
                ep.status in (EpisodeStatus.COMPLETED, EpisodeStatus.DOWNLOADED)
                for ep in season.episodes
            )
            # Revert episodes that were marked DOWNLOADING solely by the failed Season Pack
            for ep in season.episodes:
                if ep.status == EpisodeStatus.DOWNLOADING:
                    ep.status = EpisodeStatus.SEARCHING
            await session.commit()
            if has_completed_or_downloaded_eps:
                has_missing_eps = any(
                    ep.status
                    not in (
                        EpisodeStatus.COMPLETED,
                        EpisodeStatus.DOWNLOADED,
                        EpisodeStatus.DOWNLOADING,
                        EpisodeStatus.FUTURE,
                    )
                    for ep in season.episodes
                )
                if not has_missing_eps:
                    return None
                effective_cfg = {
                    **effective_cfg,
                    "allow_season_packs": False,
                    "prefer_season_packs": False,
                }

        hists = await _push_single_season_or_movie_entry(
            session=session,
            item=item,
            season=season,
            effective_cfg=effective_cfg,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            is_auto_advance=False,
            reset_fail_count=False,
        )
        return hists[0] if isinstance(hists, list) and hists else None

    # Standalone Movie
    raw_results = await _query_movie_across_indexers(session, item)
    partitioned = await _score_and_partition_candidates(
        raw_results=raw_results,
        blacklisted_guids=blacklisted_guids,
        blacklisted_titles=blacklisted_titles,
        expected_title=item.title,
        expected_year=item.year,
        expected_alt_title=item.alt_title,
        expected_season=None,
        expected_episode=None,
        expected_season_title=None,
        runtime_minutes=item.runtime_minutes,
        effective_cfg=effective_cfg,
        media_type="movie",
        session=session,
        item_id=item.id,
    )
    res_movie = await _dispatch_candidate_list_to_downloader(
        session,
        partitioned["all_valid"],
        item=item,
        season=None,
        episode=None,
        push_mode="auto",
        event_type="push_initiated",
        reset_fail_count=False,
    )
    return res_movie if isinstance(res_movie, DownloadHistory) else None


def _match_downloader_transfer(
    history: DownloadHistory, tb_map: dict[str, dict[str, Any]]
) -> tuple[str, dict[str, Any] | None]:
    """Match a DownloadHistory row against the downloader's transfer dictionary by ID, hash, or title."""
    tb_item: dict[str, Any] | None = None
    if history.downloader_transfer_id:
        tb_item = tb_map.get(str(history.downloader_transfer_id))

    if tb_item is None and history.downloader_hash:
        for cand in tb_map.values():
            if str(cand.get("hash") or "").lower() == history.downloader_hash.lower():
                history.downloader_transfer_id = str(cand.get("id"))
                tb_item = cand
                break

    if tb_item is None and history.nzb_title:
        for cand in tb_map.values():
            tb_name = str(cand.get("name") or cand.get("title") or "")
            if tb_name and (
                history.nzb_title.lower() in tb_name.lower()
                or tb_name.lower() in history.nzb_title.lower()
            ):
                history.downloader_transfer_id = str(cand.get("id"))
                tb_item = cand
                break

    if tb_item is None:
        if not history.downloader_transfer_id:
            return "downloading", None
        now_utc = datetime.now(timezone.utc)
        sent_at = history.dispatched_at
        if sent_at and sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        if (
            sent_at
            and (now_utc - sent_at).total_seconds() < NOT_FOUND_GRACE_PERIOD_SECONDS
        ):
            return "downloading", None
        return "not_found", None

    return str(tb_item.get("download_state") or "downloading"), tb_item


_STATE_TO_DOWNLOAD_STATE: dict[TransferState, str] = {
    TransferState.QUEUED: "queued",
    TransferState.DOWNLOADING: "downloading",
    TransferState.UNPACKING: "processing",
    TransferState.COMPLETED: "completed",
    TransferState.FAILED: "failed",
}


def _transfer_to_item(transfer: DownloaderTransfer) -> dict[str, Any]:
    """Project a normalized ``DownloaderTransfer`` onto the poller's transfer-dict shape."""
    return {
        "id": transfer.transfer_id,
        "name": transfer.name,
        "download_state": _STATE_TO_DOWNLOAD_STATE[transfer.state],
        "progress": transfer.progress_pct,
        "download_speed": transfer.speed_bytes,
        "eta": transfer.eta_seconds,
        "fail_message": transfer.fail_message,
        "_type": "usenet",
    }


async def _fetch_adapter_transfers(
    session: AsyncSession, downloader_type: str
) -> tuple[BaseDownloader | None, dict[str, dict[str, Any]] | None]:
    """Fetch live transfers for one adapter-based downloader type.

    Returns (client, map); map is None when the type must be skipped this tick
    (provider removed or network error).
    """
    client = await get_downloader_client_for_type(session, downloader_type)
    if client is None:
        return None, None
    try:
        transfers = await client.get_transfers()
    except DownloaderNetworkError as exc:
        logger.warning(
            "[POLLER] %s unreachable, skipping this tick: %s",
            client.display_name,
            exc,
        )
        return client, None
    return client, {t.transfer_id: _transfer_to_item(t) for t in transfers}


_OUTAGE_DETAIL_RE = re.compile(r"^paused: (indexer|downloader) outage \((.+)\)$")


def _is_history_terminal(status_detail: str | None) -> bool:
    detail = (status_detail or "").strip().lower()
    return detail in (
        "completed",
        "deleted",
        "expired",
        "replaced",
        "canceled",
    ) or detail.startswith("failed")


def _sync_season_status_from_episodes(season: Season | None) -> None:
    """Roll up Episode statuses to Season.status across any non-ignored season with episodes."""
    if season is None or season.status == SeasonStatus.IGNORED:
        return
    from sqlalchemy import inspect as sa_inspect

    hist_list = (
        list(season.download_history or [])
        if "download_history" not in sa_inspect(season).unloaded
        else []
    )
    has_active_pack = any(
        not h.is_dismissed
        and bool(h.downloader_transfer_id or h.downloader_hash)
        and h.episode_id is None
        and not _is_history_terminal(h.status_detail)
        for h in hist_list
    )
    if has_active_pack:
        season.status = SeasonStatus.DOWNLOADING
        return

    has_failed_pack = any(
        not h.is_dismissed
        and h.episode_id is None
        and (h.status_detail or "").strip().lower().startswith("failed")
        for h in hist_list
    )
    has_rate_limited_pack = any(
        not h.is_dismissed
        and h.episode_id is None
        and is_queued_detail(h.status_detail)
        for h in hist_list
    )

    eps = (
        list(season.episodes or [])
        if "episodes" not in sa_inspect(season).unloaded
        else []
    )
    if not eps:
        return

    non_future_eps = [ep for ep in eps if ep.status != EpisodeStatus.FUTURE]

    if not non_future_eps:
        # All episodes are still EpisodeStatus.FUTURE: preserve season status as FUTURE
        now_utc = datetime.now(timezone.utc)
        season_air = season.air_date
        if season_air is not None and season_air.tzinfo is None:
            season_air = season_air.replace(tzinfo=timezone.utc)
        if season_air is None or season_air > now_utc:
            season.status = SeasonStatus.FUTURE
        return

    if has_active_pack or any(
        ep.status == EpisodeStatus.DOWNLOADING for ep in non_future_eps
    ):
        season.status = SeasonStatus.DOWNLOADING
        return

    if has_rate_limited_pack or any(
        ep.status == EpisodeStatus.RATE_LIMITED for ep in non_future_eps
    ):
        season.status = SeasonStatus.RATE_LIMITED
        return

    if any(ep.status == EpisodeStatus.FAILED for ep in non_future_eps) or (
        has_failed_pack and season.status == SeasonStatus.FAILED
    ):
        season.status = SeasonStatus.FAILED
        return

    if all(
        ep.status in (EpisodeStatus.COMPLETED, EpisodeStatus.DOWNLOADED)
        for ep in non_future_eps
    ):
        season.status = SeasonStatus.COMPLETED
        return

    if any(
        ep.status == EpisodeStatus.SEARCHING for ep in non_future_eps
    ) or season.status in (
        SeasonStatus.FUTURE,
        SeasonStatus.DOWNLOADING,
        SeasonStatus.COMPLETED,
        SeasonStatus.DOWNLOADED,
        SeasonStatus.PENDING,
        SeasonStatus.FAILED,
    ):
        season.status = SeasonStatus.SEARCHING


async def _count_active_transfers_for_item(
    session: AsyncSession, media_item_id: int
) -> int:
    """Count active non-terminal transfers for a specific MediaItem."""
    stmt = (
        select(DownloadHistory)
        .where(
            DownloadHistory.media_item_id == media_item_id,
            DownloadHistory.is_dismissed.is_(False),
        )
        .options(
            selectinload(DownloadHistory.media_item),
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    active = 0
    for h in rows:
        if _is_history_terminal(h.status_detail):
            continue
        if h.episode is not None:
            if h.episode.status == EpisodeStatus.DOWNLOADING:
                active += 1
        elif h.season is not None:
            if h.season.status == SeasonStatus.DOWNLOADING:
                active += 1
        elif h.media_item is not None:
            if h.media_item.status == MediaStatus.DOWNLOADING:
                active += 1
    return active


def _downloader_name(h: DownloadHistory) -> str:
    return DOWNLOADER_DISPLAY_NAMES.get(h.downloader_type, "downloader")


def _build_failure_reason_for_history(
    history: DownloadHistory, sh_max_retries: int
) -> str:
    detail = (history.status_detail or "").strip()
    raw_reason = (
        detail[len("failed: ") :].strip()
        if detail.lower().startswith("failed:")
        else (detail or f"{_downloader_name(history)} transfer failed")
    )
    push_mode = (history.push_mode or "auto").strip().lower()
    if push_mode == "manual":
        return f"Manual pick failed ({raw_reason}). Click Re-Search & Pick in Active Pushes."

    target: Any = history.episode or history.season or history.media_item
    fail_count = int(getattr(target, "fail_count", 0) or 0) if target is not None else 0
    if fail_count > sh_max_retries:
        return (
            f"Auto-push failed ({raw_reason}) and max replacement attempts "
            f"({sh_max_retries}) were exhausted."
        )
    return f"Push failed ({raw_reason}) and no replacement candidates were found."


async def _dispatch_settled_show_notifications(
    session: AsyncSession,
    affected_items: dict[int, MediaItem],
    sh_max_retries: int,
) -> None:
    """For each affected MediaItem that now has 0 active downloading transfers,
    dispatch at most 1 consolidated Ready-on-downloader and/or 1 consolidated Failure notification.
    """
    for item_id, item in affected_items.items():
        remaining_for_item = await _count_active_transfers_for_item(session, item_id)
        if remaining_for_item > 0:
            continue

        stmt = (
            select(DownloadHistory)
            .where(
                DownloadHistory.media_item_id == item_id,
                DownloadHistory.is_dismissed.is_(False),
                DownloadHistory.notification_sent.is_(False),
            )
            .options(
                selectinload(DownloadHistory.media_item),
                selectinload(DownloadHistory.season),
                selectinload(DownloadHistory.episode).selectinload(Episode.season),
            )
            .order_by(DownloadHistory.id.asc())
        )
        unnotified_rows = list((await session.execute(stmt)).scalars().all())
        if not unnotified_rows:
            continue

        completed_rows = [
            h
            for h in unnotified_rows
            if (h.status_detail or "").strip().lower() == "completed"
        ]
        failed_rows = [
            h
            for h in unnotified_rows
            if (h.status_detail or "").strip().lower().startswith("failed")
        ]
        if not completed_rows and not failed_rows:
            continue

        # Commit notification_sent = True BEFORE outbound webhook HTTP calls
        for h in completed_rows + failed_rows:
            h.notification_sent = True
        await session.commit()

        upgrade_rows = [h for h in completed_rows if h.is_upgrade]
        regular_rows = [h for h in completed_rows if not h.is_upgrade]

        if regular_rows:
            await discord.dispatch_batch_notification_event(
                session=session,
                event_type="completed",
                histories=regular_rows,
                item=item,
                reason=f"Ready on {_downloader_name(regular_rows[0])} (Verified Playable Video)",
            )

        if upgrade_rows:
            await discord.dispatch_batch_notification_event(
                session=session,
                event_type="completed",
                histories=upgrade_rows,
                item=item,
                reason=f"⬆️ Upgraded: Ready on {_downloader_name(upgrade_rows[0])} (Verified Playable Video)",
            )

        if failed_rows:
            if len(failed_rows) == 1:
                fail_reason = _build_failure_reason_for_history(
                    failed_rows[0], sh_max_retries=sh_max_retries
                )
            else:
                first_reason = _build_failure_reason_for_history(
                    failed_rows[0], sh_max_retries=sh_max_retries
                )
                fail_reason = f"{len(failed_rows)} transfers failed ({first_reason})"
            await discord.dispatch_batch_notification_event(
                session=session,
                event_type="failure",
                histories=failed_rows,
                item=item,
                reason=fail_reason,
            )


async def run_transfer_poller_tick(
    session: AsyncSession | None = None,
    pre_fetched_downloads: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute a single tick of the Auto-Wake / Auto-Sleep Transfer Poller.

    Guarded by `_poller_lock` so concurrent invocations from the 8s HTMX active-pushes
    endpoint and the 20s APScheduler job never overlap.
    """
    if await setup_blocks("transfer poller", only=("downloader",)):
        return {
            "polled": False,
            "active_count": 0,
            "remaining_active": 0,
            "sleeping": not transfer_poller.is_awake,
        }
    async with _poller_lock:
        if session is None:
            async with async_session_factory() as owned_session:
                await purge_expired_dry_run_history(owned_session)
                return await _run_transfer_poller_tick_with_session(
                    owned_session, pre_fetched_downloads=pre_fetched_downloads
                )
        await purge_expired_dry_run_history(session)
        return await _run_transfer_poller_tick_with_session(
            session, pre_fetched_downloads=pre_fetched_downloads
        )


ACTIVE_POLLABLE_STATUS_DETAILS = (
    "downloading",
    "queued",
    "processing",
    "unpacking",
    "verifying",
    "pending",
    "",
)


async def _run_transfer_poller_tick_with_session(
    session: AsyncSession,
    pre_fetched_downloads: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    active_count = await count_downloading_entities(session)
    in_flight_count = len(transfer_poller.in_flight_pushes)
    if active_count == 0 and in_flight_count == 0:
        transfer_poller.sleep(0)
        return {
            "polled": False,
            "active_count": 0,
            "remaining_active": 0,
            "sleeping": not transfer_poller.is_awake,
        }

    logger.debug(
        "[POLLER] Tick executing: %d active downloading transfer(s), %d in-flight dispatch(es).",
        active_count,
        in_flight_count,
    )

    transfer_poller.last_polled_at = datetime.now(timezone.utc)

    active_types = set(
        (
            await session.execute(
                select(DownloadHistory.downloader_type)
                .where(
                    DownloadHistory.is_dismissed.is_(False),
                    func.lower(func.coalesce(DownloadHistory.status_detail, "")).in_(
                        ACTIVE_POLLABLE_STATUS_DETAILS
                    ),
                )
                .distinct()
            )
        )
        .scalars()
        .all()
    )
    transfer_poller.wake(active_types)
    adapter_types = sorted(t for t in active_types if t != "torbox")
    tb_map: dict[str, dict[str, Any]] = {}
    if (
        pre_fetched_downloads is not None
        or not adapter_types
        or "torbox" in active_types
    ):
        if pre_fetched_downloads is None:
            raw_downloads = await downloader_class("torbox").fetch_raw_transfers(
                bypass_cache=False, session=session
            )
        else:
            raw_downloads = pre_fetched_downloads

        for d in raw_downloads:
            d.setdefault("_type", "usenet")
            if d.get("id") is not None:
                tb_map[str(d["id"])] = d

        if tb_map:
            await reconcile_completed_history(session, "torbox", set(tb_map.keys()))

    transfer_maps: dict[str, dict[str, dict[str, Any]]] = {"torbox": tb_map}
    clients: dict[str, BaseDownloader] = {}
    skipped_types: set[str] = set()
    removed_types: set[str] = set()
    for dtype in adapter_types:
        client, live_map = await _fetch_adapter_transfers(session, dtype)
        if client is None:
            removed_types.add(dtype)
            continue
        clients[dtype] = client
        if live_map is None:
            skipped_types.add(dtype)
            continue
        transfer_maps[dtype] = live_map
        if live_map:
            await reconcile_completed_history(session, dtype, set(live_map.keys()))

    sys_settings = (
        (await session.execute(select(SystemSettings).where(SystemSettings.id == 1)))
        .scalars()
        .first()
    )
    download_timeout_minutes = (
        sys_settings.download_timeout_minutes if sys_settings else 60
    )
    sh_max_retries = (
        sys_settings.sh_max_retries
        if sys_settings and sys_settings.sh_max_retries is not None
        else 3
    )

    # Load active downloading Movies, Seasons, and Episodes
    movies = (
        (
            await session.execute(
                select(MediaItem)
                .where(
                    MediaItem.status == MediaStatus.DOWNLOADING,
                    MediaItem.media_type == MediaType.MOVIE,
                )
                .options(
                    selectinload(MediaItem.download_history),
                    selectinload(MediaItem.provider),
                )
            )
        )
        .scalars()
        .all()
    )

    seasons = (
        (
            await session.execute(
                select(Season)
                .where(Season.status == SeasonStatus.DOWNLOADING)
                .options(
                    selectinload(Season.download_history),
                    selectinload(Season.episodes),
                    selectinload(Season.media_item).selectinload(MediaItem.provider),
                    selectinload(Season.media_item).selectinload(MediaItem.seasons),
                )
            )
        )
        .scalars()
        .all()
    )

    episodes = (
        (
            await session.execute(
                select(Episode)
                .where(Episode.status == EpisodeStatus.DOWNLOADING)
                .options(
                    selectinload(Episode.download_history),
                    selectinload(Episode.season).selectinload(Season.episodes),
                    selectinload(Episode.season).selectinload(Season.download_history),
                    selectinload(Episode.season)
                    .selectinload(Season.media_item)
                    .selectinload(MediaItem.provider),
                    selectinload(Episode.season)
                    .selectinload(Season.media_item)
                    .selectinload(MediaItem.seasons),
                )
            )
        )
        .scalars()
        .all()
    )

    # Collect unique (target, history, item, season, episode) tuples to evaluate
    targets_to_evaluate: list[
        tuple[
            MediaItem | Season | Episode,
            DownloadHistory,
            MediaItem,
            Season | None,
            Episode | None,
        ]
    ] = []

    def _sent_at_utc(h: DownloadHistory) -> datetime:
        dt = h.dispatched_at
        if dt is None:
            return datetime.min.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _latest_history(
        histories: list[DownloadHistory],
        require_episode_none: bool = False,
    ) -> DownloadHistory | None:
        candidates = [
            h
            for h in histories
            if not h.is_dismissed
            and bool(h.downloader_transfer_id or h.downloader_hash)
            and (h.status_detail or "").strip().lower()
            in ACTIVE_POLLABLE_STATUS_DETAILS
            and (not require_episode_none or h.episode_id is None)
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda h: (
                _sent_at_utc(h),
                h.id or 0,
            ),
        )

    completed_item_ids: set[int] = set()
    delivery_candidates: list[DownloadHistory] = []
    affected_items: dict[int, MediaItem] = {}

    for m in movies:
        hist = _latest_history(m.download_history)
        if hist is not None:
            targets_to_evaluate.append((m, hist, m, None, None))
        elif not transfer_poller.has_in_flight_pushes and m.id is not None:
            affected_items[m.id] = m

    for s in seasons:
        hist = _latest_history(s.download_history, require_episode_none=True)
        if hist is not None:
            targets_to_evaluate.append((s, hist, s.media_item, s, None))
        elif (
            not transfer_poller.has_in_flight_pushes
            and s.media_item is not None
            and s.media_item.id is not None
        ):
            affected_items[s.media_item.id] = s.media_item

    for e in episodes:
        hist = _latest_history(e.download_history)
        if hist is not None:
            targets_to_evaluate.append((e, hist, e.season.media_item, e.season, e))
        elif (
            not transfer_poller.has_in_flight_pushes
            and e.season is not None
            and e.season.media_item is not None
            and e.season.media_item.id is not None
        ):
            affected_items[e.season.media_item.id] = e.season.media_item

    now_utc = datetime.now(timezone.utc)

    for target, history, item, season, episode in targets_to_evaluate:
        if item.id is not None:
            affected_items[item.id] = item

        dtype = history.downloader_type or "torbox"
        if dtype in removed_types:
            history.status_detail = "downloader_removed"
            history.download_speed_bytes = 0
            history.eta_seconds = None
            logger.warning(
                "[POLLER] Downloader provider '%s' removed; transfer #%s marked failed.",
                dtype,
                history.downloader_transfer_id,
            )
            if isinstance(target, Episode):
                target.status = EpisodeStatus.SEARCHING
            elif isinstance(target, Season):
                target.status = SeasonStatus.SEARCHING
            else:
                target.status = MediaStatus.SEARCHING
            continue
        if dtype in skipped_types:
            continue
        client = clients.get(dtype)
        display_name = (
            client.display_name
            if client
            else DOWNLOADER_DISPLAY_NAMES.get(dtype, "downloader")
        )
        raw_status, tb_item = _match_downloader_transfer(
            history, transfer_maps.get(dtype, {})
        )
        status_lower = raw_status.lower()

        sent_at = history.dispatched_at
        if sent_at and sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        elapsed_seconds = (now_utc - sent_at).total_seconds() if sent_at else 0.0

        # Update live progress metrics from downloader snapshot if available
        if tb_item is not None:
            history.progress_pct = _normalize_progress_pct(
                tb_item.get("progress"), status_lower
            )
            history.download_speed_bytes = int(
                tb_item.get("download_speed") or tb_item.get("speed") or 0
            )
            raw_eta = tb_item.get("eta")
            if raw_eta is not None:
                try:
                    history.eta_seconds = max(0, int(raw_eta))
                except (TypeError, ValueError):
                    history.eta_seconds = None
            else:
                history.eta_seconds = None

        tb_id_str = str(history.downloader_transfer_id or "unknown")
        speed_str = _format_speed_and_eta(int(history.download_speed_bytes or 0), None)
        eta_display = (
            f"{int(history.eta_seconds)}s"
            if history.eta_seconds is not None
            else "unknown"
        )
        logger.debug(
            "[POLLER] Transfer #%s ('%s'): %.1f%% | Speed: %s | ETA: %s | Status: %s",
            tb_id_str,
            history.nzb_title,
            float(history.progress_pct or 0.0),
            speed_str,
            eta_display,
            status_lower or "downloading",
        )

        # 1. Check Completed / Cached -> Immediate Layer 3 Playable Video Verification
        is_completed = (
            status_lower in ("completed", "cached", "paused")
            or bool(tb_item and tb_item.get("download_finished") is True)
        ) and status_lower not in ("downloading", "processing", "queued", "unknown")
        if is_completed and tb_item is not None:
            skip_layer3 = False
            tb_files: list[Any]
            if client is None:
                raw_files = tb_item.get("files")
                tb_files = (
                    raw_files
                    if isinstance(raw_files, list)
                    else [{"name": tb_item.get("name") or tb_item.get("title") or ""}]
                )
            elif client.supports_file_listing:
                try:
                    tb_files = list(
                        await client.get_transfer_files(
                            str(history.downloader_transfer_id)
                        )
                    )
                except DownloaderNetworkError:
                    continue
            else:
                tb_files = []
                skip_layer3 = True  # local client (SABnzbd): completed = verified
            is_movie = episode is None and (
                season is None or getattr(season, "entry_type", "season") == "movie"
            )
            media_type_str = "movie" if is_movie else "episode"
            is_sp = bool(season is not None and episode is None and not is_movie)
            if skip_layer3:
                is_fake, fake_reason, is_unextracted_archive = False, None, False
            elif (
                not tb_files
                and client is not None
                and client.provider_type == "sabnzbd"
            ):
                # SABnzbd unpacks before reporting Completed: empty listing = pass.
                is_fake, fake_reason, is_unextracted_archive = False, None, False
            elif not tb_files and client is not None:
                # Cloud client reported COMPLETED but lists no files yet: unpack grace.
                is_fake, fake_reason, is_unextracted_archive = (
                    True,
                    f"{display_name} returned empty file list",
                    True,
                )
            else:
                is_fake, fake_reason, is_unextracted_archive = (
                    is_transfer_filelist_fake(
                        tb_files,
                        media_type=media_type_str,
                        is_season_pack=is_sp,
                        release_name=history.nzb_title,
                    )
                )

            if is_unextracted_archive:
                if history.unpack_started_at is None:
                    history.unpack_started_at = now_utc

                unpack_dt = history.unpack_started_at
                if unpack_dt.tzinfo is None:
                    unpack_dt = unpack_dt.replace(tzinfo=timezone.utc)
                elapsed = (now_utc - unpack_dt).total_seconds()

                logger.debug(
                    "[POLLER] Transfer #%s unpacking archive: %.0fs / %ds elapsed.",
                    tb_id_str,
                    elapsed,
                    UNPACK_GRACE_PERIOD_SECONDS,
                )

                if elapsed < UNPACK_GRACE_PERIOD_SECONDS:
                    history.status_detail = "unpacking"
                    history.progress_pct = 100.0
                    history.download_speed_bytes = 0
                    history.eta_seconds = None
                    continue
                else:
                    reason = f"Layer 3 Fake Detection: {display_name} failed to extract archive within 10m"
                    await _handle_transfer_failure(
                        session=session,
                        target=target,
                        history=history,
                        item=item,
                        season=season,
                        episode=episode,
                        tb_item=tb_item,
                        reason=reason,
                        error_type="unextracted_archive",
                        log_msg=f"{reason} ({fake_reason})",
                        sh_max_retries=sh_max_retries,
                    )
                    continue

            if is_fake:
                if fake_reason and "executable" in fake_reason.lower():
                    reason = f"Layer 3 Fake Detection: {fake_reason}"
                elif fake_reason and "sample" in fake_reason.lower():
                    reason = "Layer 3 Fake Detection: Sample-only release without full playable video"
                elif fake_reason and (
                    "unrealistically small" in fake_reason.lower()
                    or "small" in fake_reason.lower()
                ):
                    reason = "Layer 3 Fake Detection: Downloaded video payload is unrealistically small (< threshold)"
                else:
                    reason = "Layer 3 Fake Detection: Downloaded payload lacks a valid playable video file"
                await _handle_transfer_failure(
                    session=session,
                    target=target,
                    history=history,
                    item=item,
                    season=season,
                    episode=episode,
                    tb_item=tb_item,
                    reason=reason,
                    error_type="unextracted_archive",
                    log_msg=f"{reason} ({fake_reason})",
                    sh_max_retries=sh_max_retries,
                )
                continue

            # Verified Playable Video! Transition immediately to COMPLETED
            logger.debug(
                "[POLLER] [FAKEDETECT] Transfer #%s verified Layer 3 clean -> Marked COMPLETED.",
                tb_id_str,
            )
            history.progress_pct = 100.0
            history.download_speed_bytes = 0
            history.eta_seconds = 0
            history.status_detail = "completed"
            history.completed_at = now_utc
            history.unpack_started_at = None

            if isinstance(target, Episode):
                target.status = EpisodeStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None
                _sync_season_status_from_episodes(season)
            elif isinstance(target, Season):
                target.status = SeasonStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None
                for ep in target.episodes:
                    if ep.status != EpisodeStatus.FUTURE:
                        ep.status = EpisodeStatus.COMPLETED
                        ep.fail_count = 0
                        ep.last_error = None
            else:
                target.status = MediaStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None

            if history.is_upgrade and history.replaces_history_id:
                await _finalize_upgrade_replacement(session, history)
            completed_item_ids.add(item.id)
            if history.deliver_enabled:
                delivery_candidates.append(history)
            continue

        # 1b. Check Expired (Previously completed transfer whose cloud files aged out on the downloader)
        if status_lower == "expired" or "expired" in status_lower:
            history.progress_pct = 100.0
            history.download_speed_bytes = 0
            history.eta_seconds = 0
            history.status_detail = "deleted"
            history.notification_sent = True

            if isinstance(target, Episode):
                target.status = EpisodeStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None
                _sync_season_status_from_episodes(season)
            elif isinstance(target, Season):
                target.status = SeasonStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None
                for ep in target.episodes:
                    if ep.status != EpisodeStatus.FUTURE:
                        ep.status = EpisodeStatus.COMPLETED
                        ep.fail_count = 0
                        ep.last_error = None
            else:
                target.status = MediaStatus.COMPLETED
                target.fail_count = 0
                target.last_error = None
            continue

        # 2. Check Stalled Timeout or Terminal Failure States
        is_stalled = (
            status_lower in ("downloading", "processing", "queued", "unknown")
            and sent_at is not None
            and elapsed_seconds > download_timeout_minutes * 60
        )
        if is_stalled or _is_terminal_failure_state(status_lower):
            if is_stalled:
                reason = (
                    f"Stalled download exceeded timeout ({download_timeout_minutes}m)"
                )
                log_msg = f"{display_name} download stalled past {download_timeout_minutes}m limit ({int(elapsed_seconds // 60)}m elapsed)"
            elif status_lower == "not_found":
                reason = f"{display_name} download missing (not_found)"
                log_msg = (
                    f"{display_name} download missing past 15m grace window (not_found)"
                )
            elif tb_item is not None and tb_item.get("fail_message"):
                reason = str(tb_item["fail_message"])
                log_msg = f"{display_name} download failed: {reason}"
            else:
                reason = f"{display_name} reported status: {raw_status}"
                log_msg = f"{display_name} download failed: {raw_status}"

            await _handle_transfer_failure(
                session=session,
                target=target,
                history=history,
                item=item,
                season=season,
                episode=episode,
                tb_item=tb_item,
                reason=reason,
                error_type="downloader_error",
                log_msg=log_msg,
                sh_max_retries=sh_max_retries,
            )
            continue

        # 3. Still actively downloading / processing
        history.status_detail = status_lower or "downloading"

    await _poll_adhoc_transfers(
        session,
        transfer_maps=transfer_maps,
        clients=clients,
        removed_types=removed_types,
        skipped_types=skipped_types,
        now_utc=now_utc,
        download_timeout_minutes=download_timeout_minutes,
        delivery_candidates=delivery_candidates,
    )

    for aff_item in affected_items.values():
        await _recalculate_parent_status(session, aff_item)

    await session.commit()
    for completed_id in completed_item_ids:
        schedule_availability_notification(completed_id)
    await _start_deliveries(session, delivery_candidates)

    # Dispatch show-level batch notifications for any show/movie whose active transfers have all settled
    await _dispatch_settled_show_notifications(
        session=session,
        affected_items=affected_items,
        sh_max_retries=sh_max_retries,
    )

    remaining_active = await count_downloading_entities(session)
    if remaining_active == 0:
        transfer_poller.sleep(0)

    return {
        "polled": True,
        "active_count": active_count,
        "remaining_active": remaining_active,
        "sleeping": not transfer_poller.is_awake,
    }


def _fail_adhoc(history: DownloadHistory, reason: str, now_utc: datetime) -> None:
    history.status_detail = f"failed: {reason}"[:200]
    history.failed_at = now_utc
    history.download_speed_bytes = 0
    history.eta_seconds = None
    history.notification_sent = True


async def _poll_adhoc_transfers(
    session: AsyncSession,
    *,
    transfer_maps: dict[str, dict[str, dict[str, Any]]],
    clients: dict[str, BaseDownloader],
    removed_types: set[str],
    skipped_types: set[str],
    now_utc: datetime,
    download_timeout_minutes: int,
    delivery_candidates: list[DownloadHistory],
) -> None:
    """Advance detached ad-hoc pushes (no MediaItem): progress, Layer 3 verify, completion.

    Library entities are never touched; no blacklist or auto-replace applies.
    """
    rows = (
        (
            await session.execute(
                select(DownloadHistory).where(
                    DownloadHistory.is_ad_hoc.is_(True),
                    DownloadHistory.is_dry_run.is_(False),
                    DownloadHistory.downloader_transfer_id.is_not(None),
                    func.lower(func.coalesce(DownloadHistory.status_detail, "")).in_(
                        ACTIVE_POLLABLE_STATUS_DETAILS
                    ),
                )
            )
        )
        .scalars()
        .all()
    )
    for history in rows:
        dtype = history.downloader_type or "torbox"
        if dtype in removed_types:
            _fail_adhoc(history, "Downloader provider removed", now_utc)
            continue
        if dtype in skipped_types:
            continue
        client = clients.get(dtype)
        display_name = (
            client.display_name
            if client
            else DOWNLOADER_DISPLAY_NAMES.get(dtype, "downloader")
        )
        raw_status, tb_item = _match_downloader_transfer(
            history, transfer_maps.get(dtype, {})
        )
        status_lower = raw_status.lower()
        if tb_item is not None:
            history.progress_pct = _normalize_progress_pct(
                tb_item.get("progress"), status_lower
            )
            history.download_speed_bytes = int(
                tb_item.get("download_speed") or tb_item.get("speed") or 0
            )
            try:
                raw_eta = tb_item.get("eta")
                history.eta_seconds = (
                    max(0, int(raw_eta)) if raw_eta is not None else None
                )
            except (TypeError, ValueError):
                history.eta_seconds = None

        is_completed = (
            status_lower in ("completed", "cached", "paused")
            or bool(tb_item and tb_item.get("download_finished") is True)
        ) and status_lower not in ("downloading", "processing", "queued", "unknown")
        if is_completed and tb_item is not None:
            skip_layer3 = False
            files: list[Any] = []
            if client is None:
                raw_files = tb_item.get("files")
                files = (
                    raw_files
                    if isinstance(raw_files, list)
                    else [{"name": tb_item.get("name") or tb_item.get("title") or ""}]
                )
            elif client.supports_file_listing:
                try:
                    files = list(
                        await client.get_transfer_files(
                            str(history.downloader_transfer_id)
                        )
                    )
                except DownloaderNetworkError:
                    continue
            else:
                skip_layer3 = True
            is_movie = (history.media_type_label or "") in ("movie", "anime-movie")
            is_sp = (
                not is_movie and parse_release_name(history.nzb_title).episode is None
            )
            is_fake, fake_reason, is_archive = False, None, False
            if not skip_layer3 and not files and client is not None:
                if client.provider_type != "sabnzbd":
                    is_fake, fake_reason, is_archive = (
                        True,
                        f"{display_name} returned empty file list",
                        True,
                    )
            elif not skip_layer3:
                is_fake, fake_reason, is_archive = is_transfer_filelist_fake(
                    files,
                    media_type="movie" if is_movie else "episode",
                    is_season_pack=is_sp,
                    release_name=history.nzb_title,
                )
            if is_archive:
                history.unpack_started_at = history.unpack_started_at or now_utc
                started = history.unpack_started_at
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                if (now_utc - started).total_seconds() < UNPACK_GRACE_PERIOD_SECONDS:
                    history.status_detail = "unpacking"
                    history.progress_pct = 100.0
                    history.download_speed_bytes = 0
                    history.eta_seconds = None
                else:
                    _fail_adhoc(
                        history,
                        f"Layer 3 Fake Detection: {display_name} failed to extract archive within 10m",
                        now_utc,
                    )
                continue
            if is_fake:
                _fail_adhoc(history, f"Layer 3 Fake Detection: {fake_reason}", now_utc)
                continue
            history.progress_pct = 100.0
            history.download_speed_bytes = 0
            history.eta_seconds = 0
            history.status_detail = "completed"
            history.completed_at = now_utc
            history.unpack_started_at = None
            if history.deliver_enabled:
                delivery_candidates.append(history)
            continue

        if status_lower == "expired" or "expired" in status_lower:
            history.progress_pct = 100.0
            history.status_detail = "deleted"
            history.notification_sent = True
            continue

        sent_at = history.dispatched_at
        if sent_at is not None and sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        stalled = (
            status_lower in ("downloading", "processing", "queued", "unknown")
            and sent_at is not None
            and (now_utc - sent_at).total_seconds() > download_timeout_minutes * 60
        )
        if stalled:
            _fail_adhoc(
                history,
                f"Stalled download exceeded timeout ({download_timeout_minutes}m)",
                now_utc,
            )
        elif _is_terminal_failure_state(status_lower):
            reason = (
                str(tb_item["fail_message"])
                if tb_item is not None and tb_item.get("fail_message")
                else f"{display_name} reported status: {raw_status}"
            )
            _fail_adhoc(history, reason, now_utc)
        else:
            history.status_detail = status_lower or "downloading"


async def _start_deliveries(
    session: AsyncSession, candidates: list[DownloadHistory]
) -> None:
    """Queue local directory delivery for freshly completed rows (delivery provider active)."""
    if not candidates:
        return
    from app.services.delivery.worker import enqueue_delivery
    from app.services.provider_service import get_active_delivery_provider

    if await get_active_delivery_provider(session) is None:
        return
    for history in candidates:
        history.delivery_status = "pending"
    await session.commit()
    for history in candidates:
        await enqueue_delivery(history.id)


async def _handle_transfer_failure(
    session: AsyncSession,
    target: MediaItem | Season | Episode,
    history: DownloadHistory,
    item: MediaItem,
    season: Season | None,
    episode: Episode | None,
    tb_item: dict[str, Any] | None,
    reason: str,
    error_type: str,
    log_msg: str,
    sh_max_retries: int = 3,
) -> None:
    """Execute Split Failure Recovery (`auto` vs `manual`) with bounded retry budget (`sh_max_retries`)."""
    log_failure(session, target, error_type, log_msg)

    bl = BlacklistedRelease(
        media_item_id=item.id,
        nzb_guid=history.nzb_guid,
        nzb_title=history.nzb_title,
        reason=reason,
    )
    session.add(bl)
    # Commit blacklist and failure log before outbound downloader delete HTTP call
    await session.commit()

    await _purge_for_row(
        session, history.downloader_transfer_id, history.downloader_type, tb_item
    )

    if await _revert_failed_upgrade(session, history, episode, season, reason):
        await session.commit()
        return

    push_mode = (history.push_mode or "auto").strip().lower()

    if push_mode == "manual":
        # Manual Pick Failure: do NOT auto-replace; mark entity FAILED and keep row in Active Pushes
        history.download_speed_bytes = 0
        history.eta_seconds = None
        history.status_detail = f"failed: {reason}"
        history.failed_at = datetime.now(timezone.utc)
        history.is_dismissed = False

        if isinstance(target, Episode):
            target.status = EpisodeStatus.FAILED
            target.last_error = reason
            _sync_season_status_from_episodes(season)
        elif isinstance(target, Season):
            target.status = SeasonStatus.FAILED
            target.last_error = reason
        else:
            target.status = MediaStatus.FAILED
            target.last_error = reason

        await session.commit()
        return

    # Auto-Push Failure (`push_mode == "auto"`): increment target.fail_count and check retry budget
    prev_auto_replaced = int(getattr(history, "auto_replaced_count", 0) or 0)
    target.fail_count = int(target.fail_count or 0) + 1
    await session.commit()

    replacement_hist: DownloadHistory | None = None
    if target.fail_count <= sh_max_retries:
        replacement_hist = await _auto_replace_failed_push(
            session, item=item, season=season, episode=episode
        )

    if replacement_hist is not None:
        replacement_hist.auto_replaced_count = prev_auto_replaced + 1
        history.status_detail = "replaced"
        history.is_dismissed = True
        history.notification_sent = True
        history.download_speed_bytes = 0
        history.eta_seconds = None
        await session.commit()
        # Intermediate auto_replaced notifications are intentionally suppressed;
        # the final settled notification will summarize any auto-replacements.
        return

    # If auto-replacement was blocked by an active downloader rate-limit cooldown while retry budget remains,
    # pause the target in the rate-limit queue rather than failing it (ADR-094).
    cooldown_rem = get_downloader_cooldown_remaining()
    if target.fail_count <= sh_max_retries and cooldown_rem > 0:
        from app.core.push_engine import _record_auto_push_failure

        history.status_detail = "replaced"
        history.is_dismissed = True
        history.notification_sent = True
        history.download_speed_bytes = 0
        history.eta_seconds = None
        queued_hist = await _record_auto_push_failure(
            session,
            item=item,
            season=season,
            episode=episode,
            reason=f"failed: {downloader_rate_limit_detail()}",
        )
        queued_hist.auto_replaced_count = prev_auto_replaced + 1
        await session.commit()
        return

    # Budget exhausted (`target.fail_count > sh_max_retries`) or zero replacement candidates found:
    # Stop auto-replacing, transition target to FAILED, keep history row in Active Pushes (`is_dismissed = False`).
    history.download_speed_bytes = 0
    history.eta_seconds = None
    history.status_detail = f"failed: {reason}"
    history.failed_at = datetime.now(timezone.utc)
    history.is_dismissed = False

    if isinstance(target, Episode):
        target.status = EpisodeStatus.FAILED
        target.last_error = reason
        _sync_season_status_from_episodes(season)
    elif isinstance(target, Season):
        target.status = SeasonStatus.FAILED
        target.last_error = reason
        for ep in target.episodes:
            if ep.status == EpisodeStatus.DOWNLOADING:
                ep.status = EpisodeStatus.FAILED
                ep.last_error = reason
    else:
        target.status = MediaStatus.FAILED
        target.last_error = reason

    await session.commit()


_DELIVERY_ACTIVE = (DeliveryStatus.PENDING.value, DeliveryStatus.DELIVERING.value)
_DELIVERY_ATTENTION = (DeliveryStatus.FAILED.value, DeliveryStatus.PAUSED.value)
_DELIVERY_BADGES = {
    DeliveryStatus.PENDING.value: "Delivery queued",
    DeliveryStatus.DELIVERING.value: "DELIVERING",
    DeliveryStatus.FAILED.value: "Delivery failed",
    DeliveryStatus.PAUSED.value: "Delivery paused",
}


async def _delivery_context(
    session: AsyncSession, history_ids: Sequence[int]
) -> tuple[dict[int, list[str]], bool]:
    """Delivered destination paths per history row, plus whether delivery is active."""
    from app.services.provider_service import get_active_delivery_provider

    paths: dict[int, list[str]] = {}
    if history_ids:
        stmt = (
            select(DeliveredFile.history_id, DeliveredFile.dest_path)
            .where(
                DeliveredFile.history_id.in_(history_ids),
                DeliveredFile.status == "delivered",
                DeliveredFile.dest_path.is_not(None),
            )
            .order_by(DeliveredFile.id)
        )
        for hid, dest in (await session.execute(stmt)).all():
            paths.setdefault(hid, []).append(dest)
    provider_active = await get_active_delivery_provider(session) is not None
    return paths, provider_active


def _delivery_fields(
    h: DownloadHistory, paths: dict[int, list[str]], provider_active: bool
) -> dict[str, Any]:
    """Delivery keys shared by Card 4 and Card 5 rows (template/data contract)."""
    from app.services.delivery.worker import is_deliver_now_allowed

    status = h.delivery_status or DeliveryStatus.NONE.value
    can_retry = status in _DELIVERY_ATTENTION
    delivered_local = to_local_datetime(h.delivered_at)
    return {
        "deliver_enabled": bool(h.deliver_enabled),
        "delivery_status": status,
        "delivery_error": h.delivery_error,
        "delivered_at": (
            delivered_local.strftime("%Y-%m-%d %H:%M") if delivered_local else None
        ),
        "delivered_paths": paths.get(h.id, []),
        "can_retry_delivery": can_retry,
        "can_deliver_now": provider_active and is_deliver_now_allowed(h),
    }


def _format_speed_and_eta(speed_bytes: int, eta_seconds: int | None) -> str:
    if speed_bytes <= 0 and not eta_seconds:
        return "—"
    speed_mb = speed_bytes / (1024 * 1024)
    speed_str = f"{speed_mb:.1f} MB/s" if speed_bytes > 0 else "0.0 MB/s"
    if eta_seconds is None or eta_seconds <= 0:
        return speed_str
    mins, secs = divmod(int(eta_seconds), 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        eta_str = f"{hours}h {mins}m"
    elif mins > 0:
        eta_str = f"{mins}m {secs}s"
    else:
        eta_str = f"{secs}s"
    return f"{speed_str} • {eta_str}"


def _row_downloader_type(h: DownloadHistory) -> str:
    """Row's downloader type; unknown values display as 'torbox' (spec §4.10)."""
    dtype = h.downloader_type or "torbox"
    return dtype if dtype in DOWNLOADER_DISPLAY_NAMES else "torbox"


async def _anime_title_language(session: AsyncSession) -> str:
    from app.core.push_engine import get_anime_title_language

    return await get_anime_title_language(session)


async def get_active_pushes(session: AsyncSession) -> list[dict[str, Any]]:
    """Return flat list of active transfers (`DOWNLOADING`), recently completed transfers within 5m (`Ready on <downloader>`), and unacknowledged failed pushes within 5m (`FAILED` & `is_dismissed == False`)."""
    stmt = (
        select(DownloadHistory)
        .where(DownloadHistory.is_dismissed.is_(False))
        .options(
            selectinload(DownloadHistory.media_item),
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
        .order_by(DownloadHistory.dispatched_at.desc(), DownloadHistory.id.desc())
    )
    rows = (await session.execute(stmt)).scalars().all()
    language = await _anime_title_language(session)

    now_utc = datetime.now(timezone.utc)
    from app.core.rate_limit_queue import (
        active_rate_limit_sources,
        rate_limit_source_label,
    )

    delivery_paths, delivery_provider_active = await _delivery_context(
        session, [h.id for h in rows]
    )
    rate_limit_source: str = rate_limit_source_label(active_rate_limit_sources())
    active_rows: list[dict[str, Any]] = []
    seen_targets: set[tuple[int, int | None, int | None]] = set()

    for h in rows:
        item = h.media_item
        if item is None:
            continue
        season = h.season
        episode = h.episode

        # Determine current entity status
        if episode is not None:
            entity_status = episode.status.value
        elif season is not None:
            entity_status = season.status.value
        else:
            entity_status = item.status.value

        detail_lower = (h.status_detail or "").lower()
        is_rate_limited = (
            is_queued_detail(h.status_detail)
            or "rate limit" in detail_lower
            or "429" in detail_lower
        )

        failed_dt_raw = h.failed_at or h.created_at
        failed_dt_utc: datetime | None = None
        if failed_dt_raw is not None:
            failed_dt_utc = (
                failed_dt_raw.replace(tzinfo=timezone.utc)
                if failed_dt_raw.tzinfo is None
                else failed_dt_raw
            )
        outage_match = _OUTAGE_DETAIL_RE.match(h.status_detail or "")
        is_failed = (
            not is_rate_limited
            and (entity_status == "failed" or detail_lower.startswith("failed"))
            and failed_dt_utc is not None
            and (now_utc - failed_dt_utc).total_seconds() < FAILED_RETENTION_SECONDS
        )
        is_active_downloading = (
            entity_status == "downloading"
            and detail_lower
            not in ("completed", "replaced", "canceled", "deleted", "expired")
            and not detail_lower.startswith("failed")
        )

        completed_at_utc: datetime | None = None
        if h.completed_at is not None:
            completed_at_utc = (
                h.completed_at.replace(tzinfo=timezone.utc)
                if h.completed_at.tzinfo is None
                else h.completed_at
            )
        is_recently_completed = (
            detail_lower == "completed"
            and completed_at_utc is not None
            and (now_utc - completed_at_utc).total_seconds()
            < COMPLETED_RETENTION_SECONDS
            and entity_status in ("completed", "downloaded")
        )

        # Delivery rows stay visible past the retention window until settled/dismissed.
        is_delivery = (
            detail_lower == "completed"
            and (h.delivery_status or "") in _DELIVERY_ACTIVE + _DELIVERY_ATTENTION
        )
        if is_delivery:
            is_recently_completed = False

        if not (
            is_active_downloading
            or is_failed
            or is_rate_limited
            or is_recently_completed
            or is_delivery
        ):
            continue

        target_key = (item.id, h.season_id, h.episode_id)
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)

        target_label = discord.format_target_label(
            item=item, season=season, episode=episode
        )
        display_title = anime_title_for(item, language)
        title_and_target = (
            f"{display_title} ({item.year})"
            if target_label == "Movie" and item.year
            else (
                display_title
                if target_label == "Movie"
                else f"{display_title} — {target_label}"
            )
        )

        if is_delivery:
            status_badge = _DELIVERY_BADGES[h.delivery_status]
        elif is_recently_completed:
            status_badge = f"Ready on {DOWNLOADER_DISPLAY_NAMES.get(h.downloader_type, 'downloader')}"
        elif is_rate_limited:
            from app.core.rate_limit_queue import resume_time_label

            if outage_match is not None:
                status_badge = (
                    f"Paused — {outage_match.group(1).capitalize()} Outage "
                    f"({resume_time_label()})"
                )
            elif "indexer" in detail_lower:
                status_badge = f"Paused — indexer limit ({resume_time_label()})"
            else:
                status_badge = f"Paused — rate limit ({resume_time_label()})"
        elif is_failed:
            status_badge = (
                "Failed (Manual Pick)"
                if h.push_mode == "manual"
                else "Failed (Auto-Push)"
            )
        elif detail_lower == "unpacking":
            status_badge = "Unpacking"
        elif detail_lower == "verifying":
            status_badge = "Verifying"
        else:
            status_badge = "Downloading"

        item_media_type = (
            item.media_type.value
            if hasattr(item.media_type, "value")
            else str(item.media_type or "")
        )
        feature_pills = build_release_feature_pills(
            release_title=h.nzb_title,
            score=h.score,
            size_bytes=h.size_bytes,
            is_fallback=bool(getattr(h, "is_fallback", False)),
            matched_language=h.grabbed_language,
            bitrate_mbps=h.bitrate_mbps,
            runtime_minutes=item.runtime_minutes,
            media_type=item_media_type,
            season_episode_count=season.episode_count if season else None,
        )

        row_progress_pct = (
            100.0 if is_recently_completed else round(float(h.progress_pct or 0.0), 1)
        )
        row_speed_and_eta = (
            "100% • Ready"
            if is_recently_completed
            else _format_speed_and_eta(int(h.download_speed_bytes or 0), h.eta_seconds)
        )
        if is_delivery:
            delivering = h.delivery_status == DeliveryStatus.DELIVERING.value
            row_progress_pct = (
                round(float(h.progress_pct or 0.0), 1) if delivering else 0.0
            )
            if h.delivery_status in _DELIVERY_ATTENTION:
                row_speed_and_eta = h.delivery_error or "—"
            elif not delivering:
                row_speed_and_eta = "Waiting for delivery slot"
            elif h.downloader_type == "sabnzbd":
                row_speed_and_eta = "Filing..."

        active_rows.append(
            {
                "history_id": h.id,
                "downloader_type": _row_downloader_type(h),
                "downloader_name": DOWNLOADER_DISPLAY_NAMES[_row_downloader_type(h)],
                "downloader_transfer_id": h.downloader_transfer_id,
                "media_item_id": item.id,
                "season_id": h.season_id,
                "episode_id": h.episode_id,
                "media_title": item.title,
                "display_title": display_title,
                "target_label": target_label,
                "title_and_target": title_and_target,
                "release_name": h.nzb_title,
                "resolution": h.resolution,
                "source": h.source,
                "language": h.grabbed_language,
                "feature_pills": feature_pills,
                "progress_pct": row_progress_pct,
                "download_speed_bytes": int(h.download_speed_bytes or 0),
                "eta_seconds": h.eta_seconds,
                "speed_and_eta": row_speed_and_eta,
                "rate_limit_source": rate_limit_source,
                "status": status_badge,
                "status_detail": h.status_detail,
                "push_mode": h.push_mode,
                "is_upgrade": bool(h.is_upgrade),
                "is_failed": is_failed,
                "is_rate_limited": is_rate_limited,
                "is_outage": outage_match is not None,
                "outage_kind": outage_match.group(1) if outage_match else None,
                "outage_label": outage_match.group(2) if outage_match else None,
                "outage_attempts": int(h.outage_attempts or 0),
                "is_completed": is_recently_completed,
                "is_delivery": is_delivery,
                "completed_at": (
                    completed_at_utc.isoformat()
                    if is_recently_completed and completed_at_utc is not None
                    else None
                ),
                **_delivery_fields(h, delivery_paths, delivery_provider_active),
            }
        )

    dry_run_recs = get_dry_run_records()
    dry_run_rows: list[dict[str, Any]] = []
    for rec in sorted(dry_run_recs, key=lambda r: r.timestamp, reverse=True):
        title_and_target = (
            f"{rec.media_title} — {rec.target_badge}"
            if rec.target_badge != "Movie"
            else rec.media_title
        )
        dry_run_rows.append(
            {
                "history_id": rec.id,
                "media_item_id": rec.media_item_id,
                "season_id": rec.season_id,
                "episode_id": rec.episode_id,
                "media_title": rec.media_title,
                "display_title": rec.media_title,
                "target_label": rec.target_badge,
                "title_and_target": title_and_target,
                "release_name": rec.release_title,
                "resolution": rec.resolution,
                "source": rec.source,
                "language": rec.language,
                "feature_pills": rec.feature_pills,
                "progress_pct": 100.0,
                "download_speed_bytes": 0,
                "eta_seconds": 0,
                "speed_and_eta": "Simulated • No Download",
                "status": "Dry Run (Simulated)",
                "status_detail": "Dispatch simulated (Dry-Run Mode)",
                "push_mode": rec.push_mode,
                "is_upgrade": bool(rec.is_upgrade),
                "is_failed": False,
                "is_rate_limited": False,
                "is_outage": False,
                "outage_kind": None,
                "outage_label": None,
                "outage_attempts": 0,
                "is_completed": False,
                "is_dry_run": True,
                "completed_at": None,
            }
        )

    return dry_run_rows + active_rows


async def dismiss_failed_push(session: AsyncSession, history_id: int) -> bool:
    """Dismiss a failed manual-pick row from Active Pushes and revert its entity status to SEARCHING."""
    stmt = (
        select(DownloadHistory)
        .where(DownloadHistory.id == history_id)
        .options(
            selectinload(DownloadHistory.media_item),
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
    )
    history = (await session.execute(stmt)).scalars().first()
    if history is None:
        return False

    history.is_dismissed = True
    if history.episode is not None and history.episode.status in (
        EpisodeStatus.FAILED,
        EpisodeStatus.RATE_LIMITED,
    ):
        history.episode.status = EpisodeStatus.SEARCHING
    elif history.season is not None and history.season.status in (
        SeasonStatus.FAILED,
        SeasonStatus.RATE_LIMITED,
    ):
        history.season.status = SeasonStatus.SEARCHING
    elif history.media_item is not None and history.media_item.status in (
        MediaStatus.FAILED,
        MediaStatus.RATE_LIMITED,
    ):
        history.media_item.status = MediaStatus.SEARCHING

    if history.media_item is not None:
        await _recalculate_parent_status(session, history.media_item)

    await session.commit()
    return True


async def dismiss_all_active_pushes(session: AsyncSession) -> int:
    """Dismiss all non-active rows (completed, failed, dry-run) from Card 5.

    - Synthetic dry-run records in memory are cleared.
    - Completed rows are marked is_dismissed = True (retaining COMPLETED entity status).
    - Failed rows are marked is_dismissed = True (reverting FAILED/RATE_LIMITED entities to SEARCHING).
    - In-progress downloading transfers are untouched.
    """
    clear_dry_run_pushes()

    stmt = (
        select(DownloadHistory)
        .where(DownloadHistory.is_dismissed.is_(False))
        .options(
            selectinload(DownloadHistory.media_item),
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()

    dismissed_count = 0
    affected_media_items: dict[int, MediaItem] = {}

    for h in rows:
        detail_lower = (h.status_detail or "").strip().lower()
        is_rate_limited = is_queued_detail(h.status_detail)

        if h.episode is not None:
            entity_status = h.episode.status.value
        elif h.season is not None:
            entity_status = h.season.status.value
        elif h.media_item is not None:
            entity_status = h.media_item.status.value
        else:
            continue

        is_completed = detail_lower == "completed" or entity_status in (
            "completed",
            "downloaded",
        )
        is_failed = not is_rate_limited and (
            entity_status == "failed" or detail_lower.startswith("failed")
        )

        if not (is_completed or is_failed):
            # Active downloading or rate-limited transfers must not be dismissed!
            continue
        if (h.delivery_status or "") in _DELIVERY_ACTIVE:
            continue  # delivery must settle first

        h.is_dismissed = True
        dismissed_count += 1

        if is_failed:
            if h.episode is not None and h.episode.status in (
                EpisodeStatus.FAILED,
                EpisodeStatus.RATE_LIMITED,
            ):
                h.episode.status = EpisodeStatus.SEARCHING
            elif h.season is not None and h.season.status in (
                SeasonStatus.FAILED,
                SeasonStatus.RATE_LIMITED,
            ):
                h.season.status = SeasonStatus.SEARCHING
            elif h.media_item is not None and h.media_item.status in (
                MediaStatus.FAILED,
                MediaStatus.RATE_LIMITED,
            ):
                h.media_item.status = MediaStatus.SEARCHING

            if h.media_item is not None and h.media_item.id is not None:
                affected_media_items[h.media_item.id] = h.media_item

    for item in affected_media_items.values():
        await _recalculate_parent_status(session, item)

    await session.commit()
    return dismissed_count


async def cancel_active_push(session: AsyncSession, history_id: int) -> bool:
    """Cancel and delete an active transfer on the downloader, mark its history row 'canceled', and revert entity to SEARCHING."""
    stmt = (
        select(DownloadHistory)
        .where(DownloadHistory.id == history_id)
        .options(
            selectinload(DownloadHistory.media_item)
            .selectinload(MediaItem.seasons)
            .selectinload(Season.episodes),
            selectinload(DownloadHistory.season).selectinload(Season.episodes),
            selectinload(DownloadHistory.episode),
        )
    )
    history = (await session.execute(stmt)).scalars().first()
    if history is None:
        return False

    old_transfer_id = history.downloader_transfer_id
    old_downloader_type = history.downloader_type

    if history.episode is not None:
        history.episode.status = (
            EpisodeStatus.COMPLETED
            if await _has_live_replaced_release(session, history)
            else EpisodeStatus.SEARCHING
        )
    elif history.season is not None:
        history.season.status = SeasonStatus.SEARCHING
        for ep in history.season.episodes:
            if ep.status == EpisodeStatus.DOWNLOADING:
                ep.status = EpisodeStatus.SEARCHING
    elif history.media_item is not None:
        history.media_item.status = MediaStatus.SEARCHING

    history.status_detail = "canceled"
    history.is_dismissed = True
    history.notification_sent = True
    history.progress_pct = 0.0
    history.download_speed_bytes = 0
    history.eta_seconds = None

    if history.media_item is not None and (
        history.season is not None or history.episode is not None
    ):
        await _recalculate_parent_status(session, history.media_item)

    await session.commit()

    if old_transfer_id:
        await _purge_for_row(session, old_transfer_id, old_downloader_type)

    if history.media_item is not None and history.media_item.id is not None:
        sys_settings = (
            (
                await session.execute(
                    select(SystemSettings).where(SystemSettings.id == 1)
                )
            )
            .scalars()
            .first()
        )
        sh_max_retries = (
            sys_settings.sh_max_retries
            if sys_settings and sys_settings.sh_max_retries is not None
            else 3
        )
        await _dispatch_settled_show_notifications(
            session=session,
            affected_items={history.media_item.id: history.media_item},
            sh_max_retries=sh_max_retries,
        )

    remaining = await count_downloading_entities(session)
    if remaining == 0 and not transfer_poller.has_in_flight_pushes:
        transfer_poller.sleep()
    return True


async def abort_all_active_transfers(session: AsyncSession) -> dict[str, Any]:
    """Abort all active downloading and rate-limited transfers across the downloader, background tasks, and database."""
    in_flight_canceled = 0
    try:
        from app.main import cancel_in_flight_push_tasks

        in_flight_canceled = await cancel_in_flight_push_tasks()
    except (ImportError, AttributeError):
        from app.core.search_lock import item_lock_manager

        in_flight_canceled = len(transfer_poller.in_flight_pushes)
        for iid in list(transfer_poller.in_flight_pushes.keys()):
            transfer_poller.unregister_in_flight_push(iid)
            item_lock_manager.release(iid)

    async with _poller_lock:
        stmt = (
            select(DownloadHistory)
            .where(DownloadHistory.is_dismissed.is_(False))
            .options(
                selectinload(DownloadHistory.media_item)
                .selectinload(MediaItem.seasons)
                .selectinload(Season.episodes),
                selectinload(DownloadHistory.season).selectinload(Season.episodes),
                selectinload(DownloadHistory.episode),
            )
        )
        rows = (await session.execute(stmt)).scalars().all()

        matching_histories: list[DownloadHistory] = []
        transfer_ids_to_delete: list[str] = []
        id_types: dict[str, str] = {}
        affected_items: dict[int, MediaItem] = {}
        reverted_episode_ids: set[int] = set()

        for h in rows:
            detail_lower = (h.status_detail or "").strip().lower()
            is_rate_limited = is_queued_detail(h.status_detail)
            is_active_downloading = not _is_history_terminal(
                h.status_detail
            ) and detail_lower not in (
                "completed",
                "replaced",
                "canceled",
                "deleted",
                "expired",
            )
            if not (is_rate_limited or is_active_downloading):
                continue

            matching_histories.append(h)
            if (
                h.downloader_transfer_id
                and str(h.downloader_transfer_id) not in transfer_ids_to_delete
            ):
                transfer_ids_to_delete.append(str(h.downloader_transfer_id))
                id_types[str(h.downloader_transfer_id)] = h.downloader_type

            reverts_to_old = h.episode is not None and (
                await _has_live_replaced_release(session, h)
            )
            h.status_detail = "canceled"
            h.is_dismissed = True
            h.notification_sent = True
            h.progress_pct = 0.0
            h.download_speed_bytes = 0
            h.eta_seconds = None

            if h.episode is not None and reverts_to_old:
                h.episode.status = EpisodeStatus.COMPLETED
                reverted_episode_ids.add(h.episode.id)
            elif h.episode is not None:
                h.episode.status = EpisodeStatus.SEARCHING
                h.episode.fail_count = 0
                h.episode.last_error = None
            if h.season is not None:
                h.season.status = SeasonStatus.SEARCHING
                h.season.fail_count = 0
                h.season.last_error = None
                for ep in h.season.episodes or []:
                    if ep.status in (
                        EpisodeStatus.DOWNLOADING,
                        EpisodeStatus.RATE_LIMITED,
                    ):
                        ep.status = EpisodeStatus.SEARCHING
                        ep.fail_count = 0
                        ep.last_error = None
            if h.media_item is not None:
                if h.season is None and h.episode is None:
                    h.media_item.status = MediaStatus.SEARCHING
                    h.media_item.fail_count = 0
                    h.media_item.last_error = None
                    if hasattr(h.media_item, "completed_at"):
                        setattr(h.media_item, "completed_at", None)
                if h.media_item.id is not None:
                    affected_items[h.media_item.id] = h.media_item

        # Also reset any entities that might still be in DOWNLOADING or RATE_LIMITED
        stmt_items = select(MediaItem).where(
            MediaItem.status.in_([MediaStatus.DOWNLOADING, MediaStatus.RATE_LIMITED])
        )
        dl_items = (await session.execute(stmt_items)).scalars().all()
        for item in dl_items:
            item.status = MediaStatus.SEARCHING
            item.fail_count = 0
            item.last_error = None
            if item.id is not None:
                affected_items[item.id] = item

        stmt_seasons = (
            select(Season)
            .where(
                Season.status.in_([SeasonStatus.DOWNLOADING, SeasonStatus.RATE_LIMITED])
            )
            .options(selectinload(Season.media_item))
        )
        dl_seasons = (await session.execute(stmt_seasons)).scalars().all()
        for season in dl_seasons:
            season.status = SeasonStatus.SEARCHING
            season.fail_count = 0
            season.last_error = None
            if season.media_item is not None and season.media_item.id is not None:
                affected_items[season.media_item.id] = season.media_item

        stmt_episodes = (
            select(Episode)
            .where(
                Episode.status.in_(
                    [EpisodeStatus.DOWNLOADING, EpisodeStatus.RATE_LIMITED]
                )
            )
            .options(selectinload(Episode.season).selectinload(Season.media_item))
        )
        dl_episodes = (await session.execute(stmt_episodes)).scalars().all()
        for ep in dl_episodes:
            if ep.id in reverted_episode_ids:
                continue
            ep.status = EpisodeStatus.SEARCHING
            ep.fail_count = 0
            ep.last_error = None
            if (
                ep.season is not None
                and ep.season.media_item is not None
                and ep.season.media_item.id is not None
            ):
                affected_items[ep.season.media_item.id] = ep.season.media_item

        for item in affected_items.values():
            await _recalculate_parent_status(session, item)

        await session.commit()

    for tb_id in transfer_ids_to_delete:
        await _purge_for_row(session, tb_id, id_types.get(tb_id))

    try:
        from app.core.rate_limit_queue import (
            count_rate_limited_pushes,
            outage_episode,
            rate_limit_episode,
            rate_limit_queue,
        )

        if (await count_rate_limited_pushes(session)) == 0:
            rate_limit_queue.sleep()
            rate_limit_episode.reset()
            outage_episode.reset()
    except Exception as exc:
        logger.debug("Rate-limit queue sleep check skipped: %s", exc)

    remaining = await count_downloading_entities(session)
    if remaining == 0 and not transfer_poller.has_in_flight_pushes:
        transfer_poller.sleep(0)

    return {
        "ok": True,
        "aborted_count": len(matching_histories),
        "canceled_transfer_ids": transfer_ids_to_delete,
        "in_flight_canceled": in_flight_canceled,
    }


async def abort_item_transfer(
    session: AsyncSession,
    item_id: int,
    season_id: int | None = None,
    episode_id: int | None = None,
) -> dict[str, Any]:
    """Abort a specific active or rate-limited transfer, delete from the downloader, mark history canceled, and revert entity status to SEARCHING."""
    try:
        from app.main import cancel_in_flight_push_tasks

        await cancel_in_flight_push_tasks(item_id=item_id)
    except (ImportError, AttributeError):
        from app.core.search_lock import item_lock_manager

        transfer_poller.unregister_in_flight_push(item_id)
        item_lock_manager.release(item_id)

    item: MediaItem | None = (
        (
            await session.execute(
                select(MediaItem)
                .where(MediaItem.id == item_id)
                .options(
                    selectinload(MediaItem.seasons).selectinload(Season.episodes),
                )
            )
        )
        .scalars()
        .first()
    )

    season: Season | None = None
    if season_id is not None:
        season = (
            (
                await session.execute(
                    select(Season)
                    .where(Season.id == season_id)
                    .options(selectinload(Season.episodes))
                )
            )
            .scalars()
            .first()
        )

    episode: Episode | None = None
    if episode_id is not None:
        episode = (
            (await session.execute(select(Episode).where(Episode.id == episode_id)))
            .scalars()
            .first()
        )

    matching_histories: list[DownloadHistory] = []
    if episode_id is not None:
        stmt = select(DownloadHistory).where(
            DownloadHistory.episode_id == episode_id,
            DownloadHistory.is_dismissed.is_(False),
        )
        hists = (await session.execute(stmt)).scalars().all()
        matching_histories.extend(hists)
    elif season_id is not None:
        ep_ids = [
            ep.id for ep in (season.episodes if season else []) if ep.id is not None
        ]
        from sqlalchemy import or_

        if ep_ids:
            stmt = select(DownloadHistory).where(
                or_(
                    DownloadHistory.season_id == season_id,
                    DownloadHistory.episode_id.in_(ep_ids),
                ),
                DownloadHistory.is_dismissed.is_(False),
            )
        else:
            stmt = select(DownloadHistory).where(
                DownloadHistory.season_id == season_id,
                DownloadHistory.is_dismissed.is_(False),
            )
        hists = (await session.execute(stmt)).scalars().all()
        matching_histories.extend(hists)
    else:
        stmt = select(DownloadHistory).where(
            DownloadHistory.media_item_id == item_id,
            DownloadHistory.is_dismissed.is_(False),
        )
        hists = (await session.execute(stmt)).scalars().all()
        matching_histories.extend(hists)

    valid_histories: list[DownloadHistory] = []
    transfer_ids_to_delete: list[str] = []
    id_types: dict[str, str] = {}
    reverted_episode_ids: set[int] = set()
    for h in matching_histories:
        detail_lower = (h.status_detail or "").strip().lower()
        is_rate_limited = is_queued_detail(h.status_detail)
        is_active_downloading = not _is_history_terminal(
            h.status_detail
        ) and detail_lower not in (
            "completed",
            "replaced",
            "canceled",
            "deleted",
            "expired",
        )
        if is_rate_limited or is_active_downloading:
            valid_histories.append(h)
            if (
                h.downloader_transfer_id
                and str(h.downloader_transfer_id) not in transfer_ids_to_delete
            ):
                transfer_ids_to_delete.append(str(h.downloader_transfer_id))
                id_types[str(h.downloader_transfer_id)] = h.downloader_type
            if h.episode_id is not None and await _has_live_replaced_release(
                session, h
            ):
                reverted_ep = await session.get(Episode, h.episode_id)
                if reverted_ep is not None:
                    reverted_ep.status = EpisodeStatus.COMPLETED
                    reverted_episode_ids.add(reverted_ep.id)
            h.status_detail = "canceled"
            h.is_dismissed = True
            h.notification_sent = True
            h.progress_pct = 0.0
            h.download_speed_bytes = 0
            h.eta_seconds = None

    if episode is not None:
        if episode.id in reverted_episode_ids:
            episode.status = EpisodeStatus.COMPLETED
        else:
            episode.status = EpisodeStatus.SEARCHING
            episode.fail_count = 0
            episode.last_error = None
        if season is not None and season.status in (
            SeasonStatus.DOWNLOADING,
            SeasonStatus.RATE_LIMITED,
        ):
            any_still_dl = any(
                ep.id != episode.id
                and ep.status in (EpisodeStatus.DOWNLOADING, EpisodeStatus.RATE_LIMITED)
                for ep in (season.episodes or [])
            )
            if not any_still_dl:
                season.status = SeasonStatus.SEARCHING
        if item is not None:
            await _recalculate_parent_status(session, item)
    elif season is not None:
        season.status = SeasonStatus.SEARCHING
        season.fail_count = 0
        season.last_error = None
        for ep in season.episodes or []:
            if ep.status in (EpisodeStatus.DOWNLOADING, EpisodeStatus.RATE_LIMITED):
                ep.status = EpisodeStatus.SEARCHING
                ep.fail_count = 0
                ep.last_error = None
        if item is not None:
            await _recalculate_parent_status(session, item)
    elif item is not None:
        item.status = MediaStatus.SEARCHING
        item.fail_count = 0
        item.last_error = None
        if hasattr(item, "completed_at"):
            setattr(item, "completed_at", None)
        for s in item.seasons or []:
            if s.status in (SeasonStatus.DOWNLOADING, SeasonStatus.RATE_LIMITED):
                s.status = SeasonStatus.SEARCHING
                s.fail_count = 0
                s.last_error = None
                for ep in s.episodes or []:
                    if ep.status in (
                        EpisodeStatus.DOWNLOADING,
                        EpisodeStatus.RATE_LIMITED,
                    ):
                        ep.status = EpisodeStatus.SEARCHING
                        ep.fail_count = 0
                        ep.last_error = None
        await _recalculate_parent_status(session, item)

    await session.commit()

    for tb_id in transfer_ids_to_delete:
        await _purge_for_row(session, tb_id, id_types.get(tb_id))

    try:
        from app.core.rate_limit_queue import (
            count_rate_limited_pushes,
            outage_episode,
            rate_limit_episode,
            rate_limit_queue,
        )

        if (await count_rate_limited_pushes(session)) == 0:
            rate_limit_queue.sleep()
            rate_limit_episode.reset()
            outage_episode.reset()
    except Exception as exc:
        logger.debug("Rate-limit queue sleep check skipped: %s", exc)

    remaining = await count_downloading_entities(session)
    if remaining == 0 and not transfer_poller.has_in_flight_pushes:
        transfer_poller.sleep(0)

    return {
        "status": "ok",
        "deleted_transfer_ids": transfer_ids_to_delete,
        "canceled_count": len(valid_histories),
        "item_id": item_id,
    }


async def reconcile_completed_history(
    session: AsyncSession,
    downloader_type: str,
    live_ids: set[str],
) -> int:
    """Passively reconcile completed DownloadHistory rows of one downloader type against live transfer IDs.

    Marks missing rows as status_detail='deleted' without altering MediaItem, Season, or Episode status.
    """
    stmt = select(DownloadHistory).where(
        DownloadHistory.status_detail == "completed",
        DownloadHistory.downloader_type == downloader_type,
        DownloadHistory.downloader_transfer_id.is_not(None),
    )
    rows = (await session.execute(stmt)).scalars().all()
    updated = 0
    for row in rows:
        if (
            row.downloader_transfer_id
            and str(row.downloader_transfer_id) not in live_ids
        ):
            row.status_detail = "deleted"
            updated += 1
    if updated > 0:
        await session.commit()
    return updated


async def reconcile_completed_history_with_downloader(
    session: AsyncSession,
    live_transfer_ids: set[str],
) -> int:
    """Downloader-specific entry point; delegates to ``reconcile_completed_history``."""
    return await reconcile_completed_history(session, "torbox", live_transfer_ids)


async def _recalculate_parent_status(
    session: AsyncSession,
    item: MediaItem | None,
) -> None:
    """Recalculate parent MediaItem, Season, and Episode statuses strictly from DownloadHistory truth."""
    if item is None or item.id is None:
        return

    seasons = (
        (
            await session.execute(
                select(Season)
                .where(Season.media_item_id == item.id)
                .options(
                    selectinload(Season.episodes),
                    selectinload(Season.download_history),
                )
            )
        )
        .scalars()
        .all()
    )

    season_ids = [s.id for s in seasons if s.id is not None]
    ep_ids = [ep.id for s in seasons for ep in (s.episodes or []) if ep.id is not None]

    from sqlalchemy import or_

    hist_conds = [DownloadHistory.media_item_id == item.id]
    if season_ids:
        hist_conds.append(DownloadHistory.season_id.in_(season_ids))
    if ep_ids:
        hist_conds.append(DownloadHistory.episode_id.in_(ep_ids))

    item_histories = (
        (await session.execute(select(DownloadHistory).where(or_(*hist_conds))))
        .scalars()
        .all()
    )

    def _is_hist_active(h: DownloadHistory) -> bool:
        if h.is_dismissed or not (h.downloader_transfer_id or h.downloader_hash):
            return False
        d = (h.status_detail or "").strip().lower()
        return d in (
            "downloading",
            "queued",
            "processing",
            "unpacking",
            "verifying",
            "pending",
        )

    def _is_hist_completed(h: DownloadHistory) -> bool:
        d = (h.status_detail or "").strip().lower()
        if d in ("deleted", "expired"):
            return not h.is_dismissed
        return d in ("completed", "")

    def _is_hist_failed(h: DownloadHistory) -> bool:
        if h.is_dismissed:
            return False
        d = (h.status_detail or "").strip().lower()
        return d.startswith("failed")

    def _is_hist_rate_limited(h: DownloadHistory) -> bool:
        return not h.is_dismissed and is_queued_detail(h.status_detail)

    if not seasons:
        has_rate_limited_movie = any(_is_hist_rate_limited(h) for h in item_histories)
        has_active_movie = any(_is_hist_active(h) for h in item_histories)
        has_completed_movie = any(_is_hist_completed(h) for h in item_histories)
        has_failed_movie = any(_is_hist_failed(h) for h in item_histories)

        if has_active_movie:
            item.status = MediaStatus.DOWNLOADING
        elif has_rate_limited_movie:
            item.status = MediaStatus.RATE_LIMITED
        elif has_failed_movie and item.status == MediaStatus.FAILED:
            item.status = MediaStatus.FAILED
        elif has_completed_movie:
            item.status = MediaStatus.COMPLETED
        else:
            if item.status != MediaStatus.FUTURE:
                item.status = MediaStatus.SEARCHING
                item.fail_count = 0
                item.last_error = None
                if hasattr(item, "completed_at"):
                    setattr(item, "completed_at", None)
        return

    for s in seasons:
        if s.season_number > 0 and s.status != SeasonStatus.IGNORED:
            s.monitored = True
        has_completed_pack = any(
            h.season_id == s.id and h.episode_id is None and _is_hist_completed(h)
            for h in item_histories
        )
        has_active_pack = any(
            h.season_id == s.id and h.episode_id is None and _is_hist_active(h)
            for h in item_histories
        )
        has_failed_pack = any(
            h.season_id == s.id and h.episode_id is None and _is_hist_failed(h)
            for h in item_histories
        )

        has_rate_limited_pack = any(
            h.season_id == s.id and h.episode_id is None and _is_hist_rate_limited(h)
            for h in item_histories
        )

        for ep in s.episodes or []:
            if ep.status != EpisodeStatus.IGNORED:
                ep.monitored = True
            if ep.status == EpisodeStatus.RATE_LIMITED and not any(
                h.episode_id == ep.id and _is_hist_rate_limited(h)
                for h in item_histories
            ):
                ep.status = EpisodeStatus.SEARCHING
            has_completed_ep = has_completed_pack or any(
                h.episode_id == ep.id and _is_hist_completed(h) for h in item_histories
            )
            has_active_ep = has_active_pack or any(
                h.episode_id == ep.id and _is_hist_active(h) for h in item_histories
            )
            if (
                ep.status in (EpisodeStatus.COMPLETED, EpisodeStatus.DOWNLOADED)
                and not has_completed_ep
            ):
                ep.status = EpisodeStatus.SEARCHING
                ep.fail_count = 0
                ep.last_error = None
            elif ep.status == EpisodeStatus.DOWNLOADING and not has_active_ep:
                ep.status = (
                    EpisodeStatus.COMPLETED
                    if has_completed_ep
                    else EpisodeStatus.SEARCHING
                )
            elif ep.status == EpisodeStatus.PENDING:
                ep.status = EpisodeStatus.SEARCHING

        _sync_season_status_from_episodes(s)

        if not s.episodes:
            if (
                s.status in (SeasonStatus.COMPLETED, SeasonStatus.DOWNLOADED)
                and not has_completed_pack
            ):
                s.status = SeasonStatus.SEARCHING
                s.fail_count = 0
                s.last_error = None
            elif s.status == SeasonStatus.DOWNLOADING and not has_active_pack:
                s.status = (
                    SeasonStatus.COMPLETED
                    if has_completed_pack
                    else SeasonStatus.SEARCHING
                )
            elif s.status == SeasonStatus.FAILED and not has_failed_pack:
                s.status = (
                    SeasonStatus.COMPLETED
                    if has_completed_pack
                    else SeasonStatus.SEARCHING
                )
            elif s.status == SeasonStatus.RATE_LIMITED and not has_rate_limited_pack:
                s.status = (
                    SeasonStatus.COMPLETED
                    if has_completed_pack
                    else SeasonStatus.SEARCHING
                )
            elif s.status == SeasonStatus.PENDING and s.season_number > 0:
                s.status = SeasonStatus.SEARCHING

    any_downloading = any(
        s.status == SeasonStatus.DOWNLOADING
        or any(ep.status == EpisodeStatus.DOWNLOADING for ep in (s.episodes or []))
        for s in seasons
    )
    any_failed = any(
        s.status == SeasonStatus.FAILED
        or any(ep.status == EpisodeStatus.FAILED for ep in (s.episodes or []))
        for s in seasons
    )
    released_seasons = [
        s for s in seasons if s.season_number > 0 and s.status != SeasonStatus.FUTURE
    ]

    any_rate_limited = any(
        s.status == SeasonStatus.RATE_LIMITED
        or any(ep.status == EpisodeStatus.RATE_LIMITED for ep in (s.episodes or []))
        for s in seasons
    )

    if any_downloading:
        item.status = MediaStatus.DOWNLOADING
    elif any_rate_limited:
        item.status = MediaStatus.RATE_LIMITED
    elif any_failed:
        item.status = MediaStatus.FAILED
    elif released_seasons and all(
        s.status in (SeasonStatus.COMPLETED, SeasonStatus.DOWNLOADED)
        for s in released_seasons
    ):
        item.status = MediaStatus.COMPLETED
    elif not released_seasons and any(
        s.status == SeasonStatus.FUTURE for s in seasons if s.season_number > 0
    ):
        item.status = MediaStatus.FUTURE
    else:
        item.status = MediaStatus.SEARCHING
        item.fail_count = 0
        item.last_error = None
        if hasattr(item, "completed_at"):
            setattr(item, "completed_at", None)


async def reconcile_all_items_transfer_truth(session: AsyncSession) -> int:
    """Reconcile all non-ignored MediaItems, Seasons, and Episodes against active/completed DownloadHistory truth."""
    stmt = (
        select(MediaItem)
        .where(MediaItem.status != MediaStatus.IGNORED)
        .options(
            selectinload(MediaItem.seasons).selectinload(Season.episodes),
        )
    )
    items = (await session.execute(stmt)).scalars().unique().all()
    reconciled = 0
    for item in items:
        needs_check = item.status in (
            MediaStatus.COMPLETED,
            MediaStatus.DOWNLOADED,
            MediaStatus.DOWNLOADING,
            MediaStatus.PENDING,
        ) or any(
            s.status
            in (
                SeasonStatus.COMPLETED,
                SeasonStatus.DOWNLOADED,
                SeasonStatus.DOWNLOADING,
                SeasonStatus.PENDING,
            )
            or not s.monitored
            or any(
                ep.status
                in (
                    EpisodeStatus.COMPLETED,
                    EpisodeStatus.DOWNLOADED,
                    EpisodeStatus.DOWNLOADING,
                    EpisodeStatus.PENDING,
                )
                or not ep.monitored
                for ep in (s.episodes or [])
            )
            for s in (item.seasons or [])
        )
        if needs_check:
            await _recalculate_parent_status(session, item)
            reconciled += 1
    return reconciled


async def manual_delete_transfer(
    session: AsyncSession,
    *,
    history_id: int | None = None,
    item_id: int | None = None,
    season_id: int | None = None,
    episode_id: int | None = None,
) -> dict[str, Any]:
    """Delete transfer(s) from the downloader, mark DownloadHistory 'deleted', reset target entities to SEARCHING, and recalculate parent MediaItem status."""
    from sqlalchemy import or_

    history_row: DownloadHistory | None = None
    if history_id is not None:
        history_row = (
            (
                await session.execute(
                    select(DownloadHistory)
                    .where(DownloadHistory.id == history_id)
                    .options(
                        selectinload(DownloadHistory.media_item),
                        selectinload(DownloadHistory.season).selectinload(
                            Season.episodes
                        ),
                        selectinload(DownloadHistory.episode),
                    )
                )
            )
            .scalars()
            .first()
        )
        if history_row is None:
            return {"status": "not_found", "deleted_transfer_ids": []}
        if item_id is None:
            item_id = history_row.media_item_id
        if season_id is None:
            season_id = history_row.season_id
        if episode_id is None:
            episode_id = history_row.episode_id

    item: MediaItem | None = None
    if item_id is not None:
        item = (
            (
                await session.execute(
                    select(MediaItem)
                    .where(MediaItem.id == item_id)
                    .options(
                        selectinload(MediaItem.seasons).selectinload(Season.episodes),
                    )
                )
            )
            .scalars()
            .first()
        )

    season: Season | None = None
    if season_id is not None:
        season = (
            (
                await session.execute(
                    select(Season)
                    .where(Season.id == season_id)
                    .options(
                        selectinload(Season.episodes),
                        selectinload(Season.media_item),
                    )
                )
            )
            .scalars()
            .first()
        )
        if item is None and season is not None:
            item = season.media_item

    episode: Episode | None = None
    if episode_id is not None:
        episode = (
            (
                await session.execute(
                    select(Episode)
                    .where(Episode.id == episode_id)
                    .options(
                        selectinload(Episode.season).selectinload(Season.episodes),
                        selectinload(Episode.season).selectinload(Season.media_item),
                    )
                )
            )
            .scalars()
            .first()
        )
        if season is None and episode is not None:
            season = episode.season
        if item is None and season is not None:
            item = season.media_item

    # Collect matching DownloadHistory rows to mark 'deleted' and extract transfer ids
    matching_histories: list[DownloadHistory] = []
    if history_row is not None:
        matching_histories.append(history_row)

    if episode_id is not None:
        ep_hists = (
            (
                await session.execute(
                    select(DownloadHistory).where(
                        DownloadHistory.episode_id == episode_id
                    )
                )
            )
            .scalars()
            .all()
        )
        for h in ep_hists:
            if all(existing.id != h.id for existing in matching_histories):
                matching_histories.append(h)
    elif season_id is not None:
        ep_ids = [
            ep.id for ep in (season.episodes if season else []) if ep.id is not None
        ]
        if ep_ids:
            s_stmt = select(DownloadHistory).where(
                or_(
                    DownloadHistory.season_id == season_id,
                    DownloadHistory.episode_id.in_(ep_ids),
                )
            )
        else:
            s_stmt = select(DownloadHistory).where(
                DownloadHistory.season_id == season_id
            )
        s_hists = (await session.execute(s_stmt)).scalars().all()
        for h in s_hists:
            if all(existing.id != h.id for existing in matching_histories):
                matching_histories.append(h)
    elif item_id is not None:
        i_stmt = select(DownloadHistory).where(
            DownloadHistory.media_item_id == item_id,
            DownloadHistory.season_id.is_(None),
            DownloadHistory.episode_id.is_(None),
        )
        i_hists = (await session.execute(i_stmt)).scalars().all()
        for h in i_hists:
            if all(existing.id != h.id for existing in matching_histories):
                matching_histories.append(h)

    transfer_ids_to_delete: list[str] = []
    id_types: dict[str, str] = {}
    for h in matching_histories:
        detail_low = (h.status_detail or "").strip().lower()
        if h.id == history_id or detail_low not in ("replaced", "canceled"):
            if h.id == history_id or detail_low != "deleted":
                if (
                    h.downloader_transfer_id
                    and str(h.downloader_transfer_id) not in transfer_ids_to_delete
                ):
                    transfer_ids_to_delete.append(str(h.downloader_transfer_id))
                    id_types[str(h.downloader_transfer_id)] = h.downloader_type
            h.status_detail = "deleted"
            h.is_dismissed = True
            h.notification_sent = True
            h.download_speed_bytes = 0
            h.eta_seconds = None

    # Reset target entities to SEARCHING (Ready to Push)
    if episode is not None:
        episode.status = EpisodeStatus.SEARCHING
        episode.fail_count = 0
        episode.last_error = None
        if season is not None and season.status in (
            SeasonStatus.COMPLETED,
            SeasonStatus.DOWNLOADED,
        ):
            season.status = SeasonStatus.SEARCHING
        await _recalculate_parent_status(session, item)
    elif season is not None:
        season.status = SeasonStatus.SEARCHING
        season.fail_count = 0
        season.last_error = None
        for ep in season.episodes or []:
            if ep.status != EpisodeStatus.FUTURE:
                ep.status = EpisodeStatus.SEARCHING
                ep.fail_count = 0
                ep.last_error = None
        await _recalculate_parent_status(session, item)
    elif item is not None:
        item.status = MediaStatus.SEARCHING
        item.fail_count = 0
        item.last_error = None
        if hasattr(item, "completed_at"):
            setattr(item, "completed_at", None)
        await _recalculate_parent_status(session, item)

    await session.commit()

    for tb_id in transfer_ids_to_delete:
        await _purge_for_row(session, tb_id, id_types.get(tb_id))

    remaining = await count_downloading_entities(session)
    if remaining == 0 and not transfer_poller.has_in_flight_pushes:
        transfer_poller.sleep()

    return {
        "status": "ok",
        "deleted_transfer_ids": transfer_ids_to_delete,
        "item_id": item.id if item else item_id,
    }


async def ensure_blacklisted_release(
    session: AsyncSession,
    *,
    media_item_id: int | None,
    nzb_guid: str | None,
    nzb_title: str,
    reason: str,
) -> tuple[BlacklistedRelease, bool]:
    """Idempotently create or return an existing BlacklistedRelease row (item-scoped or global)."""
    from sqlalchemy import or_

    clean_guid = (nzb_guid or "").strip() or None
    clean_title = (nzb_title or "").strip() or "Unknown Release"

    scope_cond = (
        BlacklistedRelease.media_item_id == media_item_id
        if media_item_id is not None
        else BlacklistedRelease.media_item_id.is_(None)
    )
    match_conds = [func.lower(BlacklistedRelease.nzb_title) == clean_title.lower()]
    if clean_guid:
        match_conds.append(BlacklistedRelease.nzb_guid == clean_guid)

    stmt = select(BlacklistedRelease).where(scope_cond, or_(*match_conds)).limit(1)
    existing = (await session.execute(stmt)).scalars().first()
    if existing is not None:
        return existing, False

    entry = BlacklistedRelease(
        media_item_id=media_item_id,
        nzb_guid=clean_guid,
        nzb_title=clean_title,
        reason=reason,
    )
    session.add(entry)
    await session.flush()
    return entry, True


async def blacklist_from_history(
    session: AsyncSession,
    history_id: int,
) -> dict[str, Any]:
    """Record BlacklistedRelease for a Push History row, purge from the downloader if present, mark deleted, and reset target to SEARCHING."""
    history = (
        (
            await session.execute(
                select(DownloadHistory).where(DownloadHistory.id == history_id)
            )
        )
        .scalars()
        .first()
    )
    if history is None:
        return {"status": "not_found", "deleted_transfer_ids": []}

    bl, _ = await ensure_blacklisted_release(
        session,
        media_item_id=history.media_item_id,
        nzb_guid=history.nzb_guid,
        nzb_title=history.nzb_title,
        reason="Manually blacklisted from Push History",
    )
    await session.commit()

    res = await manual_delete_transfer(session, history_id=history_id)
    res["blacklist_id"] = bl.id
    return res


async def blacklist_from_active_pushes(
    session: AsyncSession,
    history_id: int,
) -> bool:
    """Record BlacklistedRelease for an Active Pushes row, cancel/delete on the downloader or dismiss failed row, and reset target to SEARCHING."""
    history = (
        (
            await session.execute(
                select(DownloadHistory).where(DownloadHistory.id == history_id)
            )
        )
        .scalars()
        .first()
    )
    if history is None:
        return False

    await ensure_blacklisted_release(
        session,
        media_item_id=history.media_item_id,
        nzb_guid=history.nzb_guid,
        nzb_title=history.nzb_title,
        reason="Manually blacklisted from Active Pushes",
    )
    await session.commit()

    detail_lower = (history.status_detail or "").strip().lower()
    if detail_lower == "completed":
        await manual_delete_transfer(session, history_id=history_id)
        return True
    if (
        detail_lower.startswith("failed") or is_queued_detail(history.status_detail)
    ) and not history.downloader_transfer_id:
        return await dismiss_failed_push(session, history_id)
    return await cancel_active_push(session, history_id)


async def get_push_history_ledger(session: AsyncSession) -> list[dict[str, Any]]:
    """Query DownloadHistory ordered by COALESCE(dispatched_at, created_at) DESC, id DESC for Card 4 Push History Ledger."""
    stmt = (
        select(DownloadHistory)
        .options(
            selectinload(DownloadHistory.media_item),
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
        .order_by(
            func.coalesce(
                DownloadHistory.dispatched_at, DownloadHistory.created_at
            ).desc(),
            DownloadHistory.id.desc(),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    language = await _anime_title_language(session)
    delivery_paths, delivery_provider_active = await _delivery_context(
        session, [h.id for h in rows]
    )
    ledger: list[dict[str, Any]] = []

    for h in rows:
        item = h.media_item
        season = h.season
        episode = h.episode

        # Title & Target
        if item is not None:
            media_title = item.title
            display_title = anime_title_for(item, language)
            media_year = item.year
            alt_title = item.alt_title
            poster_url = item.poster_url or h.poster_url
            simkl_id = (
                (season.simkl_id if season and season.simkl_id else None)
                or item.simkl_id
                or h.simkl_id
            )
            tmdb_id = item.tmdb_id or h.tmdb_id
            imdb_id = item.imdb_id or h.imdb_id
            anilist_id = (
                (season.anilist_id if season and season.anilist_id else None)
                or item.anilist_id
                or h.anilist_id
            )
            target_label = discord.format_target_label(
                item=item, season=season, episode=episode
            )
            if item.media_type == MediaType.ANIME or getattr(
                item, "is_anime_movie", False
            ):
                type_label = "ANIME"
                category_key = "anime"
            elif item.media_type == MediaType.SHOW:
                type_label = "SERIES"
                category_key = "show"
            else:
                type_label = "MOVIE"
                category_key = "movie"
            is_detached = False
        else:
            media_title = h.media_title or h.nzb_title
            display_title = media_title
            media_year = h.media_year
            alt_title = None
            poster_url = h.poster_url
            simkl_id = h.simkl_id
            tmdb_id = h.tmdb_id
            imdb_id = h.imdb_id
            anilist_id = h.anilist_id
            target_label = h.target_label or "Movie"
            raw_type = (h.media_type_label or "MOVIE").upper()
            if "ANIME" in raw_type:
                type_label = "ANIME"
                category_key = "anime"
            elif "SERIES" in raw_type or "SHOW" in raw_type:
                type_label = "SERIES"
                category_key = "show"
            else:
                type_label = "MOVIE"
                category_key = "movie"
            is_detached = True

        # Status badge classification (active downloading transfers stay in Card 5 Active Pushes)
        detail_raw = (h.status_detail or "").strip()
        detail_low = detail_raw.lower()
        entity_is_downloading = (
            (episode is not None and episode.status == EpisodeStatus.DOWNLOADING)
            or (
                episode is None
                and season is not None
                and season.status == SeasonStatus.DOWNLOADING
            )
            or (
                episode is None
                and season is None
                and item is not None
                and item.status == MediaStatus.DOWNLOADING
            )
        )
        if detail_low in (
            "downloading",
            "queued",
            "processing",
            "unpacking",
            "verifying",
        ) or (not detail_low and entity_is_downloading):
            continue

        if detail_low in ("deleted", "expired"):
            downloader_status = "DELETED"
            status_key = "deleted"
            failure_reason = None
        elif detail_low == "replaced":
            downloader_status = "REPLACED"
            status_key = "replaced"
            failure_reason = None
        elif detail_low == "canceled":
            downloader_status = "CANCELED"
            status_key = "canceled"
            failure_reason = None
        elif detail_low.startswith("failed"):
            downloader_status = "FAILED"
            status_key = "failed"
            failure_reason = (
                detail_raw[len("failed:") :].strip()
                if ":" in detail_raw
                else detail_raw
            )
        else:
            downloader_status = "READY"
            status_key = "completed"
            failure_reason = None

        pushed_dt = h.dispatched_at or h.created_at
        pushed_local_dt = to_local_datetime(pushed_dt)
        pushed_ts = (
            pushed_local_dt.timestamp()
            if pushed_local_dt
            else (pushed_dt.timestamp() if pushed_dt else 0.0)
        )
        pushed_str = (
            pushed_local_dt.strftime("%Y-%m-%d %H:%M") if pushed_local_dt else "—"
        )

        size_gb = (
            f"{h.size_bytes / (1024**3):.1f} GB"
            if h.size_bytes and h.size_bytes > 0
            else None
        )

        feature_pills = build_release_feature_pills(
            release_title=h.nzb_title,
            score=h.score,
            size_bytes=h.size_bytes,
            is_fallback=bool(getattr(h, "is_fallback", False)),
            matched_language=h.grabbed_language,
            bitrate_mbps=h.bitrate_mbps,
            runtime_minutes=item.runtime_minutes if item else None,
            media_type=category_key,
            season_episode_count=season.episode_count if season else None,
        )

        ledger.append(
            {
                "id": h.id,
                "media_item_id": h.media_item_id,
                "season_id": h.season_id,
                "episode_id": h.episode_id,
                "media_title": media_title,
                "display_title": display_title,
                "media_year": media_year,
                "alt_title": alt_title,
                "poster_url": poster_url,
                "target_label": target_label,
                "type_label": type_label,
                "category_key": category_key,
                "is_detached": is_detached,
                "is_ad_hoc": bool(h.is_ad_hoc),
                "simkl_id": simkl_id,
                "tmdb_id": tmdb_id,
                "imdb_id": imdb_id,
                "anilist_id": anilist_id,
                "nzb_title": h.nzb_title,
                "resolution": h.resolution,
                "source": h.source,
                "video_codec": h.video_codec,
                "audio_codec": h.audio_codec,
                "grabbed_language": h.grabbed_language,
                "size_gb": size_gb,
                "score": int(h.score) if h.score is not None else None,
                "feature_pills": feature_pills,
                "pushed_at_str": pushed_str,
                "pushed_at_ts": pushed_ts,
                "downloader_transfer_id": h.downloader_transfer_id,
                "downloader_type": _row_downloader_type(h),
                "downloader_name": DOWNLOADER_DISPLAY_NAMES[_row_downloader_type(h)],
                "status_detail": h.status_detail,
                "is_dry_run": bool(getattr(h, "is_dry_run", False)),
                "downloader_status": downloader_status,
                "status_key": status_key,
                "failure_reason": failure_reason,
                "can_delete_transfer": status_key == "completed"
                and bool(h.downloader_transfer_id),
                "can_repush": h.media_item_id is not None,
                **_delivery_fields(h, delivery_paths, delivery_provider_active),
            }
        )

    return ledger


async def batch_delete_history_entries(
    session: AsyncSession,
    history_ids: list[int],
) -> dict[str, Any]:
    """Batch delete DownloadHistory rows from SQLite without touching the downloader, reconciling parent statuses."""
    if not history_ids:
        return {"deleted_count": 0, "deleted_ids": []}

    stmt = select(DownloadHistory).where(DownloadHistory.id.in_(history_ids))
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return {"deleted_count": 0, "deleted_ids": []}

    deleted_ids: list[int] = []
    affected_item_ids: set[int] = set()

    for row in rows:
        if row.id is not None:
            deleted_ids.append(row.id)
        if row.media_item_id is not None:
            affected_item_ids.add(row.media_item_id)
        await session.delete(row)

    await session.flush()

    for item_id in affected_item_ids:
        item = await session.get(MediaItem, item_id)
        if item is not None:
            await _recalculate_parent_status(session, item)

    await session.commit()
    return {"deleted_count": len(deleted_ids), "deleted_ids": deleted_ids}


async def clear_history_ledger(
    session: AsyncSession,
) -> dict[str, Any]:
    """Clear all DownloadHistory rows from SQLite without touching the downloader, reconciling all affected parent statuses."""
    stmt = select(DownloadHistory)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return {"cleared_count": 0}

    affected_item_ids: set[int] = set()
    for row in rows:
        if row.media_item_id is not None:
            affected_item_ids.add(row.media_item_id)
        await session.delete(row)

    await session.flush()

    for item_id in affected_item_ids:
        item = await session.get(MediaItem, item_id)
        if item is not None:
            await _recalculate_parent_status(session, item)

    await session.commit()
    return {"cleared_count": len(rows)}


async def purge_expired_dry_run_history(
    session: AsyncSession,
    ttl_hours: int = 1,
) -> int:
    """Purge simulated dry-run DownloadHistory rows older than ttl_hours (default 1 hour)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    effective_ts = func.coalesce(
        DownloadHistory.dispatched_at, DownloadHistory.created_at
    )
    stmt = delete(DownloadHistory).where(
        DownloadHistory.is_dry_run.is_(True),
        effective_ts < cutoff,
    )
    result = cast(CursorResult[Any], await session.execute(stmt))
    deleted_count = (
        int(result.rowcount)
        if result.rowcount is not None and result.rowcount > 0
        else 0
    )
    if deleted_count > 0:
        await session.commit()
    return deleted_count


async def autoclear_history_ledger(
    session: AsyncSession,
    *,
    autoclear_pruned: bool | None = None,
    retention_days: int | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """Purge terminal DownloadHistory rows per the Pruned-from-Watchlist and age rules (OR logic).

    Defaults come from ``settings``. Local SQLite only; never calls the downloader.
    """
    from app.config import settings

    if autoclear_pruned is None:
        autoclear_pruned = settings.history_autoclear_pruned
    if retention_days is None:
        retention_days = settings.history_retention_days
    age_enabled = retention_days > 0
    zero = {"pruned_count": 0, "aged_count": 0, "total": 0}
    if not autoclear_pruned and not age_enabled:
        return zero

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(
        days=max(retention_days, 0)
    )
    rows = (await session.execute(select(DownloadHistory))).scalars().all()
    # Old releases referenced by a pending (non-terminal) upgrade must survive (ADR-124).
    protected_ids = {
        r.replaces_history_id
        for r in rows
        if r.replaces_history_id is not None
        and not _is_history_terminal(r.status_detail)
    }
    pruned = 0
    aged = 0
    affected_item_ids: set[int] = set()
    for row in rows:
        if row.id in protected_ids or not _is_history_terminal(row.status_detail):
            continue
        if autoclear_pruned and row.media_item_id is None and not row.is_ad_hoc:
            pruned += 1
        elif age_enabled:
            ts = row.dispatched_at or row.created_at
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                continue
            aged += 1
            if row.media_item_id is not None:
                affected_item_ids.add(row.media_item_id)
        else:
            continue
        await session.delete(row)

    total = pruned + aged
    if total == 0:
        return zero

    await session.flush()
    for item_id in affected_item_ids:
        item = await session.get(MediaItem, item_id)
        if item is not None:
            await _recalculate_parent_status(session, item)
    await session.commit()
    return {"pruned_count": pruned, "aged_count": aged, "total": total}


async def delete_dry_run_history(
    session: AsyncSession,
) -> dict[str, Any]:
    """Delete all simulated dry-run DownloadHistory rows immediately."""
    stmt = delete(DownloadHistory).where(DownloadHistory.is_dry_run.is_(True))
    result = cast(CursorResult[Any], await session.execute(stmt))
    deleted_count = (
        int(result.rowcount)
        if result.rowcount is not None and result.rowcount > 0
        else 0
    )
    if deleted_count > 0:
        await session.commit()
    return {"status": "ok", "deleted_count": deleted_count}
