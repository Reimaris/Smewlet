"""
Rate-Limit Push Queue (ADR-094)
===============================
When a downloader rate-limits Smewlet (HTTP 429 / active cooldown), Auto-Push targets
are not failed. Instead a persisted ``rate_limited`` ``DownloadHistory`` row
(and ``RATE_LIMITED`` entity status) marks the target as paused. A lightweight
APScheduler job (``rate_limit_resume_job``) re-runs Auto-Push Best for the
queued targets in FIFO order once the downloader cooldown has expired.

The job is paused whenever the queue is empty (zero idle CPU/DB usage) and is
woken whenever a push is enqueued. Because the queue lives in the database it
survives restarts; after a restart the in-memory cooldown is unknown, so queued
pushes are attempted immediately (a fresh 429 simply re-queues them).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import func, select

from app.core.outage import (
    active_outages,
    clear_outages,
    is_queued_detail,
    queued_detail_clause,
)
from app.core.prerequisites import setup_blocks
from app.db.models import DownloadHistory, MediaItem
from app.services.discord import EVENT_HEADERS
from app.services.downloaders.base import DOWNLOADER_DISPLAY_NAMES
from app.services.downloaders.factory import downloader_classes
from app.services.indexers import newznab as newznab_client

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

RATE_LIMITED_DETAIL = "rate_limited"
RESUME_JOB_ID = "rate_limit_resume_job"


def is_rate_limit_error(message: str | None) -> bool:
    """True if a failure reason/exception text describes a downloader or Indexer rate limit."""
    msg = (message or "").lower()
    return (
        "rate limit" in msg
        or "too many requests" in msg
        or "429" in msg
        or "fair use limit" in msg
    )


def is_indexer_rate_limit_error(message: str | None) -> bool:
    """True if a failure reason/exception text specifically describes an Indexer rate limit."""
    msg = (message or "").lower()
    return is_rate_limit_error(msg) and ("indexer" in msg or "grab limit" in msg)


@dataclass(frozen=True)
class RateLimitSource:
    """One provider currently cooling down after a rate limit."""

    kind: Literal["downloader", "indexer"]
    name: str
    remaining_seconds: float
    reason: Literal["rate_limit", "outage"] = "rate_limit"
    label: str = ""


def _downloader_cooldowns() -> list[tuple[str, float]]:
    return [
        (DOWNLOADER_DISPLAY_NAMES[dtype], cls.get_cooldown_remaining(is_manual=True))
        for dtype, cls in downloader_classes().items()
    ]


def get_downloader_cooldown_remaining() -> float:
    """Longest active downloader cooldown (max over adapters)."""
    return max(rem for _, rem in _downloader_cooldowns())


def longest_downloader_cooldown() -> tuple[str, float]:
    """(display name, remaining seconds) of the longest downloader cooldown."""
    return max(_downloader_cooldowns(), key=lambda c: c[1])


def downloader_rate_limit_detail() -> str:
    """'<Downloader> rate limit exceeded (cooldown: Ns)' for the longest cooldown."""
    name, rem = longest_downloader_cooldown()
    return f"{name} rate limit exceeded (cooldown: {int(rem)}s)"


def active_rate_limit_sources() -> list[RateLimitSource]:
    """Downloaders and indexers whose cooldown is currently active."""
    outages = {(o.kind, o.provider_name): o for o in active_outages()}

    def _source(
        kind: Literal["downloader", "indexer"], name: str, rem: float
    ) -> RateLimitSource:
        outage = outages.get((kind, name))
        if outage is None:
            return RateLimitSource(kind, name, rem)
        return RateLimitSource(kind, name, rem, "outage", outage.label)

    sources = [
        _source("downloader", name, rem)
        for name, rem in _downloader_cooldowns()
        if rem > 0
    ]
    sources += [
        _source("indexer", name, rem)
        for name, rem in newznab_client.get_all_indexer_cooldowns().items()
    ]
    return sources


def rate_limit_source_label(sources: list[RateLimitSource]) -> str:
    """A downloader name, an indexer name, both joined by ' + ', or 'Rate limit' when empty."""
    names = list(dict.fromkeys(s.name for s in sources))
    return " + ".join(names) if names else "Rate limit"


def clock_label(moment: datetime) -> str:
    """Local ``HH:MM`` for a (UTC or aware) datetime."""
    return f"{moment.astimezone():%H:%M}"


def resume_time_label() -> str:
    """Human-readable resume time for the longest active provider cooldown."""
    tb_rem = get_downloader_cooldown_remaining()
    idx_rem = newznab_client.get_all_indexer_cooldowns_remaining()
    remaining = max(tb_rem, idx_rem)
    if remaining <= 0:
        return "resuming shortly"
    resume_at = datetime.now().astimezone() + timedelta(seconds=remaining)
    return f"resumes {clock_label(resume_at)}"


def clear_all_rate_limit_cooldowns() -> None:
    """Clear cooldowns across all downloaders and configured indexers."""
    for cls in downloader_classes().values():
        cls.clear_cooldown()
    newznab_client.clear_all_indexer_cooldowns()
    clear_outages()


class RateLimitQueueState:
    """APScheduler wake/sleep control for the rate-limit resume job."""

    def __init__(self) -> None:
        self._scheduler: Any | None = None

    def set_scheduler(self, scheduler: Any) -> None:
        self._scheduler = scheduler

    def wake(self) -> None:
        if self._scheduler is None:
            return
        try:
            job = self._scheduler.get_job(RESUME_JOB_ID)
            if job is not None and job.next_run_time is None:
                self._scheduler.resume_job(RESUME_JOB_ID)
                logger.info("⏸️ Rate-limit queue active — resume job woken.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not resume %s: %s", RESUME_JOB_ID, exc)

    def sleep(self) -> None:
        if self._scheduler is None:
            return
        try:
            job = self._scheduler.get_job(RESUME_JOB_ID)
            if job is not None and job.next_run_time is not None:
                self._scheduler.pause_job(RESUME_JOB_ID)
                logger.info("💤 Rate-limit queue empty — resume job paused.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not pause %s: %s", RESUME_JOB_ID, exc)


rate_limit_queue = RateLimitQueueState()


async def _reached_settle_delay() -> None:
    """Wait for a burst of enqueues to settle (``RATE_LIMIT_NOTIFY_DELAY_SECONDS``)."""
    from app.config import settings

    delay = settings.rate_limit_notify_delay_seconds
    if delay > 0:
        await asyncio.sleep(delay)


class RateLimitEpisode:
    """Tracks one rate-limit episode so only two alerts are sent per episode (ADR-095).

    An episode starts at the first enqueue, sends one ``reached`` alert, one
    ``resumed`` alert when the queue restarts, and ends when the queue is empty.
    """

    def __init__(self, *, outage: bool = False) -> None:
        self.outage = outage  # outage episodes alert via notify_on_outages
        self.active = False
        self.resumed_sent = False
        self.label = "Rate limit"
        self._reached_pending = False
        self._dirty = False
        self._task: asyncio.Task[None] | None = None

    def reset(self) -> None:
        self.active = False
        self.label = "Rate limit"
        self.resumed_sent = False
        self._reached_pending = False
        self._dirty = False

    def adopt_existing_queue(self) -> None:
        """After a restart, treat queued rows as an ongoing episode.

        The "reached" alert was already sent before the restart, so only the
        "resumed" alert remains to be sent.
        """
        self.active = True
        self.resumed_sent = False
        self._reached_pending = False

    def on_enqueue(self) -> None:
        """Called after a push is queued; starts the episode and its single alert."""
        if self.active:
            if self._reached_pending:
                self._dirty = True  # more pushes queued while settling
            return
        self.active = True
        self.resumed_sent = False
        self._reached_pending = True
        self._dirty = False
        try:
            self._task = asyncio.get_running_loop().create_task(self._notify_reached())
        except RuntimeError:  # no running loop
            self.reset()

    async def _notify_reached(self) -> None:
        from app.db.database import async_session_factory

        try:
            for _ in range(5):  # debounce: re-wait while new pushes keep arriving
                self._dirty = False
                await _reached_settle_delay()
                if not self._dirty:
                    break
            self._reached_pending = False
            self.label = rate_limit_source_label(active_rate_limit_sources())
            async with async_session_factory() as session:
                pending = await count_rate_limited_pushes(session)
                await self._alert(
                    session,
                    EVENT_HEADERS["outage_paused"]
                    if self.outage
                    else f"⏸️ {self.label} rate limit reached",
                    f"{pending} push(es) paused — {resume_time_label()}.",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Rate-limit reached alert failed: %s", exc)

    async def _alert(self, session: AsyncSession, title: str, body: str) -> None:
        if self.outage:
            await _send_alert(session, title, body, flag="notify_on_outages")
        else:
            await _send_alert(session, title, body)

    async def on_resume(self, pending: int) -> None:
        """Send the single ``resumed`` alert when the queue restarts."""
        if not self.active or self.resumed_sent:
            return
        self.resumed_sent = True
        from app.db.database import async_session_factory

        try:
            async with async_session_factory() as session:
                await self._alert(
                    session,
                    EVENT_HEADERS["outage_resumed"]
                    if self.outage
                    else f"▶️ {self.label} rate limit over — queue resumed",
                    f"Resuming {pending} paused push(es).",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Rate-limit resumed alert failed: %s", exc)


async def _send_alert(
    session: AsyncSession, title: str, body: str, flag: str = "notify_on_errors"
) -> None:
    from app.services.notifications import dispatch_alert_message

    await dispatch_alert_message(session, flag, title, body)


rate_limit_episode = RateLimitEpisode()
outage_episode = RateLimitEpisode(outage=True)


async def count_rate_limited_pushes(session: AsyncSession) -> int:
    """Number of queued (not dismissed) rate-limited pushes."""
    return (
        await session.execute(
            select(func.count(DownloadHistory.id)).where(
                queued_detail_clause(DownloadHistory.status_detail),
                DownloadHistory.is_dismissed.is_(False),
            )
        )
    ).scalar() or 0


async def _resume_adhoc_pushes(history_ids: list[int]) -> int:
    """Re-push queued detached ad-hoc rows (FIFO) from their stored release link.

    A successful push writes a fresh row, so the parked row is deleted; a push that
    is parked again by a new outage keeps only the new parked row. A hard failure
    turns the parked row into a ``failed:`` ledger entry.
    """
    from app.core.adhoc_search import parse_target_label, push_adhoc_release
    from app.db.database import async_session_factory

    resumed = 0
    for history_id in history_ids:
        if (
            max(
                get_downloader_cooldown_remaining(),
                newznab_client.get_all_indexer_cooldowns_remaining(),
            )
            > 0
        ):
            break
        try:
            async with async_session_factory() as session:
                row = await session.get(DownloadHistory, history_id)
                if (
                    row is None
                    or row.is_dismissed
                    or not is_queued_detail(row.status_detail)
                ):
                    continue
                if not row.release_link:
                    row.status_detail = "failed: queued release link missing"
                    await session.commit()
                    continue
                season, episode = parse_target_label(row.target_label)
                logger.info(
                    "▶️ Resuming queued ad-hoc push '%s' (%s).",
                    row.nzb_title,
                    row.target_label,
                )
                outcome = await push_adhoc_release(
                    session,
                    release_link=row.release_link,
                    nzb_title=row.nzb_title,
                    nzb_guid=row.nzb_guid,
                    score=row.score,
                    size_bytes=row.size_bytes,
                    is_fallback=row.is_fallback,
                    matched_language=row.grabbed_language,
                    media_title=row.media_title,
                    media_year=row.media_year,
                    media_kind=row.media_type_label,
                    poster_url=row.poster_url,
                    tmdb_id=row.tmdb_id,
                    imdb_id=row.imdb_id,
                    anilist_id=row.anilist_id,
                    season=season,
                    episode=episode,
                    deliver=row.deliver_enabled,
                )
                if outcome.ok:
                    await session.delete(row)
                    resumed += 0 if outcome.queued else 1
                else:
                    row.status_detail = f"failed: {outcome.message}"[:200]
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Resuming queued ad-hoc push #%s failed: %s", history_id, exc
            )
    return resumed


async def resume_rate_limited_pushes() -> int:
    """Re-run Auto-Push Best for queued targets (FIFO) once provider cooldowns are over.

    Returns the number of items whose queued targets were re-dispatched.
    """
    from app.core.push_engine import execute_auto_push
    from app.core.search_lock import item_lock_manager
    from app.core.transfer_poller import transfer_poller
    from app.db.database import async_session_factory

    tb_rem = get_downloader_cooldown_remaining()
    idx_rem = newznab_client.get_all_indexer_cooldowns_remaining()
    if max(tb_rem, idx_rem) > 0:
        return 0

    async with async_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(DownloadHistory)
                    .where(
                        queued_detail_clause(DownloadHistory.status_detail),
                        DownloadHistory.is_dismissed.is_(False),
                    )
                    .order_by(DownloadHistory.id)
                )
            )
            .scalars()
            .all()
        )
    adhoc_ids = [h.id for h in rows if h.media_item_id is None and h.is_ad_hoc]
    rows = [h for h in rows if h.media_item_id is not None]

    if not rows and not adhoc_ids:
        rate_limit_episode.reset()
        outage_episode.reset()
        rate_limit_queue.sleep()
        return 0

    logger.debug(
        "[QUEUE] Resume check: %d queued pushes. Downloader cooldown: %ds, Indexer cooldown: %ds.",
        len(rows) + len(adhoc_ids),
        int(tb_rem),
        int(idx_rem),
    )

    await rate_limit_episode.on_resume(len(rows) + len(adhoc_ids))
    await outage_episode.on_resume(len(rows) + len(adhoc_ids))

    # Preserve FIFO order of queued rows (manual picks keep their chosen release;
    # auto-push targets are grouped per item).
    work_items: list[tuple[str, int, dict[str, Any]]] = []
    auto_groups: dict[int, dict[str, list[int]]] = {}
    for h in rows:
        assert h.media_item_id is not None
        if (h.push_mode or "auto").lower() == "manual" and h.nzb_guid:
            work_items.append(
                (
                    "manual",
                    h.media_item_id,
                    {
                        "history_id": h.id,
                        "guid": h.nzb_guid,
                        "title": h.nzb_title,
                        "size_bytes": h.size_bytes or 0,
                        "score": h.score or 0.0,
                        "is_fallback": bool(h.is_fallback),
                        "matched_language": h.grabbed_language,
                        "season_id": h.season_id,
                        "episode_id": h.episode_id,
                    },
                )
            )
            continue
        if h.media_item_id not in auto_groups:
            grp: dict[str, list[int]] = {
                "history_ids": [h.id],
                "season_ids": [],
                "episode_ids": [],
            }
            auto_groups[h.media_item_id] = grp
            work_items.append(("auto", h.media_item_id, grp))
        else:
            grp = auto_groups[h.media_item_id]
            grp["history_ids"].append(h.id)
        if h.episode_id is not None:
            grp["episode_ids"].append(h.episode_id)
        elif h.season_id is not None:
            grp["season_ids"].append(h.season_id)

    resumed = 0
    for mode, item_id, payload in work_items:
        tb_rem = get_downloader_cooldown_remaining()
        idx_rem = newznab_client.get_all_indexer_cooldowns_remaining()
        if max(tb_rem, idx_rem) > 0:
            break
        if not item_lock_manager.try_acquire(item_id, owner="background"):
            continue  # busy — retried on the next tick
        try:
            async with async_session_factory() as session:
                item = await session.get(MediaItem, item_id)
                if item is None:
                    continue
                transfer_poller.register_in_flight_push(item_id, item.title)
                if mode == "manual":
                    from app.core.push_engine import execute_manual_grab

                    logger.info(
                        "▶️ Resuming rate-limited manual pick for '%s' (%s).",
                        item.title,
                        payload.get("title"),
                    )
                    await execute_manual_grab(session, item_id, dict(payload))
                    h_id = str(payload.get("history_id") or "unknown")
                    logger.debug(
                        "[QUEUE] Cooldown expired -> Dispatched queued push #%s for '%s'.",
                        h_id,
                        item.title,
                    )
                else:
                    logger.info(
                        "▶️ Resuming rate-limited Auto-Push for '%s' (%d target(s)).",
                        item.title,
                        len(payload["season_ids"]) + len(payload["episode_ids"]) or 1,
                    )
                    await execute_auto_push(session, item_id, dict(payload))
                    h_ids = [str(hid) for hid in payload.get("history_ids", [])]
                    logger.debug(
                        "[QUEUE] Cooldown expired -> Dispatched queued push #%s for '%s'.",
                        ",".join(h_ids) if h_ids else "unknown",
                        item.title,
                    )
                resumed += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Resuming rate-limited push for item %s failed: %s", item_id, exc
            )
        finally:
            transfer_poller.unregister_in_flight_push(item_id)
            item_lock_manager.release(item_id)

    resumed += await _resume_adhoc_pushes(adhoc_ids)

    async with async_session_factory() as session:
        if await count_rate_limited_pushes(session) == 0:
            rate_limit_episode.reset()
            outage_episode.reset()
            rate_limit_queue.sleep()
    return resumed


async def run_rate_limit_resume_tick() -> None:
    """APScheduler entry point (cheap in-memory cooldown check first)."""
    if await setup_blocks("rate-limit resume", only=("downloader", "indexer")):
        return
    tb_rem = get_downloader_cooldown_remaining()
    idx_rem = newznab_client.get_all_indexer_cooldowns_remaining()
    rem = max(tb_rem, idx_rem)
    if rem > 0:
        logger.debug(
            "Rate-limit resume tick skipped — active cooldowns: %s",
            rate_limit_source_label(active_rate_limit_sources()),
        )
        return
    try:
        await resume_rate_limited_pushes()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Rate-limit resume tick failed: %s", exc)
