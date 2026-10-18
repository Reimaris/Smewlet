"""Delivery worker: single queue, state machine and startup recovery."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.log_redaction import redact_text
from app.db.database import get_session_factory
from app.db.models import DeliveredFile, DeliveryStatus, DownloadHistory
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.engines import (
    CloudStreamEngine,
    DeliveryEngine,
    LocalLinkEngine,
)
from app.services.delivery.planner import build_delivery_plan
from app.services.delivery.refresh import refresh_media_servers, title_folder
from app.services.downloaders.base import DownloaderError
from app.services.downloaders.factory import get_downloader_client_for_type
from app.services.provider_service import get_active_delivery_provider

logger = logging.getLogger(__name__)

PAUSE_CODES = frozenset(
    {"INSUFFICIENT_STORAGE", "PATH_NOT_CONFIGURED", "PATH_UNAVAILABLE"}
)
NOT_DELIVERABLE_DETAILS = frozenset(
    {"deleted", "replaced", "canceled", "downloader_removed"}
)
PROGRESS_INTERVAL_SECONDS = 1.0

_queue: asyncio.Queue[int] | None = None
_task: asyncio.Task[None] | None = None
_refresh_folders: set[Path] = set()


def _get_queue() -> asyncio.Queue[int]:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


async def enqueue_delivery(history_id: int) -> None:
    await _get_queue().put(history_id)


def _roots(cfg: DeliveryConfig) -> list[Path]:
    paths = (cfg.movies_path, cfg.shows_path, cfg.anime_path, cfg.anime_movies_path)
    return [Path(p).resolve() for p in paths if p]


def _remove_empty_parents(path: Path, roots: list[Path]) -> None:
    """Best-effort ``rmdir`` of empty parents, never the library root itself."""
    parent = path.parent.resolve()
    while any(parent != r and parent.is_relative_to(r) for r in roots):
        try:
            parent.rmdir()
        except OSError:
            return
        parent = parent.parent


async def _remove_recorded_files(
    session: AsyncSession,
    rows: list[DeliveredFile],
    cfg: DeliveryConfig,
) -> None:
    """Delete the recorded files (and their rows); only files below a library root."""
    roots = _roots(cfg)
    for row in rows:
        dest = Path(row.dest_path) if row.dest_path else None
        if dest is not None and dest.is_file():
            if any(dest.resolve().is_relative_to(r) for r in roots):
                dest.unlink(missing_ok=True)
                _remove_empty_parents(dest, roots)
        await session.delete(row)
    await session.flush()


async def _old_slot_rows(
    session: AsyncSession,
    history: DownloadHistory,
    slots: set[tuple[int | None, int | None]],
) -> list[DeliveredFile]:
    """Delivered files of the replaced row that occupy one of ``slots``."""
    if not history.is_upgrade or history.replaces_history_id is None:
        return []
    old_rows = (
        await session.execute(
            select(DeliveredFile).where(
                DeliveredFile.history_id == history.replaces_history_id,
                DeliveredFile.status == "delivered",
            )
        )
    ).scalars()
    return [
        r
        for r in old_rows
        if (r.episode_id, r.season_id) in slots
        and (r.episode_id is not None or r.season_id is not None)
    ]


def _reset_progress(history: DownloadHistory) -> None:
    history.progress_pct = 0.0
    history.download_speed_bytes = 0
    history.eta_seconds = None


def _aggregate(history: DownloadHistory, rows: list[DeliveredFile]) -> None:
    """Fold per-file results into the row's ``delivery_status``/``delivery_error``."""
    failed = [r for r in rows if r.status == "failed"]
    hard = [r for r in failed if r.error not in PAUSE_CODES]
    paused = [r for r in failed if r.error in PAUSE_CODES]
    if hard:
        status, error = DeliveryStatus.FAILED, hard[0].error
    elif paused:
        status, error = DeliveryStatus.PAUSED, paused[0].error
    elif any(r.status == "delivered" for r in rows):
        status, error = DeliveryStatus.DELIVERED, None
    else:
        skipped = next((r.error for r in rows if r.error), None)
        status, error = DeliveryStatus.FAILED, skipped or "NO_VIDEO_FILES"
    history.delivery_status = status.value
    history.delivery_error = error
    if status is DeliveryStatus.DELIVERED:
        history.delivered_at = datetime.now(timezone.utc)


async def process_delivery(session: AsyncSession, history_id: int) -> None:
    """Deliver one history row and settle its ``delivery_status``."""
    history = await session.get(DownloadHistory, history_id)
    if history is None or history.delivery_status not in (
        DeliveryStatus.PENDING.value,
        DeliveryStatus.DELIVERING.value,
    ):
        return
    history.delivery_status = DeliveryStatus.DELIVERING.value
    history.delivery_error = None
    await session.commit()
    try:
        await _deliver(session, history)
    except asyncio.CancelledError:
        raise  # stays 'delivering'; startup recovery resets it
    except Exception as exc:
        logger.exception("[DELIVERY] history %s crashed", history_id)
        await session.rollback()
        history.delivery_status = DeliveryStatus.FAILED.value
        history.delivery_error = redact_text(f"{type(exc).__name__}: {exc}")[:200]
    _reset_progress(history)
    await session.commit()
    if history.delivery_status == DeliveryStatus.DELIVERED.value:
        await _queue_refresh(session, history)
    await _notify(session, history)


async def _collect_refresh_folders(
    session: AsyncSession, history: DownloadHistory, cfg: DeliveryConfig
) -> set[Path]:
    """Title folders of every delivered file of ``history``."""
    paths = (
        await session.execute(
            select(DeliveredFile.dest_path).where(
                DeliveredFile.history_id == history.id,
                DeliveredFile.status == "delivered",
                DeliveredFile.dest_path.is_not(None),
            )
        )
    ).scalars()
    folders = (title_folder(p, cfg) for p in paths if p)
    return {f for f in folders if f is not None}


async def _queue_refresh(session: AsyncSession, history: DownloadHistory) -> None:
    """Remember the delivered title folders for the end-of-burst refresh."""
    try:
        provider = await get_active_delivery_provider(session)
        if provider is None:
            return
        cfg = DeliveryConfig.from_provider(provider)
        if cfg.plex_configured or cfg.jellyfin_configured:
            _refresh_folders.update(
                await _collect_refresh_folders(session, history, cfg)
            )
    except Exception:
        logger.exception("[DELIVERY] refresh bookkeeping failed for %s", history.id)


async def _flush_refresh() -> None:
    """Refresh the media servers once for the collected folders; never raises."""
    folders = set(_refresh_folders)
    _refresh_folders.clear()
    if not folders:
        return
    try:
        async with get_session_factory()() as session:
            provider = await get_active_delivery_provider(session)
            if provider is None:
                return
            cfg = DeliveryConfig.from_provider(provider)
            if not (cfg.plex_configured or cfg.jellyfin_configured):
                return
            results = await refresh_media_servers(cfg, sorted(folders))
            failed = [r for r in results if not r.ok]
            for r in failed:
                logger.warning(
                    "[DELIVERY] %s refresh failed (%s): %s",
                    r.server,
                    r.mode,
                    r.detail,
                )
            if failed:
                from app.services.notifications import dispatch_alert_message

                body = "Delivered, but " + "; ".join(
                    f"{r.server.capitalize()} refresh failed: {r.detail}"
                    for r in failed
                )
                await dispatch_alert_message(
                    session, "notify_on_delivery_failed", "Library refresh failed", body
                )
    except Exception:
        logger.exception("[DELIVERY] library refresh failed")


async def _deliver(session: AsyncSession, history: DownloadHistory) -> None:
    provider = await get_active_delivery_provider(session)
    if provider is None:
        history.delivery_status = DeliveryStatus.PAUSED.value
        history.delivery_error = "PATH_NOT_CONFIGURED"
        return
    cfg = DeliveryConfig.from_provider(provider)
    client = await get_downloader_client_for_type(
        session, history.downloader_type or ""
    )
    if client is None or not history.downloader_transfer_id:
        history.delivery_status = DeliveryStatus.FAILED.value
        history.delivery_error = "No downloader available"
        return
    try:
        files = await client.get_transfer_files(history.downloader_transfer_id)
    except DownloaderError as exc:
        history.delivery_status = DeliveryStatus.FAILED.value
        history.delivery_error = redact_text(str(exc))[:200]
        return

    await session.execute(
        delete(DeliveredFile).where(
            DeliveredFile.history_id == history.id,
            DeliveredFile.status != "delivered",
        )
    )
    plan = await build_delivery_plan(session, history, files, cfg)

    # Upgrade: an old recorded file at the very path the new one lands on is
    # removed first (the engine never overwrites).
    planned_slots = {(p.episode_id, p.season_id) for p in plan if p.status == "pending"}
    planned_paths = {str(p.dest.full) for p in plan if p.status == "pending"}
    await _remove_recorded_files(
        session,
        [
            r
            for r in await _old_slot_rows(session, history, planned_slots)
            if r.dest_path in planned_paths
        ],
        cfg,
    )

    engine: DeliveryEngine
    if history.downloader_type == "sabnzbd":
        engine = LocalLinkEngine()
    else:
        started, last_flush = time.monotonic(), 0.0

        async def progress(done: int, total: int) -> None:
            nonlocal last_flush
            now = time.monotonic()
            if now - last_flush < PROGRESS_INTERVAL_SECONDS and done < total:
                return
            last_flush = now
            speed = int(done / max(now - started, 0.001))
            history.progress_pct = round(100.0 * done / total, 1) if total else 0.0
            history.download_speed_bytes = speed
            history.eta_seconds = int((total - done) / speed) if speed else None
            await session.commit()

        engine = CloudStreamEngine(progress)
    await engine.deliver(session, history, plan, cfg)

    rows = list(
        (
            await session.execute(
                select(DeliveredFile).where(DeliveredFile.history_id == history.id)
            )
        ).scalars()
    )
    _aggregate(history, rows)
    if history.delivery_status == DeliveryStatus.DELIVERED.value:
        await _after_delivered(session, history, rows, cfg)


async def _after_delivered(
    session: AsyncSession,
    history: DownloadHistory,
    rows: list[DeliveredFile],
    cfg: DeliveryConfig,
) -> None:
    """Upgrade replace of the old slot files, then optional cloud purge."""
    delivered = [r for r in rows if r.status == "delivered" and r.dest_path]
    if (
        history.is_upgrade
        and delivered
        and all(Path(r.dest_path or "").is_file() for r in delivered)
    ):
        slots = {(r.episode_id, r.season_id) for r in delivered}
        await _remove_recorded_files(
            session, await _old_slot_rows(session, history, slots), cfg
        )
    if (
        cfg.delete_cloud_after_delivery
        and history.downloader_type != "sabnzbd"
        and history.downloader_transfer_id
    ):
        client = await get_downloader_client_for_type(
            session, history.downloader_type or ""
        )
        try:
            if client is not None and await client.delete_transfer(
                history.downloader_transfer_id
            ):
                history.status_detail = "deleted"
        except DownloaderError as exc:
            logger.warning(
                "[DELIVERY] cloud purge failed for history %s: %s",
                history.id,
                redact_text(str(exc)),
            )


async def _notify(session: AsyncSession, history: DownloadHistory) -> None:
    """One message per settled row, gated by the delivery notification toggles."""
    status, error = history.delivery_status, history.delivery_error
    title = history.media_title or history.nzb_title
    label = f"{title} ({history.target_label})" if history.target_label else title
    if status == DeliveryStatus.DELIVERED.value:
        flag, head, body = "notify_on_delivered", "Delivered to library", label
    elif status == DeliveryStatus.FAILED.value or (
        status == DeliveryStatus.PAUSED.value and error == "INSUFFICIENT_STORAGE"
    ):
        flag, head = "notify_on_delivery_failed", "Delivery failed"
        body = f"{label}\n{error or 'unknown error'}"
    else:
        return
    try:
        from app.services.notifications import dispatch_alert_message

        await dispatch_alert_message(session, flag, head, body)
    except Exception:
        logger.exception("[DELIVERY] notification failed for history %s", history.id)


async def _worker_loop() -> None:
    queue = _get_queue()
    while True:
        history_id = await queue.get()
        try:
            async with get_session_factory()() as session:
                await process_delivery(session, history_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[DELIVERY] worker error for history %s", history_id)
        finally:
            queue.task_done()
        if queue.empty() and _refresh_folders:
            await _flush_refresh()


async def start_delivery_worker() -> None:
    """Start the single delivery task (``delivery_max_parallel`` is fixed at 1)."""
    global _task
    if _task is None or _task.done():
        _task = asyncio.create_task(_worker_loop())


async def stop_delivery_worker() -> None:
    global _task, _queue
    if _task is not None:
        _task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _task
    _task = None
    _queue = None


async def recover_interrupted_deliveries(session: AsyncSession) -> int:
    """Startup recovery; returns the number of re-enqueued rows."""
    provider = await get_active_delivery_provider(session)
    roots: list[Path] = []
    if provider is not None:
        cfg = DeliveryConfig.from_provider(provider)
        roots = _roots(cfg)
    unfinished = (
        await session.execute(
            select(DeliveredFile.dest_path).where(
                DeliveredFile.status != "delivered",
                DeliveredFile.dest_path.is_not(None),
            )
        )
    ).scalars()
    for dest in unfinished:
        part = Path(f"{dest}.part")
        if part.is_file() and any(part.resolve().is_relative_to(r) for r in roots):
            part.unlink(missing_ok=True)

    rows = (
        (
            await session.execute(
                select(DownloadHistory).where(
                    DownloadHistory.delivery_status.in_(
                        (DeliveryStatus.DELIVERING.value, DeliveryStatus.PENDING.value)
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    for h in rows:
        h.delivery_status = DeliveryStatus.PENDING.value
    await session.commit()
    for h in rows:
        await enqueue_delivery(h.id)
    return len(rows)


async def retry_delivery(session: AsyncSession, history_id: int) -> bool:
    history = await session.get(DownloadHistory, history_id)
    if history is None or history.delivery_status not in (
        DeliveryStatus.FAILED.value,
        DeliveryStatus.PAUSED.value,
    ):
        return False
    history.delivery_status = DeliveryStatus.PENDING.value
    history.delivery_error = None
    await session.commit()
    await enqueue_delivery(history_id)
    return True


def is_deliver_now_allowed(history: DownloadHistory) -> bool:
    """Card 4 'Deliver to Library': finished cloud row, still on the downloader."""
    return (
        history.completed_at is not None
        and history.downloader_type != "sabnzbd"
        and (history.status_detail or "") not in NOT_DELIVERABLE_DETAILS
        and history.delivery_status
        in (
            DeliveryStatus.NONE.value,
            DeliveryStatus.FAILED.value,
            DeliveryStatus.PAUSED.value,
        )
    )


async def deliver_now(session: AsyncSession, history_id: int) -> bool:
    """Manual 'Deliver to Library' for a finished cloud row."""
    history = await session.get(DownloadHistory, history_id)
    if history is None or not is_deliver_now_allowed(history):
        return False
    history.deliver_enabled = True
    history.delivery_status = DeliveryStatus.PENDING.value
    history.delivery_error = None
    await session.commit()
    await enqueue_delivery(history_id)
    return True
