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


def _get_queue() -> asyncio.Queue[int]:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


async def enqueue_delivery(history_id: int) -> None:
    await _get_queue().put(history_id)


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
        roots = [
            Path(p).resolve()
            for p in (
                cfg.movies_path,
                cfg.shows_path,
                cfg.anime_path,
                cfg.anime_movies_path,
            )
            if p
        ]
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


async def deliver_now(session: AsyncSession, history_id: int) -> bool:
    """Manual 'Deliver to Library' for a finished cloud row."""
    history = await session.get(DownloadHistory, history_id)
    if (
        history is None
        or history.completed_at is None
        or history.downloader_type == "sabnzbd"
        or (history.status_detail or "") in NOT_DELIVERABLE_DETAILS
        or history.delivery_status
        not in (
            DeliveryStatus.NONE.value,
            DeliveryStatus.FAILED.value,
            DeliveryStatus.PAUSED.value,
        )
    ):
        return False
    history.deliver_enabled = True
    history.delivery_status = DeliveryStatus.PENDING.value
    history.delivery_error = None
    await session.commit()
    await enqueue_delivery(history_id)
    return True
