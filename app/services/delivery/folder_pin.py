"""Pin the anime library folder per Season after its first delivery."""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DeliveredFile, Season
from app.services.delivery.config import DeliveryConfig
from app.services.provider_service import get_active_delivery_provider

logger = logging.getLogger(__name__)


def _first_component(dest_path: str, cfg: DeliveryConfig) -> str | None:
    """First path component of ``dest_path`` below an anime root, if any."""
    for root in (cfg.anime_path, cfg.anime_movies_path):
        if not root:
            continue
        try:
            parts = PurePosixPath(dest_path).relative_to(PurePosixPath(root)).parts
        except ValueError:
            continue
        if len(parts) > 1:
            return parts[0]
    return None


async def pin_delivery_folders(
    session: AsyncSession, rows: list[DeliveredFile], cfg: DeliveryConfig
) -> None:
    """Pin ``Season.delivery_folder`` from delivered anime video rows."""
    for row in rows:
        if (
            row.status != "delivered"
            or row.kind != "video"
            or row.season_id is None
            or row.anilist_id is None
            or not row.dest_path
        ):
            continue
        season = await session.get(Season, row.season_id)
        if season is None or season.delivery_folder:
            continue
        folder = _first_component(row.dest_path, cfg)
        if folder:
            season.delivery_folder = folder[:512]
    await session.flush()


async def backfill_delivery_folders(session: AsyncSession) -> int:
    """Pin already delivered anime seasons; returns the number pinned."""
    provider = await get_active_delivery_provider(session)
    if provider is None:
        return 0
    cfg = DeliveryConfig.from_provider(provider)
    stmt = (
        select(Season, DeliveredFile.dest_path)
        .join(DeliveredFile, DeliveredFile.season_id == Season.id)
        .where(
            Season.delivery_folder.is_(None),
            DeliveredFile.status == "delivered",
            DeliveredFile.kind == "video",
            DeliveredFile.dest_path.is_not(None),
        )
        .order_by(DeliveredFile.id)
    )
    pinned = 0
    for season, dest_path in (await session.execute(stmt)).all():
        if season.delivery_folder:
            continue
        folder = _first_component(dest_path, cfg)
        if folder:
            season.delivery_folder = folder[:512]
            pinned += 1
    await session.commit()
    logger.info("[DELIVERY] pinned %d anime delivery folder(s)", pinned)
    return pinned
