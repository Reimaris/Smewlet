"""
Provider Service
================
Central service for managing and resolving third-party providers (Watchlist, Reading, Metadata, Downloader, Indexer).
"""

from __future__ import annotations

import logging
from typing import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DownloadHistory, Provider, ProviderCategory

logger = logging.getLogger(__name__)

_active_name = "TorBox"

_METADATA_TYPES: frozenset[str] = frozenset({"tmdb"})

DELIVERY_TYPES: frozenset[str] = frozenset({"local_directory"})


class DeliveryProviderExistsError(Exception):
    """Raised when a second delivery provider is created (routes map this to HTTP 409)."""

    def __init__(self) -> None:
        super().__init__("A delivery provider already exists")


def provider_category_for_type(type: str) -> str:
    """Map a provider type to its category value; unknown types raise ``ValueError``."""
    from app.services.downloaders.base import DOWNLOADER_TYPES
    from app.services.indexers.catalog import is_indexer_type
    from app.services.watchlists import WATCHLIST_TYPES

    t = (type or "").strip().lower()
    if t in WATCHLIST_TYPES:
        return ProviderCategory.WATCHLIST.value
    if t in DOWNLOADER_TYPES:
        return ProviderCategory.DOWNLOADER.value
    if t == "newznab" or is_indexer_type(t):
        return ProviderCategory.INDEXER.value
    if t in _METADATA_TYPES:
        return ProviderCategory.METADATA.value
    if t in DELIVERY_TYPES:
        return ProviderCategory.DELIVERY.value
    raise ValueError(f"Unknown provider type: {type!r}")


class ActiveDownloaderName:
    """Template global: renders the active downloader's display name ('TorBox' fallback)."""

    def __str__(self) -> str:
        return _active_name


def _remember_active(provider: Provider | None) -> None:
    from app.services.downloaders.base import DOWNLOADER_DISPLAY_NAMES

    global _active_name
    _active_name = (
        DOWNLOADER_DISPLAY_NAMES.get(provider.type, "TorBox") if provider else "TorBox"
    )


async def get_active_downloader(db: AsyncSession) -> Provider | None:
    """Retrieve the single active downloader provider."""
    stmt = (
        select(Provider)
        .where(
            Provider.category == ProviderCategory.DOWNLOADER.value,
            Provider.is_active == True,  # noqa: E712
        )
        .limit(1)
    )
    result = await db.execute(stmt)
    provider = result.scalars().first()
    _remember_active(provider)
    return provider


async def set_active_downloader(db: AsyncSession, provider_id: int) -> None:
    """Set the specified downloader as active and deactivate all other downloaders."""
    # Deactivate all downloaders
    await db.execute(
        update(Provider)
        .where(Provider.category == ProviderCategory.DOWNLOADER.value)
        .values(is_active=False)
    )
    # Activate the target downloader
    await db.execute(
        update(Provider)
        .where(
            Provider.category == ProviderCategory.DOWNLOADER.value,
            Provider.id == provider_id,
        )
        .values(is_active=True)
    )
    await db.commit()
    await get_active_downloader(db)


async def count_foreign_downloading(
    db: AsyncSession, downloader_type: str
) -> dict[str, int]:
    """Count in-flight (downloading) history rows per downloader type other than the given one."""
    stmt = (
        select(DownloadHistory.downloader_type, func.count())
        .where(
            DownloadHistory.status_detail == "downloading",
            DownloadHistory.downloader_type != downloader_type,
        )
        .group_by(DownloadHistory.downloader_type)
    )
    return {t: n for t, n in (await db.execute(stmt)).all()}


async def get_active_indexers(db: AsyncSession) -> Sequence[Provider]:
    """Retrieve all active Usenet indexers ordered by priority ASC, id ASC."""
    stmt = (
        select(Provider)
        .where(
            Provider.category == ProviderCategory.INDEXER.value,
            Provider.is_active == True,  # noqa: E712
        )
        .order_by(Provider.priority.asc(), Provider.id.asc())
    )
    result = await db.execute(stmt)
    indexers = list(result.scalars().all())
    if not indexers:
        from app.config import settings
        from app.services.indexers.catalog import INDEXER_PRESETS

        preset = INDEXER_PRESETS[0]
        api_key = getattr(settings, "treasure_maps_api_key", "")
        return [
            Provider(
                id=0,
                name=preset.name,
                type=preset.type,
                category=ProviderCategory.INDEXER.value,
                api_url=preset.api_url,
                api_key=api_key,
                priority=1,
                is_active=True,
            )
        ]
    return indexers


async def get_metadata_provider(
    db: AsyncSession, provider_type: str
) -> Provider | None:
    """Retrieve an active metadata provider by type (e.g. 'tmdb', 'anilist')."""
    stmt = (
        select(Provider)
        .where(
            Provider.category == ProviderCategory.METADATA.value,
            Provider.type == provider_type,
            Provider.is_active == True,  # noqa: E712
        )
        .limit(1)
    )
    result = await db.execute(stmt)
    return result.scalars().first()


async def get_providers_by_category(
    db: AsyncSession, category: str | ProviderCategory
) -> Sequence[Provider]:
    """Retrieve all providers in a given category ordered by priority and name."""
    cat_val = (
        category.value if isinstance(category, ProviderCategory) else str(category)
    )
    stmt = (
        select(Provider)
        .where(Provider.category == cat_val)
        .order_by(Provider.priority.asc(), Provider.name.asc())
    )
    result = await db.execute(stmt)
    return result.scalars().all()


async def get_delivery_provider(db: AsyncSession) -> Provider | None:
    """Retrieve the single delivery provider row (active or not)."""
    stmt = (
        select(Provider)
        .where(Provider.category == ProviderCategory.DELIVERY.value)
        .limit(1)
    )
    return (await db.execute(stmt)).scalars().first()


async def get_active_delivery_provider(db: AsyncSession) -> Provider | None:
    """Retrieve the delivery provider only when it is active."""
    provider = await get_delivery_provider(db)
    return provider if provider is not None and provider.is_active else None


async def create_delivery_provider(
    db: AsyncSession, *, name: str = "Local directory", config_json: str = "{}"
) -> Provider:
    """Create the delivery provider (inactive); raises if one already exists."""
    if await get_delivery_provider(db) is not None:
        raise DeliveryProviderExistsError()
    provider = Provider(
        name=name,
        type="local_directory",
        category=ProviderCategory.DELIVERY.value,
        is_active=False,
        config_json=config_json,
    )
    db.add(provider)
    await db.flush()
    return provider
