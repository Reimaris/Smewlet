"""Central immutable provider capability registry (m70, ADR-153)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.db.models import MediaItemWatchlistSource, Provider
from app.services.indexers.catalog import INDEXER_PRESETS

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.db.models import MediaItem

CATEGORY_ORDER: tuple[str, ...] = (
    "watchlist",
    "downloader",
    "metadata",
    "indexer",
    "delivery",
)

AUTO_ADVANCE_UNAVAILABLE_MESSAGE: str = (
    "⚠ Unavailable: None of this title's connected watchlist sources (e.g. Plex) "
    "report watch progress. Auto-advance requires Simkl, AniList, or MyAnimeList."
)

_ALL_MEDIA: tuple[str, ...] = ("movies", "series", "anime")
_NO_PROGRESS = "No watch progress (cannot trigger season auto-advance)"
_TMDB_DATES = "No exact air dates (uses TMDB dates)"


@dataclass(frozen=True, slots=True)
class ProviderCapability:
    provider_type: str
    category: str  # "watchlist" | "downloader" | "indexer" | "metadata" | "delivery"
    display_name: str
    media_coverage: tuple[str, ...]  # subset of ("movies", "series", "anime")
    supports_watch_progress: bool = False
    supports_exact_air_dates: bool = False
    downloader_kind: str = ""  # "cloud" | "local" | ""
    supports_unpack_verification: bool = False
    supports_remote_delete: bool = False
    supports_category_mapping: bool = False
    supports_custom_categories: bool = False
    summary: str = ""
    limitations: tuple[str, ...] = ()


def _indexer(provider_type: str, display_name: str) -> ProviderCapability:
    return ProviderCapability(
        provider_type,
        "indexer",
        display_name,
        _ALL_MEDIA,
        supports_custom_categories=True,
        summary="Newznab-compatible indexer searched on demand.",
    )


def _build() -> dict[str, ProviderCapability]:
    entries: list[ProviderCapability] = [
        ProviderCapability(
            "simkl",
            "watchlist",
            "Simkl",
            _ALL_MEDIA,
            supports_watch_progress=True,
            supports_exact_air_dates=True,
            summary="Watchlist with watch progress and exact TV air dates.",
            limitations=("Exact air dates for TV only",),
        ),
        ProviderCapability(
            "anilist",
            "watchlist",
            "AniList",
            ("anime",),
            supports_watch_progress=True,
            supports_exact_air_dates=True,
            summary="Anime watchlist with watch progress and exact air dates.",
            limitations=("Exact air dates for anime only",),
        ),
        ProviderCapability(
            "mal",
            "watchlist",
            "MyAnimeList",
            ("anime",),
            supports_watch_progress=True,
            summary="Anime watchlist with watch progress.",
            limitations=("No exact air dates (uses AniList/TMDB dates)",),
        ),
        ProviderCapability(
            "plex",
            "watchlist",
            "Plex",
            ("movies", "series"),
            summary="Plex watchlist for movies and series.",
            limitations=(_NO_PROGRESS, _TMDB_DATES),
        ),
        ProviderCapability(
            "jellyseerr",
            "watchlist",
            "Jellyseerr",
            ("movies", "series"),
            summary="Request-based provider for movies and series.",
            limitations=(_NO_PROGRESS, _TMDB_DATES),
        ),
        ProviderCapability(
            "overseerr",
            "watchlist",
            "Overseerr",
            ("movies", "series"),
            summary="Request-based provider for movies and series.",
            limitations=(_NO_PROGRESS, _TMDB_DATES),
        ),
        ProviderCapability(
            "tmdb",
            "metadata",
            "TMDB",
            ("movies", "series"),
            summary="Metadata, posters and air dates for movies and series.",
            limitations=("No exact air dates for anime (AniList provides them)",),
        ),
        ProviderCapability(
            "torbox",
            "downloader",
            "TorBox",
            _ALL_MEDIA,
            downloader_kind="cloud",
            supports_unpack_verification=True,
            supports_remote_delete=True,
            summary="Cloud downloader with unpack verification.",
        ),
        ProviderCapability(
            "premiumize",
            "downloader",
            "Premiumize",
            _ALL_MEDIA,
            downloader_kind="cloud",
            supports_remote_delete=True,
            summary="Cloud downloader.",
            limitations=(
                "No unpack verification (completion is reported by Premiumize)",
            ),
        ),
        ProviderCapability(
            "sabnzbd",
            "downloader",
            "SABnzbd",
            _ALL_MEDIA,
            downloader_kind="local",
            supports_unpack_verification=True,
            supports_remote_delete=True,
            supports_category_mapping=True,
            summary="Local downloader with unpack verification and category mapping.",
        ),
        _indexer("newznab", "Newznab"),
        ProviderCapability(
            "local_directory",
            "delivery",
            "Local directory",
            _ALL_MEDIA,
            summary="Delivers finished downloads into library folders.",
        ),
    ]
    entries.extend(_indexer(p.type, p.name) for p in INDEXER_PRESETS)
    return {e.provider_type: e for e in entries}


CAPABILITIES: Mapping[str, ProviderCapability] = MappingProxyType(_build())


def get_capability(provider_type: str) -> ProviderCapability | None:
    """Return the capability entry for *provider_type* (case-insensitive) or None."""
    return CAPABILITIES.get((provider_type or "").strip().lower())


def capabilities_by_category() -> dict[str, list[ProviderCapability]]:
    """Group registry entries by category in ``CATEGORY_ORDER`` (stable order)."""
    grouped: dict[str, list[ProviderCapability]] = {c: [] for c in CATEGORY_ORDER}
    for cap in CAPABILITIES.values():
        grouped[cap.category].append(cap)
    return grouped


def sources_support_auto_advance(provider_types: Iterable[str]) -> bool:
    """True iff any known type reports watch progress; unknown types count as none."""
    for t in provider_types:
        cap = get_capability(t)
        if cap is not None and cap.supports_watch_progress:
            return True
    return False


async def can_auto_advance(session: AsyncSession, item: MediaItem) -> bool:
    """True iff a linked watchlist source or the origin provider reports watch progress."""
    stmt = (
        select(Provider.type)
        .join(
            MediaItemWatchlistSource,
            MediaItemWatchlistSource.provider_id == Provider.id,
        )
        .where(MediaItemWatchlistSource.media_item_id == item.id)
    )
    types = set((await session.execute(stmt)).scalars().all())
    if item.provider_id is not None:
        origin = await session.execute(
            select(Provider.type).where(Provider.id == item.provider_id)
        )
        origin_type = origin.scalar_one_or_none()
        if origin_type:
            types.add(origin_type)
    return sources_support_auto_advance(types)
