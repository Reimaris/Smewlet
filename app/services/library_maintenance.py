"""Provider-neutral library maintenance pass (m61, ADR-138).

Runs franchising, TBA-season discovery, dedup and prune once per sync cycle
over the whole library, regardless of which providers were synced.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("app.core.automation")


@dataclass
class MaintenanceStats:
    sequels_consolidated: int = 0
    anime_seasons_updated: int = 0
    tba_seasons_discovered: int = 0
    duplicates_merged: int = 0
    pruned: int = 0


async def run_library_maintenance(
    session: AsyncSession, *, include_anime: bool = True
) -> MaintenanceStats:
    """Run the four maintenance phases in order and return their counts."""
    from app.core import automation as au
    from app.core.phase_stats import TbaDiscoveryStats

    stats = MaintenanceStats()

    if include_anime:
        stats.sequels_consolidated = await au.consolidate_standalone_anime_sequels(
            session
        )
        stats.anime_seasons_updated = await au.refresh_due_future_anime_seasons(session)
    logger.info(
        "[SYNC] [MAINT] [1/4] %d sequels consolidated, %d TBA seasons updated",
        stats.sequels_consolidated,
        stats.anime_seasons_updated,
    )

    tba_stats = TbaDiscoveryStats()
    await au.refresh_due_future_tv_seasons(session, stats=tba_stats)
    stats.tba_seasons_discovered = tba_stats.seasons_discovered
    logger.info(
        "[SYNC] [MAINT] [2/4] %d TBA seasons discovered", stats.tba_seasons_discovered
    )

    stats.duplicates_merged = await au.deduplicate_media_items(session)
    logger.info("[SYNC] [MAINT] [3/4] %d duplicates merged", stats.duplicates_merged)

    stats.pruned = await au.prune_inactive_media_items(session)
    logger.info("[SYNC] [MAINT] [4/4] %d inactive titles pruned", stats.pruned)

    return stats
