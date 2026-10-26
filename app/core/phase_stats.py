"""In-memory count dataclasses for background-job phase summaries (m55)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IngestStats:
    ingested: int = 0
    added: int = 0


@dataclass
class AirDateRefreshStats:
    episodes_updated: int = 0
    titles_updated: int = 0
    skipped_frozen: int = 0
    skipped_paced: int = 0


@dataclass
class TbaDiscoveryStats:
    shows_evaluated: int = 0
    seasons_discovered: int = 0


@dataclass
class EpisodeGrowthStats:
    probed: int = 0
    seasons_grown: int = 0
    episodes_added: int = 0


@dataclass
class AirPushStats:
    eligible: int = 0
    dispatched: int = 0


@dataclass(frozen=True)
class WatchlistPeriodicSyncStatus:
    state: str  # "due" | "not_due" | "disabled" | "no_provider"
    interval_minutes: int
    elapsed_minutes: int | None
    remaining_minutes: int
