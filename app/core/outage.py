"""
Service Outage Registry & Backoff (ADR-159)
===========================================
Backoff schedule, pinned ``status_detail`` strings, queue-membership predicate,
outage marker encoding and the in-memory registry of active provider outages.
Cooldowns are applied through the existing per-indexer / per-downloader setters
so every existing cooldown gate engages automatically.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import ColumnElement, or_

from app.services.downloaders.base import DOWNLOADER_DISPLAY_NAMES
from app.services.downloaders.factory import downloader_classes
from app.services.indexers import newznab

logger = logging.getLogger(__name__)

MAX_OUTAGE_RETRIES: int = 5
OUTAGE_BACKOFF_SECONDS: tuple[int, ...] = (600, 1200, 1800, 1800, 1800)
QUEUED_DETAIL_PREFIX = "paused: "
RATE_LIMITED_DETAIL = "rate_limited"
_MARKER_PREFIX = "outage|"

Kind = Literal["indexer", "downloader"]


def outage_backoff_seconds(attempt: int) -> int:
    """Backoff for the given 1-based attempt, clamped to 1..MAX_OUTAGE_RETRIES."""
    idx = min(max(attempt, 1), MAX_OUTAGE_RETRIES) - 1
    return OUTAGE_BACKOFF_SECONDS[idx]


def paused_detail(kind: Kind, label: str) -> str:
    return f"{QUEUED_DETAIL_PREFIX}{kind} outage ({label})"


def terminal_detail(kind: Kind) -> str:
    return f"failed: {kind} outage ({MAX_OUTAGE_RETRIES} attempts failed)"


def is_queued_detail(detail: str | None) -> bool:
    """True for ``rate_limited`` or ``paused: ...`` details."""
    d = (detail or "").strip().lower()
    return d == RATE_LIMITED_DETAIL or d.startswith(QUEUED_DETAIL_PREFIX)


def queued_detail_clause(column: Any) -> ColumnElement[bool]:
    return or_(column == RATE_LIMITED_DETAIL, column.like(f"{QUEUED_DETAIL_PREFIX}%"))


@dataclass(frozen=True)
class OutageMarker:
    kind: Kind
    name: str
    label: str


def make_outage_marker(kind: str, name: str, label: str) -> str:
    return f"{_MARKER_PREFIX}{kind}|{name}|{label}"


def parse_outage_marker(message: str | None) -> OutageMarker | None:
    if not message or not message.startswith(_MARKER_PREFIX):
        return None
    parts = message[len(_MARKER_PREFIX) :].split("|", 2)
    if len(parts) != 3 or parts[0] not in ("indexer", "downloader"):
        return None
    kind: Kind = "indexer" if parts[0] == "indexer" else "downloader"
    return OutageMarker(kind, parts[1], parts[2])


@dataclass(frozen=True)
class ActiveOutage:
    kind: Kind
    provider_name: str
    label: str
    attempt: int
    resume_at_monotonic: float

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self.resume_at_monotonic - time.monotonic())


_active: dict[tuple[str, str], ActiveOutage] = {}


def record_outage(
    kind: Kind, provider_key: str, provider_name: str, label: str, attempt: int
) -> int:
    """Register an outage and set cooldown = max(existing, backoff). Returns seconds."""
    seconds = outage_backoff_seconds(attempt)
    if kind == "indexer":
        existing = newznab.get_indexer_cooldown_remaining(provider_key)
        seconds = max(seconds, int(existing))
        newznab.set_indexer_cooldown(provider_key, seconds)
    else:
        cls = downloader_classes()[provider_key]
        existing = cls.get_cooldown_remaining(is_manual=True)
        seconds = max(seconds, int(existing))
        cls.set_cooldown(seconds)
    _active[(kind, provider_name)] = ActiveOutage(
        kind, provider_name, label, attempt, time.monotonic() + seconds
    )
    return seconds


def record_outage_from_marker(marker: OutageMarker, attempt: int) -> int:
    """Resolve the provider key from the marker's display name and ``record_outage``."""
    if marker.kind == "indexer":
        key = newznab.indexer_key_for_name(marker.name)
    else:
        key = next(
            (
                t
                for t, display in DOWNLOADER_DISPLAY_NAMES.items()
                if display == marker.name
            ),
            marker.name,
        )
    return record_outage(marker.kind, key, marker.name, marker.label, attempt)


def active_outages() -> list[ActiveOutage]:
    """Outages whose cooldown has not yet expired."""
    return [o for o in _active.values() if o.remaining_seconds > 0]


def has_active_outage() -> bool:
    return bool(active_outages())


def clear_outages() -> None:
    _active.clear()
    _metadata_outages.clear()


METADATA_OUTAGE_TTL_SECONDS: int = 1800

# Metadata providers (e.g. AniList) are informational only: they never engage
# ``has_active_outage`` so pushes and searches keep running.
_metadata_outages: dict[str, tuple[str, float]] = {}
_alert_tasks: set[asyncio.Task[None]] = set()


def _notify_metadata_transition(title: str, body: str) -> None:
    """Fire-and-forget alert gated by ``notify_on_outages``; never raises."""

    async def _send() -> None:
        from app.db.database import async_session_factory
        from app.services.notifications import dispatch_alert_message

        try:
            async with async_session_factory() as session:
                await dispatch_alert_message(session, "notify_on_outages", title, body)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Metadata outage alert failed: %s", exc)

    try:
        task = asyncio.get_running_loop().create_task(_send())
    except RuntimeError:  # no running loop
        return
    _alert_tasks.add(task)
    task.add_done_callback(_alert_tasks.discard)


def record_metadata_outage(provider_name: str, label: str) -> None:
    """Flag a metadata provider as down until a success clears it or the TTL lapses.

    The first record after a clear sends one "down" alert; refreshes stay silent.
    """
    is_new = provider_name not in _metadata_outages
    _metadata_outages[provider_name] = (
        label,
        time.monotonic() + METADATA_OUTAGE_TTL_SECONDS,
    )
    if is_new:
        _notify_metadata_transition(
            f"🚨 {provider_name} outage",
            f"{provider_name} returned {label}. Watchlist sync and metadata "
            "lookups will fail until it recovers. Downloads are not affected.",
        )


def clear_metadata_outage(provider_name: str) -> None:
    """Clear the flag; sends one "recovered" alert if a "down" alert was sent."""
    if _metadata_outages.pop(provider_name, None) is not None:
        _notify_metadata_transition(
            f"✅ {provider_name} recovered",
            f"{provider_name} is reachable again.",
        )


def active_metadata_outages() -> list[tuple[str, str]]:
    """``(provider_name, label)`` pairs whose TTL has not yet lapsed."""
    now = time.monotonic()
    return [
        (n, label) for n, (label, until) in _metadata_outages.items() if until > now
    ]


def outage_banner_context() -> dict[str, Any] | None:
    """Banner data for the longest-remaining active outage, else None.

    Push-blocking outages take precedence; otherwise a metadata-provider outage
    is reported with ``kind == "metadata"``.
    """
    import math

    from app.core.rate_limit_queue import resume_time_label

    outages = active_outages()
    if not outages:
        metadata = active_metadata_outages()
        if not metadata:
            return None
        name, label = metadata[0]
        return {"provider_name": name, "label": label, "kind": "metadata"}
    o = max(outages, key=lambda x: x.remaining_seconds)
    return {
        "provider_name": o.provider_name,
        "label": o.label,
        "remaining_minutes": max(1, math.ceil(o.remaining_seconds / 60)),
        "kind": o.kind,
        "attempt": o.attempt,
        "resume_time": resume_time_label(),
    }
