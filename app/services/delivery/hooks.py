"""Push-time delivery hooks: deliver flag default and dry-run plan preview."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DownloadHistory
from app.services.delivery.config import DeliveryConfig
from app.services.downloaders.base import DownloaderFile
from app.services.provider_service import (
    get_active_delivery_provider,
    get_delivery_provider,
)

logger = logging.getLogger(__name__)

# Dry-run has no real transfer: plan a single stand-in video named after the release.
_DRY_RUN_VIDEO_BYTES = 1024**3


# Request-scoped choice from the push UI; read when no explicit ``deliver`` is passed so
# it reaches every dispatch call site without threading a parameter through each one.
deliver_override: ContextVar[bool | None] = ContextVar("deliver_override", default=None)


def parse_deliver(value: Any) -> bool | None:
    """Parse a form/JSON ``deliver`` value; ``None`` when absent or unrecognised."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower() if value is not None else ""
    if text in ("1", "true", "on", "yes"):
        return True
    if text in ("0", "false", "off", "no"):
        return False
    return None


def deliver_from_payload(payload: Mapping[str, Any]) -> bool | None:
    return parse_deliver(payload.get("deliver"))


async def delivery_toggle_context(session: AsyncSession) -> dict[str, bool]:
    """Template context for the "Deliver to Library" checkbox (absent provider hides it)."""
    provider = await get_delivery_provider(session)
    return {
        "delivery_provider_exists": provider is not None,
        "delivery_provider_active": provider is not None and provider.is_active,
    }


async def resolve_deliver_enabled(session: AsyncSession, deliver: bool | None) -> bool:
    """Explicit ``deliver`` wins, then the request override; else deliver iff the provider is active."""
    if deliver is None:
        deliver = deliver_override.get()
    if deliver is not None:
        return deliver
    return await get_active_delivery_provider(session) is not None


async def log_dry_run_delivery_plan(
    session: AsyncSession, history: DownloadHistory
) -> None:
    """Log where a simulated push would be delivered; never writes files, never raises."""
    try:
        provider = await get_active_delivery_provider(session)
        if provider is None:
            return
        from app.services.delivery.planner import build_delivery_plan

        files = [
            DownloaderFile(
                name=f"{history.nzb_title}.mkv", size_bytes=_DRY_RUN_VIDEO_BYTES
            )
        ]
        plan = await build_delivery_plan(
            session, history, files, DeliveryConfig.from_provider(provider)
        )
        for planned in plan:
            target = (
                str(planned.dest.full)
                if planned.status == "pending"
                else f"<{planned.error}>"
            )
            logger.info("[DELIVERY] [DRY RUN] %s -> %s", planned.source.name, target)
    except Exception:
        logger.exception("[DELIVERY] dry-run plan failed for '%s'", history.nzb_title)
