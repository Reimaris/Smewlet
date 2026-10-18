"""TMDB episode titles for regular (non-anime) shows."""

from __future__ import annotations

import logging
import re

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ttl_cache import TTLCache
from app.services import tmdb

logger = logging.getLogger(__name__)

_PLACEHOLDER = re.compile(r"^Episode \d+$")
_season_titles: TTLCache[dict[int, str]] = TTLCache(600)


async def tmdb_episode_title(
    session: AsyncSession, tmdb_id: int, season_number: int, episode_number: int
) -> str | None:
    """Episode name from TMDB; ``None`` when missing, a placeholder, or on failure."""
    key = f"{tmdb_id}:{season_number}"
    titles = _season_titles.get(key)
    if titles is None:
        try:
            details = await tmdb.get_season_details(
                tmdb_id, season_number, session=session
            )
        except Exception:
            logger.warning(
                "[DELIVERY] TMDB episode title lookup failed for %s S%s",
                tmdb_id,
                season_number,
            )
            return None
        if not details:
            return None
        titles = {}
        for ep in details.get("episodes") or []:
            number, name = ep.get("episode_number"), (ep.get("name") or "").strip()
            if isinstance(number, int) and name:
                titles[number] = name
        _season_titles.set(key, titles)
    title = titles.get(episode_number)
    if title is None or _PLACEHOLDER.match(title):
        return None
    return title
