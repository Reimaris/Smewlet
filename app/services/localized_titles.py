"""TMDB translated titles, used as an extra Newznab query title (m82, ADR-178).

Releases of dubbed content carry the dub-language title, so the English title
alone finds nothing. The translation list is cached per ``(is_movie, tmdb_id)``.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from app.services import tmdb

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_translations_cache: dict[tuple[bool, int], list[dict[str, Any]]] = {}


def clear_localized_title_cache() -> None:
    _translations_cache.clear()


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


async def _load_translations(
    is_movie: bool, tmdb_id: int, session: AsyncSession | None
) -> list[dict[str, Any]] | None:
    cached = _translations_cache.get((is_movie, tmdb_id))
    if cached is not None:
        return cached
    fetch = tmdb.get_movie_details if is_movie else tmdb.get_show_details
    try:
        details = await fetch(tmdb_id, session=session)
    except Exception as exc:
        logger.debug("Localized title lookup failed for tmdb %s: %s", tmdb_id, exc)
        return None
    if not details:
        return None
    translations = (details.get("translations") or {}).get("translations") or []
    _translations_cache[(is_movie, tmdb_id)] = translations
    return translations


async def get_localized_title(
    kind: str,
    tmdb_id: int | None,
    language: str | None,
    display_title: str,
    session: AsyncSession | None = None,
) -> str | None:
    """Return the TMDB title in ``language`` or ``None`` when no extra query is useful."""
    lang = (language or "").strip().lower()
    if tmdb_id is None or not lang or lang == "en":
        return None
    is_movie = kind in ("movie", "anime-movie")
    translations = await _load_translations(is_movie, tmdb_id, session)
    if not translations:
        return None
    field = "title" if is_movie else "name"
    for tr in translations:
        if str(tr.get("iso_639_1") or "").lower() != lang:
            continue
        title = str((tr.get("data") or {}).get(field) or "").strip()
        if title and _norm(title) != _norm(display_title):
            return title
    return None
