"""Static catalog of curated Newznab indexer presets and category helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.services.indexers import normalize_indexer_type

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.db.models import Provider

DEFAULT_MOVIE_CATEGORIES: str = "2000,2040,2045,2050,2060"
DEFAULT_TV_CATEGORIES: str = "5000,5030,5040,5045"
ANIME_CATEGORY_FALLBACK: str = "5070"

# Indexers such as TreasureMaps list dubbed releases in language-specific root
# categories (Movies/TV - DE = 2100/5100, ES = 2200/5200, FR = 2300/5300) that are
# not children of 2000/5000, so each language adds its offset to the default ids.
LANGUAGE_CATEGORY_OFFSETS: dict[str, int] = {"de": 100, "es": 200, "fr": 300}


@dataclass(frozen=True)
class IndexerPreset:
    type: str
    name: str
    api_url: str
    key_url: str
    badge: str
    color: str
    experimental: bool = True


INDEXER_PRESETS: tuple[IndexerPreset, ...] = (
    IndexerPreset(
        "treasuremaps",
        "TreasureMaps",
        "https://treasure-maps.com/api",
        "https://treasure-maps.com/profile",
        "TM",
        "blue",
        experimental=False,
    ),
    IndexerPreset(
        "drunkenslug",
        "DrunkenSlug",
        "https://api.drunkenslug.com/api",
        "https://drunkenslug.com/profile",
        "DS",
        "emerald",
    ),
    IndexerPreset(
        "nzbgeek",
        "NZBGeek",
        "https://api.nzbgeek.info/api",
        "https://nzbgeek.info/dashboard.php",
        "NG",
        "orange",
    ),
    IndexerPreset(
        "nzbplanet",
        "NzbPlanet",
        "https://api.nzbplanet.net/api",
        "https://nzbplanet.net/profile",
        "NP",
        "rose",
    ),
    IndexerPreset(
        "althub",
        "AltHUB",
        "https://api.althub.co.za/api",
        "https://althub.co.za/profile",
        "AH",
        "cyan",
    ),
    IndexerPreset(
        "ninjacentral",
        "NinjaCentral",
        "https://ninjacentral.co.za/api",
        "https://ninjacentral.co.za/profile",
        "NC",
        "violet",
    ),
    IndexerPreset(
        "tabula_rasa",
        "Tabula Rasa",
        "https://api.nzb-tabularasa.com/api",
        "https://nzb-tabularasa.com/profile",
        "TR",
        "amber",
    ),
    IndexerPreset(
        "nzbfinder",
        "NZBFinder",
        "https://nzbfinder.ws/api",
        "https://nzbfinder.ws/profile",
        "NF",
        "lime",
    ),
    IndexerPreset(
        "nzb_su",
        "NZB.su",
        "https://api.nzb.su/api",
        "https://nzb.su/profile",
        "SU",
        "teal",
    ),
    IndexerPreset(
        "dognzb",
        "DogNZB",
        "https://api.dognzb.cr/api",
        "https://dognzb.cr/profile",
        "DN",
        "fuchsia",
    ),
    IndexerPreset("newznab", "Custom Newznab", "", "", "NZB", "zinc"),
)

_PRESETS_BY_TYPE = {p.type: p for p in INDEXER_PRESETS}


def get_preset(provider_type: str) -> IndexerPreset | None:
    slug = (provider_type or "").strip().lower()
    if slug == "treasure_maps":  # legacy domain migration (allowlisted)
        slug = normalize_indexer_type(slug)
    return _PRESETS_BY_TYPE.get(slug)


def is_indexer_type(provider_type: str) -> bool:
    return get_preset(provider_type) is not None


def _canonical_csv(raw: str) -> str:
    tokens = [t.strip() for t in raw.split(",")]
    ids: set[int] = set()
    for token in tokens:
        if not (token.isascii() and token.isdigit()):
            raise ValueError(f"Invalid category id: {token!r}")
        value = int(token)
        if not 1 <= value <= 999999:
            raise ValueError(f"Category id out of range: {value}")
        ids.add(value)
    return ",".join(str(i) for i in sorted(ids))


def normalize_category_csv(raw: str | None, default: str) -> str | None:
    """Normalize a category CSV; blank or default-equal input -> None, bad tokens -> ValueError."""
    if raw is None or not raw.strip():
        return None
    normalized = _canonical_csv(raw)
    return None if normalized == _canonical_csv(default) else normalized


def _with_language_categories(defaults: str, languages: Iterable[str | None]) -> str:
    """Extend a default category CSV with the language-specific ids of ``languages``."""
    base = {int(t) for t in defaults.split(",")}
    ids = set(base)
    for lang in languages:
        offset = LANGUAGE_CATEGORY_OFFSETS.get((lang or "").strip().lower())
        if offset:
            ids |= {i + offset for i in base}
    return ",".join(str(i) for i in sorted(ids))


def resolve_categories(
    provider: Provider, kind: str, languages: Iterable[str | None] = ()
) -> str:
    """Return the Newznab `cat` CSV for a search kind: movie|series|anime|anime-movie.

    ``languages`` (Primary/Fallback) extend the built-in defaults only; categories
    the user configured explicitly are used as given.
    """
    cfg = provider.indexer_config
    movie = cfg["custom_movie_categories"]
    tv = cfg["custom_tv_categories"]
    if kind == "movie":
        return movie or _with_language_categories(DEFAULT_MOVIE_CATEGORIES, languages)
    if kind == "series":
        return tv or _with_language_categories(DEFAULT_TV_CATEGORIES, languages)
    if kind == "anime-movie":
        return movie or _with_language_categories(ANIME_CATEGORY_FALLBACK, languages)
    return tv or _with_language_categories(ANIME_CATEGORY_FALLBACK, languages)
