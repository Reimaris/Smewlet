"""Typed configuration for the local directory delivery provider."""

from __future__ import annotations

import json
import string
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from app.db.models import Provider

ALLOWED_PLACEHOLDERS: frozenset[str] = frozenset(
    {
        "title",
        "year",
        "entry_title",
        "season",
        "episode",
        "episode_title",
        "resolution",
        "source",
        "ext",
    }
)

_TEMPLATE_FIELDS = (
    "movie_template",
    "show_template",
    "anime_template",
    "anime_movie_template",
)


_OPTIONAL_STR_FIELDS = (
    "scan_plex_url",
    "scan_plex_token",
    "scan_jellyfin_url",
    "scan_jellyfin_api_key",
)
_URL_FIELDS = ("scan_plex_url", "scan_jellyfin_url")


@dataclass(frozen=True)
class DeliveryConfig:
    """Parsed ``config_json`` of the ``local_directory`` delivery provider."""

    movies_path: str = ""
    shows_path: str = ""
    anime_path: str = ""
    anime_movies_path: str = ""
    movie_template: str = (
        "{title} ({year})/{title} ({year}) [{resolution} {source}].{ext}"
    )
    show_template: str = (
        "{title} ({year})/Season {season:02d}/"
        "{title} - S{season:02d}E{episode:02d} - {episode_title}.{ext}"
    )
    anime_template: str = (
        "{entry_title} ({year})/Season 01/"
        "{entry_title} - S01E{episode:02d} - {episode_title}.{ext}"
    )
    anime_movie_template: str = (
        "{entry_title} ({year})/{entry_title} ({year}) [{resolution} {source}].{ext}"
    )
    min_free_buffer_bytes: int = 5368709120
    delete_cloud_after_delivery: bool = False
    delivery_max_parallel: int = 1
    sab_path_map_remote: str = ""
    sab_path_map_local: str = ""
    scan_plex_url: str | None = None
    scan_plex_token: str | None = None
    scan_jellyfin_url: str | None = None
    scan_jellyfin_api_key: str | None = None
    scan_path_map_remote: str = ""
    scan_path_map_local: str = ""

    @property
    def plex_configured(self) -> bool:
        """True when both the Plex URL and token are set."""
        return bool(self.scan_plex_url and self.scan_plex_token)

    @property
    def jellyfin_configured(self) -> bool:
        """True when both the Jellyfin URL and API key are set."""
        return bool(self.scan_jellyfin_url and self.scan_jellyfin_api_key)

    @classmethod
    def from_provider(cls, provider: Provider) -> DeliveryConfig:
        """Build a config from a provider; missing or wrong-typed values use defaults."""
        try:
            raw: Any = json.loads(provider.config_json or "{}")
        except (TypeError, ValueError):
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        defaults = cls()
        values: dict[str, Any] = {}
        for f in fields(cls):
            default = getattr(defaults, f.name)
            value = raw.get(f.name, default)
            if f.name in _OPTIONAL_STR_FIELDS:
                values[f.name] = _clean_optional_str(value, f.name in _URL_FIELDS)
                continue
            # bool is an int subclass: only accept it for bool fields
            if type(value) is not type(default):
                value = default
            values[f.name] = value
        if values["min_free_buffer_bytes"] < 0:
            values["min_free_buffer_bytes"] = defaults.min_free_buffer_bytes
        values["delivery_max_parallel"] = 1  # pinned in m78
        return cls(**values)


def _clean_optional_str(value: Any, is_url: bool) -> str | None:
    """Keep a non-empty stripped string (URLs without trailing ``/``), else ``None``."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if is_url:
        value = value.rstrip("/")
    return value or None


def validate_scan_settings(cfg: DeliveryConfig) -> str:
    """Return an error message for invalid media-server scan settings, or ``""``."""
    for name in _URL_FIELDS:
        url = getattr(cfg, name)
        if url is None:
            continue
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return f"{name} must be an http(s) URL."
    remote, local = cfg.scan_path_map_remote, cfg.scan_path_map_local
    if (remote or local) and not (remote.startswith("/") and local.startswith("/")):
        return "Scan path map needs two absolute paths (or leave both empty)."
    return ""


def validate_templates(cfg: DeliveryConfig) -> list[str]:
    """Return the sorted unknown placeholder names used across all templates."""
    unknown: set[str] = set()
    for name in _TEMPLATE_FIELDS:
        try:
            parsed = list(string.Formatter().parse(getattr(cfg, name)))
        except ValueError:
            continue
        for _, field, _, _ in parsed:
            if field is not None and field not in ALLOWED_PLACEHOLDERS:
                unknown.add(field)
    return sorted(unknown)
