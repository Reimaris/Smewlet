"""Typed configuration for the local directory delivery provider."""

from __future__ import annotations

import json
import string
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any

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
            # bool is an int subclass: only accept it for bool fields
            if type(value) is not type(default):
                value = default
            values[f.name] = value
        if values["min_free_buffer_bytes"] < 0:
            values["min_free_buffer_bytes"] = defaults.min_free_buffer_bytes
        values["delivery_max_parallel"] = 1  # pinned in m78
        return cls(**values)


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
