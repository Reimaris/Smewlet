"""
Smewlet Database Models
=======================
SQLAlchemy 2.0 ORM definitions for all database entities.

Models:
    - MediaItem:       A movie or series tracked from a watchlist provider.
    - Season:          A season belonging to a series MediaItem.
    - DownloadHistory: A record of an NZB sent to the downloader (for upgrade logic).
"""

from __future__ import annotations

import enum
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    validates,
)

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class MediaType(str, enum.Enum):
    """Distinguishes movies from series and anime."""

    MOVIE = "movie"
    SHOW = "show"
    ANIME = "anime"


class MediaStatus(str, enum.Enum):
    """Lifecycle status of a MediaItem within Smewlet."""

    FUTURE = "future"
    PENDING = "pending"
    SEARCHING = "searching"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    COMPLETED = "completed"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    CANCELED = "canceled"
    IGNORED = "ignored"


class SeasonStatus(str, enum.Enum):
    """Lifecycle status of an individual season."""

    FUTURE = "future"
    PENDING = "pending"
    SEARCHING = "searching"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    COMPLETED = "completed"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    CANCELED = "canceled"
    IGNORED = "ignored"


class EpisodeStatus(str, enum.Enum):
    """Lifecycle status of an individual episode."""

    FUTURE = "future"
    PENDING = "pending"
    SEARCHING = "searching"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    COMPLETED = "completed"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    CANCELED = "canceled"
    IGNORED = "ignored"


# ---------------------------------------------------------------------------
# Declarative Base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Shared base class for all ORM models."""


# ---------------------------------------------------------------------------
# SystemSettings Model
# ---------------------------------------------------------------------------


class SystemSettings(Base):
    """Stores the global application configuration in the database.

    This is designed as a single-row table (id=1).
    """

    __tablename__ = "system_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Downloader Transfer Poller & Failure Recovery
    sh_max_retries: Mapped[int] = mapped_column(Integer, default=3)
    download_timeout_minutes: Mapped[int] = mapped_column(
        Integer, default=60, server_default="60", nullable=False
    )

    # Anime title language (m65): romaji | english | native
    anime_title_language: Mapped[str] = mapped_column(
        String(20), default="romaji", server_default="romaji", nullable=False
    )

    # Air-date auto-push & episode upgrades (m45)
    air_delay_hours: Mapped[int] = mapped_column(
        Integer, default=2, server_default="2", nullable=False
    )
    upgrade_window_hours: Mapped[int] = mapped_column(
        Integer, default=48, server_default="48", nullable=False
    )
    max_upgrade_checks: Mapped[int] = mapped_column(
        Integer, default=3, server_default="3", nullable=False
    )

    # History auto-clearing (m51)
    history_autoclear_pruned: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", nullable=False
    )
    history_retention_days: Mapped[int] = mapped_column(
        Integer, default=30, server_default="30", nullable=False
    )

    @property
    def download_timeout_hours(self) -> int:
        """Legacy helper for code referencing timeout in hours."""
        return max(1, self.download_timeout_minutes // 60)

    @download_timeout_hours.setter
    def download_timeout_hours(self, val: int) -> None:
        """Legacy setter converting hours to minutes."""
        self.download_timeout_minutes = int(val) * 60

    # Structured Scoring Settings
    scoring_settings: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=True)

    # Discord Webhook & Per-Event Notification Triggers (v3.0.0)
    discord_webhook_url: Mapped[str | None] = mapped_column(
        String(500), nullable=True, default=None
    )
    discord_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    notify_on_push_initiated: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    notify_on_completed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_failure: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_auto_advance: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_errors: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_dry_run: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_outages: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    notify_on_delivery_failed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    notify_on_delivered: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    onboarding_completed: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", nullable=False
    )

    # Decoupled Debug Mode & Dry-Run Sandbox (ADR-108)
    debug_mode: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    dry_run: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# Provider & Notification Models
# ---------------------------------------------------------------------------


class NotificationChannel(Base):
    """Represents a notification target, e.g., a Telegram bot."""

    __tablename__ = "notification_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g., 'telegram'
    name: Mapped[str] = mapped_column(String(200), nullable=False)

    # Provider specific fields (could be JSON, but let's keep them explicit for Telegram)
    bot_token: Mapped[str] = mapped_column(String(500), nullable=True)
    chat_id: Mapped[str] = mapped_column(String(200), nullable=True)


class ProviderCategory(str, enum.Enum):
    """Broad functional category for third-party providers and integrations."""

    WATCHLIST = "watchlist"
    METADATA = "metadata"
    DOWNLOADER = "downloader"
    INDEXER = "indexer"
    DELIVERY = "delivery"


class DeliveryStatus(str, enum.Enum):
    """Per-push local directory delivery state."""

    NONE = "none"  # delivery off for this push
    PENDING = "pending"  # downloader finished, delivery not started
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    FAILED = "failed"
    PAUSED = "paused"  # INSUFFICIENT_STORAGE or PATH_UNAVAILABLE


ACTIVE_LIST_STATUSES: frozenset[str] = frozenset({"plantowatch", "watching", "on_hold"})
INACTIVE_LIST_STATUSES: frozenset[str] = frozenset({"completed", "dropped"})
# Best first: an item with several sources resolves to the highest-precedence status.
LIST_STATUS_PRECEDENCE: tuple[str, ...] = (
    "watching",
    "on_hold",
    "plantowatch",
    "completed",
    "dropped",
)


def best_list_status(statuses: Iterable[str]) -> str | None:
    """Return the highest-precedence status present (unknown values ignored)."""
    present = set(statuses)
    return next((s for s in LIST_STATUS_PRECEDENCE if s in present), None)


class Provider(Base):
    """Represents a third-party integration or service provider (Watchlist, Metadata, Downloader, Indexer)."""

    __tablename__ = "providers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="watchlist",
        server_default="watchlist",
        index=True,
    )
    type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # e.g., 'simkl', 'torbox', 'newznab', 'tmdb', 'anilist'
    name: Mapped[str] = mapped_column(String(200), nullable=False)

    # Generic API & Endpoint Credentials
    api_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    api_url: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Priority & Lifecycle State
    priority: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")

    # Watchlist/OAuth credential fields (provider-type dependent)
    username: Mapped[str | None] = mapped_column(String(200), nullable=True)
    client_id: Mapped[str | None] = mapped_column(String(500), nullable=True)
    access_token: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Provider-specific JSON configuration (e.g. watchlist sync interval, category flags, last_synced_at)
    config_json: Mapped[str] = mapped_column(
        Text, nullable=False, default="{}", server_default="{}"
    )

    @validates("config_json")
    def _validate_config_json(self, _key: str, value: Any) -> str:
        if value is None:
            return "{}"
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)

    @property
    def indexer_config(self) -> dict[str, str | None]:
        """Parsed indexer category overrides; malformed or invalid values coerce to None."""
        from app.services.indexers.catalog import (
            DEFAULT_MOVIE_CATEGORIES,
            DEFAULT_TV_CATEGORIES,
            normalize_category_csv,
        )

        result: dict[str, str | None] = {
            "custom_movie_categories": None,
            "custom_tv_categories": None,
        }
        defaults = {
            "custom_movie_categories": DEFAULT_MOVIE_CATEGORIES,
            "custom_tv_categories": DEFAULT_TV_CATEGORIES,
        }
        try:
            raw = json.loads(self.config_json or "{}")
        except Exception:
            return result
        if not isinstance(raw, dict):
            return result
        for key, default in defaults.items():
            value = raw.get(key)
            if isinstance(value, str):
                try:
                    result[key] = normalize_category_csv(value, default)
                except ValueError:
                    pass
        return result

    @property
    def sabnzbd_config(self) -> dict[str, str]:
        """Parsed SABnzbd categories with defaults; non-str values coerced to default."""
        defaults = {
            "category_movie": "movies",
            "category_series": "tv",
            "category_anime": "anime",
        }
        try:
            raw = json.loads(self.config_json or "{}")
        except Exception:
            return defaults
        if isinstance(raw, dict):
            for key in defaults:
                if isinstance(raw.get(key), str):
                    defaults[key] = raw[key]
        return defaults

    @property
    def watchlist_config(self) -> dict[str, Any]:
        """Return parsed watchlist provider configuration (any watchlist type) with defaults."""
        defaults: dict[str, Any] = {
            "sync_interval_minutes": 60,
            "sync_movies": True,
            "sync_series": True,
            "sync_anime": True,
            "last_synced_at": None,
            "refresh_token": None,
            "token_expires_at": None,
            "server_url": "",
            "api_key": "",
            "sync_admin_watchlist": False,
            "notify_available": True,
        }
        try:
            raw = json.loads(self.config_json or "{}")
            if isinstance(raw, dict):
                defaults.update(raw)
        except Exception:
            pass
        if not defaults.get("client_id") and self.client_id:
            defaults["client_id"] = self.client_id
        if not defaults.get("access_token") and self.access_token:
            defaults["access_token"] = self.access_token
        return defaults

    @property
    def is_authenticated(self) -> bool:
        """True when the watchlist provider holds valid credentials for its type."""
        ptype = (self.type or "").lower()
        cfg = self.watchlist_config
        token = str(self.access_token or cfg.get("access_token") or "").strip()
        if ptype == "simkl":
            return bool(token and str(cfg.get("refresh_token") or "").strip())
        if ptype == "anilist":
            return bool((self.username or "").strip())
        if ptype in ("plex", "mal"):
            return bool(token)
        if ptype in ("jellyseerr", "overseerr"):
            return self._seerr_configured(cfg)
        return False

    @staticmethod
    def _seerr_configured(cfg: dict[str, Any]) -> bool:
        return bool(
            str(cfg.get("server_url") or "").strip()
            and str(cfg.get("api_key") or "").strip()
        )

    @property
    def sync_eligible(self) -> bool:
        """True when the provider is active and configured well enough to attempt a sync.

        Legacy tokens without a refresh token still count as configured for the token-refresh provider type.
        """
        if not self.is_active:
            return False
        if (self.type or "").lower() == "simkl":
            cfg = self.watchlist_config
            return bool(self.client_id or cfg.get("client_id")) and bool(
                self.access_token or cfg.get("access_token") or cfg.get("refresh_token")
            )
        return self.watchlist_connection_status == "connected"

    @property
    def auth_status_label(self) -> str:
        """Return human-readable connection status label."""
        if self.is_authenticated:
            if (self.type or "").lower() == "simkl":
                return "Connected · AUTH V2 (Auto-Refresh)"
            return "Connected"
        cfg = self.watchlist_config
        token = str(self.access_token or cfg.get("access_token") or "").strip()
        if (self.type or "").lower() == "simkl" and token:
            return "Reconnect Required (Legacy V1 / Expired)"
        return "Not Connected"

    @property
    def watchlist_connection_status(self) -> str:
        """Return ``"connected"``, ``"not_connected"`` or ``"reconnect_required"``."""
        ptype = (self.type or "").lower()
        cfg = self.watchlist_config
        token = str(self.access_token or cfg.get("access_token") or "").strip()
        if ptype == "simkl":
            if self.is_authenticated:
                return "connected"
            return "reconnect_required" if token else "not_connected"
        if ptype == "anilist":
            return "connected" if (self.username or "").strip() else "not_connected"
        if ptype == "plex":
            return "connected" if token else "not_connected"
        if ptype in ("jellyseerr", "overseerr"):
            return "connected" if self._seerr_configured(cfg) else "not_connected"
        if ptype == "mal":
            if not token:
                return "not_connected"
            if str(cfg.get("refresh_token") or "").strip():
                return "connected"
            expires = cfg.get("token_expires_at")
            if not expires:
                return "connected"
            try:
                exp = datetime.fromisoformat(str(expires))
            except ValueError:
                return "reconnect_required"
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp > datetime.now(timezone.utc):
                return "connected"
            return "reconnect_required"
        return "not_connected"

    media_items: Mapped[list[MediaItem]] = relationship(
        "MediaItem", back_populates="provider", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# SearchPreset Model (v3.0.0)
# ---------------------------------------------------------------------------


class SearchPreset(Base):
    """User-defined search preset storing reusable language, video, and audio preferences."""

    __tablename__ = "search_presets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    is_default: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    primary_language: Mapped[str] = mapped_column(
        String(20), nullable=False, default="en", server_default="en"
    )
    fallback_language: Mapped[str | None] = mapped_column(
        String(20), nullable=True, default=None
    )
    video_quality_mode: Mapped[str] = mapped_column(
        String(20), nullable=False, default="best", server_default="best"
    )
    audio_quality_mode: Mapped[str] = mapped_column(
        String(20), nullable=False, default="best", server_default="best"
    )
    allow_season_packs: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    prefer_season_packs: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    custom_config_json: Mapped[str] = mapped_column(
        Text, nullable=False, default="{}", server_default="{}"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    @validates("custom_config_json")
    def _validate_custom_config_json(self, _key: str, value: Any) -> str:
        if value is None:
            return "{}"
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)

    @property
    def custom_config(self) -> dict[str, Any]:
        """Return parsed dictionary of custom quality overrides."""
        try:
            parsed = json.loads(self.custom_config_json or "{}")
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def to_dict(self) -> dict[str, Any]:
        """Serialize preset to a JSON-compatible dictionary."""
        allow_sp = bool(self.allow_season_packs)
        return {
            "id": self.id,
            "name": self.name,
            "is_default": bool(self.is_default),
            "primary_language": self.primary_language,
            "fallback_language": self.fallback_language,
            "video_quality_mode": self.video_quality_mode,
            "audio_quality_mode": self.audio_quality_mode,
            "allow_season_packs": allow_sp,
            "prefer_season_packs": bool(self.prefer_season_packs)
            if allow_sp
            else False,
            "custom_config_json": self.custom_config_json or "{}",
            "custom_config": self.custom_config,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# ---------------------------------------------------------------------------
# MediaItem Model
# ---------------------------------------------------------------------------


class MediaItem(Base):
    """Represents a movie or TV series tracked from a watchlist provider."""

    __tablename__ = "media_items"

    # --- Primary key ---
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Foreign key ---
    provider_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("providers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # --- External identifiers ---
    simkl_id: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0", index=True
    )
    associated_franchise_ids_json: Mapped[str] = mapped_column(
        Text, nullable=False, default="[]", server_default="[]"
    )
    imdb_id: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    mal_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    anilist_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    @property
    def is_watchlist_active(self) -> bool:
        """True when at least one watchlist source still lists this item as active."""
        return any(
            s.list_status in ACTIVE_LIST_STATUSES for s in self.watchlist_sources
        )

    @property
    def effective_list_status(self) -> str | None:
        """Highest-precedence list status across all watchlist sources."""
        return best_list_status(s.list_status for s in self.watchlist_sources)

    @property
    def is_list_on_hold(self) -> bool:
        """True when the item's effective list status is ``on_hold``."""
        return self.effective_list_status == "on_hold"

    @property
    def associated_franchise_ids(self) -> set[int]:
        """Return all external IDs associated with this franchise MediaItem (including merged cours/OVAs/seasons)."""
        ids: set[int] = set()
        if self.simkl_id and int(self.simkl_id) > 0:
            ids.add(int(self.simkl_id))
        if self.associated_franchise_ids_json:
            try:
                raw = json.loads(self.associated_franchise_ids_json)
                if isinstance(raw, list):
                    for x in raw:
                        if x is not None and int(x) > 0:
                            ids.add(int(x))
            except (ValueError, TypeError):
                pass
        return ids

    def add_associated_franchise_id(self, external_id: int | None) -> None:
        """Record an external ID as permanently associated with this franchise MediaItem."""
        if not external_id or int(external_id) <= 0:
            return
        current = self.associated_franchise_ids
        current.add(int(external_id))
        self.associated_franchise_ids_json = json.dumps(sorted(current))

    # --- Metadata ---
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    alt_title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title_english: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title_romaji: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title_native: Mapped[str | None] = mapped_column(String(500), nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[MediaType] = mapped_column(
        Enum(MediaType, name="media_type_enum"), nullable=False
    )
    is_anime_movie: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    status: Mapped[MediaStatus] = mapped_column(
        Enum(MediaStatus, name="media_status_enum"),
        nullable=False,
        default=MediaStatus.PENDING,
        server_default=MediaStatus.PENDING.value,
        index=True,
    )
    poster_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    overview: Mapped[str | None] = mapped_column(Text, nullable=True)
    runtime_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- Release date (movies: digital release; shows: first air date) ---
    release_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- On-Demand Search Preset & Sticky Configuration (v3.0.0) ---
    preset_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("search_presets.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
        index=True,
    )
    custom_search_config_json: Mapped[str | None] = mapped_column(
        Text, nullable=True, default=None
    )
    allow_season_packs: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    prefer_season_packs: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    auto_advance_seasons: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    last_watched_order: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    watched_completed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )

    @validates("custom_search_config_json")
    def _validate_custom_search_config_json(self, _key: str, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)

    # --- Sync & Search Timestamps ---
    watchlist_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fail_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_searched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_metadata_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_air_dates_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_episode_growth_check_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    auto_push_episodes: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )

    # --- Audit timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # --- Relationships ---
    provider: Mapped[Provider] = relationship("Provider", back_populates="media_items")
    preset: Mapped[SearchPreset | None] = relationship("SearchPreset", lazy="selectin")
    watchlist_sources: Mapped[list[MediaItemWatchlistSource]] = relationship(
        "MediaItemWatchlistSource",
        back_populates="media_item",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    seasons: Mapped[list[Season]] = relationship(
        "Season",
        back_populates="media_item",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    download_history: Mapped[list[DownloadHistory]] = relationship(
        "DownloadHistory",
        back_populates="media_item",
        cascade="save-update, merge",
        passive_deletes=True,
        lazy="selectin",
    )

    @property
    def total_seasons(self) -> int:
        """Total TV/ONA seasons excluding season 0 (specials) and franchise movies."""
        if self.media_type not in (MediaType.SHOW, MediaType.ANIME) or not self.seasons:
            return 0
        return sum(
            1
            for s in self.seasons
            if s.season_number > 0 and getattr(s, "entry_type", "season") != "movie"
        )

    @property
    def downloaded_seasons(self) -> int:
        """Total downloaded or completed TV/ONA seasons excluding season 0 and franchise movies."""
        if self.media_type not in (MediaType.SHOW, MediaType.ANIME) or not self.seasons:
            return 0
        return sum(
            1
            for s in self.seasons
            if s.season_number > 0
            and getattr(s, "entry_type", "season") != "movie"
            and s.status in [SeasonStatus.DOWNLOADED, SeasonStatus.COMPLETED]
        )

    @property
    def total_movies(self) -> int:
        """Total canonical franchise movies for an Anime item (or 1 for a standalone anime movie)."""
        if self.seasons:
            movie_entries = [
                s for s in self.seasons if getattr(s, "entry_type", "season") == "movie"
            ]
            if movie_entries:
                return len(movie_entries)
        if getattr(self, "is_anime_movie", False):
            return 1
        return 0

    @property
    def downloaded_movies(self) -> int:
        """Total downloaded or completed franchise movies for an Anime item (or standalone anime movie)."""
        if self.seasons:
            movie_entries = [
                s for s in self.seasons if getattr(s, "entry_type", "season") == "movie"
            ]
            if movie_entries:
                return sum(
                    1
                    for s in movie_entries
                    if s.status in [SeasonStatus.DOWNLOADED, SeasonStatus.COMPLETED]
                )
        if getattr(self, "is_anime_movie", False):
            return (
                1
                if self.status in [MediaStatus.DOWNLOADED, MediaStatus.COMPLETED]
                else 0
            )
        return 0

    @property
    def progress_pills(self) -> list[str]:
        """Formatted progress pills for Series and Anime (e.g. ['1/2 Seasons', '1/2 Movies', 'S02 - E 2/4'])."""
        from sqlalchemy import inspect as sa_inspect

        pills: list[str] = []
        t_seasons = self.total_seasons
        if t_seasons > 0:
            s_label = "Season" if t_seasons == 1 else "Seasons"
            pills.append(f"{self.downloaded_seasons}/{t_seasons} {s_label}")
        t_movies = self.total_movies
        if t_movies > 0:
            m_label = "Movie" if t_movies == 1 else "Movies"
            pills.append(f"{self.downloaded_movies}/{t_movies} {m_label}")

        if "seasons" not in sa_inspect(self).unloaded and self.seasons:
            non_special_seasons = [
                s
                for s in self.seasons
                if s.season_number > 0 and getattr(s, "entry_type", "season") != "movie"
            ]
            non_special_seasons.sort(
                key=lambda s: (
                    int(getattr(s, "watch_order", None) or s.season_number),
                    int(s.season_number),
                )
            )
            for s in non_special_seasons:
                if s.status in (SeasonStatus.DOWNLOADED, SeasonStatus.COMPLETED):
                    continue
                if "episodes" in sa_inspect(s).unloaded or not s.episodes:
                    continue
                released_eps = [
                    ep for ep in s.episodes if ep.status != EpisodeStatus.FUTURE
                ]
                total_eps = len(released_eps) or (s.episode_count or len(s.episodes))
                dl_eps = sum(
                    1
                    for ep in s.episodes
                    if ep.status in (EpisodeStatus.DOWNLOADED, EpisodeStatus.COMPLETED)
                )
                if 0 < dl_eps < total_eps:
                    s_num = int(getattr(s, "type_number", None) or s.season_number)
                    pills.append(f"S{s_num:02d} - E {dl_eps}/{total_eps}")

        return pills

    def display_title(self, language: str = "romaji") -> str:
        """Title in the requested anime language (falls back along the chain to ``title``)."""
        from app.core.anime_titles import resolve_display_title

        return resolve_display_title(
            language,
            english=self.title_english,
            romaji=self.title_romaji,
            native=self.title_native,
            title=self.title,
        )

    @property
    def effective_release_date(self) -> datetime | None:
        """Return release_date, or the earliest non-special Season.air_date when release_date is None."""
        if self.release_date is not None:
            return self.release_date
        from sqlalchemy import inspect as sa_inspect

        if "seasons" not in sa_inspect(self).unloaded and self.seasons:
            air_dates = [
                s.air_date
                for s in self.seasons
                if s.season_number > 0 and s.air_date is not None
            ]
            if air_dates:
                return min(
                    d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d
                    for d in air_dates
                )
        return None

    @property
    def is_tba(self) -> bool:
        """True if the item lacks an exact release date or its year is in the future."""
        now_year = datetime.now(timezone.utc).year
        if self.year is not None and self.year > now_year:
            return True
        return self.effective_release_date is None

    def __repr__(self) -> str:
        return (
            f"<MediaItem id={self.id} simkl_id={self.simkl_id} "
            f"title={self.title!r} type={self.media_type} status={self.status}>"
        )

    @property
    def is_released(self) -> bool:
        """True if the digital release date is in the past (or not set)."""
        if self.release_date is None:
            return True
        return datetime.now(tz=self.release_date.tzinfo) >= self.release_date

    @property
    def status_tier(self) -> str:
        """v3.0.0 3-tier status section ('in_progress', 'ready_to_push', or 'upcoming')."""
        if hasattr(self, "_status_tier"):
            return self._status_tier
        from app.core.automation import classify_v3_status_tier

        return classify_v3_status_tier(self)

    @status_tier.setter
    def status_tier(self, val: str) -> None:
        self._status_tier = val

    @property
    def best_score(self) -> float | None:
        """Returns the highest score from all associated download history entries."""
        if hasattr(self, "_best_score_override"):
            return self._best_score_override
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return None
        scores = [h.score for h in hist if getattr(h, "score", None) is not None]
        return max(scores) if scores else None

    @best_score.setter
    def best_score(self, value: float | None) -> None:
        self._best_score_override = value

    @property
    def is_fallback(self) -> bool:
        """Returns True if the active download (or any season/episode download) is in fallback language."""
        if hasattr(self, "_is_fallback_override"):
            return self._is_fallback_override
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return False
        if self.media_type == MediaType.MOVIE:
            latest = max(
                hist,
                key=lambda h: (
                    getattr(h, "dispatched_at", None)
                    or datetime.min.replace(tzinfo=timezone.utc),
                    getattr(h, "id", 0) or 0,
                ),
            )
            return bool(getattr(latest, "is_fallback", False))
        else:
            grouped: dict[tuple[int | None, int | None], Any] = {}
            for h in hist:
                key = (getattr(h, "season_id", None), getattr(h, "episode_id", None))
                if key not in grouped or (getattr(h, "id", 0) or 0) > (
                    getattr(grouped[key], "id", 0) or 0
                ):
                    grouped[key] = h
            return any(bool(getattr(h, "is_fallback", False)) for h in grouped.values())

    @is_fallback.setter
    def is_fallback(self, value: bool) -> None:
        self._is_fallback_override = value

    # ---------------------------------------------------------------------------
    # Season Model
    # ---------------------------------------------------------------------------
    failure_logs: Mapped[list["FailureLog"]] = relationship(
        "FailureLog",
        back_populates="media_item",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class MediaItemWatchlistSource(Base):
    """Junction linking a MediaItem to one watchlist provider (multi-provider sync)."""

    __tablename__ = "watchlist_sources"
    __table_args__ = (
        UniqueConstraint("provider_id", "remote_id", name="uq_watchlist_source_remote"),
        UniqueConstraint(
            "media_item_id", "provider_id", name="uq_watchlist_source_item_provider"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    media_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    provider_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("providers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    remote_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    list_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="plantowatch", server_default="plantowatch"
    )
    last_watched_order: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    availability_notified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    media_item: Mapped[MediaItem] = relationship(
        "MediaItem", back_populates="watchlist_sources"
    )
    provider: Mapped[Provider] = relationship("Provider")


class Season(Base):
    """Represents a single season of a TV series.

    Only seasons with ``monitored = True`` are searched by the automation
    engine. By default, Season 1 is monitored; all others are not.

    Attributes:
        id:            Auto-incremented primary key.
        media_item_id: FK → MediaItem.id.
        season_number: 1-based season index (0 = specials).
        monitored:     Whether the automation engine should search this season.
        status:        Current automation lifecycle status.
        episode_count: Total number of episodes (from TMDB), if known.
        air_date:      Season premiere date (from TMDB).
        created_at:    Row creation timestamp.
        updated_at:    Row last-modified timestamp.

        media_item:       Back-reference to the parent MediaItem.
        download_history: One-to-many to DownloadHistory rows for this season.
    """

    __tablename__ = "seasons"
    __table_args__ = (
        UniqueConstraint(
            "media_item_id", "season_number", name="uq_season_item_number"
        ),
    )

    # --- Primary key ---
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Foreign key ---
    media_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # --- Season / Chronological Franchise Entry data ---
    season_number: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_type: Mapped[str] = mapped_column(
        String(20), nullable=False, default="season", server_default="season"
    )
    watch_order: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    type_number: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    simkl_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    anilist_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Anime library folder pinned after the first delivery (first path component)
    delivery_folder: Mapped[str | None] = mapped_column(
        String(512), nullable=True, default=None
    )
    mal_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    title_english: Mapped[str | None] = mapped_column(String(512), nullable=True)
    title_romaji: Mapped[str | None] = mapped_column(String(512), nullable=True)
    title_native: Mapped[str | None] = mapped_column(String(512), nullable=True)

    def display_title(self, language: str = "romaji") -> str:
        """Title in the requested anime language; '' when nothing is set."""
        from app.core.anime_titles import resolve_display_title

        return resolve_display_title(
            language,
            english=self.title_english,
            romaji=self.title_romaji,
            native=self.title_native,
            title=self.title,
        )

    monitored: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[SeasonStatus] = mapped_column(
        Enum(SeasonStatus, name="season_status_enum"),
        nullable=False,
        default=SeasonStatus.PENDING,
        server_default=SeasonStatus.PENDING.value,
    )
    episode_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    air_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Error & Search Timestamps ---
    fail_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_searched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Audit timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # --- Relationships ---
    media_item: Mapped[MediaItem] = relationship("MediaItem", back_populates="seasons")
    episodes: Mapped[list[Episode]] = relationship(
        "Episode",
        back_populates="season",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    download_history: Mapped[list[DownloadHistory]] = relationship(
        "DownloadHistory",
        back_populates="season",
        cascade="save-update, merge",
        passive_deletes=True,
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Season id={self.id} item_id={self.media_item_id} "
            f"S{self.season_number:02d} monitored={self.monitored} status={self.status}>"
        )

    @property
    def best_score(self) -> float | None:
        """Returns the highest score from all download history entries for this season."""
        if hasattr(self, "_best_score_override"):
            return self._best_score_override
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return None
        scores = [h.score for h in hist if getattr(h, "score", None) is not None]
        return max(scores) if scores else None

    @best_score.setter
    def best_score(self, value: float | None) -> None:
        self._best_score_override = value

    @property
    def is_fallback(self) -> bool:
        """Returns True if the latest download history entry for this season is in fallback language."""
        if hasattr(self, "_is_fallback_override"):
            return self._is_fallback_override
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return False
        latest = max(
            hist,
            key=lambda h: (
                getattr(h, "dispatched_at", None)
                or datetime.min.replace(tzinfo=timezone.utc),
                getattr(h, "id", 0) or 0,
            ),
        )
        return bool(getattr(latest, "is_fallback", False))

    @is_fallback.setter
    def is_fallback(self, value: bool) -> None:
        self._is_fallback_override = value

    @property
    def is_tba(self) -> bool:
        """True if the season/entry lacks an exact air date."""
        return self.air_date is None

    @property
    def has_deletable_transfers(self) -> bool:
        """Return True when this season or any child episode has active, completed, failed, or rate-limited transfers."""
        if self.status in (
            SeasonStatus.COMPLETED,
            SeasonStatus.DOWNLOADED,
            SeasonStatus.DOWNLOADING,
            SeasonStatus.FAILED,
            SeasonStatus.RATE_LIMITED,
        ):
            return True

        from sqlalchemy import inspect as sa_inspect

        insp = sa_inspect(self)
        if "episodes" not in insp.unloaded and self.episodes:
            if any(
                ep.status
                in (
                    EpisodeStatus.COMPLETED,
                    EpisodeStatus.DOWNLOADED,
                    EpisodeStatus.DOWNLOADING,
                    EpisodeStatus.FAILED,
                    EpisodeStatus.RATE_LIMITED,
                )
                for ep in self.episodes
            ):
                return True

        if "download_history" not in insp.unloaded and self.download_history:
            for h in self.download_history:
                detail = (h.status_detail or "").strip().lower()
                if not h.is_dismissed or detail == "completed":
                    return True

        return False

    # ---------------------------------------------------------------------------
    # Episode Model
    # ---------------------------------------------------------------------------
    failure_logs: Mapped[list["FailureLog"]] = relationship(
        "FailureLog",
        back_populates="season",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class Episode(Base):
    """Represents a single episode of a season."""

    __tablename__ = "episodes"
    __table_args__ = (
        UniqueConstraint(
            "season_id", "episode_number", name="uq_episode_season_number"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("seasons.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    episode_number: Mapped[int] = mapped_column(Integer, nullable=False)
    monitored: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    status: Mapped[EpisodeStatus] = mapped_column(
        Enum(EpisodeStatus, name="episode_status_enum"),
        nullable=False,
        default=EpisodeStatus.PENDING,
        server_default=EpisodeStatus.PENDING.value,
    )
    air_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Error & Search Timestamps ---
    fail_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_searched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    first_pushed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    upgrades_applied: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    last_upgrade_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    season: Mapped[Season] = relationship("Season", back_populates="episodes")
    download_history: Mapped[list[DownloadHistory]] = relationship(
        "DownloadHistory",
        back_populates="episode",
        cascade="save-update, merge",
        passive_deletes=True,
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Episode id={self.id} season_id={self.season_id} "
            f"E{self.episode_number:02d} monitored={self.monitored} status={self.status}>"
        )

    @property
    def best_score(self) -> float | None:
        """Returns the highest score from all download history entries for this episode."""
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return None
        scores = [h.score for h in hist if getattr(h, "score", None) is not None]
        return max(scores) if scores else None

    @property
    def is_fallback(self) -> bool:
        """Returns True if the latest download history entry for this episode is in fallback language."""
        if hasattr(self, "_is_fallback_override"):
            return self._is_fallback_override
        from sqlalchemy.orm import attributes

        hist = attributes.instance_state(self).dict.get("download_history")
        if not hist:
            return False
        latest = max(
            hist,
            key=lambda h: (
                getattr(h, "dispatched_at", None)
                or datetime.min.replace(tzinfo=timezone.utc),
                getattr(h, "id", 0) or 0,
            ),
        )
        return bool(getattr(latest, "is_fallback", False))

    @is_fallback.setter
    def is_fallback(self, value: bool) -> None:
        self._is_fallback_override = value

    @property
    def is_released(self) -> bool:
        """True if the air date is in the past (or not set)."""
        if self.air_date is None:
            return True
        return datetime.now(tz=self.air_date.tzinfo) >= self.air_date

    # ---------------------------------------------------------------------------
    # DownloadHistory Model
    # ---------------------------------------------------------------------------
    failure_logs: Mapped[list["FailureLog"]] = relationship(
        "FailureLog",
        back_populates="episode",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class DownloadHistory(Base):
    """A record of an NZB dispatched to a downloader (Push History Ledger).

    Stores nullable foreign keys to MediaItem/Season/Episode and denormalized
    snapshot columns so push history survives watchlist pruning or manual
    MediaItem deletion.
    """

    __tablename__ = "download_history"

    # --- Primary key ---
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Foreign keys (nullable with SET NULL so history survives MediaItem deletion) ---
    media_item_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("media_items.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    season_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("seasons.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    episode_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("episodes.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # --- Denormalized Media Snapshot (ADR-082) ---
    media_title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    media_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type_label: Mapped[str | None] = mapped_column(String(30), nullable=True)
    target_label: Mapped[str | None] = mapped_column(String(200), nullable=True)
    poster_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    simkl_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    anilist_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # --- NZB metadata ---
    nzb_title: Mapped[str] = mapped_column(String(1000), nullable=False)
    nzb_guid: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)

    # --- Scoring at time of download ---
    score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Parsed release attributes (denormalized for quick display/comparison) ---
    size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    resolution: Mapped[str | None] = mapped_column(String(20), nullable=True)
    video_codec: Mapped[str | None] = mapped_column(String(50), nullable=True)
    audio_codec: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    release_group: Mapped[str | None] = mapped_column(String(100), nullable=True)
    bitrate_mbps: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Downloader tracking ---
    downloader_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    downloader_type: Mapped[str] = mapped_column(
        String(50), nullable=False, default="torbox", server_default="torbox"
    )
    downloader_transfer_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Language tracking ---
    is_fallback: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    is_upgrade: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    is_ad_hoc: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    replaces_history_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    grabbed_language: Mapped[str | None] = mapped_column(
        String(50), nullable=True, default=None
    )

    # --- Live Transfer & Push Mode Tracking (v3.0.0) ---
    push_mode: Mapped[str] = mapped_column(
        String(20), nullable=False, default="auto", server_default="auto"
    )
    progress_pct: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0, server_default="0.0"
    )
    download_speed_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    eta_seconds: Mapped[int | None] = mapped_column(
        Integer, nullable=True, default=None
    )
    status_detail: Mapped[str | None] = mapped_column(
        String(200), nullable=True, default=None
    )
    is_dismissed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    notification_sent: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    auto_replaced_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    outage_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    # Ad-hoc pushes parked by an outage keep their download link for the resume job.
    release_link: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    deliver_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    delivery_status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=DeliveryStatus.NONE.value,
        server_default=DeliveryStatus.NONE.value,
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    delivery_error: Mapped[str | None] = mapped_column(
        String(500), nullable=True, default=None
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    unpack_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    is_dry_run: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )

    # --- Audit timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # --- Relationships ---
    media_item: Mapped[MediaItem | None] = relationship(
        "MediaItem", back_populates="download_history"
    )
    season: Mapped[Season | None] = relationship(
        "Season", back_populates="download_history"
    )
    episode: Mapped[Episode | None] = relationship(
        "Episode", back_populates="download_history"
    )

    def __repr__(self) -> str:
        return (
            f"<DownloadHistory id={self.id} item_id={self.media_item_id} "
            f"score={self.score} title={self.nzb_title!r:.40}>"
        )


# ---------------------------------------------------------------------------
# BlacklistedRelease Model
# ---------------------------------------------------------------------------


class BlacklistedRelease(Base):
    """Tracks failed NZBs (e.g. removed by the downloader or invalid) so they are
    skipped during the next search cycle (Self-Healing).
    """

    __tablename__ = "blacklisted_releases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    media_item_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    nzb_guid: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    nzb_title: Mapped[str] = mapped_column(String(1000), nullable=False)
    reason: Mapped[str] = mapped_column(String(500), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FailureLog(Base):
    """Centralized audit trail for transient and hard failures."""

    __tablename__ = "failure_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    media_item_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    season_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("seasons.id", ondelete="CASCADE"), index=True, nullable=True
    )
    episode_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("episodes.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    category: Mapped[str] = mapped_column(String(100), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    media_item: Mapped[MediaItem | None] = relationship(
        "MediaItem", back_populates="failure_logs"
    )
    season: Mapped[Season | None] = relationship(
        "Season", back_populates="failure_logs"
    )
    episode: Mapped[Episode | None] = relationship(
        "Episode", back_populates="failure_logs"
    )


class DeliveredFile(Base):
    """One file delivered (or planned for delivery) to the local directory."""

    __tablename__ = "delivered_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    history_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("download_history.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    episode_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("episodes.id", ondelete="SET NULL"), nullable=True
    )
    season_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("seasons.id", ondelete="SET NULL"), nullable=True
    )
    source_name: Mapped[str] = mapped_column(String(1000), nullable=False)
    dest_path: Mapped[str | None] = mapped_column(
        String(2000), nullable=True, default=None, index=True
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False)  # video | subtitle
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )  # pending | delivered | failed | skipped
    error: Mapped[str | None] = mapped_column(String(500), nullable=True, default=None)
    anilist_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
