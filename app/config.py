"""
Smewlet Application Configuration
====================================
Loads settings from environment variables and the database.
Provides a single validated settings object to the rest of the application.

Usage::

    from app.config import settings, scoring_config

    print(settings.database_url)
    print(scoring_config["scoring"]["resolution"]["1080p"])
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from app.core.default_scoring import DEFAULT_SCORING_CONFIG
from app.version import APP_NAME, APP_VERSION

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

DATABASE_FILE: str = "smewlet.db"
DEFAULT_DATABASE_URL: str = "sqlite+aiosqlite:///./config/smewlet.db"
DEFAULT_USER_AGENT: str = f"{APP_NAME}/{APP_VERSION} (Linux; x64)"


class Settings:
    """Application settings loaded from environment variables.

    All settings have sensible defaults so the app can start without a
    fully configured .env (useful for testing).
    """

    # --- Database ---
    database_url: str = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

    # Downloader Transfer Poller & Failure Recovery
    sh_max_retries: int = int(os.getenv("SH_MAX_RETRIES", "3"))
    download_timeout_minutes: int = (
        int(os.environ["DOWNLOAD_TIMEOUT_MINUTES"])
        if "DOWNLOAD_TIMEOUT_MINUTES" in os.environ
        else int(os.environ["DOWNLOAD_TIMEOUT_HOURS"]) * 60
        if "DOWNLOAD_TIMEOUT_HOURS" in os.environ
        else 60
    )

    # Air-date auto-push & episode upgrades (m45)
    air_delay_hours: int = 2
    upgrade_window_hours: int = 48
    max_upgrade_checks: int = 3

    # History auto-clearing (m51)
    history_autoclear_pruned: bool = False
    history_retention_days: int = 30

    # Discord & Event Notification Settings (v3.0.0)
    discord_webhook_url: str | None = os.getenv("DISCORD_WEBHOOK_URL") or None
    discord_enabled: bool = os.getenv("DISCORD_ENABLED", "False").lower() == "true"
    notify_on_push_initiated: bool = (
        os.getenv("NOTIFY_ON_PUSH_INITIATED", "False").lower() == "true"
    )
    notify_on_completed: bool = (
        os.getenv("NOTIFY_ON_COMPLETED", "True").lower() == "true"
    )
    notify_on_failure: bool = os.getenv("NOTIFY_ON_FAILURE", "True").lower() == "true"
    notify_on_auto_advance: bool = (
        os.getenv("NOTIFY_ON_AUTO_ADVANCE", "True").lower() == "true"
    )
    notify_on_errors: bool = os.getenv("NOTIFY_ON_ERRORS", "True").lower() == "true"
    notify_on_dry_run: bool = os.getenv("NOTIFY_ON_DRY_RUN", "True").lower() == "true"
    notify_on_outages: bool = os.getenv("NOTIFY_ON_OUTAGES", "False").lower() == "true"
    # Seconds the "rate limit reached" alert waits for a burst of queued pushes to settle
    rate_limit_notify_delay_seconds: float = float(
        os.getenv("RATE_LIMIT_NOTIFY_DELAY_SECONDS", "3.0")
    )

    # Decoupled Debug Mode & Dry-Run Sandbox (ADR-108)
    debug_mode: bool = (
        os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
        or os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG"
    )
    dry_run: bool = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")

    # --- App ---
    app_env: str = os.getenv("APP_ENV", "development")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        self.sh_max_retries = int(os.getenv("SH_MAX_RETRIES", "3"))
        self.download_timeout_minutes = (
            int(os.environ["DOWNLOAD_TIMEOUT_MINUTES"])
            if "DOWNLOAD_TIMEOUT_MINUTES" in os.environ
            else int(os.environ["DOWNLOAD_TIMEOUT_HOURS"]) * 60
            if "DOWNLOAD_TIMEOUT_HOURS" in os.environ
            else 60
        )
        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL") or None
        self.discord_enabled = os.getenv("DISCORD_ENABLED", "False").lower() == "true"
        self.notify_on_push_initiated = (
            os.getenv("NOTIFY_ON_PUSH_INITIATED", "False").lower() == "true"
        )
        self.notify_on_completed = (
            os.getenv("NOTIFY_ON_COMPLETED", "True").lower() == "true"
        )
        self.notify_on_failure = (
            os.getenv("NOTIFY_ON_FAILURE", "True").lower() == "true"
        )
        self.notify_on_auto_advance = (
            os.getenv("NOTIFY_ON_AUTO_ADVANCE", "True").lower() == "true"
        )
        self.notify_on_errors = os.getenv("NOTIFY_ON_ERRORS", "True").lower() == "true"
        self.notify_on_dry_run = (
            os.getenv("NOTIFY_ON_DRY_RUN", "True").lower() == "true"
        )
        self.notify_on_outages = (
            os.getenv("NOTIFY_ON_OUTAGES", "False").lower() == "true"
        )
        self.rate_limit_notify_delay_seconds = float(
            os.getenv("RATE_LIMIT_NOTIFY_DELAY_SECONDS", "3.0")
        )
        self.debug_mode = (
            os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
            or os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG"
        )
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.app_env = os.getenv("APP_ENV", "development")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    def is_development(self) -> bool:
        return self.app_env.lower() == "development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance (cached after first call)."""
    return Settings()


@lru_cache(maxsize=1)
def get_scoring_config() -> dict[str, Any]:
    """Load and cache the default scoring configuration."""
    import copy

    return copy.deepcopy(DEFAULT_SCORING_CONFIG)


# Convenience module-level singletons
settings: Settings = get_settings()
scoring_config: dict[str, Any] = get_scoring_config()


async def reload_settings_from_db(session: AsyncSession) -> None:
    """Load settings from the SQLite database and update the global cache."""
    from sqlalchemy import select

    from app.db.models import SystemSettings

    stmt = select(SystemSettings).where(SystemSettings.id == 1)
    result = await session.execute(stmt)
    db_settings = result.scalars().first()

    if not db_settings:
        db_settings = SystemSettings(
            id=1,
            sh_max_retries=settings.sh_max_retries,
            download_timeout_minutes=settings.download_timeout_minutes,
            discord_webhook_url=settings.discord_webhook_url,
            discord_enabled=settings.discord_enabled,
            notify_on_push_initiated=settings.notify_on_push_initiated,
            notify_on_completed=settings.notify_on_completed,
            notify_on_failure=settings.notify_on_failure,
            notify_on_auto_advance=settings.notify_on_auto_advance,
            notify_on_errors=settings.notify_on_errors,
            notify_on_dry_run=settings.notify_on_dry_run,
            notify_on_outages=settings.notify_on_outages,
            debug_mode=settings.debug_mode,
            dry_run=settings.dry_run,
            scoring_settings=scoring_config,
        )
        session.add(db_settings)
        await session.commit()
        await session.refresh(db_settings)

    # Overwrite global settings cache
    settings.sh_max_retries = db_settings.sh_max_retries
    settings.download_timeout_minutes = getattr(
        db_settings, "download_timeout_minutes", settings.download_timeout_minutes
    )
    settings.air_delay_hours = getattr(db_settings, "air_delay_hours", 2)
    settings.upgrade_window_hours = getattr(db_settings, "upgrade_window_hours", 48)
    settings.max_upgrade_checks = getattr(db_settings, "max_upgrade_checks", 3)
    settings.history_autoclear_pruned = bool(
        getattr(db_settings, "history_autoclear_pruned", False)
    )
    settings.history_retention_days = getattr(db_settings, "history_retention_days", 30)
    settings.discord_webhook_url = getattr(db_settings, "discord_webhook_url", None)
    settings.discord_enabled = bool(getattr(db_settings, "discord_enabled", False))
    settings.notify_on_push_initiated = bool(
        getattr(db_settings, "notify_on_push_initiated", False)
    )
    settings.notify_on_completed = bool(
        getattr(db_settings, "notify_on_completed", True)
    )
    settings.notify_on_failure = bool(getattr(db_settings, "notify_on_failure", True))
    settings.notify_on_auto_advance = bool(
        getattr(db_settings, "notify_on_auto_advance", True)
    )
    settings.notify_on_errors = bool(getattr(db_settings, "notify_on_errors", True))
    settings.notify_on_dry_run = bool(getattr(db_settings, "notify_on_dry_run", True))
    settings.notify_on_outages = bool(getattr(db_settings, "notify_on_outages", False))

    env_debug = (
        os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
        or os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG"
    )
    env_dry = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")

    settings.debug_mode = bool(getattr(db_settings, "debug_mode", False)) or env_debug
    settings.dry_run = bool(getattr(db_settings, "dry_run", False)) or env_dry

    # Dynamically adjust logger levels (ADR-108)
    app_logger = logging.getLogger("app")
    app_logger.setLevel(logging.DEBUG if settings.debug_mode else logging.INFO)

    for noisy in ("httpx", "httpcore", "apscheduler", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    import copy

    authoritative_cfg = copy.deepcopy(DEFAULT_SCORING_CONFIG)
    if isinstance(db_settings.scoring_settings, dict):
        if "manual_search_defaults" in db_settings.scoring_settings:
            authoritative_cfg["manual_search_defaults"] = copy.deepcopy(
                db_settings.scoring_settings["manual_search_defaults"]
            )
    scoring_config.clear()
    scoring_config.update(authoritative_cfg)
    if db_settings.scoring_settings != authoritative_cfg:
        db_settings.scoring_settings = copy.deepcopy(authoritative_cfg)
        await session.commit()
