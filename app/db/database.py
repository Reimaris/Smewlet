"""
Smewlet Database Engine & Session Factory
=========================================
Configures the async SQLAlchemy engine, session factory, and provides
a dependency-injection helper for FastAPI route handlers.

Usage (in route handlers)::

    from app.db.database import get_session
    from sqlalchemy.ext.asyncio import AsyncSession

    @router.get("/items")
    async def list_items(session: AsyncSession = Depends(get_session)):
        ...

Usage (in background jobs / non-FastAPI contexts)::

    from app.db.database import async_session_factory

    async with async_session_factory() as session:
        ...
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.db.models import Base

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine — created lazily; initialized on app startup.
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the application-wide async SQLAlchemy engine.

    Raises:
        RuntimeError: If the engine has not been initialized yet.
    """
    if _engine is None:
        raise RuntimeError(
            "Database engine is not initialized. "
            "Call `init_db(database_url)` during application startup."
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the application-wide async session factory."""
    if _session_factory is None:
        raise RuntimeError(
            "Session factory is not initialized. "
            "Call `init_db(database_url)` during application startup."
        )
    return _session_factory


async def _rename_or_merge_column(
    conn: AsyncConnection,
    table: str,
    old: str,
    new: str,
    *,
    sentinel_default: str | None = None,
) -> None:
    """Rename ``old`` to ``new`` in place, or merge into ``new`` and drop ``old``.

    Idempotent (m59, ADR-134): ``old`` absent is a no-op; ``new`` absent renames;
    both present copies ``old`` into ``new`` where ``new`` is NULL (or equals
    ``sentinel_default`` for NOT NULL columns), drops indexes on ``old``, then
    drops the column. Runs inside the caller's transaction.
    """
    from sqlalchemy import text as sa_text

    try:
        cols = {
            r[1] for r in (await conn.execute(sa_text(f'PRAGMA table_info("{table}")')))
        }
        if old not in cols:
            return
        if new not in cols:
            await conn.execute(
                sa_text(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}"')
            )
            return
        cond = f'"{new}" IS NULL'
        params: dict[str, str] = {}
        if sentinel_default is not None:
            cond = f'({cond} OR "{new}" = :sentinel)'
            params["sentinel"] = sentinel_default
        await conn.execute(
            sa_text(
                f'UPDATE "{table}" SET "{new}" = "{old}" '
                f'WHERE {cond} AND "{old}" IS NOT NULL'
            ),
            params,
        )
        for idx in (await conn.execute(sa_text(f'PRAGMA index_list("{table}")'))).all():
            idx_name, origin = idx[1], idx[3]
            if origin != "c":
                continue
            idx_cols = {
                r[2]
                for r in (
                    await conn.execute(sa_text(f'PRAGMA index_info("{idx_name}")'))
                )
            }
            if old in idx_cols:
                await conn.execute(sa_text(f'DROP INDEX "{idx_name}"'))
        await conn.execute(sa_text(f'ALTER TABLE "{table}" DROP COLUMN "{old}"'))
    except Exception as exc:
        raise RuntimeError(
            f"m59 migration failed for {table}.{old} -> {new}: {exc}. "
            "Restore the database from the .pre-m59.bak backup if needed."
        ) from exc


async def _migrate_downloader_columns(conn: AsyncConnection) -> None:
    """m59 (spec §2.1): legacy ``torbox_*`` tracking columns -> ``downloader_*``."""
    from sqlalchemy import text as sa_text

    for old, new in (
        ("torbox_hash", "downloader_hash"),
        ("torbox_id", "downloader_transfer_id"),
        ("torbox_sent_at", "dispatched_at"),
    ):
        await _rename_or_merge_column(conn, "download_history", old, new)
    for legacy_idx in ("torbox_hash", "torbox_id"):
        await conn.execute(
            sa_text(f"DROP INDEX IF EXISTS ix_download_history_{legacy_idx}")
        )
    await conn.execute(
        sa_text(
            "CREATE INDEX IF NOT EXISTS ix_download_history_downloader_hash "
            "ON download_history (downloader_hash)"
        )
    )


async def _migrate_indexer_type(conn: AsyncConnection) -> None:
    """m59 (spec §6): provider type ``treasure_maps`` -> ``newznab`` (idempotent)."""
    from sqlalchemy import text as sa_text

    await conn.execute(
        sa_text(
            "UPDATE providers SET type = 'newznab' "
            "WHERE type = 'treasure_maps'"  # legacy domain migration (allowlisted)
        )
    )


async def _migrate_retire_anilist_metadata(conn: AsyncConnection) -> None:
    """m67 (ADR-146): drop dummy AniList metadata provider rows (idempotent)."""
    from sqlalchemy import text as sa_text

    await conn.execute(
        sa_text(
            "DELETE FROM providers "
            "WHERE category = 'metadata' AND LOWER(type) = 'anilist'"
        )
    )


async def _migrate_watchlist_neutral_columns(conn: AsyncConnection) -> None:
    """m59 (spec §2.2): legacy ``*_simkl*`` media_items columns -> watchlist-neutral."""
    from sqlalchemy import text as sa_text

    for old, new in (
        ("last_watched_order_simkl", "last_watched_order"),
        ("simkl_watched_completed", "watched_completed"),
        ("associated_simkl_ids_json", "associated_franchise_ids_json"),
        ("simkl_synced_at", "watchlist_synced_at"),
    ):
        await _rename_or_merge_column(conn, "media_items", old, new)

    async def _cols() -> dict[str, tuple[int, str | None]]:
        rows = await conn.execute(sa_text('PRAGMA table_info("media_items")'))
        return {r[1]: (r[3], r[4]) for r in rows}

    cols = await _cols()
    for col, ddl in (
        ("last_watched_order", "INTEGER NOT NULL DEFAULT 0"),
        ("watched_completed", "BOOLEAN NOT NULL DEFAULT 0"),
        ("watchlist_synced_at", "DATETIME"),
    ):
        if col not in cols:
            await conn.execute(
                sa_text(f"ALTER TABLE media_items ADD COLUMN {col} {ddl}")
            )

    # SQLite cannot add NOT NULL to an existing column: add a NOT NULL twin,
    # copy, drop the nullable original and rename the twin into place.
    name = "associated_franchise_ids_json"
    if name not in cols:
        await conn.execute(
            sa_text(
                f"ALTER TABLE media_items ADD COLUMN {name} TEXT NOT NULL DEFAULT '[]'"
            )
        )
    elif cols[name][0] == 0:
        await conn.execute(
            sa_text(f"UPDATE media_items SET {name} = '[]' WHERE {name} IS NULL")
        )
        await conn.execute(
            sa_text(
                f"ALTER TABLE media_items ADD COLUMN {name}_tmp TEXT NOT NULL DEFAULT '[]'"
            )
        )
        await conn.execute(sa_text(f"UPDATE media_items SET {name}_tmp = {name}"))
        await conn.execute(sa_text(f"ALTER TABLE media_items DROP COLUMN {name}"))
        await conn.execute(
            sa_text(f"ALTER TABLE media_items RENAME COLUMN {name}_tmp TO {name}")
        )


async def _migrate_anime_title_columns(conn: AsyncConnection) -> None:
    """m65 (spec §2.4): per-language anime title columns + title language setting."""
    from sqlalchemy import text as sa_text

    wanted = {
        "system_settings": [
            ("anime_title_language", "VARCHAR(20) NOT NULL DEFAULT 'romaji'")
        ],
        "media_items": [
            (f"title_{lang}", "VARCHAR(500)")
            for lang in ("english", "romaji", "native")
        ],
        "seasons": [
            (f"title_{lang}", "VARCHAR(512)")
            for lang in ("english", "romaji", "native")
        ],
    }
    for table, columns in wanted.items():
        rows = await conn.execute(sa_text(f'PRAGMA table_info("{table}")'))
        existing = {r[1] for r in rows}
        if not existing:
            continue
        for col, ddl in columns:
            if col not in existing:
                await conn.execute(
                    sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
                )


async def _migrate_availability_notified(conn: AsyncConnection) -> None:
    """m68: availability-notification marker on watchlist source rows."""
    from sqlalchemy import text as sa_text

    rows = await conn.execute(sa_text('PRAGMA table_info("watchlist_sources")'))
    existing = {r[1] for r in rows}
    if existing and "availability_notified_at" not in existing:
        await conn.execute(
            sa_text(
                "ALTER TABLE watchlist_sources "
                "ADD COLUMN availability_notified_at DATETIME"
            )
        )


def _backup_db_before_m59(db_path: Path) -> None:
    """Copy the SQLite file to ``<name>.pre-m59.bak`` once (skipped if present)."""
    if str(db_path) == ":memory:" or not db_path.is_file():
        return
    backup_path = db_path.with_name(f"{db_path.name}.pre-m59.bak")
    if backup_path.exists():
        return
    shutil.copy2(db_path, backup_path)
    logger.info("Backed up database before m59 migrations to %s", backup_path)


SQLITE_BUSY_TIMEOUT_MS = 30_000
POOL_SIZE = 10
POOL_MAX_OVERFLOW = 20
# Longer than the SQLite lock wait: sessions queued behind a writer must not
# hit a pool timeout before their own lock wait ends.
POOL_TIMEOUT_SECONDS = 60


def engine_kwargs(database_url: str) -> dict[str, Any]:
    """``create_async_engine`` keyword arguments for ``database_url``."""
    from sqlalchemy.pool import StaticPool

    connect_args: dict[str, Any] = {"check_same_thread": False}
    kwargs: dict[str, Any] = {
        "echo": False,
        "future": True,
        "connect_args": connect_args,
    }
    if database_url.startswith("sqlite"):
        connect_args["timeout"] = SQLITE_BUSY_TIMEOUT_MS / 1000
    if ":memory:" in database_url or "mode=memory" in database_url:
        kwargs["poolclass"] = StaticPool
    else:
        kwargs["pool_size"] = POOL_SIZE
        kwargs["max_overflow"] = POOL_MAX_OVERFLOW
        kwargs["pool_timeout"] = POOL_TIMEOUT_SECONDS
    return kwargs


async def init_db(database_url: str) -> None:
    """Initialize the async engine, create all tables, and configure WAL mode.

    Args:
        database_url: SQLAlchemy async database URL.
                      Example: 'sqlite+aiosqlite:///./smewlet.db'
    """
    global _engine, _session_factory

    logger.info("Initializing database: %s", database_url)

    from sqlalchemy import event

    if database_url.startswith("sqlite"):
        from sqlalchemy.engine import make_url

        db_file = make_url(database_url).database
        if db_file and db_file != ":memory:" and "mode=memory" not in database_url:
            _backup_db_before_m59(Path(db_file))

    kwargs = engine_kwargs(database_url)

    _engine = create_async_engine(database_url, **kwargs)

    if database_url.startswith("sqlite"):

        @event.listens_for(_engine.sync_engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
                cursor.execute("PRAGMA foreign_keys=ON")
            finally:
                cursor.close()

    _session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,  # Prevent lazy-loading errors after commit
        autoflush=False,
        autocommit=False,
    )

    # Detect legacy v2 database before creating v3 tables (ADR-089)
    retired_v2_tables = (
        "manga_volumes",
        "manga_items",
        "magazine_issues",
        "magazine_subscriptions",
        "provider_profiles",
        # legacy schema marker (allowlisted)
        "seen_torbox_downloads",
        "daily_grab_counters",
        "book_items",
    )
    is_v2_upgrade = False
    saved_providers: list[dict[str, Any]] = []
    saved_settings: list[dict[str, Any]] = []
    saved_channels: list[dict[str, Any]] = []
    saved_presets: list[dict[str, Any]] = []

    async with _engine.begin() as conn:
        if database_url.startswith("sqlite"):
            import json

            from sqlalchemy import text as sa_text
            from sqlalchemy.engine import make_url

            existing_tables = {
                row[0]
                for row in (
                    await conn.execute(
                        sa_text("SELECT name FROM sqlite_master WHERE type='table'")
                    )
                ).fetchall()
            }
            has_legacy_settings_cols = False
            if "system_settings" in existing_tables:
                settings_cols = {
                    row[1]
                    for row in (
                        await conn.execute(
                            sa_text("PRAGMA table_info(system_settings)")
                        )
                    ).fetchall()
                }
                # legacy schema marker (allowlisted)
                if settings_cols & {
                    "treasure_maps_api_key",
                    "torbox_api_key",
                    "tmdb_api_key",
                    "automation_state",
                }:
                    has_legacy_settings_cols = True

            is_v2_upgrade = (
                bool(existing_tables & set(retired_v2_tables))
                or has_legacy_settings_cols
            )

            if is_v2_upgrade:
                logger.info(
                    "Detected legacy v2 database — executing one-time v3 soft reset "
                    "(preserving settings, providers, and notification channels)..."
                )
                # 1. Auto-backup file-based SQLite DB before modifying
                try:
                    db_file_str = make_url(database_url).database
                    if (
                        db_file_str
                        and db_file_str != ":memory:"
                        and "mode=memory" not in database_url
                    ):
                        db_path = Path(db_file_str)
                        if db_path.is_file():
                            backup_path = db_path.with_name(f"{db_path.name}.v2_backup")
                            if not backup_path.exists():
                                shutil.copy2(db_path, backup_path)
                                logger.info(
                                    "Backed up legacy v2 database to %s", backup_path
                                )
                except Exception as exc:
                    logger.warning("Could not create v2 database backup: %s", exc)

                # 2. Snapshot user configuration tables (providers, system_settings, notification_channels, search_presets)
                if "providers" in existing_tables:
                    rows = (
                        (await conn.execute(sa_text("SELECT * FROM providers")))
                        .mappings()
                        .all()
                    )
                    for r in rows:
                        p_type = str(r.get("type") or "").strip()
                        p_cat = str(r.get("category") or "watchlist").strip()
                        if (
                            p_type in ("hardcover", "openlibrary")
                            or p_cat == "print_media"
                        ):
                            continue
                        if p_type in ("anilist", "tmdb"):
                            p_cat = "metadata"
                        elif p_type == "torbox":
                            p_cat = "downloader"
                        # legacy schema marker (allowlisted)
                        elif (
                            p_type == "treasure_maps"
                        ):  # legacy schema marker (allowlisted)
                            p_cat = "indexer"
                        elif p_type == "simkl":
                            p_cat = "watchlist"

                        api_url = r.get("api_url")
                        # legacy domain migration (allowlisted)
                        if isinstance(api_url, str) and "treasuremaps.net" in api_url:
                            api_url = api_url.replace(
                                "treasuremaps.net", "treasure-maps.com"
                            )

                        raw_cfg = r.get("config_json") or "{}"
                        try:
                            cfg_dict = (
                                json.loads(raw_cfg)
                                if isinstance(raw_cfg, str)
                                else dict(raw_cfg)
                            )
                            if not isinstance(cfg_dict, dict):
                                cfg_dict = {}
                        except Exception:
                            cfg_dict = {}
                        if p_type == "simkl":
                            cfg_dict.pop("last_synced_at", None)

                        raw_prio = r.get("priority")
                        raw_active = r.get("is_active")
                        saved_providers.append(
                            {
                                "id": r.get("id"),
                                "category": p_cat,
                                "type": p_type,
                                "name": str(r.get("name") or p_type or "Provider"),
                                "api_key": r.get("api_key"),
                                "api_url": api_url,
                                "priority": int(raw_prio)
                                if raw_prio is not None
                                else 1,
                                "is_active": bool(raw_active)
                                if raw_active is not None
                                else True,
                                "username": r.get("username"),
                                "client_id": r.get("client_id"),
                                "access_token": r.get("access_token"),
                                "config_json": json.dumps(cfg_dict),
                            }
                        )

                if "system_settings" in existing_tables:
                    rows = (
                        (await conn.execute(sa_text("SELECT * FROM system_settings")))
                        .mappings()
                        .all()
                    )
                    for r in rows:
                        raw_retries = r.get("sh_max_retries")
                        raw_timeout_min = r.get("download_timeout_minutes")
                        if raw_timeout_min is None:
                            raw_hours = r.get("download_timeout_hours")
                            if raw_hours is not None:
                                h_val = int(raw_hours)
                                raw_timeout_min = 60 if h_val > 4 else h_val * 60
                            else:
                                raw_timeout_min = 60
                        raw_disc_en = r.get("discord_enabled")
                        raw_not_push = r.get("notify_on_push_initiated")
                        raw_not_comp = r.get("notify_on_completed")
                        raw_not_fail = r.get("notify_on_failure")
                        raw_not_adv = r.get("notify_on_auto_advance")
                        saved_settings.append(
                            {
                                "id": int(r.get("id") or 1),
                                "sh_max_retries": int(raw_retries)
                                if raw_retries is not None
                                else 3,
                                "download_timeout_minutes": int(raw_timeout_min),
                                "discord_webhook_url": r.get("discord_webhook_url"),
                                "discord_enabled": bool(raw_disc_en)
                                if raw_disc_en is not None
                                else False,
                                "notify_on_push_initiated": bool(raw_not_push)
                                if raw_not_push is not None
                                else False,
                                "notify_on_completed": bool(raw_not_comp)
                                if raw_not_comp is not None
                                else True,
                                "notify_on_failure": bool(raw_not_fail)
                                if raw_not_fail is not None
                                else True,
                                "notify_on_auto_advance": bool(raw_not_adv)
                                if raw_not_adv is not None
                                else True,
                            }
                        )

                if "notification_channels" in existing_tables:
                    rows = (
                        (
                            await conn.execute(
                                sa_text("SELECT * FROM notification_channels")
                            )
                        )
                        .mappings()
                        .all()
                    )
                    for r in rows:
                        saved_channels.append(
                            {
                                "id": r.get("id"),
                                "type": str(r.get("type") or "telegram"),
                                "name": str(r.get("name") or "Telegram"),
                                "bot_token": r.get("bot_token"),
                                "chat_id": r.get("chat_id"),
                            }
                        )

                if "search_presets" in existing_tables:
                    rows = (
                        (await conn.execute(sa_text("SELECT * FROM search_presets")))
                        .mappings()
                        .all()
                    )
                    for r in rows:
                        allow_packs = bool(r.get("allow_season_packs") or False)
                        prefer_packs = (
                            bool(r.get("prefer_season_packs") or False)
                            if allow_packs
                            else False
                        )
                        saved_presets.append(
                            {
                                "id": r.get("id"),
                                "name": str(r.get("name") or "Default (Best)"),
                                "is_default": bool(r.get("is_default") or False),
                                "primary_language": str(
                                    r.get("primary_language") or "en"
                                ),
                                "fallback_language": r.get("fallback_language"),
                                "video_quality_mode": str(
                                    r.get("video_quality_mode") or "best"
                                ),
                                "audio_quality_mode": str(
                                    r.get("audio_quality_mode") or "best"
                                ),
                                "allow_season_packs": allow_packs,
                                "prefer_season_packs": prefer_packs,
                                "custom_config_json": str(
                                    r.get("custom_config_json") or "{}"
                                ),
                            }
                        )

                # 3. Drop all retired v2 tables, media/history tables, and legacy-schema config tables
                await conn.execute(sa_text("PRAGMA foreign_keys=OFF;"))
                for tbl_to_drop in (
                    *retired_v2_tables,
                    "download_history",
                    "episodes",
                    "seasons",
                    "blacklisted_releases",
                    "failure_logs",
                    "media_items",
                    "providers",
                    "system_settings",
                    "notification_channels",
                    "search_presets",
                ):
                    await conn.execute(sa_text(f"DROP TABLE IF EXISTS {tbl_to_drop};"))
                await conn.execute(sa_text("PRAGMA foreign_keys=ON;"))

        # Create all tables defined in models.py (idempotent — safe to call on restart)
        await conn.run_sync(Base.metadata.create_all)

        # Restore preserved settings after v2 -> v3 soft reset
        if is_v2_upgrade and database_url.startswith("sqlite"):
            from sqlalchemy import text as sa_text

            for p_row in saved_providers:
                await conn.execute(
                    sa_text(
                        "INSERT INTO providers "
                        "(id, category, type, name, api_key, api_url, priority, is_active, "
                        "username, client_id, access_token, config_json) "
                        "VALUES (:id, :category, :type, :name, :api_key, :api_url, :priority, :is_active, "
                        ":username, :client_id, :access_token, :config_json)"
                    ),
                    p_row,
                )
            for s_row in saved_settings:
                await conn.execute(
                    sa_text(
                        "INSERT INTO system_settings "
                        "(id, sh_max_retries, download_timeout_minutes, discord_webhook_url, "
                        "discord_enabled, notify_on_push_initiated, notify_on_completed, "
                        "notify_on_failure, notify_on_auto_advance) "
                        "VALUES (:id, :sh_max_retries, :download_timeout_minutes, :discord_webhook_url, "
                        ":discord_enabled, :notify_on_push_initiated, :notify_on_completed, "
                        ":notify_on_failure, :notify_on_auto_advance)"
                    ),
                    s_row,
                )
            for c_row in saved_channels:
                await conn.execute(
                    sa_text(
                        "INSERT INTO notification_channels (id, type, name, bot_token, chat_id) "
                        "VALUES (:id, :type, :name, :bot_token, :chat_id)"
                    ),
                    c_row,
                )
            for pr_row in saved_presets:
                await conn.execute(
                    sa_text(
                        "INSERT INTO search_presets "
                        "(id, name, is_default, primary_language, fallback_language, "
                        "video_quality_mode, audio_quality_mode, allow_season_packs, "
                        "prefer_season_packs, custom_config_json) "
                        "VALUES (:id, :name, :is_default, :primary_language, :fallback_language, "
                        ":video_quality_mode, :audio_quality_mode, :allow_season_packs, "
                        ":prefer_season_packs, :custom_config_json)"
                    ),
                    pr_row,
                )
            logger.info(
                "Completed one-time v2 -> v3 database soft reset "
                "(preserved %d provider(s), %d settings row(s), %d notification channel(s)).",
                len(saved_providers),
                len(saved_settings),
                len(saved_channels),
            )

    # Enable SQLite WAL mode for better concurrent access
    if database_url.startswith("sqlite"):
        async with _session_factory() as session:
            # Perform a crude migration if the table exists but column is missing
            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE system_settings ADD COLUMN scoring_settings JSON;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN provider_id INTEGER DEFAULT 1;"
                    )
                )
                # Create a default provider if we just added the column, so foreign keys don't break
                await session.execute(
                    __import__("sqlalchemy").text(
                        # legacy schema marker (allowlisted)
                        "INSERT OR IGNORE INTO providers (id, type, name) VALUES (1, 'simkl', 'Default Simkl');"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN tvdb_id INTEGER;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN mal_id INTEGER;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN anilist_id INTEGER;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN alt_title VARCHAR(500);"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN is_anime_movie BOOLEAN NOT NULL DEFAULT 0;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE download_history ADD COLUMN episode_id INTEGER;"
                    )
                )
            except Exception:
                pass

            for tbl in ["media_items", "seasons", "episodes"]:
                try:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            f"ALTER TABLE {tbl} ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0;"
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            f"ALTER TABLE {tbl} ADD COLUMN last_error TEXT;"
                        )
                    )
                except Exception:
                    pass

            await session.execute(
                __import__("sqlalchemy").text("PRAGMA journal_mode=WAL;")
            )
            await session.execute(
                __import__("sqlalchemy").text("PRAGMA foreign_keys=ON;")
            )

            # Providers multi-category columns migration
            for col, col_type in [
                ("category", "VARCHAR(50) NOT NULL DEFAULT 'watchlist'"),
                ("api_key", "VARCHAR(500)"),
                ("api_url", "VARCHAR(500)"),
                ("priority", "INTEGER NOT NULL DEFAULT 1"),
                ("is_active", "BOOLEAN NOT NULL DEFAULT 1"),
            ]:
                try:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            f"ALTER TABLE providers ADD COLUMN {col} {col_type};"
                        )
                    )
                except Exception:
                    pass

            # Season external identifiers (simkl_id, anilist_id, mal_id)
            for col in ["simkl_id", "anilist_id", "mal_id"]:
                try:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            f"ALTER TABLE seasons ADD COLUMN {col} INTEGER;"
                        )
                    )
                except Exception:
                    pass
                try:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            f"CREATE INDEX IF NOT EXISTS ix_seasons_{col} ON seasons ({col});"
                        )
                    )
                except Exception:
                    pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE seasons ADD COLUMN title VARCHAR(512);"
                    )
                )
            except Exception:
                pass

            # Metadata TTL tracking
            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE media_items ADD COLUMN last_metadata_refreshed_at DATETIME;"
                    )
                )
            except Exception:
                pass

            # Paced air date refresh tracking (m49)
            try:
                mi_res = await session.execute(
                    __import__("sqlalchemy").text("PRAGMA table_info(media_items);")
                )
                mi_cols = {r[1] for r in mi_res.fetchall()}
                if "last_air_dates_refreshed_at" not in mi_cols:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "ALTER TABLE media_items ADD COLUMN last_air_dates_refreshed_at DATETIME;"
                        )
                    )
            except Exception:
                pass

            # Air-date auto-push & episode upgrades (m45)
            for table, column, ddl in (
                ("media_items", "auto_push_episodes", "BOOLEAN NOT NULL DEFAULT 0"),
                ("episodes", "last_upgrade_checked_at", "DATETIME"),
                ("download_history", "is_upgrade", "BOOLEAN NOT NULL DEFAULT 0"),
                ("download_history", "is_ad_hoc", "BOOLEAN NOT NULL DEFAULT 0"),
                ("system_settings", "air_delay_hours", "INTEGER NOT NULL DEFAULT 2"),
                (
                    "system_settings",
                    "upgrade_window_hours",
                    "INTEGER NOT NULL DEFAULT 48",
                ),
                ("system_settings", "max_upgrade_checks", "INTEGER NOT NULL DEFAULT 3"),
                (
                    "system_settings",
                    "history_autoclear_pruned",
                    "BOOLEAN NOT NULL DEFAULT 0",
                ),
                (
                    "system_settings",
                    "history_retention_days",
                    "INTEGER NOT NULL DEFAULT 30",
                ),
            ):
                try:
                    info = await session.execute(
                        __import__("sqlalchemy").text(f"PRAGMA table_info({table});")
                    )
                    if column not in {r[1] for r in info.fetchall()}:
                        await session.execute(
                            __import__("sqlalchemy").text(
                                f"ALTER TABLE {table} ADD COLUMN {column} {ddl};"
                            )
                        )
                        await session.commit()
                except Exception:
                    pass

            # Air-push / upgrade window rework (m50)
            sa_text = __import__("sqlalchemy").text
            try:
                ep_cols = {
                    r[1]
                    for r in (
                        await session.execute(sa_text("PRAGMA table_info(episodes);"))
                    ).fetchall()
                }
                if "upgrades_applied" not in ep_cols:
                    if "upgrade_checks" in ep_cols:
                        await session.execute(
                            sa_text(
                                "ALTER TABLE episodes RENAME COLUMN upgrade_checks TO upgrades_applied;"
                            )
                        )
                        await session.execute(
                            sa_text("UPDATE episodes SET upgrades_applied = 0;")
                        )
                    else:
                        await session.execute(
                            sa_text(
                                "ALTER TABLE episodes ADD COLUMN upgrades_applied INTEGER NOT NULL DEFAULT 0;"
                            )
                        )
                    await session.commit()
                dh_cols = {
                    r[1]
                    for r in (
                        await session.execute(
                            sa_text("PRAGMA table_info(download_history);")
                        )
                    ).fetchall()
                }
                if "replaces_history_id" not in dh_cols:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history ADD COLUMN replaces_history_id INTEGER;"
                        )
                    )
                    await session.commit()
                if "first_pushed_at" not in ep_cols:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE episodes ADD COLUMN first_pushed_at DATETIME;"
                        )
                    )
                    # legacy schema marker (allowlisted)
                    if {"episode_id", "torbox_sent_at", "is_upgrade"} <= dh_cols:
                        await session.execute(
                            sa_text(
                                "UPDATE episodes SET first_pushed_at = ("
                                "SELECT MIN(h.torbox_sent_at) FROM download_history h "
                                "WHERE h.episode_id = episodes.id AND h.is_upgrade = 0 "
                                "AND h.torbox_sent_at IS NOT NULL) "
                                "WHERE first_pushed_at IS NULL;"
                            )
                        )
                    await session.commit()
            except Exception as exc:
                logger.warning("Error migrating m50 upgrade columns: %s", exc)

            # Stalled download timeout migration (ADR-054, ADR-112)
            try:
                ti_res = await session.execute(
                    __import__("sqlalchemy").text("PRAGMA table_info(system_settings);")
                )
                cols = {r[1] for r in ti_res.fetchall()}
                if "download_timeout_minutes" not in cols:
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "ALTER TABLE system_settings ADD COLUMN download_timeout_minutes INTEGER NOT NULL DEFAULT 60;"
                        )
                    )
                    if "download_timeout_hours" in cols:
                        # Migrate legacy value: > 4 -> 60, <= 4 -> val * 60
                        await session.execute(
                            __import__("sqlalchemy").text(
                                "UPDATE system_settings SET download_timeout_minutes = "
                                "CASE WHEN download_timeout_hours > 4 THEN 60 "
                                "ELSE download_timeout_hours * 60 END "
                                "WHERE download_timeout_hours IS NOT NULL;"
                            )
                        )
                    await session.commit()
            except Exception as exc:
                logger.warning("Error migrating download_timeout_minutes: %s", exc)

            # Download history language tracking (ADR-060)
            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE download_history ADD COLUMN is_fallback BOOLEAN NOT NULL DEFAULT 0;"
                    )
                )
            except Exception:
                pass

            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "ALTER TABLE download_history ADD COLUMN grabbed_language VARCHAR(50);"
                    )
                )
            except Exception:
                pass

            # Backfill existing providers category
            try:
                await _migrate_indexer_type(await session.connection())
                await session.execute(
                    __import__("sqlalchemy").text(
                        "DELETE FROM providers WHERE type IN ('hardcover', 'openlibrary') OR category = 'print_media';"
                    )
                )
                from app.services.provider_service import provider_category_for_type

                sa_text = __import__("sqlalchemy").text
                type_rows = await session.execute(
                    sa_text("SELECT DISTINCT type FROM providers")
                )
                for (ptype,) in type_rows.fetchall():
                    try:
                        cat = provider_category_for_type(ptype)
                    except ValueError:
                        continue
                    await session.execute(
                        sa_text("UPDATE providers SET category = :c WHERE type = :t"),
                        {"c": cat, "t": ptype},
                    )
            except Exception:
                pass

            # --- v3.0.0 Schema Migrations ---
            v3_migrations = [
                # SearchPreset two-checkbox season pack columns (ADR-078)
                "ALTER TABLE search_presets ADD COLUMN allow_season_packs BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE search_presets ADD COLUMN prefer_season_packs BOOLEAN NOT NULL DEFAULT 0;",
                # MediaItem sticky & watchlist progress columns
                "ALTER TABLE media_items ADD COLUMN preset_id INTEGER REFERENCES search_presets(id) ON DELETE SET NULL;",
                "ALTER TABLE media_items ADD COLUMN custom_search_config_json TEXT;",
                "ALTER TABLE media_items ADD COLUMN allow_season_packs BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE media_items ADD COLUMN prefer_season_packs BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE media_items ADD COLUMN auto_advance_seasons BOOLEAN NOT NULL DEFAULT 0;",
                # Season chronological franchise columns
                "ALTER TABLE seasons ADD COLUMN entry_type VARCHAR(20) NOT NULL DEFAULT 'season';",
                "ALTER TABLE seasons ADD COLUMN watch_order INTEGER NOT NULL DEFAULT 1;",
                "ALTER TABLE seasons ADD COLUMN type_number INTEGER NOT NULL DEFAULT 1;",
                "ALTER TABLE seasons ADD COLUMN delivery_folder VARCHAR(512);",
                # DownloadHistory live transfer, push mode, and denormalized snapshot columns (ADR-082)
                "ALTER TABLE download_history ADD COLUMN push_mode VARCHAR(20) NOT NULL DEFAULT 'auto';",
                "ALTER TABLE download_history ADD COLUMN progress_pct FLOAT NOT NULL DEFAULT 0.0;",
                "ALTER TABLE download_history ADD COLUMN download_speed_bytes BIGINT NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN eta_seconds INTEGER;",
                "ALTER TABLE download_history ADD COLUMN status_detail VARCHAR(200);",
                "ALTER TABLE download_history ADD COLUMN is_dismissed BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN notification_sent BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN auto_replaced_count INTEGER NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN outage_attempts INTEGER NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN release_link VARCHAR(2000);",
                "ALTER TABLE download_history ADD COLUMN media_title VARCHAR(500);",
                "ALTER TABLE download_history ADD COLUMN media_year INTEGER;",
                "ALTER TABLE download_history ADD COLUMN media_type_label VARCHAR(30);",
                "ALTER TABLE download_history ADD COLUMN target_label VARCHAR(200);",
                "ALTER TABLE download_history ADD COLUMN poster_url VARCHAR(1000);",
                "ALTER TABLE download_history ADD COLUMN simkl_id INTEGER;",
                "ALTER TABLE download_history ADD COLUMN tmdb_id INTEGER;",
                "ALTER TABLE download_history ADD COLUMN imdb_id VARCHAR(20);",
                "ALTER TABLE download_history ADD COLUMN anilist_id INTEGER;",
                "ALTER TABLE download_history ADD COLUMN unpack_started_at TIMESTAMP;",
                "ALTER TABLE download_history ADD COLUMN is_dry_run BOOLEAN NOT NULL DEFAULT 0;",
                # SystemSettings Discord webhook & per-event notification columns
                "ALTER TABLE system_settings ADD COLUMN discord_webhook_url VARCHAR(500);",
                "ALTER TABLE system_settings ADD COLUMN discord_enabled BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_push_initiated BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_completed BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_failure BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_auto_advance BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_errors BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_dry_run BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_outages BOOLEAN NOT NULL DEFAULT 0;",
                # SystemSettings decoupled Debug Mode & Dry-Run Sandbox (ADR-108)
                "ALTER TABLE system_settings ADD COLUMN debug_mode BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE system_settings ADD COLUMN dry_run BOOLEAN NOT NULL DEFAULT 0;",
                # Provider watchlist sync config_json column
                "ALTER TABLE providers ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}';",
                # Migrate retired v2 MANUAL_GRAB status values to SEARCHING
                "UPDATE media_items SET status = 'SEARCHING' WHERE status IN ('MANUAL_GRAB', 'manual_grab');",
                "UPDATE seasons SET status = 'SEARCHING' WHERE status IN ('MANUAL_GRAB', 'manual_grab');",
                "UPDATE episodes SET status = 'SEARCHING' WHERE status IN ('MANUAL_GRAB', 'manual_grab');",
                # Clean up orphaned v2 print media failure logs
                "DELETE FROM failure_logs WHERE media_item_id IS NULL;",
                # Enforce two-checkbox season pack invariant (!allow_season_packs => !prefer_season_packs)
                "UPDATE search_presets SET prefer_season_packs = 0 WHERE allow_season_packs = 0 AND prefer_season_packs = 1;",
                "UPDATE media_items SET prefer_season_packs = 0 WHERE allow_season_packs = 0 AND prefer_season_packs = 1;",
                # Backfill existing terminal DownloadHistory rows to notification_sent = 1
                "UPDATE download_history SET notification_sent = 1 WHERE status_detail IN ('completed', 'failed', 'deleted', 'replaced', 'canceled') OR LOWER(COALESCE(status_detail, '')) LIKE 'failed%';",
                # Local directory delivery state and notification toggles (m78)
                "ALTER TABLE download_history ADD COLUMN deliver_enabled BOOLEAN NOT NULL DEFAULT 0;",
                "ALTER TABLE download_history ADD COLUMN delivery_status VARCHAR(20) NOT NULL DEFAULT 'none';",
                "ALTER TABLE download_history ADD COLUMN delivered_at TIMESTAMP;",
                "ALTER TABLE download_history ADD COLUMN delivery_error VARCHAR(500);",
                "ALTER TABLE system_settings ADD COLUMN notify_on_delivery_failed BOOLEAN NOT NULL DEFAULT 1;",
                "ALTER TABLE system_settings ADD COLUMN notify_on_delivered BOOLEAN NOT NULL DEFAULT 0;",
                # First-run onboarding sentinel (m54)
                "ALTER TABLE system_settings ADD COLUMN onboarding_completed BOOLEAN NOT NULL DEFAULT 0;",
            ]
            for sql_stmt in v3_migrations:
                try:
                    await session.execute(__import__("sqlalchemy").text(sql_stmt))
                except Exception:
                    pass

            # --- Migrate system_settings notify_on_dry_run if missing ---
            try:
                from sqlalchemy import text as sa_text

                ti_settings = (
                    await session.execute(sa_text("PRAGMA table_info(system_settings)"))
                ).fetchall()
                settings_cols = {r[1] for r in ti_settings}
                if "notify_on_dry_run" not in settings_cols:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE system_settings ADD COLUMN notify_on_dry_run BOOLEAN NOT NULL DEFAULT 1;"
                        )
                    )
                    await session.commit()
            except Exception as e:
                logger.warning("Error migrating system_settings columns: %s", e)

            # --- Rebuild download_history if media_item_id is still NOT NULL or ON DELETE CASCADE (ADR-082) ---
            try:
                from sqlalchemy import text as sa_text

                ti_rows = (
                    await session.execute(
                        sa_text("PRAGMA table_info(download_history)")
                    )
                ).fetchall()
                dh_col_names = {r[1] for r in ti_rows}
                if "completed_at" not in dh_col_names:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history ADD COLUMN completed_at DATETIME;"
                        )
                    )
                    await session.commit()
                    ti_rows = (
                        await session.execute(
                            sa_text("PRAGMA table_info(download_history)")
                        )
                    ).fetchall()
                    dh_col_names = {r[1] for r in ti_rows}

                if "unpack_started_at" not in dh_col_names:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history ADD COLUMN unpack_started_at TIMESTAMP;"
                        )
                    )
                    await session.commit()
                    ti_rows = (
                        await session.execute(
                            sa_text("PRAGMA table_info(download_history)")
                        )
                    ).fetchall()
                    dh_col_names = {r[1] for r in ti_rows}

                if "failed_at" not in dh_col_names:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history ADD COLUMN failed_at DATETIME;"
                        )
                    )
                    await session.commit()
                    ti_rows = (
                        await session.execute(
                            sa_text("PRAGMA table_info(download_history)")
                        )
                    ).fetchall()
                    dh_col_names = {r[1] for r in ti_rows}

                if "is_dry_run" not in dh_col_names:
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history ADD COLUMN is_dry_run BOOLEAN NOT NULL DEFAULT 0;"
                        )
                    )
                    await session.commit()
                    ti_rows = (
                        await session.execute(
                            sa_text("PRAGMA table_info(download_history)")
                        )
                    ).fetchall()
                    dh_col_names = {r[1] for r in ti_rows}

                # legacy schema marker (allowlisted)
                # m57: generalize TorBox tracking columns (ADR-131); m59 then
                # renames/merges the legacy torbox_* columns away (ADR-134).
                for col, ddl in (
                    ("downloader_hash", "VARCHAR(64)"),
                    (
                        "downloader_type",
                        "VARCHAR(50) NOT NULL DEFAULT 'torbox'",
                    ),
                    ("downloader_transfer_id", "VARCHAR(255)"),
                    ("dispatched_at", "DATETIME"),
                ):
                    if col not in dh_col_names:
                        await session.execute(
                            sa_text(
                                f"ALTER TABLE download_history ADD COLUMN {col} {ddl};"
                            )
                        )
                await _migrate_downloader_columns(await session.connection())
                await _migrate_watchlist_neutral_columns(await session.connection())
                await _migrate_indexer_type(await session.connection())
                await _migrate_retire_anilist_metadata(await session.connection())
                await _migrate_anime_title_columns(await session.connection())
                await _migrate_availability_notified(await session.connection())
                await session.execute(
                    sa_text(
                        "CREATE INDEX IF NOT EXISTS ix_download_history_downloader_transfer_id "
                        "ON download_history (downloader_transfer_id);"
                    )
                )
                await session.commit()
                ti_rows = (
                    await session.execute(
                        sa_text("PRAGMA table_info(download_history)")
                    )
                ).fetchall()
                dh_col_names = {r[1] for r in ti_rows}

                fk_rows = (
                    await session.execute(
                        sa_text("PRAGMA foreign_key_list(download_history)")
                    )
                ).fetchall()
                needs_rebuild = any(
                    r[1] == "media_item_id" and int(r[3] or 0) == 1 for r in ti_rows
                ) or any(str(r[6] or "").upper() == "CASCADE" for r in fk_rows)

                if needs_rebuild:
                    await session.commit()
                    await session.execute(sa_text("PRAGMA foreign_keys=OFF;"))
                    await session.execute(
                        sa_text("DROP TABLE IF EXISTS download_history_v3_new;")
                    )
                    await session.execute(
                        sa_text(
                            """
                            CREATE TABLE download_history_v3_new (
                                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                media_item_id INTEGER REFERENCES media_items(id) ON DELETE SET NULL,
                                season_id INTEGER REFERENCES seasons(id) ON DELETE SET NULL,
                                episode_id INTEGER REFERENCES episodes(id) ON DELETE SET NULL,
                                media_title VARCHAR(500),
                                media_year INTEGER,
                                media_type_label VARCHAR(30),
                                target_label VARCHAR(200),
                                poster_url VARCHAR(1000),
                                simkl_id INTEGER,
                                tmdb_id INTEGER,
                                imdb_id VARCHAR(20),
                                anilist_id INTEGER,
                                nzb_title VARCHAR(1000) NOT NULL,
                                nzb_guid VARCHAR(500),
                                score FLOAT,
                                size_bytes INTEGER,
                                resolution VARCHAR(20),
                                video_codec VARCHAR(50),
                                audio_codec VARCHAR(100),
                                source VARCHAR(100),
                                release_group VARCHAR(100),
                                bitrate_mbps FLOAT,
                                downloader_hash VARCHAR(64),
                                downloader_type VARCHAR(50) NOT NULL DEFAULT 'torbox',
                                downloader_transfer_id VARCHAR(255),
                                dispatched_at DATETIME,
                                is_fallback BOOLEAN NOT NULL DEFAULT 0,
                                is_upgrade BOOLEAN NOT NULL DEFAULT 0,
                                is_ad_hoc BOOLEAN NOT NULL DEFAULT 0,
                                replaces_history_id INTEGER,
                                grabbed_language VARCHAR(50),
                                push_mode VARCHAR(20) NOT NULL DEFAULT 'auto',
                                progress_pct FLOAT NOT NULL DEFAULT 0.0,
                                download_speed_bytes BIGINT NOT NULL DEFAULT 0,
                                eta_seconds INTEGER,
                                status_detail VARCHAR(200),
                                is_dismissed BOOLEAN NOT NULL DEFAULT 0,
                                notification_sent BOOLEAN NOT NULL DEFAULT 0,
                                auto_replaced_count INTEGER NOT NULL DEFAULT 0,
                                outage_attempts INTEGER NOT NULL DEFAULT 0,
                                release_link VARCHAR(2000),
                                deliver_enabled BOOLEAN NOT NULL DEFAULT 0,
                                delivery_status VARCHAR(20) NOT NULL DEFAULT 'none',
                                delivered_at TIMESTAMP,
                                delivery_error VARCHAR(500),
                                completed_at DATETIME,
                                failed_at DATETIME,
                                unpack_started_at TIMESTAMP,
                                is_dry_run BOOLEAN NOT NULL DEFAULT 0,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            );
                            """
                        )
                    )
                    target_cols = [
                        "id",
                        "media_item_id",
                        "season_id",
                        "episode_id",
                        "media_title",
                        "media_year",
                        "media_type_label",
                        "target_label",
                        "poster_url",
                        "simkl_id",
                        "tmdb_id",
                        "imdb_id",
                        "anilist_id",
                        "nzb_title",
                        "nzb_guid",
                        "score",
                        "size_bytes",
                        "resolution",
                        "video_codec",
                        "audio_codec",
                        "source",
                        "release_group",
                        "bitrate_mbps",
                        "downloader_hash",
                        "downloader_type",
                        "downloader_transfer_id",
                        "dispatched_at",
                        "is_fallback",
                        "is_upgrade",
                        "is_ad_hoc",
                        "replaces_history_id",
                        "grabbed_language",
                        "push_mode",
                        "progress_pct",
                        "download_speed_bytes",
                        "eta_seconds",
                        "status_detail",
                        "is_dismissed",
                        "notification_sent",
                        "auto_replaced_count",
                        "outage_attempts",
                        "release_link",
                        "deliver_enabled",
                        "delivery_status",
                        "delivered_at",
                        "delivery_error",
                        "completed_at",
                        "failed_at",
                        "unpack_started_at",
                        "is_dry_run",
                        "created_at",
                    ]
                    existing_col_names = {r[1] for r in ti_rows}
                    shared_cols = [c for c in target_cols if c in existing_col_names]
                    cols_csv = ", ".join(shared_cols)
                    await session.execute(
                        sa_text(
                            f"INSERT INTO download_history_v3_new ({cols_csv}) "
                            f"SELECT {cols_csv} FROM download_history;"
                        )
                    )
                    await session.execute(sa_text("DROP TABLE download_history;"))
                    await session.execute(
                        sa_text(
                            "ALTER TABLE download_history_v3_new RENAME TO download_history;"
                        )
                    )
                    for idx_col in (
                        "media_item_id",
                        "season_id",
                        "episode_id",
                        "simkl_id",
                        "tmdb_id",
                        "imdb_id",
                        "anilist_id",
                        "nzb_guid",
                        "downloader_hash",
                        "downloader_transfer_id",
                    ):
                        await session.execute(
                            sa_text(
                                f"CREATE INDEX IF NOT EXISTS ix_download_history_{idx_col} "
                                f"ON download_history ({idx_col});"
                            )
                        )
                    await session.commit()
                    await session.execute(sa_text("PRAGMA foreign_keys=ON;"))
                    logger.info(
                        "Rebuilt download_history table with nullable foreign keys (ON DELETE SET NULL)."
                    )
            except Exception as e:
                logger.warning("Error rebuilding download_history table: %s", e)

            # --- Rebuild blacklisted_releases if media_item_id still has NOT NULL (ADR-100) ---
            try:
                ti_res = await session.execute(
                    __import__("sqlalchemy").text(
                        "PRAGMA table_info(blacklisted_releases);"
                    )
                )
                ti_rows = ti_res.fetchall()
                needs_rebuild = any(
                    r[1] == "media_item_id" and int(r[3] or 0) == 1 for r in ti_rows
                )
                if needs_rebuild:
                    logger.info(
                        "Rebuilding blacklisted_releases table to allow NULL media_item_id (ADR-100)..."
                    )
                    await session.commit()
                    await session.execute(
                        __import__("sqlalchemy").text("PRAGMA foreign_keys=OFF;")
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            """
                            CREATE TABLE blacklisted_releases_new (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                media_item_id INTEGER NULL REFERENCES media_items(id) ON DELETE CASCADE,
                                nzb_guid VARCHAR(500) NULL,
                                nzb_title VARCHAR(1000) NOT NULL,
                                reason VARCHAR(500) NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            );
                            """
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            """
                            INSERT INTO blacklisted_releases_new (
                                id, media_item_id, nzb_guid, nzb_title, reason, created_at
                            )
                            SELECT
                                id, media_item_id, nzb_guid, nzb_title, reason, created_at
                            FROM blacklisted_releases;
                            """
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "DROP TABLE blacklisted_releases;"
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "ALTER TABLE blacklisted_releases_new RENAME TO blacklisted_releases;"
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "CREATE INDEX IF NOT EXISTS ix_blacklisted_releases_media_item_id ON blacklisted_releases (media_item_id);"
                        )
                    )
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "CREATE INDEX IF NOT EXISTS ix_blacklisted_releases_nzb_guid ON blacklisted_releases (nzb_guid);"
                        )
                    )
                    await session.commit()
                    await session.execute(
                        __import__("sqlalchemy").text("PRAGMA foreign_keys=ON;")
                    )
            except Exception as e:
                logger.warning("Error rebuilding blacklisted_releases table: %s", e)

            # --- Backfill denormalized snapshot fields on existing DownloadHistory rows (ADR-082) ---
            try:
                from sqlalchemy import select
                from sqlalchemy.orm import selectinload

                from app.core.push_engine import populate_history_snapshot
                from app.db.models import DownloadHistory

                unpopulated_stmt = (
                    select(DownloadHistory)
                    .where(
                        DownloadHistory.media_item_id.isnot(None),
                        DownloadHistory.media_title.is_(None),
                    )
                    .options(
                        selectinload(DownloadHistory.media_item),
                        selectinload(DownloadHistory.season),
                        selectinload(DownloadHistory.episode),
                    )
                )
                unpopulated_rows = (
                    (await session.execute(unpopulated_stmt)).scalars().all()
                )
                for dh in unpopulated_rows:
                    if dh.media_item is not None:
                        populate_history_snapshot(
                            dh,
                            dh.media_item,
                            season=dh.season,
                            episode=dh.episode,
                            only_missing=True,
                        )
                if unpopulated_rows:
                    await session.commit()
            except Exception as e:
                logger.warning("Error backfilling download_history snapshots: %s", e)

            # Backfill existing seasons watch_order and type_number from season_number
            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "UPDATE seasons SET watch_order = season_number, type_number = season_number "
                        "WHERE watch_order = 1 AND season_number > 1;"
                    )
                )
            except Exception:
                pass

            # Backfill missing parent media_items.release_date from earliest non-special season air_date
            try:
                await session.execute(
                    __import__("sqlalchemy").text(
                        "UPDATE media_items SET release_date = ("
                        "  SELECT MIN(seasons.air_date) FROM seasons "
                        "  WHERE seasons.media_item_id = media_items.id "
                        "    AND seasons.season_number > 0 "
                        "    AND seasons.air_date IS NOT NULL"
                        ") WHERE release_date IS NULL AND EXISTS ("
                        "  SELECT 1 FROM seasons "
                        "  WHERE seasons.media_item_id = media_items.id "
                        "    AND seasons.season_number > 0 "
                        "    AND seasons.air_date IS NOT NULL"
                        ");"
                    )
                )
                await session.commit()
            except Exception as e:
                logger.warning("Error backfilling media_items.release_date: %s", e)

            # Deduplicate any duplicate MediaItem rows (e.g. from concurrent syncs)
            try:
                from app.core.automation import deduplicate_media_items

                removed_dups = await deduplicate_media_items(session)
                if removed_dups > 0:
                    await session.commit()
            except Exception as e:
                logger.warning("Error deduplicating media_items during startup: %s", e)

            # Reconcile any COMPLETED/DOWNLOADED items, seasons, or episodes with zero active/completed DownloadHistory rows
            try:
                from app.core.transfer_poller import (
                    reconcile_all_items_transfer_truth,
                )

                await reconcile_all_items_transfer_truth(session)
            except Exception as e:
                logger.warning("Error reconciling transfer truth during startup: %s", e)

            # legacy domain migration (allowlisted)
            # --- Domain migration: treasuremaps.net → treasure-maps.com ---
            try:
                from sqlalchemy import func, select, update

                from app.db.models import Provider

                # legacy domain migration (allowlisted)
                migrated = await session.execute(
                    update(Provider)
                    .where(Provider.api_url.contains("treasuremaps.net"))
                    .values(
                        api_url=func.replace(
                            Provider.api_url,
                            "treasuremaps.net",
                            "treasure-maps.com",
                        )
                    )
                )
                row_count = getattr(migrated, "rowcount", 0)
                if row_count:
                    logger.info(
                        "Migrated %d provider(s) from treasuremaps.net to treasure-maps.com",
                        row_count,
                    )
            except Exception as e:
                logger.warning("Error migrating indexer domain: %s", e)

            # --- Migrate legacy cutoffs.target_score if set to 2500 ---
            try:
                from app.core.default_scoring import DEFAULT_SCORING_CONFIG
                from app.db.models import SystemSettings

                settings_res = await session.execute(
                    select(SystemSettings).where(SystemSettings.id == 1)
                )
                db_settings = settings_res.scalars().first()
                if db_settings and db_settings.scoring_settings:
                    import copy

                    sc = copy.deepcopy(db_settings.scoring_settings)
                    cutoffs = sc.get("cutoffs")
                    if (
                        isinstance(cutoffs, dict)
                        and cutoffs.get("target_score") == 2500
                    ):
                        raw_cutoffs = (
                            DEFAULT_SCORING_CONFIG.get("cutoffs")
                            if isinstance(DEFAULT_SCORING_CONFIG, dict)
                            else None
                        )
                        default_target = (
                            raw_cutoffs.get("target_score", 8000)
                            if isinstance(raw_cutoffs, dict)
                            else 8000
                        )
                        cutoffs["target_score"] = default_target
                        db_settings.scoring_settings = sc
                        logger.info(
                            "Migrated legacy target_score cutoff from 2500 to %d",
                            default_target,
                        )
            except Exception as e:
                logger.warning("Error migrating scoring cutoffs: %s", e)

            # Auto-seed default SearchPreset if no presets exist
            try:
                from sqlalchemy import select

                from app.db.models import SearchPreset

                existing_presets = (
                    (
                        await session.execute(
                            select(SearchPreset).order_by(SearchPreset.id.asc())
                        )
                    )
                    .scalars()
                    .all()
                )
                if not existing_presets:
                    default_preset = SearchPreset(
                        name="Default (Best)",
                        is_default=True,
                        primary_language="en",
                        fallback_language=None,
                        video_quality_mode="best",
                        audio_quality_mode="best",
                        allow_season_packs=False,
                        prefer_season_packs=False,
                        custom_config_json="{}",
                    )
                    session.add(default_preset)
                elif not any(p.is_default for p in existing_presets):
                    existing_presets[0].is_default = True
            except Exception as e:
                logger.warning("Error seeding default SearchPreset: %s", e)

            await session.commit()

    async with _session_factory() as session:
        await backfill_watchlist_sources(session)

    logger.info("Database initialized successfully.")


async def backfill_watchlist_sources(session: AsyncSession) -> int:
    """Insert one ``watchlist_sources`` row per MediaItem that has none (idempotent).

    Returns the number of rows inserted.
    """
    from sqlalchemy import select

    from app.db.models import MediaItem, MediaItemWatchlistSource

    items = (
        await session.execute(
            select(
                MediaItem.id,
                MediaItem.provider_id,
                MediaItem.simkl_id,
                MediaItem.last_watched_order,
            ).where(
                MediaItem.provider_id.is_not(None),
                ~select(MediaItemWatchlistSource.id)
                .where(MediaItemWatchlistSource.media_item_id == MediaItem.id)
                .exists(),
            )
        )
    ).all()
    created = 0
    for item in items:
        order = item.last_watched_order or 0
        session.add(
            MediaItemWatchlistSource(
                media_item_id=item.id,
                provider_id=item.provider_id,
                remote_id=str(item.simkl_id) if item.simkl_id else str(item.id),
                list_status="watching" if order > 0 else "plantowatch",
                last_watched_order=order,
            )
        )
        created += 1
    if created:
        await session.commit()
        logger.info("Backfilled %d watchlist source row(s).", created)
    return created


async def close_db() -> None:
    """Dispose the async engine (called on application shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Database connection closed.")


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a managed async database session.

    The session is automatically committed on success and rolled back on
    any exception. Always closed at the end of the request lifecycle.

    Yields:
        AsyncSession: An active SQLAlchemy async session.
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# Convenience alias for use in background jobs (non-FastAPI context)
async_session_factory = lambda: get_session_factory()()
