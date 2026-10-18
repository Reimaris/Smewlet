"""Sanitized Database Export (m71, ADR-154, ADR-155).

Creates a credential-free copy of the live SQLite database for bug reports.
"""

import asyncio
import json
import sqlite3
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.log_redaction import redact_text

SENSITIVE_CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "refresh_token",
        "access_token",
        "client_id",
        "api_key",
        "token",
        "password",
        "client_secret",
        "code_verifier",
        "code_challenge",
        "authToken",
        "X-Plex-Token",
        "scan_plex_token",
        "scan_jellyfin_api_key",
    }
)

# (table, column) pairs set to NULL on the clone.
_NULLED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("providers", "api_key"),
    ("providers", "access_token"),
    ("providers", "client_id"),
    ("providers", "username"),
    ("notification_channels", "bot_token"),
    ("notification_channels", "chat_id"),
    ("system_settings", "discord_webhook_url"),
)

# (table, column) pairs passed through redact_text() on the clone.
_REDACTED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("failure_logs", "message"),
    ("media_items", "last_error"),
    ("seasons", "last_error"),
    ("episodes", "last_error"),
    ("download_history", "status_detail"),
)


def _strip_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: _strip_sensitive(v)
            for k, v in value.items()
            if k not in SENSITIVE_CONFIG_KEYS
        }
    if isinstance(value, list):
        return [_strip_sensitive(v) for v in value]
    return value


def sanitize_config_json(raw: str | None) -> str:
    """Return ``raw`` as compact JSON without sensitive keys (``"{}"`` if unusable)."""
    if not raw:
        return "{}"
    try:
        parsed = json.loads(raw)
    except ValueError:
        return "{}"
    if not isinstance(parsed, dict):
        return "{}"
    return json.dumps(_strip_sensitive(parsed), separators=(",", ":"))


def scrub_sqlite_file(path: str) -> None:
    """Scrub credentials from the SQLite file at ``path`` and vacuum it (blocking)."""
    conn = sqlite3.connect(path)
    try:
        with conn:
            for table, column in _NULLED_COLUMNS:
                conn.execute(f"UPDATE {table} SET {column} = NULL")  # noqa: S608
            rows = conn.execute("SELECT id, config_json FROM providers").fetchall()
            for pid, raw in rows:
                conn.execute(
                    "UPDATE providers SET config_json = ? WHERE id = ?",
                    (sanitize_config_json(raw), pid),
                )
            for table, column in _REDACTED_COLUMNS:
                rows = conn.execute(
                    f"SELECT id, {column} FROM {table} WHERE {column} IS NOT NULL"  # noqa: S608
                ).fetchall()
                for rid, text in rows:
                    clean = redact_text(text)
                    if clean != text:
                        conn.execute(
                            f"UPDATE {table} SET {column} = ? WHERE id = ?",  # noqa: S608
                            (clean, rid),
                        )
            for table, column in _NULLED_COLUMNS:
                leaked = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"  # noqa: S608
                ).fetchone()[0]
                if leaked:
                    raise RuntimeError(
                        f"Scrub verification failed for {table}.{column}"
                    )
    finally:
        conn.close()
    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()


async def create_sanitized_snapshot(engine: AsyncEngine, dest_path: str) -> None:
    """Clone the live DB into ``dest_path`` via ``VACUUM INTO`` and scrub the clone."""
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.exec_driver_sql("VACUUM INTO ?", (dest_path,))
    await asyncio.to_thread(scrub_sqlite_file, dest_path)


def sanitized_export_filename(now: datetime | None = None) -> str:
    """Return ``smewlet_sanitized_<YYYYMMDD_HHMMSS>.db`` (server timezone)."""
    now = now or datetime.now()
    return f"smewlet_sanitized_{now:%Y%m%d_%H%M%S}.db"
