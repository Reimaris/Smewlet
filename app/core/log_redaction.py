"""
Log Secret Redaction (ADR-097)
==============================
A single `logging.Filter` that masks API keys and other secrets in every log
record (message, args, exception text and stack info). It is attached to every
log handler so console, file and any forwarding handler only ever see masked
values.

Two layers:
1. Regex patterns for ``apikey=...``, ``api_key=...``, ``token=...``,
   ``Authorization: ...`` and JSON-style ``"apikey": "..."`` values.
2. A literal secret set (provider keys, tokens, bot tokens, webhook URLs)
   populated from the database at startup and kept fresh via SQLAlchemy
   mapper events when those rows are inserted or updated.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from typing import Any

MIN_MASKABLE_LENGTH = 8
FULL_MASK_BELOW = 12
FULL_MASK = "***"

_SENSITIVE_KEYS = (
    r"apikey|api_key|access_token|token|password|secret|client_secret"
    r"|code_verifier|code_challenge|authToken|X-Plex-Token|refresh_token"
    r"|scan_plex_token|scan_jellyfin_api_key"
)

_QUERY_PARAM_RE = re.compile(
    rf"(?i)(?P<prefix>\b(?:{_SENSITIVE_KEYS})=)(?P<value>[^&\s\"'#]+)"
)
_JSON_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>[\"'](?:{_SENSITIVE_KEYS})[\"']\s*:\s*[\"'])(?P<value>[^\"']+)"
)
_AUTH_HEADER_RE = re.compile(
    r"(?i)(?P<prefix>authorization[\"']?\s*[:=]\s*[\"']?(?:bearer\s+|basic\s+)?)"
    r"(?P<value>[^\s\"',}]+)"
)

_PLEX_HEADER_RE = re.compile(
    r"(?i)(?P<prefix>x-plex-token[\"']?\s*:\s*[\"']?)(?P<value>[^\s\"',}]+)"
)

_PATTERNS = (_QUERY_PARAM_RE, _JSON_VALUE_RE, _AUTH_HEADER_RE, _PLEX_HEADER_RE)

_secrets: set[str] = set()
_lock = threading.Lock()


def mask_secret(value: str) -> str:
    """Mask a secret: first 4 + last 2 chars, or ``***`` for short values."""
    if len(value) < FULL_MASK_BELOW:
        return FULL_MASK
    return f"{value[:4]}…{value[-2:]}"


def register_secret(value: str | None) -> None:
    """Add a literal secret to the redaction set (ignored if empty or too short)."""
    if not value:
        return
    value = value.strip()
    if len(value) < MIN_MASKABLE_LENGTH:
        return
    with _lock:
        _secrets.add(value)


def clear_secrets() -> None:
    """Remove all literal secrets (used by tests)."""
    with _lock:
        _secrets.clear()


def redact_text(text: str) -> str:
    """Return ``text`` with all known secrets and secret-looking patterns masked."""
    if not text:
        return text
    with _lock:
        literals = sorted(_secrets, key=len, reverse=True)
    for secret in literals:
        if secret in text:
            text = text.replace(secret, mask_secret(secret))
    for pattern in _PATTERNS:
        text = pattern.sub(
            lambda m: m.group("prefix") + mask_secret(m.group("value")), text
        )
    return text


class RedactionFilter(logging.Filter):
    """Masks secrets in a record's message, exception text and stack info."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - never let logging formatting break
            message = str(record.msg)
        record.msg = redact_text(message)
        record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redact_text(record.exc_text)
        if record.stack_info:
            record.stack_info = redact_text(record.stack_info)
        return True


def _register_row_secrets(_mapper: Any, _connection: Any, target: Any) -> None:
    """Mapper-event hook: register credential fields of a saved row."""
    for attr in (
        "api_key",
        "access_token",
        "client_id",
        "bot_token",
        "discord_webhook_url",
    ):
        register_secret(getattr(target, attr, None))
    # OAuth refresh tokens live in config_json (MAL).
    try:
        cfg = json.loads(getattr(target, "config_json", None) or "{}")
    except (TypeError, ValueError):
        return
    if isinstance(cfg, dict):
        register_secret(str(cfg.get("refresh_token") or ""))
        register_secret(str(cfg.get("api_key") or ""))
        register_secret(str(cfg.get("scan_plex_token") or ""))
        register_secret(str(cfg.get("scan_jellyfin_api_key") or ""))


_listeners_installed = False


def install_model_listeners() -> None:
    """Register secrets whenever credential-bearing rows are inserted/updated."""
    global _listeners_installed
    if _listeners_installed:
        return
    from sqlalchemy import event

    from app.db.models import NotificationChannel, Provider, SystemSettings

    for model in (Provider, NotificationChannel, SystemSettings):
        event.listen(model, "after_insert", _register_row_secrets)
        event.listen(model, "after_update", _register_row_secrets)
    _listeners_installed = True


async def refresh_secrets(session: Any) -> None:
    """Load all stored credentials from the database into the secret set."""
    from sqlalchemy import select

    from app.db.models import NotificationChannel, Provider, SystemSettings

    for model in (Provider, NotificationChannel, SystemSettings):
        rows = (await session.execute(select(model))).scalars().all()
        for row in rows:
            _register_row_secrets(None, None, row)


def install_redaction(handlers: list[logging.Handler | None]) -> RedactionFilter:
    """Attach one shared RedactionFilter to each non-None handler."""
    redaction_filter = RedactionFilter()
    for handler in handlers:
        if handler is not None:
            handler.addFilter(redaction_filter)
    return redaction_filter
