"""Tests for the Sanitized Database Export (m71, ADR-154, ADR-155)."""

import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from app.db.models import (
    FailureLog,
    NotificationChannel,
    SystemSettings,
)
from app.services import db_export
from app.services.db_export import sanitize_config_json, sanitized_export_filename
from tests.conftest import make_history, make_movie, make_watchlist_provider

KEY = "test-key-0123456789abcdef"
WEBHOOK = "https://discord.com/api/webhooks/000000000000000000/test-webhook-token"
BOT = "123456:TEST-BOT-TOKEN"
BEARER = "test-bearer-token-abcdef"
SECRETS = [
    KEY,
    "test-access-token",
    "test-client-id",
    "test-user",
    "test-refresh",
    WEBHOOK,
    BOT,
    "-1000000000000",
    BEARER,
]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, "{}"),
        ("", "{}"),
        ("not json", "{}"),
        ("[1,2]", "{}"),
        (
            '{"refresh_token":"fake-refresh","sync_enabled":true}',
            '{"sync_enabled":true}',
        ),
        (
            '{"auth":{"authToken":"fake-auth","keep":1},"X-Plex-Token":"fake-plex"}',
            '{"auth":{"keep":1}}',
        ),
        ('{"password":"p"}', "{}"),
        (
            '{"scan_plex_token":"test-plex-token",'
            '"scan_jellyfin_api_key":"test-jellyfin-key","scan_plex_url":"http://x"}',
            '{"scan_plex_url":"http://x"}',
        ),
        (
            '{"access_token":"fake-access","client_id":"fake-cid","sync_movies":true}',
            '{"sync_movies":true}',
        ),
    ],
)
def test_sanitize_config_json(raw, expected):
    assert sanitize_config_json(raw) == expected


def test_filename():
    name = sanitized_export_filename(datetime(2026, 10, 16, 14, 25, 30))
    assert name == "smewlet_sanitized_20261016_142530.db"


async def _seed(db_session):
    provider = await make_watchlist_provider(
        db_session,
        "simkl",
        api_key=KEY,
        access_token="test-access-token",
        client_id="test-client-id",
        username="test-user",
        api_url="http://example.com:8080",
        priority=3,
        config_json='{"refresh_token":"test-refresh","sync_enabled":true}',
    )
    db_session.add(
        NotificationChannel(
            type="telegram", name="tg", bot_token=BOT, chat_id="-1000000000000"
        )
    )
    db_session.add(SystemSettings(id=1, discord_webhook_url=WEBHOOK))
    item = await make_movie(
        db_session,
        provider_id=provider.id,
        last_error=f"GET https://x.example.com/?apikey={KEY} failed",
    )
    await make_history(
        db_session, item=item, status_detail=f"Authorization: Bearer {BEARER}"
    )
    db_session.add(
        FailureLog(
            media_item_id=item.id,
            category="search",
            message=f"GET https://indexer.example.com/api?apikey={KEY} failed: 401",
        )
    )
    await db_session.commit()
    return provider, item


async def test_export_scrubs_and_preserves(async_client, db_session, tmp_path):
    provider, item = await _seed(db_session)
    resp = await async_client.get("/system/export-sanitized-db")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/vnd.sqlite3"
    assert re.fullmatch(
        r'attachment; filename="smewlet_sanitized_\d{8}_\d{6}\.db"',
        resp.headers["content-disposition"],
    )
    assert resp.headers["cache-control"] == "no-store"
    assert resp.content.startswith(b"SQLite format 3\x00")
    for secret in SECRETS:
        assert secret.encode() not in resp.content

    out = tmp_path / "out.db"
    out.write_bytes(resp.content)
    con = sqlite3.connect(out)
    row = con.execute(
        "SELECT api_key, access_token, client_id, username, api_url, type, name,"
        " priority, is_active, config_json FROM providers"
    ).fetchone()
    assert row[:4] == (None, None, None, None)
    assert row[4:9] == ("http://example.com:8080", "simkl", "simkl", 3, 1)
    assert row[9] == '{"sync_enabled":true}'
    assert con.execute(
        "SELECT bot_token, chat_id FROM notification_channels"
    ).fetchone() == (None, None)
    assert con.execute(
        "SELECT discord_webhook_url FROM system_settings"
    ).fetchone() == (None,)
    msg = con.execute("SELECT message FROM failure_logs").fetchone()[0]
    assert KEY not in msg and "401" in msg
    assert con.execute("SELECT COUNT(*) FROM media_items").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM download_history").fetchone()[0] == 1
    con.close()

    # Live DB untouched
    await db_session.refresh(provider)
    assert provider.api_key == KEY


async def test_tempfile_removed(async_client, monkeypatch):
    seen: list[str] = []
    real = db_export.create_sanitized_snapshot

    async def spy(engine, dest):
        seen.append(dest)
        await real(engine, dest)

    monkeypatch.setattr(db_export, "create_sanitized_snapshot", spy)
    resp = await async_client.get("/system/export-sanitized-db")
    assert resp.status_code == 200
    assert seen and not Path(seen[0]).exists()


async def test_failure_returns_500_and_cleans_up(async_client, monkeypatch):
    seen: list[str] = []

    def boom(path):
        seen.append(path)
        raise RuntimeError("secret /path detail")

    monkeypatch.setattr(db_export, "scrub_sqlite_file", boom)
    resp = await async_client.get("/system/export-sanitized-db")
    assert resp.status_code == 500
    assert resp.json() == {"detail": "Sanitized export failed"}
    assert seen and not Path(seen[0]).exists()


async def test_available_on_incomplete_install(async_client):
    from app.main import app

    app.state.setup_guard_enabled = True
    resp = await async_client.get("/system/export-sanitized-db")
    assert resp.status_code == 200


async def test_templates_expose_export(async_client, db_session):
    settings = await async_client.get("/settings")
    assert 'id="export-sanitized-db-btn"' in settings.text
    assert 'href="/system/export-sanitized-db"' in settings.text
    modal = await async_client.get("/feedback/modal")
    assert "feedback-export-sanitized-db" in modal.text
    assert "export a sanitized database" in modal.text
