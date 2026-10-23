"""Requests must stay cheap on the DB: cached setup guard, read-only dashboard."""

from __future__ import annotations

import pytest
from sqlalchemy import event

from app.core import prerequisites
from app.db.database import get_engine
from tests.conftest import make_downloader_provider


@pytest.fixture
def status_queries(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    real = prerequisites.get_setup_status

    async def counting(session):
        calls.append(1)
        return await real(session)

    monkeypatch.setattr(prerequisites, "get_setup_status", counting)
    return calls


async def test_setup_status_is_cached_between_requests(status_queries) -> None:
    first = await prerequisites.get_cached_setup_status()
    second = await prerequisites.get_cached_setup_status()
    assert first is second
    assert len(status_queries) == 1


async def test_setup_status_cache_expires(status_queries, monkeypatch) -> None:
    monkeypatch.setattr(prerequisites, "SETUP_CACHE_TTL_SECONDS", 0.0)
    await prerequisites.get_cached_setup_status()
    await prerequisites.get_cached_setup_status()
    assert len(status_queries) == 2


async def test_provider_change_invalidates_setup_cache(db_session) -> None:
    before = await prerequisites.get_cached_setup_status()
    assert "downloader" in before.missing_core
    await make_downloader_provider(db_session)
    await db_session.commit()
    after = await prerequisites.get_cached_setup_status()
    assert "downloader" not in after.missing_core


async def test_dashboard_page_load_issues_no_writes(async_client) -> None:
    statements: list[str] = []

    def record(_conn, _cur, statement, *_rest) -> None:
        if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            statements.append(statement)

    engine = get_engine().sync_engine
    event.listen(engine, "before_cursor_execute", record)
    try:
        resp = await async_client.get("/")
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert resp.status_code == 200
    assert statements == []
