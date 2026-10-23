"""Connection-pool exhaustion: poller never queues on its lock, pool is explicit,
delivery survives transient DB errors."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app.core import transfer_poller as tp
from app.db import database
from app.services.delivery import worker
from tests.conftest import FakeDownloaderClient, make_delivery_provider, make_history


async def test_tick_is_skipped_without_db_access_while_another_tick_runs() -> None:
    with patch.object(tp, "async_session_factory") as factory:
        async with tp._poller_lock:
            result = await tp.run_transfer_poller_tick()
    assert result["polled"] is False
    factory.assert_not_called()


async def test_active_pushes_endpoint_skips_fetch_while_poller_busy(
    async_client,
) -> None:
    from app.services.downloaders.factory import downloader_class

    fetch = AsyncMock(return_value=[])
    with (
        patch.object(tp, "count_downloading_entities", AsyncMock(return_value=2)),
        patch.object(downloader_class("torbox"), "fetch_raw_transfers", fetch),
    ):
        async with tp._poller_lock:
            resp = await async_client.get("/api/pushes/active")
    assert resp.status_code == 200
    fetch.assert_not_awaited()


def test_sqlite_pool_is_explicit_and_lock_wait_is_shorter_than_pool_wait() -> None:
    kwargs = database.engine_kwargs("sqlite+aiosqlite:///./x.db")
    assert kwargs["pool_size"] >= 10
    assert kwargs["max_overflow"] >= 10
    assert kwargs["connect_args"]["timeout"] < kwargs["pool_timeout"]
    assert database.SQLITE_BUSY_TIMEOUT_MS / 1000 < kwargs["pool_timeout"]


def test_memory_database_keeps_static_pool() -> None:
    from sqlalchemy.pool import StaticPool

    kwargs = database.engine_kwargs("sqlite+aiosqlite:///:memory:")
    assert kwargs["poolclass"] is StaticPool
    assert "pool_size" not in kwargs


async def _pending(session: Any) -> Any:
    h = await make_history(
        session,
        is_ad_hoc=True,
        media_type_label="series",
        media_title="Example Show",
        media_year=2018,
        deliver_enabled=True,
        delivery_status="pending",
    )
    await session.commit()
    return h


@pytest.mark.parametrize(
    "exc",
    [
        PoolTimeoutError("QueuePool limit reached"),
        OperationalError("stmt", {}, Exception("database is locked")),
    ],
)
async def test_transient_db_error_requeues_instead_of_failing(
    exc, db_session, monkeypatch
) -> None:
    h = await _pending(db_session)
    monkeypatch.setattr(worker, "DB_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(worker, "_deliver", AsyncMock(side_effect=exc))
    worker._queued_ids.clear()

    await worker.process_delivery(db_session, h.id)

    await db_session.refresh(h)
    assert (h.delivery_status, h.delivery_error) == ("pending", None)
    assert worker.queued_history_ids() == [h.id]
    worker._queued_ids.clear()


async def test_transient_db_error_fails_after_retry_budget(
    db_session, monkeypatch
) -> None:
    h = await _pending(db_session)
    monkeypatch.setattr(worker, "DB_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(
        worker, "_deliver", AsyncMock(side_effect=PoolTimeoutError("pool"))
    )
    worker._queued_ids.clear()
    for _ in range(worker.MAX_DB_RETRIES + 1):
        await worker.process_delivery(db_session, h.id)
        worker._queued_ids.clear()

    await db_session.refresh(h)
    assert h.delivery_status == "failed"


async def test_failed_progress_commit_does_not_poison_session(
    db_session, monkeypatch, tmp_path
) -> None:
    await make_delivery_provider(
        db_session, shows_path=str(tmp_path), min_free_buffer_bytes=0
    )
    h = await _pending(db_session)
    monkeypatch.setattr(worker, "PROGRESS_INTERVAL_SECONDS", 0)
    real_commit = db_session.commit
    calls = {"n": 0}

    async def flaky_commit() -> None:
        calls["n"] += 1
        if calls["n"] == 2:  # first commit is the 'delivering' marker
            raise PoolTimeoutError("QueuePool limit reached")
        await real_commit()

    outcome: list[str] = []

    class FakeEngine:
        def __init__(self, progress: Any) -> None:
            self._progress = progress

        async def deliver(self, *_a: Any) -> None:
            monkeypatch.setattr(db_session, "commit", flaky_commit)
            for done in (3, 6, 10):  # first raises inside, rest must work
                await self._progress(done, 10)
            outcome.append("ok")

    monkeypatch.setattr(worker, "CloudStreamEngine", FakeEngine)
    monkeypatch.setattr(
        worker,
        "get_downloader_client_for_type",
        AsyncMock(return_value=FakeDownloaderClient()),
    )

    await worker.process_delivery(db_session, h.id)

    assert outcome == ["ok"]
    assert calls["n"] >= 3


async def test_poll_endpoint_does_not_hang_when_tick_running(async_client) -> None:
    async with tp._poller_lock:
        resp = await asyncio.wait_for(async_client.post("/api/pushes/poll"), 5)
    assert resp.status_code == 200
    assert resp.json()["polled"] is False
