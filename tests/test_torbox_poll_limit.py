"""Large TorBox accounts: the poller fetches only recent transfers, not the full list."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.core import transfer_poller as tp
from app.services.downloaders import torbox
from app.services.downloaders.factory import downloader_class
from tests.conftest import make_history, make_movie

LIMIT = tp.TORBOX_POLL_RECENT_LIMIT


@pytest.fixture(autouse=True)
def _fresh_full_sync():
    """Each test starts as if a full sync happened just now (limited path active)."""
    import time

    old = tp._last_torbox_full_sync
    tp._last_torbox_full_sync = time.monotonic()
    yield
    tp._last_torbox_full_sync = old


@pytest.fixture
def fetch():
    mock = AsyncMock()
    with patch.object(downloader_class("torbox"), "fetch_raw_transfers", mock):
        yield mock


def _items(n: int, start: int = 1) -> list[dict]:
    return [{"id": i, "download_state": "downloading"} for i in range(start, start + n)]


async def test_limit_is_sent_to_torbox_and_not_cached():
    torbox.clear_usenet_cache()
    response = httpx.Response(
        200,
        json={"success": True, "data": [{"id": 1}]},
        request=httpx.Request("GET", "https://api.torbox.app/v1/api/usenet/mylist"),
    )
    with (
        patch("httpx.AsyncClient") as client_cls,
        patch.object(torbox._poll_limiter, "wait", new_callable=AsyncMock),
    ):
        client = AsyncMock()
        client.get.return_value = response
        client.__aenter__.return_value = client
        client_cls.return_value = client
        await torbox.get_usenet_downloads(explicit_key="k", limit=50)
        assert "offset=0&limit=50" in client.get.call_args.args[0]
        await torbox.get_usenet_downloads(explicit_key="k", limit=50)
        assert client.get.call_count == 2  # partial result must not be cached
    torbox.clear_usenet_cache()


async def test_small_account_is_complete(db_session, fetch):
    fetch.return_value = _items(3)
    items, complete = await tp._fetch_torbox_snapshot(db_session)
    assert len(items) == 3 and complete is True
    fetch.assert_awaited_once()


async def test_truncated_snapshot_is_incomplete_when_tracked_ids_present(
    db_session, fetch
):
    movie = await make_movie(db_session)
    await make_history(
        db_session,
        item=movie,
        downloader_type="torbox",
        downloader_transfer_id="5",
        status_detail="downloading",
        is_dismissed=False,
    )
    fetch.return_value = _items(LIMIT)
    items, complete = await tp._fetch_torbox_snapshot(db_session)
    assert len(items) == LIMIT and complete is False
    fetch.assert_awaited_once()


async def test_falls_back_to_full_list_when_tracked_id_missing(db_session, fetch):
    movie = await make_movie(db_session)
    await make_history(
        db_session,
        item=movie,
        downloader_type="torbox",
        downloader_transfer_id="99999",
        status_detail="downloading",
        is_dismissed=False,
    )
    fetch.side_effect = [_items(LIMIT), _items(LIMIT + 50)]
    items, complete = await tp._fetch_torbox_snapshot(db_session)
    assert len(items) == LIMIT + 50 and complete is True
    assert fetch.await_count == 2
    assert "limit" not in fetch.await_args_list[1].kwargs


async def test_periodic_full_sync_is_taken(db_session, fetch):
    tp._last_torbox_full_sync = float("-inf")
    fetch.return_value = _items(LIMIT + 10)
    items, complete = await tp._fetch_torbox_snapshot(db_session)
    assert complete is True and len(items) == LIMIT + 10
    fetch.assert_awaited_once()
    assert "limit" not in fetch.await_args.kwargs


def test_tick_timings_warn_only_when_slow(caplog):
    timings = tp._TickTimings()
    timings.mark("a")
    with caplog.at_level("DEBUG", logger=tp.logger.name):
        timings.log(7)
    assert any(
        "Tick finished" in r.message and r.levelname == "DEBUG" for r in caplog.records
    )
    with patch.object(tp, "SLOW_TICK_SECONDS", -1.0):
        timings.log(7)
    assert any(r.levelname == "WARNING" for r in caplog.records)


async def test_snapshot_bypasses_torbox_server_cache(db_session, fetch):
    fetch.return_value = _items(3)
    await tp._fetch_torbox_snapshot(db_session)
    assert fetch.await_args.kwargs["bypass_cache"] is True
