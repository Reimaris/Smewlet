"""TorBox adapter over app.services.downloaders.torbox (m57-03)."""

from unittest.mock import AsyncMock

import pytest

from app.services.downloaders import (
    DownloaderError,
    DownloaderNetworkError,
    TransferState,
    get_downloader_client,
    torbox,
)
from app.services.downloaders.torbox import TorBoxDownloader
from tests.conftest import make_downloader_provider


@pytest.fixture
async def client(db_session) -> TorBoxDownloader:
    provider = await make_downloader_provider(db_session, "torbox", api_key="tb-key")
    c = get_downloader_client(provider)
    assert isinstance(c, TorBoxDownloader)
    return c


def test_error_hierarchy():
    assert issubclass(torbox.TorBoxError, DownloaderError)
    assert issubclass(torbox.DownloaderNetworkError, DownloaderNetworkError)
    assert issubclass(torbox.DownloaderNetworkError, torbox.TorBoxError)


@pytest.mark.parametrize(
    ("item", "state", "pct"),
    [
        (
            {"download_state": "downloading", "progress": 0.5},
            TransferState.DOWNLOADING,
            50.0,
        ),
        ({"download_state": "queued"}, TransferState.QUEUED, 0.0),
        (
            {"download_state": "repairing", "progress": 1},
            TransferState.UNPACKING,
            100.0,
        ),
        (
            {"download_state": "completed", "progress": 1},
            TransferState.COMPLETED,
            100.0,
        ),
        ({"download_state": "cached"}, TransferState.COMPLETED, 100.0),
        ({"download_state": "failed (aborted)"}, TransferState.FAILED, 0.0),
        ({"download_state": "not_found"}, TransferState.FAILED, 0.0),
    ],
)
async def test_get_transfers_state_mapping(client, monkeypatch, item, state, pct):
    base = {"id": 7, "name": "Rel", "size": 100, "download_speed": 5, "eta": 9}
    monkeypatch.setattr(
        torbox, "get_usenet_downloads", AsyncMock(return_value=[{**base, **item}])
    )
    (t,) = await client.get_transfers()
    assert (t.transfer_id, t.name, t.state, t.progress_pct) == ("7", "Rel", state, pct)
    assert (t.size_bytes, t.speed_bytes, t.eta_seconds) == (100, 5, 9)
    assert (t.fail_message is not None) == (state is TransferState.FAILED)


@pytest.mark.parametrize(
    ("ret", "ok", "tid"),
    [
        ({"id": 42, "hash": "h"}, True, "42"),
        ({"error": "bad nzb"}, False, None),
        ({}, False, None),
    ],
)
async def test_upload_result(client, monkeypatch, ret, ok, tid):
    send = AsyncMock(return_value=ret)
    monkeypatch.setattr(torbox, "send_nzb_file", send)
    res = await client.upload_nzb(b"x", "a.nzb", password="pw", category="tv")
    assert (res.success, res.transfer_id) == (ok, tid)
    send.assert_awaited_once()
    assert send.await_args.kwargs["password"] == "pw"
    assert send.await_args.kwargs["explicit_key"] == "tb-key"


async def test_upload_rate_limited_and_network_error(client, monkeypatch):
    monkeypatch.setattr(
        torbox,
        "send_nzb_file",
        AsyncMock(
            side_effect=torbox.DownloaderNetworkError(
                "TorBox rate limit exceeded (cooldown: 30s)"
            )
        ),
    )
    res = await client.upload_nzb(b"x", "a.nzb")
    assert res.is_rate_limited and not res.success

    monkeypatch.setattr(
        torbox,
        "send_nzb_file",
        AsyncMock(
            side_effect=torbox.DownloaderNetworkError("TorBox network error: boom")
        ),
    )
    with pytest.raises(DownloaderNetworkError):
        await client.upload_nzb(b"x", "a.nzb")


async def test_delete_delegation(client, monkeypatch):
    delete = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(torbox, "delete_usenet_download", delete)
    assert await client.delete_transfer("12") is True
    assert delete.await_args.args[0] == 12
    delete.return_value = {"success": False}
    assert await client.delete_transfer("12") is False
    assert await client.delete_transfer("abc") is False


async def test_get_transfer_files(client, monkeypatch):
    items = [{"id": 1, "files": [{"name": "a/b.mkv", "size": 10}]}, {"id": 2}]
    monkeypatch.setattr(torbox, "get_usenet_downloads", AsyncMock(return_value=items))
    (f,) = await client.get_transfer_files("1")
    assert (f.name, f.size_bytes) == ("a/b.mkv", 10)
    assert await client.get_transfer_files("2") == []
    assert await client.get_transfer_files("9") == []


async def test_get_transfer_files_bypasses_cache_and_falls_back_to_full_list(
    client, monkeypatch
):
    recent = [{"id": i} for i in range(200)]
    full = [*recent, {"id": 500, "files": [{"name": "old.mkv", "size": 5}]}]
    mock = AsyncMock(side_effect=[recent, full])
    monkeypatch.setattr(torbox, "get_usenet_downloads", mock)
    (f,) = await client.get_transfer_files("500")
    assert f.name == "old.mkv"
    assert all(c.kwargs["bypass_cache"] is True for c in mock.await_args_list)
    assert mock.await_args_list[0].kwargs["limit"] == 200
    assert "limit" not in mock.await_args_list[1].kwargs
