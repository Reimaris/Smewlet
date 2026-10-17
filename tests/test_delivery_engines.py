"""Delivery engines and the download-links downloader capability (m78)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.core.log_redaction import redact_text
from app.db.models import Provider
from app.services.downloaders import premiumize, torbox
from app.services.downloaders.base import (
    DownloaderAuthError,
    DownloaderError,
    DownloaderNetworkError,
    DownloadLink,
)
from app.services.downloaders.sabnzbd import SabnzbdDownloader

NAME = "Example.Show.S01E01.1080p.WEB-DL.x265-GRP.mkv"
URL = "https://example.com/dl/file.mkv"
EXPECTED = DownloadLink(name=NAME, url=URL, size_bytes=1_500_000_000)


def _mock(
    monkeypatch: pytest.MonkeyPatch, module: Any, handler: Any, **kw: Any
) -> None:
    def factory(*_a: Any, **_k: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(module, "_client_factory", factory)


def _torbox(monkeypatch: pytest.MonkeyPatch, files: list[dict[str, Any]]) -> Any:
    async def fake_list(**_kw: Any) -> list[dict[str, Any]]:
        return [{"id": 7, "files": files}]

    monkeypatch.setattr(torbox, "get_usenet_downloads", fake_list)
    return torbox.TorBoxDownloader(Provider(api_key="test-key"))


async def test_torbox_links(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"success": True, "data": URL})

    dl = _torbox(
        monkeypatch, [{"id": 3, "name": NAME, "size": 1_500_000_000}, {"name": "no-id"}]
    )
    _mock(monkeypatch, torbox, handler)
    assert await dl.get_download_links("7") == [EXPECTED]
    assert seen[0].url.params["usenet_id"] == "7"
    assert seen[0].url.params["file_id"] == "3"


@pytest.mark.parametrize(
    ("status", "exc"),
    [
        (401, DownloaderAuthError),
        (403, DownloaderAuthError),
        (404, DownloaderNetworkError),
    ],
)
async def test_torbox_link_errors(
    monkeypatch: pytest.MonkeyPatch, status: int, exc: type[Exception]
) -> None:
    dl = _torbox(monkeypatch, [{"id": 3, "name": NAME, "size": 1}])
    _mock(monkeypatch, torbox, lambda r: httpx.Response(status))
    with pytest.raises(exc) as info:
        await dl.get_download_links("7")
    assert "test-key" not in str(info.value)


async def test_torbox_unknown_transfer(monkeypatch: pytest.MonkeyPatch) -> None:
    dl = _torbox(monkeypatch, [])
    with pytest.raises(DownloaderError, match="not found"):
        await dl.get_download_links("999")


def _premiumize(monkeypatch: pytest.MonkeyPatch, folder_status: int = 200) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/transfer/list"):
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "transfers": [
                        {"id": "T1", "status": "finished", "folder_id": "F0"}
                    ],
                },
            )
        if request.url.params["id"] == "F0":
            return httpx.Response(
                folder_status,
                json={
                    "status": "success",
                    "content": [
                        {"id": "F9", "type": "folder", "name": "sub"},
                        {"type": "file", "name": "n.nfo", "size": 1},
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "status": "success",
                "content": [
                    {"type": "file", "name": NAME, "size": 1_500_000_000, "link": URL}
                ],
            },
        )

    _mock(monkeypatch, premiumize, handler)
    return premiumize.PremiumizeDownloader(Provider(api_key="test-key"))


async def test_premiumize_links_walk_subfolders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert await _premiumize(monkeypatch).get_download_links("T1") == [EXPECTED]


async def test_premiumize_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(DownloaderAuthError):
        await _premiumize(monkeypatch, folder_status=403).get_download_links("T1")


async def test_sabnzbd_links_unsupported() -> None:
    dl = SabnzbdDownloader(
        Provider(api_url="http://sab.example.com", api_key="test-key")
    )
    with pytest.raises(DownloaderError, match="not supported"):
        await dl.get_download_links("x")


def test_link_url_masked_and_not_in_repr() -> None:
    assert "secret-value-123456" not in redact_text(
        "GET https://example.com/dl/file.mkv?token=secret-value-123456"
    )
    assert URL not in repr(EXPECTED)
