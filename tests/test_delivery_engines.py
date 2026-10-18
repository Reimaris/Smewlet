"""Delivery engines and the download-links downloader capability (m78)."""

from __future__ import annotations

import errno
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.log_redaction import redact_text
from app.db.models import DeliveredFile, Provider
from app.services.delivery import engines
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.engines import (
    CloudStreamEngine,
    LocalLinkEngine,
    map_sab_path,
)
from app.services.delivery.naming import Destination
from app.services.delivery.planner import PlannedFile
from app.services.downloaders import premiumize, torbox
from app.services.downloaders.base import (
    DownloaderAuthError,
    DownloaderError,
    DownloaderFile,
    DownloaderNetworkError,
    DownloaderTransfer,
    DownloadLink,
    TransferState,
)
from app.services.downloaders.sabnzbd import SabnzbdDownloader
from tests.conftest import make_history

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


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

CFG = DeliveryConfig(min_free_buffer_bytes=0)
PAYLOAD = b"x" * 3000


class _FakeClient:
    def __init__(self, links: list[DownloadLink], folder: str | None = None) -> None:
        self._links, self._folder = links, folder

    async def get_download_links(self, transfer_id: str) -> list[DownloadLink]:
        return self._links

    async def get_transfers(self) -> list[Any]:
        return [
            DownloaderTransfer(
                transfer_id="tb-9000",
                name="x",
                state=TransferState.COMPLETED,
                progress_pct=100.0,
                folder_id=self._folder,
            )
        ]


@pytest.fixture
def use_client(monkeypatch: pytest.MonkeyPatch):
    def _set(client: _FakeClient) -> None:
        async def get(_s: Any, _t: str) -> _FakeClient:
            return client

        monkeypatch.setattr(engines, "get_downloader_client_for_type", get)

    return _set


def _plan(name: str, dest: Path, size: int = len(PAYLOAD)) -> PlannedFile:
    return PlannedFile(
        DownloaderFile(name=name, size_bytes=size),
        Destination(dest.parent, Path(dest.name), None),
        "video",
        None,
        None,
        None,
    )


async def _row(session: AsyncSession) -> DeliveredFile:
    return (await session.execute(select(DeliveredFile))).scalars().one()


def _serve(monkeypatch: pytest.MonkeyPatch, status: int = 200, body: bytes = PAYLOAD):
    def factory() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(status, content=body)
            )
        )

    monkeypatch.setattr(engines, "_client_factory", factory)


@pytest.mark.parametrize(
    ("path", "remote", "local", "expected"),
    [
        ("/data/dl/x", "", "", "/data/dl/x"),
        ("/data/dl/x", "/data/dl", "/mnt/usenet", "/mnt/usenet/x"),
        ("/data/dl2/x", "/data/dl", "/mnt/usenet", "/data/dl2/x"),
        ("/other/x", "/data/dl/", "/mnt/usenet/", "/other/x"),
    ],
)
def test_map_sab_path(path: str, remote: str, local: str, expected: str) -> None:
    cfg = DeliveryConfig(sab_path_map_remote=remote, sab_path_map_local=local)
    assert map_sab_path(path, cfg) == expected


async def test_cloud_stream_delivers_atomically(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g1")
    use_client(_FakeClient([DownloadLink(name="a/ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch)
    dest = tmp_path / "lib" / "ep.mkv"
    seen: list[int] = []

    async def progress(done: int, total: int) -> None:
        seen.append(done)

    await CloudStreamEngine(progress).deliver(
        db_session, history, [_plan("a/ep.mkv", dest)], CFG
    )

    assert dest.read_bytes() == PAYLOAD
    assert not dest.with_name("ep.mkv.part").exists()
    row = await _row(db_session)
    assert (row.status, row.error, row.dest_path) == ("delivered", None, str(dest))
    assert seen[-1] == 3000


async def test_size_mismatch_removes_part(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g2")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=9999)]))
    _serve(monkeypatch)
    dest = tmp_path / "ep.mkv"

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", dest, 9999)], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "SIZE_MISMATCH")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("status", [401, 403, 404])
async def test_expired_link(
    status, db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g3")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch, status=status, body=b"")

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "LINK_EXPIRED")
    assert not (tmp_path / "ep.mkv.part").exists()


async def test_existing_target_same_size_not_rewritten_else_conflict(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g4")
    use_client(
        _FakeClient(
            [
                DownloadLink(name="same.mkv", url=URL, size_bytes=3000),
                DownloadLink(name="diff.mkv", url=URL, size_bytes=3000),
            ]
        )
    )
    _serve(monkeypatch, body=b"y" * 3000)
    same, diff = tmp_path / "same.mkv", tmp_path / "diff.mkv"
    same.write_bytes(PAYLOAD)
    diff.write_bytes(b"short")

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("same.mkv", same), _plan("diff.mkv", diff)], CFG
    )

    rows = {
        r.source_name: r
        for r in (await db_session.execute(select(DeliveredFile))).scalars()
    }
    assert rows["same.mkv"].status == "delivered"
    assert same.read_bytes() == PAYLOAD
    assert (rows["diff.mkv"].status, rows["diff.mkv"].error) == (
        "failed",
        "TARGET_EXISTS",
    )
    assert diff.read_bytes() == b"short"


async def test_insufficient_storage_blocks_cloud_write(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g5")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch)
    cfg = DeliveryConfig(min_free_buffer_bytes=10**18)

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], cfg
    )

    assert (await _row(db_session)).error == "INSUFFICIENT_STORAGE"
    assert list(tmp_path.iterdir()) == []


async def test_local_engine_hardlinks_on_same_filesystem(
    db_session, use_client, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g6", downloader_type="sabnzbd")
    src_dir = tmp_path / "sab"
    src_dir.mkdir()
    (src_dir / "ep.mkv").write_bytes(PAYLOAD)
    use_client(_FakeClient([], folder="/remote/job"))
    cfg = DeliveryConfig(
        sab_path_map_remote="/remote/job", sab_path_map_local=str(src_dir)
    )
    dest = tmp_path / "lib" / "Show" / "ep.mkv"

    await LocalLinkEngine().deliver(db_session, history, [_plan("ep.mkv", dest)], cfg)

    assert os.path.samefile(src_dir / "ep.mkv", dest)
    assert (await _row(db_session)).status == "delivered"


async def test_local_engine_moves_across_filesystems(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g7", downloader_type="sabnzbd")
    src_dir = tmp_path / "sab"
    src_dir.mkdir()
    (src_dir / "ep.mkv").write_bytes(PAYLOAD)
    use_client(_FakeClient([], folder=str(src_dir)))

    def exdev(*_a: Any, **_k: Any) -> None:
        raise OSError(errno.EXDEV, "cross-device")

    monkeypatch.setattr(os, "link", exdev)
    dest = tmp_path / "lib" / "ep.mkv"

    await LocalLinkEngine().deliver(db_session, history, [_plan("ep.mkv", dest)], CFG)

    assert dest.read_bytes() == PAYLOAD
    assert not (src_dir / "ep.mkv").exists()
    assert (await _row(db_session)).status == "delivered"


async def test_local_engine_missing_source_is_path_unavailable(
    db_session, use_client, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g8", downloader_type="sabnzbd")
    use_client(_FakeClient([], folder=str(tmp_path / "gone")))

    await LocalLinkEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "lib" / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "PATH_UNAVAILABLE")


async def test_skipped_plan_entries_are_recorded(db_session, tmp_path) -> None:
    history = await make_history(db_session, nzb_guid="g9")
    skipped = replace(
        _plan("Extras.mkv", Path("Extras.mkv")),
        dest=Destination(Path(), Path("Extras.mkv"), None),
        status="skipped",
        error="UNMATCHED_EPISODE",
    )

    await CloudStreamEngine().deliver(db_session, history, [skipped], CFG)

    row = await _row(db_session)
    assert (row.status, row.error, row.dest_path) == (
        "skipped",
        "UNMATCHED_EPISODE",
        None,
    )


async def test_permission_error_is_readable(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g-eacces")
    use_client(_FakeClient([DownloadLink(name="a/ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch)

    def deny(*_a: Any, **_k: Any) -> None:
        raise PermissionError(errno.EACCES, "denied")

    monkeypatch.setattr(os, "replace", deny)
    dest = tmp_path / "lib" / "ep.mkv"

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("a/ep.mkv", dest)], CFG
    )

    row = await _row(db_session)
    assert row.status == "failed"
    assert (row.error or "").startswith("Permission denied:")
    assert "OS error" not in (row.error or "")
