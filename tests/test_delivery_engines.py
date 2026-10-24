"""Delivery engines and the download-links downloader capability (m78)."""

from __future__ import annotations

import asyncio
import errno
import io
import os
import urllib.error
from dataclasses import replace
from email.message import Message
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import DEFAULT_USER_AGENT
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
from app.services.delivery.offline import StorageOffline
from app.services.delivery.planner import PlannedFile
from app.services.downloaders import premiumize, torbox
from app.services.downloaders.base import (
    DownloaderAuthError,
    DownloaderError,
    DownloaderFile,
    DownloaderNetworkError,
    DownloaderOutageError,
    DownloaderTransfer,
    DownloadLink,
    TransferState,
)
from app.services.downloaders.sabnzbd import SabnzbdDownloader
from app.services.outage_classifier import OutageVerdict
from tests.conftest import fake_urlopen, make_history

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


async def test_torbox_list_outage_is_not_missing_transfer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_list(**kw: Any) -> list[dict[str, Any]]:
        assert kw["raise_on_error"] is True
        raise DownloaderNetworkError("TorBox unreachable: ReadTimeout")

    monkeypatch.setattr(torbox, "get_usenet_downloads", fake_list)
    dl = torbox.TorBoxDownloader(Provider(api_key="test-key"))
    with pytest.raises(DownloaderNetworkError, match="unreachable"):
        await dl.get_download_links("7")


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


def _raise(exc: Exception) -> Any:
    def _open(*_a: Any, **_k: Any) -> None:
        raise exc

    return _open


def _serve(monkeypatch: pytest.MonkeyPatch, status: int = 200, body: bytes = PAYLOAD):
    monkeypatch.setattr(engines, "_urlopen", fake_urlopen(status, body))


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
    status, db_session, use_client, monkeypatch, tmp_path, caplog
) -> None:
    history = await make_history(db_session, nzb_guid="g3")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch, status=status, body=b"")

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "LINK_EXPIRED")
    assert f"HTTP {status} from example.com" in caplog.text
    assert not (tmp_path / "ep.mkv.part").exists()


@pytest.mark.parametrize(
    "make_exc",
    [
        lambda: DownloaderNetworkError("TorBox unreachable: ReadTimeout"),
        lambda: DownloaderOutageError("HTTP 503", OutageVerdict("outage", "HTTP 503")),
    ],
)
async def test_downloader_outage_pauses_instead_of_failing(
    make_exc, db_session, use_client, monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(engines, "LINK_FETCH_RETRY_SECONDS", 0)
    calls = 0

    class _Down(_FakeClient):
        async def get_download_links(self, transfer_id: str) -> list[DownloadLink]:
            nonlocal calls
            calls += 1
            raise make_exc()

    history = await make_history(db_session, nzb_guid="g-out")
    use_client(_Down([]))

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "DOWNLOADER_UNAVAILABLE")
    assert calls == engines.LINK_FETCH_ATTEMPTS


async def test_http_error_status_is_recorded(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g3b")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch, status=503, body=b"")

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "STREAM_INTERRUPTED")


class _ResettingStream(io.BytesIO):
    """Delivers one full buffer, then the connection drops."""

    def readinto(self, b: Any) -> int:
        if self.tell():
            raise ConnectionResetError("peer reset")
        return super().readinto(b)


@pytest.mark.parametrize(
    "opener",
    [
        _raise(urllib.error.URLError("dns failure")),
        lambda *_a, **_k: _ResettingStream(PAYLOAD),
    ],
)
async def test_network_errors_are_recorded_without_leftovers(
    opener, db_session, use_client, monkeypatch, tmp_path
) -> None:
    history = await make_history(db_session, nzb_guid="g3c")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    monkeypatch.setattr(engines, "_urlopen", opener)
    monkeypatch.setattr(engines, "CHUNK_BYTES", 1000)

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "STREAM_INTERRUPTED")
    assert "example.com" not in (row.error or "")
    assert list(tmp_path.iterdir()) == []


async def test_multi_chunk_download_is_ordered_and_url_is_quoted(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    body = bytes(range(256)) * 10  # 2560 bytes -> 3 chunks of 1000
    history = await make_history(db_session, nzb_guid="g3d")
    link = DownloadLink(
        name="ep.mkv", url="https://example.com/dl/my file.mkv?t=a", size_bytes=2560
    )
    use_client(_FakeClient([link]))
    opened: list[str] = []

    def opener(request: Any, timeout: float | None = None) -> io.BytesIO:
        opened.append(request.full_url)
        return io.BytesIO(body)

    monkeypatch.setattr(engines, "_urlopen", opener)
    monkeypatch.setattr(engines, "CHUNK_BYTES", 1000)
    dest = tmp_path / "ep.mkv"

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", dest, 2560)], CFG
    )

    assert dest.read_bytes() == body
    assert opened == ["https://example.com/dl/my%20file.mkv?t=a"]


@pytest.mark.parametrize("url", ["file:///etc/hostname", "ftp://example.com/x.mkv"])
def test_only_http_links_are_opened(url: str) -> None:
    with pytest.raises(engines._FileError) as info:
        engines._open_stream(url)
    assert info.value.code == "Network error: URLError"


BIG = bytes(range(256)) * 14  # 3584 bytes -> 4 ranges of 1000


@pytest.fixture
def ranged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engines, "RANGE_BYTES", 1000)
    monkeypatch.setattr(engines, "RANGED_MIN_BYTES", 1000)
    monkeypatch.setattr(engines, "CHUNK_BYTES", 400)
    monkeypatch.setattr(engines, "PROGRESS_THROTTLE", 0)
    monkeypatch.setattr(engines, "RANGE_RETRY_DELAY", 0)


async def _deliver_big(db_session, use_client, tmp_path, nzb_guid: str) -> Path:
    history = await make_history(db_session, nzb_guid=nzb_guid)
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=len(BIG))]))
    dest = tmp_path / "ep.mkv"
    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", dest, len(BIG))], CFG
    )
    return dest


@pytest.mark.parametrize("honors_ranges", [True, False])
async def test_large_file_uses_ranges_or_falls_back(
    honors_ranges, ranged, db_session, use_client, monkeypatch, tmp_path
) -> None:
    base = fake_urlopen(body=BIG, ranges=honors_ranges)
    requested: list[str | None] = []

    def opener(request: Any, timeout: float | None = None) -> Any:
        requested.append(request.get_header("Range"))
        return base(request, timeout)

    monkeypatch.setattr(engines, "_urlopen", opener)

    dest = await _deliver_big(db_session, use_client, tmp_path, "g-rng")

    assert dest.read_bytes() == BIG
    assert (await _row(db_session)).status == "delivered"
    assert list(tmp_path.iterdir()) == [dest]
    if honors_ranges:
        assert sorted(r for r in requested if r != "bytes=0-0") == [
            "bytes=0-999",
            "bytes=1000-1999",
            "bytes=2000-2999",
            "bytes=3000-3583",
        ]
    else:
        assert requested == ["bytes=0-0", None]


async def test_range_retries_transient_network_error(
    ranged, db_session, use_client, monkeypatch, tmp_path
) -> None:
    base = fake_urlopen(body=BIG)
    failed: list[str] = []

    def opener(request: Any, timeout: float | None = None) -> Any:
        if request.get_header("Range") == "bytes=1000-1999" and not failed:
            failed.append("once")
            raise urllib.error.URLError("reset")
        return base(request, timeout)

    monkeypatch.setattr(engines, "_urlopen", opener)

    dest = await _deliver_big(db_session, use_client, tmp_path, "g-rng2")

    assert failed == ["once"]
    assert dest.read_bytes() == BIG


async def test_range_hard_failure_fails_file_and_removes_part(
    ranged, db_session, use_client, monkeypatch, tmp_path
) -> None:
    base = fake_urlopen(body=BIG)

    def opener(request: Any, timeout: float | None = None) -> Any:
        if request.get_header("Range") == "bytes=2000-2999":
            raise urllib.error.HTTPError(URL, 503, "busy", Message(), None)
        return base(request, timeout)

    monkeypatch.setattr(engines, "_urlopen", opener)

    await _deliver_big(db_session, use_client, tmp_path, "g-rng3")

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "STREAM_INTERRUPTED")
    assert list(tmp_path.iterdir()) == []


async def test_range_abort_removes_part(
    ranged, db_session, use_client, monkeypatch, tmp_path
) -> None:
    base = fake_urlopen(body=BIG)

    def opener(request: Any, timeout: float | None = None) -> Any:
        if request.get_header("Range") == "bytes=1000-1999":
            engines.request_abort(history.id)
        return base(request, timeout)

    monkeypatch.setattr(engines, "_urlopen", opener)
    history = await make_history(db_session, nzb_guid="g-rng4")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=len(BIG))]))
    engine = CloudStreamEngine()

    with pytest.raises(engines.DeliveryAborted):
        await engine.deliver(
            db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv", len(BIG))], CFG
        )

    engines.clear_abort(history.id)
    assert list(tmp_path.iterdir()) == []


async def test_progress_finishes_before_engine_continues(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    """Progress runs on the shared session, so it must never overlap later work."""
    history = await make_history(db_session, nzb_guid="g3e")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch)
    monkeypatch.setattr(engines, "CHUNK_BYTES", 1000)
    monkeypatch.setattr(engines, "PROGRESS_THROTTLE", 0)
    running = 0
    overlaps: list[int] = []

    async def slow_progress(done: int, total: int) -> None:
        nonlocal running
        running += 1
        await asyncio.sleep(0.05)
        running -= 1

    real_record = engines._record

    async def record(*args: Any) -> None:
        overlaps.append(running)
        await real_record(*args)

    monkeypatch.setattr(engines, "_record", record)

    await CloudStreamEngine(slow_progress).deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    assert overlaps == [0]
    assert (await _row(db_session)).status == "delivered"


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


async def _offline_error(
    db_session, use_client, monkeypatch, tmp_path, code: int, reachable: bool
) -> tuple[DeliveryConfig, Path]:
    history = await make_history(db_session, nzb_guid=f"g-{code}")
    use_client(_FakeClient([DownloadLink(name="ep.mkv", url=URL, size_bytes=3000)]))
    _serve(monkeypatch)
    root = tmp_path / "lib"
    dest = root / "ep.mkv"

    def fail(*_a: Any, **_k: Any) -> None:
        raise OSError(code, os.strerror(code))

    async def probe(_root: Path) -> bool:
        return reachable

    monkeypatch.setattr(os, "replace", fail)
    monkeypatch.setattr(engines, "probe_root", probe)
    cfg = replace(CFG, movies_path=str(root))
    await CloudStreamEngine().deliver(db_session, history, [_plan("ep.mkv", dest)], cfg)
    return cfg, dest


async def test_offline_errno_with_failed_probe_raises_storage_offline(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    with pytest.raises(StorageOffline) as info:
        await _offline_error(
            db_session, use_client, monkeypatch, tmp_path, errno.ESTALE, False
        )
    assert info.value.root == (tmp_path / "lib").resolve()
    assert (await db_session.execute(select(DeliveredFile))).scalars().all() == []
    assert not (tmp_path / "lib" / "ep.mkv.part").exists()


async def test_offline_errno_with_reachable_root_fails_readably(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    await _offline_error(
        db_session, use_client, monkeypatch, tmp_path, errno.ENOENT, True
    )
    row = await _row(db_session)
    assert row.status == "failed"
    assert "OS error" not in (row.error or "")


async def test_erofs_does_not_probe(
    db_session, use_client, monkeypatch, tmp_path
) -> None:
    await _offline_error(
        db_session, use_client, monkeypatch, tmp_path, errno.EROFS, False
    )
    assert (await _row(db_session)).status == "failed"


async def test_missing_link_logs_offered_names(
    db_session, use_client, tmp_path, caplog
) -> None:
    history = await make_history(db_session, nzb_guid="g-nolink")
    use_client(_FakeClient([DownloadLink(name="other.mkv", url=URL, size_bytes=1)]))

    await CloudStreamEngine().deliver(
        db_session, history, [_plan("ep.mkv", tmp_path / "ep.mkv")], CFG
    )

    row = await _row(db_session)
    assert (row.status, row.error) == ("failed", "LINK_EXPIRED")
    assert "no download link for 'ep.mkv'" in caplog.text
    assert "other.mkv" in caplog.text


def test_downloads_send_the_app_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []
    base = fake_urlopen()

    def opener(request: Any, timeout: float | None = None) -> Any:
        seen.append(request)
        return base(request, timeout)

    monkeypatch.setattr(engines, "_urlopen", opener)

    engines._open_stream(URL).close()

    assert seen[0].get_header("User-agent") == DEFAULT_USER_AGENT
