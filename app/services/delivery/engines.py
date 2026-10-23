"""Delivery engines: stream cloud files to disk or link/move SABnzbd files."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import errno
import http.client
import io
import logging
import os
import posixpath
import queue
import shutil
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.log_redaction import redact_text
from app.db.models import DeliveredFile, DownloadHistory
from app.services.delivery.config import DeliveryConfig
from app.services.delivery.errors import describe_os_error
from app.services.delivery.guard import (
    InsufficientStorage,
    check_free_space,
    same_filesystem,
)
from app.services.delivery.offline import (
    OFFLINE_ERRNOS,
    StorageOffline,
    probe_root,
    run_fs,
)
from app.services.delivery.planner import PlannedFile
from app.services.downloaders.base import DownloaderError, DownloadLink
from app.services.downloaders.factory import get_downloader_client_for_type

logger = logging.getLogger(__name__)

CHUNK_BYTES = 4 * 1024 * 1024
WRITE_QUEUE_DEPTH = 8
RANGE_BYTES = 32 * 1024 * 1024
RANGE_WORKERS = 4
RANGE_RETRIES = 2
RANGE_RETRY_DELAY = 1.0
RANGED_MIN_BYTES = 2 * RANGE_BYTES
REQUEST_TIMEOUT = 120.0
PROGRESS_THROTTLE = 0.25
PART_CLEANUP_TIMEOUT = 5.0
EXPIRED_STATUSES = frozenset({401, 403, 404})

_abort_requested: set[int] = set()


class DeliveryAborted(Exception):
    """The user aborted a running delivery."""


def request_abort(history_id: int) -> None:
    _abort_requested.add(history_id)


def is_abort_requested(history_id: int) -> bool:
    return history_id in _abort_requested


def clear_abort(history_id: int) -> None:
    _abort_requested.discard(history_id)


ProgressCallback = Callable[[int, int], Awaitable[None]]  # (bytes_done, bytes_total)


# HTTP(S) only: the default opener would also serve file:// and ftp:// (and ftp
# redirects), which a provider-supplied link must never reach.
_opener = urllib.request.OpenerDirector()
for _handler in (
    urllib.request.ProxyHandler(),
    urllib.request.UnknownHandler(),
    urllib.request.HTTPHandler(),
    urllib.request.HTTPSHandler(),
    urllib.request.HTTPDefaultErrorHandler(),
    urllib.request.HTTPRedirectHandler(),
    urllib.request.HTTPErrorProcessor(),
):
    _opener.add_handler(_handler)
_urlopen = _opener.open  # test seam


def _open_stream(
    url: str, start: int | None = None, end: int | None = None
) -> io.BufferedIOBase:
    """Open ``url`` (redirects followed), mapping failures to ``_FileError`` codes.

    Bulk downloads use the stdlib instead of httpx: its asyncio/h11 stack was
    CPU-bound at ~30 MB/s on one core, while ``readinto`` on a plain socket
    roughly doubles that.
    """
    safe = urllib.parse.quote(url, safe="%/:=&?~#+!$,;'@()*[]")
    request = urllib.request.Request(safe)
    if start is not None:
        request.add_header("Range", f"bytes={start}-{end}")
    try:
        return _urlopen(request, timeout=REQUEST_TIMEOUT)  # type: ignore[no-any-return]
    except urllib.error.HTTPError as exc:
        exc.close()
        if exc.code in EXPIRED_STATUSES:
            raise _FileError("LINK_EXPIRED") from None
        raise _FileError(f"HTTP {exc.code}") from None
    except (OSError, http.client.HTTPException) as exc:
        raise _FileError(f"Network error: {type(exc).__name__}") from None


class DeliveryEngine(Protocol):
    async def deliver(
        self,
        session: AsyncSession,
        history: DownloadHistory,
        plan: Sequence[PlannedFile],
        cfg: DeliveryConfig,
    ) -> None: ...


def map_sab_path(path: str, cfg: DeliveryConfig) -> str:
    """Replace the remote SABnzbd prefix with the local one (no-op when unmapped)."""
    remote = cfg.sab_path_map_remote
    if not remote:
        return path
    base = remote.rstrip("/")
    if path == base or path.startswith(base + "/"):
        return cfg.sab_path_map_local.rstrip("/") + path[len(base) :]
    return path


class _FileError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _basename(name: str) -> str:
    return posixpath.basename(name.replace("\\", "/"))


async def _record(
    session: AsyncSession,
    history: DownloadHistory,
    planned: PlannedFile,
    status: str,
    error: str | None,
) -> None:
    """Upsert the ``DeliveredFile`` row of ``planned`` and commit."""
    stmt = select(DeliveredFile).where(
        DeliveredFile.history_id == history.id,
        DeliveredFile.source_name == planned.source.name,
    )
    row = (await session.execute(stmt)).scalars().first()
    if row is None:
        row = DeliveredFile(
            history_id=history.id,
            source_name=planned.source.name,
            kind=planned.kind,
        )
        session.add(row)
    has_dest = bool(planned.dest.root.parts)
    row.dest_path = str(planned.dest.full) if has_dest else None
    row.episode_id = planned.episode_id
    row.season_id = planned.season_id
    row.anilist_id = planned.anilist_id
    row.status = status
    row.error = error[:500] if error else None
    await session.commit()


def _existing_target(dest: Path, expected_size: int) -> bool:
    """True when ``dest`` already holds the file; raises TARGET_EXISTS on a size clash."""
    if not os.path.lexists(dest):
        return False
    if dest.is_file() and dest.stat().st_size == expected_size:
        return True
    raise _FileError("TARGET_EXISTS")


def library_roots(cfg: DeliveryConfig) -> list[Path]:
    """Configured library roots (resolved), in config order."""
    paths = (cfg.movies_path, cfg.shows_path, cfg.anime_path, cfg.anime_movies_path)
    return [Path(p).resolve() for p in paths if p]


def configured_root_of(dest: Path, cfg: DeliveryConfig) -> Path:
    """The configured library root containing ``dest`` (its parent if none does).

    Purely lexical, so it never touches the (possibly dead) mount.
    """
    for p in (cfg.movies_path, cfg.shows_path, cfg.anime_path, cfg.anime_movies_path):
        if p and dest.is_relative_to(p):
            return Path(p)
    return dest.parent


def library_root_of(planned: PlannedFile, cfg: DeliveryConfig) -> Path:
    """The configured root containing ``planned.dest.full`` (its parent if none does)."""
    return configured_root_of(planned.dest.full, cfg)


async def _remove_part(dest: Path) -> None:
    """Best-effort removal of ``<dest>.part`` that cannot hang on a dead mount."""
    part = dest.with_name(dest.name + ".part")
    with contextlib.suppress(Exception):
        await run_fs(
            dest.parent, part.unlink, missing_ok=True, timeout=PART_CLEANUP_TIMEOUT
        )


class _BaseEngine:
    async def _run(
        self,
        session: AsyncSession,
        history: DownloadHistory,
        plan: Sequence[PlannedFile],
        cfg: DeliveryConfig,
        one: Callable[[PlannedFile], Awaitable[None]],
    ) -> None:
        for planned in plan:
            if is_abort_requested(history.id):
                raise DeliveryAborted
            if planned.status != "pending":
                await _record(session, history, planned, planned.status, planned.error)
                continue
            try:
                await one(planned)
            except _FileError as exc:
                await _record(session, history, planned, "failed", exc.code)
            except InsufficientStorage:
                await _record(
                    session, history, planned, "failed", "INSUFFICIENT_STORAGE"
                )
            except OSError as exc:
                if exc.errno in OFFLINE_ERRNOS:
                    root = library_root_of(planned, cfg)
                    if not await probe_root(root):
                        await _remove_part(planned.dest.full)
                        raise StorageOffline(root) from exc
                message = describe_os_error(exc, planned.dest.full)
                logger.warning(
                    "[DELIVERY] history %s: %s (errno=%s)",
                    history.id,
                    message,
                    errno.errorcode.get(exc.errno or 0, "?"),
                )
                await _record(session, history, planned, "failed", message)
            else:
                await _record(session, history, planned, "delivered", None)


class CloudStreamEngine(_BaseEngine):
    """Stream fresh cloud links to ``<dest>.part`` and atomically rename."""

    def __init__(self, progress: ProgressCallback | None = None) -> None:
        self._progress = progress

    async def deliver(
        self,
        session: AsyncSession,
        history: DownloadHistory,
        plan: Sequence[PlannedFile],
        cfg: DeliveryConfig,
    ) -> None:
        links: dict[str, DownloadLink] = {}
        pending = [p for p in plan if p.status == "pending"]
        link_error: str | None = None
        if pending:
            client = await get_downloader_client_for_type(
                session, history.downloader_type or ""
            )
            try:
                if client is None or not history.downloader_transfer_id:
                    raise DownloaderError("No downloader available")
                for link in await client.get_download_links(
                    history.downloader_transfer_id
                ):
                    links[link.name] = link
                    links.setdefault(_basename(link.name), link)
            except DownloaderError as exc:
                link_error = redact_text(str(exc))[:200]

        async def one(planned: PlannedFile) -> None:
            if link_error is not None:
                raise _FileError(link_error)
            link = links.get(planned.source.name) or links.get(
                _basename(planned.source.name)
            )
            if link is None:
                raise _FileError("LINK_EXPIRED")
            await self._stream(link, planned.dest.full, cfg, history.id)

        await self._run(session, history, plan, cfg, one)

    async def _stream(
        self, link: DownloadLink, dest: Path, cfg: DeliveryConfig, history_id: int
    ) -> None:
        root = configured_root_of(dest, cfg)
        if await run_fs(root, _existing_target, dest, link.size_bytes):
            return
        await run_fs(
            root,
            check_free_space,
            dest.parent,
            link.size_bytes,
            cfg.min_free_buffer_bytes,
        )
        await run_fs(root, dest.parent.mkdir, parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        loop = asyncio.get_running_loop()
        stop = threading.Event()
        last_report = 0.0
        pending: concurrent.futures.Future[None] | None = None

        async def report(done: int) -> None:
            if self._progress is not None:
                await self._progress(done, link.size_bytes)

        def on_bytes(done: int) -> None:
            # Runs in the writer thread: hand throttled progress to the event loop.
            nonlocal last_report, pending
            now = time.monotonic()
            if (
                self._progress is None
                or now - last_report < PROGRESS_THROTTLE
                or (pending is not None and not pending.done())
            ):
                return
            last_report = now
            pending = asyncio.run_coroutine_threadsafe(report(done), loop)

        job = asyncio.ensure_future(
            asyncio.to_thread(
                _fetch_to_file,
                link.url,
                part,
                link.size_bytes,
                history_id,
                stop,
                on_bytes,
            )
        )

        async def drain() -> None:
            # The session is not shared across tasks: finish progress first.
            if pending is not None:
                await asyncio.wrap_future(pending)

        try:
            try:
                written = await asyncio.shield(job)
            except asyncio.CancelledError:
                stop.set()
                with contextlib.suppress(BaseException):
                    await job
                with contextlib.suppress(BaseException):
                    await drain()
                raise
            except BaseException:
                with contextlib.suppress(Exception):
                    await drain()
                raise
            await drain()
            if written != link.size_bytes:
                raise _FileError("SIZE_MISMATCH")
            await report(written)
            await run_fs(root, os.replace, part, dest)
        except BaseException:
            await _remove_part(dest)
            raise


def _read_into(resp: io.BufferedIOBase, view: memoryview) -> int:
    """``readinto`` that reports socket/HTTP failures as network ``_FileError``s."""
    try:
        return resp.readinto(view) or 0
    except (OSError, http.client.HTTPException) as exc:
        raise _FileError(f"Network error: {type(exc).__name__}") from None


def _fetch_to_file(
    url: str,
    part: Path,
    size: int,
    history_id: int,
    stop: threading.Event,
    on_bytes: Callable[[int], None],
) -> int:
    """Blocking GET of ``url`` into ``part``; returns the bytes written.

    Large files use parallel range requests when the server honors them; anything
    else streams over a single connection.
    """
    if size >= RANGED_MIN_BYTES and _supports_ranges(url, size):
        return _fetch_ranged(url, part, size, history_id, stop, on_bytes)
    return _fetch_single(url, part, history_id, stop, on_bytes)


def _supports_ranges(url: str, size: int) -> bool:
    """True when a 1-byte range probe returns 206 with the expected total size."""
    try:
        with _open_stream(url, 0, 0) as resp:
            if getattr(resp, "status", None) != 206:
                return False
            header = getattr(resp, "headers", None)
            total = (header.get("Content-Range") if header else None) or ""
            return total.rpartition("/")[2] == str(size)
    except _FileError as exc:
        if exc.code.startswith("HTTP "):
            return False  # e.g. 416: let the plain request report what is wrong
        raise


def _pwrite_all(fd: int, data: memoryview, offset: int) -> None:
    while data:
        n = os.pwrite(fd, data, offset)
        data, offset = data[n:], offset + n


def _fetch_ranged(
    url: str,
    part: Path,
    size: int,
    history_id: int,
    stop: threading.Event,
    on_bytes: Callable[[int], None],
) -> int:
    """Download ``size`` bytes with ``RANGE_WORKERS`` parallel range requests.

    One connection tops out at one core of TLS work; each worker writes its range
    at its own offset (``pwrite``), so reads and writes overlap across workers.
    """
    ranges = [(a, min(a + RANGE_BYTES, size) - 1) for a in range(0, size, RANGE_BYTES)]
    lock = threading.Lock()
    next_range = 0
    written = 0

    def take() -> tuple[int, int] | None:
        nonlocal next_range
        with lock:
            if next_range >= len(ranges) or stop.is_set():
                return None
            next_range += 1
            return ranges[next_range - 1]

    def worker(fd: int) -> None:
        nonlocal written
        buf = bytearray(CHUNK_BYTES)
        while (rng := take()) is not None:
            pos, end = rng
            retries = RANGE_RETRIES
            while pos <= end and not stop.is_set():
                try:
                    with _open_stream(url, pos, end) as resp:
                        if getattr(resp, "status", None) != 206:
                            raise _FileError("RANGE_UNSUPPORTED")
                        while pos <= end and not stop.is_set():
                            view = memoryview(buf)[: min(CHUNK_BYTES, end - pos + 1)]
                            n = _read_into(resp, view)
                            if not n:
                                raise _FileError("Network error: IncompleteRead")
                            _pwrite_all(fd, view[:n], pos)
                            pos += n
                            with lock:
                                written += n
                            if is_abort_requested(history_id):
                                raise DeliveryAborted
                except _FileError as exc:
                    if not exc.code.startswith("Network error") or retries == 0:
                        raise
                    retries -= 1
                    stop.wait(RANGE_RETRY_DELAY)

    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        os.ftruncate(fd, size)
        count = min(RANGE_WORKERS, len(ranges))
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=count, thread_name_prefix="delivery-range"
        ) as pool:
            futures = [pool.submit(worker, fd) for _ in range(count)]
            try:
                while True:
                    done, pending = concurrent.futures.wait(
                        futures,
                        timeout=PROGRESS_THROTTLE,
                        return_when=concurrent.futures.FIRST_EXCEPTION,
                    )
                    on_bytes(written)
                    if not pending or any(f.exception() for f in done):
                        break
            finally:
                stop.set()  # lets the remaining workers wind down on failure
        errors = [e for f in futures if (e := f.exception()) is not None]
        if errors:
            raise next(
                (e for e in errors if not isinstance(e, DeliveryAborted)), errors[0]
            )
        os.fsync(fd)
    finally:
        os.close(fd)
    return written


def _fetch_single(
    url: str,
    part: Path,
    history_id: int,
    stop: threading.Event,
    on_bytes: Callable[[int], None],
) -> int:
    """Stream ``url`` into ``part`` over one connection; returns the bytes written.

    A reader thread fills 4 MiB buffers while this thread writes them, so network
    and (NFS) writes overlap. Both release the GIL while they wait on I/O.
    """
    chunks: queue.Queue[tuple[bytearray, int] | BaseException | None] = queue.Queue(
        WRITE_QUEUE_DEPTH
    )

    def put(item: tuple[bytearray, int] | BaseException | None) -> None:
        while not stop.is_set():
            try:
                chunks.put(item, timeout=0.2)
                return
            except queue.Full:
                continue

    def reader() -> None:
        try:
            with _open_stream(url) as resp:
                while not stop.is_set():
                    buf = bytearray(CHUNK_BYTES)
                    n = _read_into(resp, memoryview(buf))
                    if not n:
                        break
                    put((buf, n))
        except BaseException as exc:
            put(exc)
        finally:
            put(None)

    thread = threading.Thread(target=reader, name="delivery-reader", daemon=True)
    thread.start()
    written = 0
    try:
        with part.open("wb") as fh:
            while (item := chunks.get()) is not None:
                if isinstance(item, BaseException):
                    raise item
                buf, n = item
                fh.write(memoryview(buf)[:n])
                written += n
                if stop.is_set() or is_abort_requested(history_id):
                    raise DeliveryAborted
                on_bytes(written)
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        stop.set()
        thread.join()
    return written


class LocalLinkEngine(_BaseEngine):
    """Hardlink SABnzbd output into the library (move across filesystems)."""

    async def deliver(
        self,
        session: AsyncSession,
        history: DownloadHistory,
        plan: Sequence[PlannedFile],
        cfg: DeliveryConfig,
    ) -> None:
        folder: str | None = None
        if any(p.status == "pending" for p in plan):
            client = await get_downloader_client_for_type(
                session, history.downloader_type or ""
            )
            if client is not None and history.downloader_transfer_id:
                for transfer in await client.get_transfers():
                    if transfer.transfer_id == history.downloader_transfer_id:
                        folder = transfer.folder_id
                        break

        async def one(planned: PlannedFile) -> None:
            if not folder:
                raise _FileError("PATH_UNAVAILABLE")
            base = Path(map_sab_path(folder, cfg))
            rel = Path(planned.source.path or planned.source.name.replace("\\", "/"))
            src = rel if rel.is_absolute() else base / rel
            if ".." in rel.parts or not await run_fs(
                configured_root_of(planned.dest.full, cfg), src.is_file
            ):
                raise _FileError("PATH_UNAVAILABLE")
            await self._place(src, planned.dest.full, cfg)

        await self._run(session, history, plan, cfg, one)

    async def _place(self, src: Path, dest: Path, cfg: DeliveryConfig) -> None:
        root = configured_root_of(dest, cfg)
        size = (await run_fs(root, src.stat)).st_size
        if await run_fs(root, _existing_target, dest, size):
            return
        if await run_fs(root, same_filesystem, src, dest.parent):
            await run_fs(root, dest.parent.mkdir, parents=True, exist_ok=True)
            try:
                await run_fs(root, os.link, src, dest)
                return
            except FileExistsError:
                raise _FileError("TARGET_EXISTS") from None
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
        await run_fs(
            root, check_free_space, dest.parent, size, cfg.min_free_buffer_bytes
        )
        await run_fs(root, dest.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.move, str(src), str(dest))
