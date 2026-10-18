"""Delivery engines: stream cloud files to disk or link/move SABnzbd files."""

from __future__ import annotations

import asyncio
import errno
import logging
import os
import posixpath
import shutil
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Protocol

import httpx
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
from app.services.delivery.planner import PlannedFile
from app.services.downloaders.base import DownloaderError, DownloadLink
from app.services.downloaders.factory import get_downloader_client_for_type

logger = logging.getLogger(__name__)

CHUNK_BYTES = 1024 * 1024
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


def _client_factory() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, read=120.0), follow_redirects=True
    )


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
            except httpx.HTTPError as exc:
                await _record(
                    session,
                    history,
                    planned,
                    "failed",
                    f"Network error: {type(exc).__name__}",
                )
            except OSError as exc:
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
        if _existing_target(dest, link.size_bytes):
            return
        check_free_space(dest.parent, link.size_bytes, cfg.min_free_buffer_bytes)
        await asyncio.to_thread(dest.parent.mkdir, parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        written = 0
        try:
            async with _client_factory() as client:
                async with client.stream("GET", link.url) as resp:
                    if resp.status_code in EXPIRED_STATUSES:
                        raise _FileError("LINK_EXPIRED")
                    if resp.status_code >= 400:
                        raise _FileError(f"HTTP {resp.status_code}")
                    with part.open("wb") as fh:
                        async for chunk in resp.aiter_bytes(CHUNK_BYTES):
                            if is_abort_requested(history_id):
                                raise DeliveryAborted
                            await asyncio.to_thread(fh.write, chunk)
                            written += len(chunk)
                            if self._progress is not None:
                                await self._progress(written, link.size_bytes)
                        await asyncio.to_thread(fh.flush)
                        await asyncio.to_thread(os.fsync, fh.fileno())
            if written != link.size_bytes:
                raise _FileError("SIZE_MISMATCH")
            await asyncio.to_thread(os.replace, part, dest)
        except BaseException:
            part.unlink(missing_ok=True)
            raise


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
            if ".." in rel.parts or not src.is_file():
                raise _FileError("PATH_UNAVAILABLE")
            await self._place(src, planned.dest.full, cfg)

        await self._run(session, history, plan, cfg, one)

    async def _place(self, src: Path, dest: Path, cfg: DeliveryConfig) -> None:
        size = src.stat().st_size
        if _existing_target(dest, size):
            return
        if same_filesystem(src, dest.parent):
            await asyncio.to_thread(dest.parent.mkdir, parents=True, exist_ok=True)
            try:
                await asyncio.to_thread(os.link, src, dest)
                return
            except FileExistsError:
                raise _FileError("TARGET_EXISTS") from None
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
        check_free_space(dest.parent, size, cfg.min_free_buffer_bytes)
        await asyncio.to_thread(dest.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.move, str(src), str(dest))
