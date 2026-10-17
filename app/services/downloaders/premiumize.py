"""Premiumize.me client (m57, ADR-131)."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

import httpx

from app.config import DEFAULT_USER_AGENT
from app.core.rate_limiter import RateLimitExceeded, RollingWindowRateLimiter
from app.services.downloaders.base import (
    BaseDownloader,
    DownloaderAuthError,
    DownloaderError,
    DownloaderFile,
    DownloaderNetworkError,
    DownloaderTransfer,
    DownloaderUploadResult,
    DownloadLink,
    TransferState,
    outage_upload_result,
)
from app.services.outage_classifier import classify_exception, classify_http_status

if TYPE_CHECKING:
    from app.db.models import Provider

logger = logging.getLogger(__name__)

BASE_URL = "https://www.premiumize.me/api"
FAIR_USE_COOLDOWN_SECONDS = 900
_FOLDER_MAX_DEPTH = 3
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
_UPLOAD_TIMEOUT = httpx.Timeout(120.0, connect=5.0)

_send_limiter = RollingWindowRateLimiter(60, 3600.0, max_wait_seconds=0.0)
_cooldown_until: float = 0.0

_STATES = {
    "waiting": TransferState.QUEUED,
    "queued": TransferState.QUEUED,
    "pending": TransferState.QUEUED,
    "running": TransferState.DOWNLOADING,
    "seeding": TransferState.DOWNLOADING,
    "finished": TransferState.COMPLETED,
    "error": TransferState.FAILED,
    "timeout": TransferState.FAILED,
    "banned": TransferState.FAILED,
    "deleted": TransferState.FAILED,
}


def get_cooldown_remaining() -> float:
    return max(0.0, _cooldown_until - time.monotonic())


def set_cooldown(seconds: float) -> None:
    global _cooldown_until
    _cooldown_until = time.monotonic() + max(seconds, 0.0)


def clear_cooldown() -> None:
    global _cooldown_until
    _cooldown_until = 0.0


def _is_fair_use(message: Any) -> bool:
    return "fair use limit reached" in str(message or "").lower()


def _client_factory(timeout: httpx.Timeout) -> httpx.AsyncClient:
    """Patch point for tests (inject ``httpx.MockTransport``)."""
    return httpx.AsyncClient(
        timeout=timeout, headers={"User-Agent": DEFAULT_USER_AGENT}
    )


def _rate_limited(detail: str = "Fair use limit reached") -> DownloaderUploadResult:
    return DownloaderUploadResult(
        success=False,
        detail=detail,
        is_rate_limited=True,
        cooldown_seconds=FAIR_USE_COOLDOWN_SECONDS,
    )


class PremiumizeDownloader(BaseDownloader):
    @classmethod
    def get_cooldown_remaining(cls, is_manual: bool = False) -> float:
        return get_cooldown_remaining()

    @classmethod
    def set_cooldown(cls, seconds: float) -> None:
        set_cooldown(seconds)

    @classmethod
    def clear_cooldown(cls) -> None:
        clear_cooldown()

    provider_type = "premiumize"
    display_name = "Premiumize"
    poll_interval_seconds = 20
    supports_file_listing = True

    def __init__(self, provider: Provider):
        self.provider = provider
        self._api_key = provider.api_key or ""

    async def _request(
        self,
        method: str,
        path: str,
        *,
        timeout: httpx.Timeout = _TIMEOUT,
        **kwargs: Any,
    ) -> tuple[int, dict[str, Any]]:
        """Return (http_status, json body). Transport failures raise."""
        key = {"apikey": self._api_key}
        if method == "GET":
            kwargs["params"] = {**key, **kwargs.get("params", {})}
        else:
            kwargs["data"] = {**key, **kwargs.get("data", {})}
        try:
            async with _client_factory(timeout) as client:
                resp = await client.request(method, f"{BASE_URL}{path}", **kwargs)
        except httpx.HTTPError as exc:
            raise DownloaderNetworkError(
                f"Premiumize unreachable: {type(exc).__name__}"
            ) from exc
        try:
            body = resp.json()
        except ValueError:
            body = {}
        return resp.status_code, body if isinstance(body, dict) else {}

    async def test_connection(self) -> tuple[bool, str]:
        try:
            _, body = await self._request("GET", "/account/info")
        except DownloaderNetworkError as exc:
            return False, str(exc)
        if body.get("status") != "success":
            return False, str(body.get("message") or "Premiumize request failed")
        used = float(body.get("limit_used") or 0.0) * 100
        return True, f"Premiumize account OK ({used:.0f}% fair-use used)"

    async def upload_nzb(
        self,
        nzb_bytes: bytes,
        filename: str,
        password: str | None = None,
        category: str | None = None,
        is_manual: bool = False,
    ) -> DownloaderUploadResult:
        if get_cooldown_remaining() > 0:
            return _rate_limited()
        if password:
            logger.warning(
                "Premiumize does not support NZB passwords; ignoring password for %s",
                filename,
            )
        try:
            await _send_limiter.wait()
        except RateLimitExceeded as exc:
            return DownloaderUploadResult(
                success=False,
                detail=str(exc),
                is_rate_limited=True,
                cooldown_seconds=int(exc.wait_time),
            )
        try:
            status, body = await self._request(
                "POST",
                "/transfer/create",
                timeout=_UPLOAD_TIMEOUT,
                files={"file": (filename, nzb_bytes, "application/x-nzb")},
            )
        except DownloaderNetworkError as exc:
            cause = exc.__cause__
            verdict = classify_exception(cause) if cause is not None else None
            if verdict is not None:
                return outage_upload_result(verdict, str(exc))
            return DownloaderUploadResult(success=False, detail=str(exc))
        message = body.get("message")
        if status == 429 or _is_fair_use(message):
            set_cooldown(FAIR_USE_COOLDOWN_SECONDS)
            return _rate_limited()
        status_verdict = classify_http_status(status)
        if status_verdict is not None:
            return outage_upload_result(
                status_verdict, f"Premiumize {status_verdict.label}"
            )
        if body.get("status") != "success" or not body.get("id"):
            return DownloaderUploadResult(
                success=False,
                detail=str(message or "Premiumize rejected the NZB"),
                raw=body,
            )
        return DownloaderUploadResult(
            success=True, transfer_id=str(body["id"]), raw=body
        )

    async def _list_raw(self) -> list[dict[str, Any]]:
        status, body = await self._request("GET", "/transfer/list")
        if status == 429 or _is_fair_use(body.get("message")):
            set_cooldown(FAIR_USE_COOLDOWN_SECONDS)
            raise DownloaderNetworkError("Premiumize rate limited")
        if body.get("status") != "success":
            raise DownloaderNetworkError(
                str(body.get("message") or "Premiumize transfer list failed")
            )
        return list(body.get("transfers") or [])

    async def get_transfers(self) -> list[DownloaderTransfer]:
        transfers = []
        for item in await self._list_raw():
            state = _STATES.get(
                str(item.get("status", "")).lower(), TransferState.QUEUED
            )
            pct = min(100.0, max(0.0, float(item.get("progress") or 0.0) * 100))
            transfers.append(
                DownloaderTransfer(
                    transfer_id=str(item["id"]),
                    name=item.get("name") or "",
                    state=state,
                    progress_pct=pct,
                    fail_message=(
                        item.get("message") if state == TransferState.FAILED else None
                    ),
                    folder_id=item.get("folder_id") or None,
                    raw=item,
                )
            )
        return transfers

    async def get_transfer_files(self, transfer_id: str) -> list[DownloaderFile]:
        folder_id = next(
            (
                t.get("folder_id")
                for t in await self._list_raw()
                if str(t.get("id")) == transfer_id
            ),
            None,
        )
        if not folder_id:
            return []
        return await self._walk_folder(str(folder_id), "", 1)

    async def _walk_folder(
        self, folder_id: str, prefix: str, depth: int
    ) -> list[DownloaderFile]:
        _, body = await self._request("GET", "/folder/list", params={"id": folder_id})
        files: list[DownloaderFile] = []
        for entry in body.get("content") or []:
            path = f"{prefix}{entry.get('name', '')}"
            if entry.get("type") == "folder":
                if depth < _FOLDER_MAX_DEPTH:
                    files += await self._walk_folder(
                        str(entry["id"]), f"{path}/", depth + 1
                    )
            else:
                files.append(
                    DownloaderFile(
                        name=entry.get("name", ""),
                        size_bytes=int(entry.get("size") or 0),
                        path=path,
                    )
                )
        return files

    async def get_download_links(self, transfer_id: str) -> list[DownloadLink]:
        folder_id = next(
            (
                t.get("folder_id")
                for t in await self._list_raw()
                if str(t.get("id")) == transfer_id
            ),
            None,
        )
        if not folder_id:
            raise DownloaderError("Premiumize transfer has no cloud folder")
        return await self._walk_links(str(folder_id), 1)

    async def _walk_links(self, folder_id: str, depth: int) -> list[DownloadLink]:
        status, body = await self._request(
            "GET", "/folder/list", params={"id": folder_id}
        )
        verdict = classify_http_status(status)
        if verdict is not None and verdict.kind == "auth":
            raise DownloaderAuthError("Premiumize rejected credentials", verdict)
        links: list[DownloadLink] = []
        for entry in body.get("content") or []:
            if entry.get("type") == "folder":
                if depth < _FOLDER_MAX_DEPTH:
                    links += await self._walk_links(str(entry["id"]), depth + 1)
            elif entry.get("link"):
                links.append(
                    DownloadLink(
                        name=str(entry.get("name") or ""),
                        url=str(entry["link"]),
                        size_bytes=int(entry.get("size") or 0),
                    )
                )
        return links

    async def delete_transfer(
        self, transfer_id: str, delete_files: bool = True
    ) -> bool:
        item_id = None
        if delete_files:
            try:
                for t in await self._list_raw():
                    if str(t.get("id")) == transfer_id:
                        item_id = t.get("file_id") or t.get("folder_id")
            except DownloaderNetworkError:
                logger.warning("Premiumize: could not resolve cloud item to delete")
        _, body = await self._request(
            "POST", "/transfer/delete", data={"id": transfer_id}
        )
        if body.get("status") != "success":
            return False
        if item_id:
            await self._delete_item(str(item_id))
        return True

    async def _delete_item(self, item_id: str) -> None:
        try:
            _, body = await self._request("POST", "/item/delete", data={"id": item_id})
            if (
                body.get("status") != "success"
                and "item not found" in str(body.get("message", "")).lower()
            ):
                _, body = await self._request(
                    "POST", "/folder/delete", data={"id": item_id}
                )
            if body.get("status") != "success":
                logger.warning("Premiumize item delete failed: %s", body.get("message"))
        except DownloaderNetworkError as exc:
            logger.warning("Premiumize item delete failed: %s", exc)
