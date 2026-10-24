"""TorBox client and ``BaseDownloader`` adapter (m57, ADR-131; merged in m59).

Module-level cooldowns, caches and limiters live here next to the adapter.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import TYPE_CHECKING, Any

import httpx

from app.config import DEFAULT_USER_AGENT
from app.core.rate_limiter import (
    RateLimiter,
    RateLimitExceeded,
    RollingWindowRateLimiter,
)
from app.services.downloaders.base import (
    BaseDownloader,
    DownloaderAuthError,
    DownloaderError,
    DownloaderFile,
    DownloaderOutageError,
    DownloaderTransfer,
    DownloaderUploadResult,
    DownloadLink,
    TransferState,
    outage_upload_result,
)
from app.services.downloaders.base import (
    DownloaderNetworkError as BaseDownloaderNetworkError,
)
from app.services.outage_classifier import classify_exception, classify_http_status

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.db.models import Provider

logger = logging.getLogger(__name__)
_auto_send_limiter = RollingWindowRateLimiter(
    50, 3600.0, max_wait_seconds=0.0
)  # 50/hour for background automation
_manual_send_limiter = RollingWindowRateLimiter(
    60, 3600.0, max_wait_seconds=0.0
)  # 60/hour hard ceiling for manual grabs
_send_limiter = _manual_send_limiter  # Backwards compatibility alias
TORBOX_USENET_CACHE_TTL = 6.0
_poll_limiter = RateLimiter(6.0)

_torbox_cooldown_until: float = 0.0

_cached_usenet_downloads: list[dict[str, Any]] = []
_cached_usenet_downloads_at: float = 0.0
_cache_lock = asyncio.Lock()


def get_cooldown_remaining(is_manual: bool = False) -> float:
    """Return remaining cooldown seconds, or 0.0 if not in cooldown.

    Checks the global circuit breaker (e.g. from HTTP 429).
    If is_manual is False, also checks whether the background automation rate
    limiter budget (50/hr) has slots available.
    """
    now = time.monotonic()
    circuit_breaker = max(0.0, _torbox_cooldown_until - now)
    if circuit_breaker > 0:
        return circuit_breaker

    limiter = _manual_send_limiter if is_manual else _auto_send_limiter
    return limiter.get_wait_time()


def set_cooldown(seconds: float) -> None:
    """Activate global TorBox cooldown for the specified duration."""
    global _torbox_cooldown_until
    _torbox_cooldown_until = time.monotonic() + max(seconds, 0.0)


def clear_cooldown() -> None:
    """Clear global TorBox cooldown."""
    global _torbox_cooldown_until
    _torbox_cooldown_until = 0.0


def clear_usenet_cache() -> None:
    """Clear in-memory Usenet download cache."""
    global _cached_usenet_downloads, _cached_usenet_downloads_at
    _cached_usenet_downloads = []
    _cached_usenet_downloads_at = 0.0


TORBOX_BASE_URL = "https://api.torbox.app/v1"
DEFAULT_API_URL = "https://api.torbox.app/v1/api"


class TorBoxError(DownloaderError):
    pass


class DownloaderNetworkError(TorBoxError, BaseDownloaderNetworkError):
    pass


def _client_factory() -> httpx.AsyncClient:
    """Patch point for tests (inject ``httpx.MockTransport``)."""
    return httpx.AsyncClient(timeout=15.0, headers={"User-Agent": DEFAULT_USER_AGENT})


def _sanitize_error_body(text: str) -> str:
    if not text:
        return ""
    clean = re.sub(r"<.*?>", " ", text)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:200]


async def resolve_api_key(
    session: AsyncSession | None = None, explicit_key: str | None = None
) -> str:
    """Resolve active TorBox API key from explicit argument or DB provider."""
    if explicit_key:
        return explicit_key

    from app.services.provider_service import get_active_downloader

    if session is not None:
        provider = await get_active_downloader(session)
        if provider and provider.api_key:
            return provider.api_key
    else:
        from app.db.database import async_session_factory

        try:
            async with async_session_factory() as db:
                provider = await get_active_downloader(db)
                if provider and provider.api_key:
                    return provider.api_key
        except Exception:
            pass

    return ""


async def send_nzb_file(
    nzb_bytes: bytes,
    filename: str = "file.nzb",
    api_key: str | None = None,
    session: AsyncSession | None = None,
    explicit_key: str | None = None,
    is_manual: bool = False,
    password: str | None = None,
) -> dict[str, str | int | None]:
    """Send an NZB file directly to TorBox.

    Args:
        nzb_bytes: The raw NZB file bytes.
        filename: Optional filename for the upload.
        api_key: Optional explicit API key.
        session: Optional DB session to resolve active provider.
        explicit_key: Optional explicit key override.
        is_manual: Whether the upload was triggered by interactive manual action.
        password: Optional archive password for automated extraction.

    Returns:
        A dictionary with "hash" and "id" if successful, else error dict.
    """
    key = await resolve_api_key(session=session, explicit_key=explicit_key or api_key)
    if not key:
        logger.warning("TorBox API key missing.")
        return {}

    remaining = get_cooldown_remaining(is_manual=is_manual)
    if remaining > 0:
        scope = "manual" if is_manual else "automation"
        raise DownloaderNetworkError(
            f"TorBox {scope} cooldown active ({int(remaining)}s remaining)"
        )

    limiter = _manual_send_limiter if is_manual else _auto_send_limiter

    url = f"{TORBOX_BASE_URL}/api/usenet/createusenetdownload"
    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    files = {"file": (filename, nzb_bytes, "application/x-nzb")}
    data: dict[str, Any] = {}
    if password:
        data["password"] = password

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await limiter.wait()
        except RateLimitExceeded as rle:
            scope = "manual" if is_manual else "automation"
            raise DownloaderNetworkError(
                f"TorBox {scope} rate limit exceeded (cooldown: {int(rle.wait_time)}s)"
            ) from rle
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(
                    url, headers=headers, files=files, data=data if data else None
                )
                response.raise_for_status()

                result = response.json()
                if result.get("success"):
                    resp_data = result.get("data", {})
                    tb_id = (
                        resp_data.get("usenetdownload_id")
                        or resp_data.get("usenet_id")
                        or resp_data.get("id")
                        or resp_data.get("download_id")
                        or resp_data.get("usenet_download_id")
                    )
                    if not tb_id:
                        logger.warning(
                            "TorBox success but no known ID field found in data: %s",
                            resp_data,
                        )
                    return {
                        "hash": resp_data.get("hash"),
                        "id": tb_id,
                    }
                else:
                    err_msg = result.get("detail") or "TorBox API Error"
                    logger.error("TorBox API returned error: %s", err_msg)
                    return {"error": err_msg}

            except httpx.HTTPError as e:
                is_retriable = True
                err_detail = str(e)
                verdict = classify_exception(e)
                if verdict is not None and verdict.kind == "auth":
                    raise DownloaderAuthError(
                        f"TorBox authentication failed ({verdict.label})", verdict
                    ) from e
                if isinstance(e, httpx.HTTPStatusError):
                    status = e.response.status_code
                    if status == 429:
                        retry_after_hdr = e.response.headers.get("Retry-After")
                        cooldown_secs = 300.0
                        if retry_after_hdr:
                            try:
                                cooldown_secs = float(retry_after_hdr)
                            except (ValueError, TypeError):
                                cooldown_secs = 300.0
                        set_cooldown(cooldown_secs)
                        logger.warning(
                            "⚠️ TorBox rate limit hit (429). Activating cooldown for %ss.",
                            int(cooldown_secs),
                        )
                        raise DownloaderNetworkError(
                            f"TorBox rate limit exceeded (cooldown: {int(cooldown_secs)}s)"
                        ) from e

                    if not (
                        500 <= status < 600
                        or status in (429, 520, 521, 522, 523, 524, 525, 526, 530)
                    ):
                        is_retriable = False
                    try:
                        err_json = e.response.json()
                        err_detail = (
                            err_json.get("detail")
                            or err_json.get("error")
                            or e.response.text
                        )
                    except Exception:
                        err_detail = e.response.text

                err_detail = _sanitize_error_body(str(err_detail))

                if not is_retriable:
                    logger.error("TorBox API error (non-retriable): %s", err_detail)
                    return {"error": str(err_detail)}

                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    logger.warning(
                        "Failed to send NZB file to TorBox: [%s] %s. Retrying in %ss...",
                        type(e).__name__,
                        e,
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                else:
                    err_msg = _sanitize_error_body(str(e))
                    logger.error(
                        "Failed to send NZB file to TorBox after %d attempts: [%s] %s",
                        max_retries,
                        type(e).__name__,
                        err_msg,
                    )
                    if verdict is not None:
                        raise DownloaderOutageError(
                            f"TorBox outage ({verdict.label}): {err_msg}", verdict
                        ) from e
                    raise DownloaderNetworkError(
                        f"TorBox network error: {err_msg}"
                    ) from e

    return {}


async def send_magnet_link(
    magnet_url: str,
    api_key: str | None = None,
    session: AsyncSession | None = None,
    explicit_key: str | None = None,
    is_manual: bool = False,
) -> dict[str, str | int | None]:
    """Send a Magnet/Torrent URL to TorBox.

    Args:
        magnet_url: The magnet URI or torrent URL.
        api_key: Optional explicit API key.
        session: Optional DB session to resolve active provider.
        explicit_key: Optional explicit key override.
        is_manual: Whether the upload was triggered by interactive manual action.

    Returns:
        A dictionary with "hash" and "id" if successful, else empty dict.
    """
    key = await resolve_api_key(session=session, explicit_key=explicit_key or api_key)
    if not key:
        logger.warning("TorBox API key missing.")
        return {}

    remaining = get_cooldown_remaining(is_manual=is_manual)
    if remaining > 0:
        scope = "manual" if is_manual else "automation"
        raise DownloaderNetworkError(
            f"TorBox {scope} cooldown active ({int(remaining)}s remaining)"
        )

    limiter = _manual_send_limiter if is_manual else _auto_send_limiter

    url = f"{TORBOX_BASE_URL}/api/torrents/createtorrent"

    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    data = {"magnet": magnet_url}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await limiter.wait()
        except RateLimitExceeded as rle:
            scope = "manual" if is_manual else "automation"
            raise DownloaderNetworkError(
                f"TorBox {scope} rate limit exceeded (cooldown: {int(rle.wait_time)}s)"
            ) from rle
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, headers=headers, data=data)
                response.raise_for_status()

                result = response.json()
                if result.get("success"):
                    resp_data = result.get("data", {})
                    tb_id = (
                        resp_data.get("torrentdownload_id")
                        or resp_data.get("torrent_id")
                        or resp_data.get("id")
                        or resp_data.get("download_id")
                        or resp_data.get("torrent_download_id")
                    )
                    if not tb_id:
                        logger.warning(
                            "TorBox magnet upload success but no known ID field found in data: %s",
                            resp_data,
                        )
                    return {
                        "hash": resp_data.get("hash"),
                        "id": tb_id,
                    }
                else:
                    err_msg = result.get("detail") or "TorBox API Error"
                    logger.error("TorBox API returned error: %s", err_msg)
                    return {"error": err_msg}

            except httpx.HTTPError as e:
                is_retriable = True
                err_detail = str(e)
                if isinstance(e, httpx.HTTPStatusError):
                    status = e.response.status_code
                    if status == 429:
                        retry_after_hdr = e.response.headers.get("Retry-After")
                        cooldown_secs = 300.0
                        if retry_after_hdr:
                            try:
                                cooldown_secs = float(retry_after_hdr)
                            except (ValueError, TypeError):
                                cooldown_secs = 300.0
                        set_cooldown(cooldown_secs)
                        logger.warning(
                            "⚠️ TorBox rate limit hit (429). Activating cooldown for %ss.",
                            int(cooldown_secs),
                        )
                        raise DownloaderNetworkError(
                            f"TorBox rate limit exceeded (cooldown: {int(cooldown_secs)}s)"
                        ) from e

                    if not (
                        500 <= status < 600
                        or status in (429, 520, 521, 522, 523, 524, 525, 526, 530)
                    ):
                        is_retriable = False
                    try:
                        err_json = e.response.json()
                        err_detail = (
                            err_json.get("detail")
                            or err_json.get("error")
                            or e.response.text
                        )
                    except Exception:
                        err_detail = e.response.text

                err_detail = _sanitize_error_body(str(err_detail))

                if not is_retriable:
                    logger.error("TorBox API error (non-retriable): %s", err_detail)
                    return {"error": str(err_detail)}

                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    logger.warning(
                        "Failed to send Magnet to TorBox: [%s] %s. Retrying in %ss...",
                        type(e).__name__,
                        e,
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                else:
                    err_msg = _sanitize_error_body(str(e))
                    logger.error(
                        "Failed to send Magnet to TorBox after %d attempts: [%s] %s",
                        max_retries,
                        type(e).__name__,
                        err_msg,
                    )
                    raise DownloaderNetworkError(
                        f"TorBox network error: {err_msg}"
                    ) from e

    return {}


async def delete_usenet_download(
    download_id: int,
    api_key: str | None = None,
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    """Deletes a usenet download from TorBox."""
    key = await resolve_api_key(session=session, explicit_key=api_key)
    if not key:
        return {"success": False, "detail": "No API key configured"}

    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(
                "https://api.torbox.app/v1/api/usenet/controlusenetdownload",
                headers=headers,
                json={"usenet_id": download_id, "operation": "delete"},
            )
            response.raise_for_status()
            res: dict[str, Any] = response.json()
            return res
        except httpx.HTTPError as e:
            logger.error(f"Failed to delete TorBox usenet download {download_id}: {e}")
            return {"success": False, "detail": str(e)}


async def delete_torrent_download(
    download_id: int,
    api_key: str | None = None,
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    """Deletes a torrent download from TorBox."""
    key = await resolve_api_key(session=session, explicit_key=api_key)
    if not key:
        return {"success": False, "detail": "No API key configured"}

    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(
                "https://api.torbox.app/v1/api/torrents/controltorrent",
                headers=headers,
                json={"torrent_id": download_id, "operation": "delete"},
            )
            response.raise_for_status()
            res: dict[str, Any] = response.json()
            return res
        except httpx.HTTPError as e:
            logger.error(f"Failed to delete TorBox torrent download {download_id}: {e}")
            return {"success": False, "detail": str(e)}


async def get_usenet_downloads(
    bypass_cache: bool = False,
    api_key: str | None = None,
    session: AsyncSession | None = None,
    explicit_key: str | None = None,
    raise_on_error: bool = False,
) -> list[dict[str, Any]]:
    """Retrieve list of active Usenet downloads from TorBox.

    With ``raise_on_error`` a transport failure raises ``DownloaderNetworkError``
    instead of returning ``[]``, so callers can tell an outage from a missing
    transfer.
    """
    global _cached_usenet_downloads, _cached_usenet_downloads_at

    key = await resolve_api_key(session=session, explicit_key=explicit_key or api_key)
    if not key:
        return []

    now = time.monotonic()
    if (
        not bypass_cache
        and (now - _cached_usenet_downloads_at < TORBOX_USENET_CACHE_TTL)
        and _cached_usenet_downloads
    ):
        return _cached_usenet_downloads

    url = f"{TORBOX_BASE_URL}/api/usenet/mylist"
    if bypass_cache:
        url += "?bypass_cache=true"

    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    await _poll_limiter.wait()
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if result.get("success"):
                data: list[dict[str, Any]] = result.get("data", [])
                async with _cache_lock:
                    _cached_usenet_downloads = data
                    _cached_usenet_downloads_at = time.monotonic()
                return data
            return []
        except httpx.HTTPError as e:
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429:
                retry_after_hdr = e.response.headers.get("Retry-After")
                cooldown_secs = 300.0
                if retry_after_hdr:
                    try:
                        cooldown_secs = float(retry_after_hdr)
                    except (ValueError, TypeError):
                        cooldown_secs = 300.0
                set_cooldown(cooldown_secs)
                logger.warning(
                    "⚠️ TorBox rate limit hit (429) during get_usenet_downloads. Activating cooldown for %ss.",
                    int(cooldown_secs),
                )
            logger.error(
                f"Failed to get TorBox usenet downloads: {type(e).__name__}: {e}"
            )
            if raise_on_error:
                raise DownloaderNetworkError(
                    f"TorBox unreachable: {type(e).__name__}"
                ) from None
            return []


async def get_torrent_downloads(
    api_key: str | None = None,
    session: AsyncSession | None = None,
    explicit_key: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve list of active Torrent downloads from TorBox."""
    key = await resolve_api_key(session=session, explicit_key=explicit_key or api_key)
    if not key:
        return []

    url = f"{TORBOX_BASE_URL}/api/torrents/mylist"
    headers = {
        "Authorization": f"Bearer {key}",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    await _poll_limiter.wait()
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if result.get("success"):
                return result.get("data", []) or []
            return []
        except httpx.HTTPError as e:
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429:
                retry_after_hdr = e.response.headers.get("Retry-After")
                cooldown_secs = 300.0
                if retry_after_hdr:
                    try:
                        cooldown_secs = float(retry_after_hdr)
                    except (ValueError, TypeError):
                        cooldown_secs = 300.0
                set_cooldown(cooldown_secs)
                logger.warning(
                    "⚠️ TorBox rate limit hit (429) during get_torrent_downloads. Activating cooldown for %ss.",
                    int(cooldown_secs),
                )
            logger.error(f"Failed to get TorBox torrent downloads: {e}")
            return []


_UNPACKING_STATES = ("processing", "repairing", "unpacking", "extracting", "verifying")
_COMPLETED_STATES = ("completed", "cached", "paused")


def _map_state(status_lower: str, item: dict[str, Any]) -> TransferState:
    from app.core.transfer_poller import _is_terminal_failure_state

    if _is_terminal_failure_state(status_lower):
        return TransferState.FAILED
    if status_lower in _COMPLETED_STATES or (
        item.get("download_finished") is True
        and status_lower not in ("downloading", "processing", "queued", "unknown")
    ):
        return TransferState.COMPLETED
    if status_lower in _UNPACKING_STATES:
        return TransferState.UNPACKING
    if status_lower in ("queued", "waiting"):
        return TransferState.QUEUED
    return TransferState.DOWNLOADING


class TorBoxDownloader(BaseDownloader):
    provider_type = "torbox"
    display_name = "TorBox"
    poll_interval_seconds = 20
    supports_file_listing = True
    supports_magnet = True

    # Classmethods look module functions up at call time so test patches apply.
    @classmethod
    def get_cooldown_remaining(cls, is_manual: bool = False) -> float:
        return get_cooldown_remaining(is_manual=is_manual)

    @classmethod
    def set_cooldown(cls, seconds: float) -> None:
        set_cooldown(seconds)

    @classmethod
    def clear_cooldown(cls) -> None:
        clear_cooldown()

    @classmethod
    def clear_transfer_cache(cls) -> None:
        clear_usenet_cache()

    @classmethod
    async def fetch_raw_transfers(
        cls, bypass_cache: bool = False, session: AsyncSession | None = None
    ) -> list[dict[str, Any]]:
        return await get_usenet_downloads(bypass_cache=bypass_cache, session=session)

    @classmethod
    async def delete_raw_transfer(
        cls,
        transfer_id: int | str,
        kind: str = "usenet",
        session: AsyncSession | None = None,
    ) -> None:
        parsed: Any = int(transfer_id) if str(transfer_id).isdigit() else transfer_id
        if kind == "torrent":
            await delete_torrent_download(parsed, session=session)
        else:
            await delete_usenet_download(parsed, session=session)

    @classmethod
    async def send_magnet(
        cls, magnet: str, is_manual: bool = False, session: AsyncSession | None = None
    ) -> dict[str, Any]:
        return await send_magnet_link(magnet, session=session, is_manual=is_manual)

    def __init__(self, provider: Provider, session: AsyncSession | None = None):
        self.provider = provider
        self.session = session

    @property
    def _key(self) -> str | None:
        return (self.provider.api_key or "").strip() or None

    async def test_connection(self) -> tuple[bool, str]:
        if not self._key:
            return False, "API Key missing"
        base_url = (self.provider.api_url or DEFAULT_API_URL).rstrip("/")
        endpoint = (
            f"{base_url}/user/me"
            if base_url.endswith("/api")
            else f"{base_url}/api/user/me"
        )
        try:
            async with httpx.AsyncClient(
                timeout=5.0, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as client:
                r = await client.get(
                    endpoint, headers={"Authorization": f"Bearer {self._key}"}
                )
        except httpx.HTTPError as e:
            return False, f"TorBox unreachable: {e}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        detail = "Account verified"
        try:
            data = r.json().get("data") or {}
            if data.get("email"):
                plan = data.get("plan")
                detail = (
                    f"{data['email']} (Plan {plan})"
                    if plan is not None
                    else data["email"]
                )
        except Exception:
            pass
        return True, detail

    async def upload_nzb(
        self,
        nzb_bytes: bytes,
        filename: str,
        password: str | None = None,
        category: str | None = None,
        is_manual: bool = False,
    ) -> DownloaderUploadResult:
        try:
            result = await send_nzb_file(
                nzb_bytes,
                filename=filename,
                session=self.session,
                explicit_key=self._key,
                is_manual=is_manual,
                password=password,
            )
        except (DownloaderOutageError, DownloaderAuthError) as e:
            return outage_upload_result(e.verdict, str(e))
        except DownloaderNetworkError as e:
            msg = str(e)
            if "cooldown" in msg.lower() or "rate limit" in msg.lower():
                remaining = get_cooldown_remaining(is_manual=is_manual)
                return DownloaderUploadResult(
                    success=False,
                    detail=msg,
                    is_rate_limited=True,
                    cooldown_seconds=int(remaining) if remaining > 0 else None,
                )
            raise
        if not result:
            return DownloaderUploadResult(
                success=False, detail="TorBox API key missing"
            )
        if result.get("error"):
            return DownloaderUploadResult(
                success=False, detail=str(result["error"]), raw=dict(result)
            )
        tb_id = result.get("id")
        if not tb_id and not result.get("hash"):
            return DownloaderUploadResult(
                success=False, detail="Failed to send", raw=dict(result)
            )
        return DownloaderUploadResult(
            success=True,
            transfer_id=str(tb_id) if tb_id else None,
            raw=dict(result),
        )

    async def get_transfers(self) -> list[DownloaderTransfer]:
        from app.core.transfer_poller import _normalize_progress_pct

        items = await get_usenet_downloads(session=self.session, explicit_key=self._key)
        transfers: list[DownloaderTransfer] = []
        for item in items:
            status = str(item.get("download_state") or "downloading").lower()
            state = _map_state(status, item)
            eta: int | None
            try:
                eta = max(0, int(item["eta"])) if item.get("eta") is not None else None
            except (TypeError, ValueError):
                eta = None
            size = item.get("size")
            transfers.append(
                DownloaderTransfer(
                    transfer_id=str(item.get("id")),
                    name=str(item.get("name") or item.get("title") or ""),
                    state=state,
                    progress_pct=_normalize_progress_pct(item.get("progress"), status),
                    size_bytes=int(size) if isinstance(size, int | float) else None,
                    speed_bytes=int(
                        item.get("download_speed") or item.get("speed") or 0
                    ),
                    eta_seconds=eta,
                    fail_message=status if state is TransferState.FAILED else None,
                    raw=item,
                )
            )
        return transfers

    async def get_transfer_files(self, transfer_id: str) -> list[DownloaderFile]:
        items = await get_usenet_downloads(session=self.session, explicit_key=self._key)
        for item in items:
            if str(item.get("id")) != str(transfer_id):
                continue
            raw_files = item.get("files")
            if not isinstance(raw_files, list):
                return []
            return [
                DownloaderFile(
                    name=str(f.get("name") or f.get("short_name") or ""),
                    size_bytes=int(f.get("size") or 0),
                    path=f.get("name"),
                )
                for f in raw_files
                if isinstance(f, dict)
            ]
        return []

    async def get_download_links(self, transfer_id: str) -> list[DownloadLink]:
        items = await get_usenet_downloads(
            bypass_cache=True,
            session=self.session,
            explicit_key=self._key,
            raise_on_error=True,
        )
        item = next((i for i in items if str(i.get("id")) == str(transfer_id)), None)
        if item is None:
            raise DownloaderError("TorBox transfer not found")
        links: list[DownloadLink] = []
        async with _client_factory() as client:
            for f in item.get("files") or []:
                if not isinstance(f, dict) or f.get("id") is None:
                    continue
                try:
                    resp = await client.get(
                        f"{TORBOX_BASE_URL}/api/usenet/requestdl",
                        params={
                            "token": self._key or "",
                            "usenet_id": transfer_id,
                            "file_id": f["id"],
                            "redirect": "false",
                        },
                    )
                    resp.raise_for_status()
                    body = resp.json()
                except httpx.HTTPStatusError as e:
                    verdict = classify_http_status(e.response.status_code)
                    if verdict is not None and verdict.kind == "auth":
                        raise DownloaderAuthError(
                            "TorBox rejected credentials", verdict
                        ) from None
                    raise DownloaderNetworkError(
                        f"TorBox link request failed: HTTP {e.response.status_code}"
                    ) from None
                except (httpx.HTTPError, ValueError) as e:
                    raise DownloaderNetworkError(
                        f"TorBox unreachable: {type(e).__name__}"
                    ) from None
                url = body.get("data") if isinstance(body, dict) else None
                if not (isinstance(body, dict) and body.get("success") and url):
                    raise DownloaderError("TorBox returned no download link")
                links.append(
                    DownloadLink(
                        name=str(f.get("name") or f.get("short_name") or ""),
                        url=str(url),
                        size_bytes=int(f.get("size") or 0),
                    )
                )
        return links

    async def delete_transfer(
        self, transfer_id: str, delete_files: bool = True
    ) -> bool:
        try:
            tb_id = int(transfer_id)
        except (TypeError, ValueError):
            return False
        res = await delete_usenet_download(
            tb_id, api_key=self._key, session=self.session
        )
        return bool(res.get("success"))
