"""Provider-neutral downloader contract (m57, ADR-131)."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from app.services.outage_classifier import OutageVerdict

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.db.models import Provider

DOWNLOADER_DISPLAY_NAMES: dict[str, str] = {
    "torbox": "TorBox",
    "sabnzbd": "SABnzbd",
    "premiumize": "Premiumize",
}
DOWNLOADER_TYPES: frozenset[str] = frozenset(DOWNLOADER_DISPLAY_NAMES)


class TransferState(str, enum.Enum):
    QUEUED = "queued"  # accepted, not started
    DOWNLOADING = "downloading"
    UNPACKING = "unpacking"  # post-processing / repair / extract
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class DownloaderUploadResult:
    success: bool
    transfer_id: str | None = None  # None when not dispatched
    detail: str = ""  # human-readable error/status
    is_rate_limited: bool = False
    cooldown_seconds: int | None = None  # set when is_rate_limited
    is_outage: bool = False  # transient upstream outage (ADR-158)
    outage_label: str | None = None  # OutageVerdict.label when is_outage
    is_auth_error: bool = False  # credential failure, never retried
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DownloaderTransfer:
    transfer_id: str
    name: str
    state: TransferState
    progress_pct: float  # 0.0-100.0
    size_bytes: int | None = None
    speed_bytes: int = 0
    eta_seconds: int | None = None
    fail_message: str | None = None
    folder_id: str | None = None  # Premiumize cloud folder / SAB storage path
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DownloaderFile:
    name: str
    size_bytes: int
    path: str | None = None


@dataclass(frozen=True)
class DownloadLink:
    """Fresh per-file HTTP link; never persisted or logged."""

    name: str
    url: str = field(repr=False)
    size_bytes: int


class DownloaderError(Exception):
    """Base class for downloader client failures."""


class DownloaderNetworkError(DownloaderError):
    """Transport failure; the poller must NOT mark transfers failed."""


class DownloaderOutageError(DownloaderError):
    """Transient downloader outage (5xx, timeout, TLS); the push is queued."""

    def __init__(self, message: str, verdict: OutageVerdict) -> None:
        super().__init__(message)
        self.verdict = verdict


class DownloaderAuthError(DownloaderError):
    """Downloader rejected the credentials (HTTP 401/403); fail fast."""

    def __init__(self, message: str, verdict: OutageVerdict) -> None:
        super().__init__(message)
        self.verdict = verdict


def outage_upload_result(verdict: OutageVerdict, detail: str) -> DownloaderUploadResult:
    """Map an outage/auth verdict to a failed ``DownloaderUploadResult``."""
    if verdict.kind == "auth":
        return DownloaderUploadResult(success=False, detail=detail, is_auth_error=True)
    return DownloaderUploadResult(
        success=False, detail=detail, is_outage=True, outage_label=verdict.label
    )


class BaseDownloader(ABC):
    provider: Provider
    provider_type: str  # "torbox" | "sabnzbd" | "premiumize"
    display_name: str  # "TorBox" | "SABnzbd" | "Premiumize"
    poll_interval_seconds: int  # SAB 5, cloud 20
    supports_file_listing: bool
    supports_magnet: bool = False
    session: AsyncSession | None = None  # optional DB session for key resolution

    # --- Process-global cooldown state (classmethods: no provider row needed) ---

    @classmethod
    def get_cooldown_remaining(cls, is_manual: bool = False) -> float:
        """Seconds until this downloader accepts dispatches again (0.0 = ready)."""
        return 0.0

    @classmethod
    def set_cooldown(cls, seconds: float) -> None:
        """Activate a rate-limit cooldown (no-op for downloaders without one)."""

    @classmethod
    def clear_cooldown(cls) -> None:
        """Clear the rate-limit cooldown (no-op for downloaders without one)."""

    # --- Optional capabilities (defaults for downloaders without them) ---

    @classmethod
    def clear_transfer_cache(cls) -> None:
        """Drop any in-memory live-transfer cache."""

    @classmethod
    async def fetch_raw_transfers(
        cls, bypass_cache: bool = False, session: AsyncSession | None = None
    ) -> list[dict[str, Any]]:
        """Provider-shaped live transfer dicts for the poller (default: none)."""
        return []

    @classmethod
    async def delete_raw_transfer(
        cls,
        transfer_id: int | str,
        kind: str = "usenet",
        session: AsyncSession | None = None,
    ) -> None:
        """Delete a transfer identified by a provider-shaped raw entry."""
        raise DownloaderError("Raw transfer deletion is not supported")

    @classmethod
    async def send_magnet(
        cls, magnet: str, is_manual: bool = False, session: AsyncSession | None = None
    ) -> dict[str, Any]:
        """Dispatch a magnet link (only when ``supports_magnet``)."""
        raise DownloaderError("Magnet links are not supported")

    @abstractmethod
    async def test_connection(self) -> tuple[bool, str]: ...

    @abstractmethod
    async def upload_nzb(
        self,
        nzb_bytes: bytes,
        filename: str,
        password: str | None = None,
        category: str | None = None,
        is_manual: bool = False,
    ) -> DownloaderUploadResult: ...

    @abstractmethod
    async def get_transfers(self) -> list[DownloaderTransfer]: ...

    @abstractmethod
    async def get_transfer_files(self, transfer_id: str) -> list[DownloaderFile]: ...

    async def get_download_links(self, transfer_id: str) -> list[DownloadLink]:
        """Fresh per-file HTTP links of a finished transfer (cloud downloaders only)."""
        raise DownloaderError("Download links are not supported")

    @abstractmethod
    async def delete_transfer(
        self, transfer_id: str, delete_files: bool = True
    ) -> bool: ...
