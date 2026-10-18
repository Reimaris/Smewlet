"""Best-effort Plex/Jellyfin library refresh after delivery (ADR-172).

Endpoints are based on forum/SDK sources and are unverified against live servers;
they live in module constants so they are easy to adjust.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx

from app.config import DEFAULT_USER_AGENT
from app.services.delivery.config import DeliveryConfig

logger = logging.getLogger(__name__)

REFRESH_TIMEOUT = 10.0
TEST_TIMEOUT = 5.0

PLEX_SECTIONS_PATH = "/library/sections"
PLEX_SECTION_REFRESH_PATH = "/library/sections/{key}/refresh"
PLEX_FULL_REFRESH_PATH = "/library/sections/all/refresh"
JELLYFIN_INFO_PATH = "/System/Info"
JELLYFIN_UPDATED_PATH = "/Library/Media/Updated"
JELLYFIN_REFRESH_PATH = "/Library/Refresh"


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of one server call; ``mode`` is ``targeted``, ``full`` or ``test``."""

    server: str
    ok: bool
    mode: str
    detail: str
    latency_ms: int = 0


def _is_under(path: str, root: str) -> bool:
    """True when ``path`` equals ``root`` or lies below it (component boundary)."""
    root = root.rstrip("/")
    return path == root or path.startswith(root + "/")


def map_scan_path(path: str, cfg: DeliveryConfig) -> str:
    """Translate a local path into the media server's view of it."""
    local = cfg.scan_path_map_local.rstrip("/")
    remote = cfg.scan_path_map_remote.rstrip("/")
    if not local or not remote or not _is_under(path, local):
        return path
    return remote + path[len(local) :]


def title_folder(dest_path: str, cfg: DeliveryConfig) -> Path | None:
    """First folder below the configured root that contains ``dest_path``."""
    roots = (
        cfg.movies_path,
        cfg.shows_path,
        cfg.anime_path,
        cfg.anime_movies_path,
    )
    for root in roots:
        if not root:
            continue
        root_path = Path(root)
        try:
            rel = Path(dest_path).relative_to(root_path)
        except ValueError:
            continue
        if len(rel.parts) >= 2:  # title folder + at least the file
            return root_path / rel.parts[0]
    return None


def _client(timeout: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    )


def _elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def _error_detail(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        try:
            return f"HTTP {code} {HTTPStatus(code).phrase}"
        except ValueError:
            return f"HTTP {code}"
    if isinstance(exc, httpx.TimeoutException):
        return "timed out"
    return "connection failed"


async def _call(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> httpx.Response:
    resp = await client.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


# --- Plex -----------------------------------------------------------------


def _plex_headers(cfg: DeliveryConfig) -> dict[str, str]:
    return {"X-Plex-Token": cfg.scan_plex_token or "", "Accept": "application/json"}


def _plex_section_for(sections: list[dict[str, Any]], folder: str) -> str | None:
    best_key: str | None = None
    best_len = -1
    for section in sections:
        for loc in section.get("Location", []):
            root = str(loc.get("path") or "").rstrip("/")
            if root and _is_under(folder, root) and len(root) > best_len:
                best_key, best_len = str(section.get("key")), len(root)
    return best_key


async def _plex_refresh(cfg: DeliveryConfig, folders: list[str]) -> RefreshResult:
    base = cfg.scan_plex_url or ""
    headers = _plex_headers(cfg)
    try:
        async with _client(REFRESH_TIMEOUT) as client:
            try:
                sections: list[dict[str, Any]] = []
                if folders:
                    resp = await _call(
                        client, "GET", base + PLEX_SECTIONS_PATH, headers=headers
                    )
                    sections = (
                        resp.json().get("MediaContainer", {}).get("Directory", [])
                    )
                for folder in folders:
                    key = _plex_section_for(sections, folder)
                    if key is None:
                        raise LookupError(folder)
                    await _call(
                        client,
                        "GET",
                        base + PLEX_SECTION_REFRESH_PATH.format(key=key),
                        headers=headers,
                        params={"path": folder},
                    )
                if folders:
                    return RefreshResult("plex", True, "targeted", "")
            except (httpx.HTTPError, LookupError, ValueError, AttributeError):
                pass
            await _call(client, "GET", base + PLEX_FULL_REFRESH_PATH, headers=headers)
            return RefreshResult("plex", True, "full", "")
    except Exception as exc:  # noqa: BLE001 - best-effort, never raise
        return RefreshResult("plex", False, "full", _error_detail(exc))


async def _plex_test(cfg: DeliveryConfig) -> RefreshResult:
    start = time.perf_counter()
    try:
        async with _client(TEST_TIMEOUT) as client:
            await _call(
                client,
                "GET",
                (cfg.scan_plex_url or "") + PLEX_SECTIONS_PATH,
                headers=_plex_headers(cfg),
            )
        return RefreshResult("plex", True, "test", "", _elapsed_ms(start))
    except Exception as exc:  # noqa: BLE001
        return RefreshResult(
            "plex", False, "test", _error_detail(exc), _elapsed_ms(start)
        )


# --- Jellyfin -------------------------------------------------------------


def _jellyfin_headers(cfg: DeliveryConfig) -> dict[str, str]:
    return {"Authorization": f'MediaBrowser Token="{cfg.scan_jellyfin_api_key}"'}


async def _jellyfin_refresh(cfg: DeliveryConfig, folders: list[str]) -> RefreshResult:
    base = cfg.scan_jellyfin_url or ""
    headers = _jellyfin_headers(cfg)
    try:
        async with _client(REFRESH_TIMEOUT) as client:
            try:
                if folders:
                    await _call(
                        client,
                        "POST",
                        base + JELLYFIN_UPDATED_PATH,
                        headers=headers,
                        json={
                            "Updates": [
                                {"Path": f, "UpdateType": "Created"} for f in folders
                            ]
                        },
                    )
                    return RefreshResult("jellyfin", True, "targeted", "")
            except httpx.HTTPError:
                pass
            await _call(client, "POST", base + JELLYFIN_REFRESH_PATH, headers=headers)
            return RefreshResult("jellyfin", True, "full", "")
    except Exception as exc:  # noqa: BLE001
        return RefreshResult("jellyfin", False, "full", _error_detail(exc))


async def _jellyfin_test(cfg: DeliveryConfig) -> RefreshResult:
    start = time.perf_counter()
    try:
        async with _client(TEST_TIMEOUT) as client:
            await _call(
                client,
                "GET",
                (cfg.scan_jellyfin_url or "") + JELLYFIN_INFO_PATH,
                headers=_jellyfin_headers(cfg),
            )
        return RefreshResult("jellyfin", True, "test", "", _elapsed_ms(start))
    except Exception as exc:  # noqa: BLE001
        return RefreshResult(
            "jellyfin", False, "test", _error_detail(exc), _elapsed_ms(start)
        )


# --- Public API -----------------------------------------------------------


async def refresh_media_servers(
    cfg: DeliveryConfig, folders: Sequence[Path]
) -> list[RefreshResult]:
    """Refresh every configured server for the delivered folders; never raises."""
    mapped = list(dict.fromkeys(map_scan_path(str(f), cfg) for f in folders))
    results: list[RefreshResult] = []
    if cfg.plex_configured:
        results.append(await _plex_refresh(cfg, mapped))
    if cfg.jellyfin_configured:
        results.append(await _jellyfin_refresh(cfg, mapped))
    for r in results:
        if r.ok:
            logger.info(
                "%s library refresh: %s (%d folder(s))", r.server, r.mode, len(mapped)
            )
        else:
            logger.warning("%s library refresh failed: %s", r.server, r.detail)
    return results


async def test_media_servers(cfg: DeliveryConfig) -> list[RefreshResult]:
    """Probe every configured server; never raises."""
    results: list[RefreshResult] = []
    if cfg.plex_configured:
        results.append(await _plex_test(cfg))
    if cfg.jellyfin_configured:
        results.append(await _jellyfin_test(cfg))
    return results


test_media_servers.__test__ = False  # type: ignore[attr-defined]  # not a pytest test
