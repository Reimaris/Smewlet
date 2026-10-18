"""Plex/Jellyfin refresh client tests (m79-02), HTTP mocked via httpx.MockTransport."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from app.services.delivery import refresh
from app.services.delivery.config import DeliveryConfig

PLEX = "http://plex.example.com:32400"
JF = "http://jellyfin.example.com:8096"

SECTIONS = {
    "MediaContainer": {
        "Directory": [
            {"key": "1", "Location": [{"path": "/media/movies"}]},
            {"key": "2", "Location": [{"path": "/media/anime"}]},
        ]
    }
}


def _cfg(**kw: object) -> DeliveryConfig:
    base: dict[str, object] = {
        "movies_path": "/data/media/movies",
        "anime_path": "/data/media/anime",
        "scan_path_map_local": "/data/media",
        "scan_path_map_remote": "/media",
    }
    return DeliveryConfig(**{**base, **kw})  # type: ignore[arg-type]


def _plex_cfg(**kw: object) -> DeliveryConfig:
    return _cfg(
        scan_server="plex", scan_plex_url=PLEX, scan_plex_token="test-plex-token", **kw
    )


def _jf_cfg(**kw: object) -> DeliveryConfig:
    return _cfg(
        scan_server="jellyfin",
        scan_jellyfin_url=JF,
        scan_jellyfin_api_key="test-jf-key",
        **kw,
    )


@pytest.fixture
def requests(monkeypatch: pytest.MonkeyPatch) -> Callable[..., list[httpx.Request]]:
    """Install a mock transport; returns ``install(handler)`` -> recorded requests."""

    def install(handler: Callable[[httpx.Request], httpx.Response]):
        seen: list[httpx.Request] = []

        def wrapped(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return handler(request)

        monkeypatch.setattr(
            refresh,
            "_client",
            lambda timeout: httpx.AsyncClient(
                transport=httpx.MockTransport(wrapped), timeout=timeout
            ),
        )
        return seen

    return install


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (
            "/data/media/anime/Example Anime (2020)",
            "/media/anime/Example Anime (2020)",
        ),
        ("/data/mediax/anime", "/data/mediax/anime"),
        ("/data/media", "/media"),
        ("/other/x", "/other/x"),
    ],
)
def test_map_scan_path(path: str, expected: str) -> None:
    assert refresh.map_scan_path(path, _cfg()) == expected


def test_map_scan_path_noop_without_map() -> None:
    cfg = DeliveryConfig()
    assert refresh.map_scan_path("/data/media/x", cfg) == "/data/media/x"


@pytest.mark.parametrize(
    ("dest", "expected"),
    [
        (
            "/data/media/movies/Example (2020)/Example (2020).mkv",
            Path("/data/media/movies/Example (2020)"),
        ),
        ("/data/media/anime/Show/Season 01/e.mkv", Path("/data/media/anime/Show")),
        ("/data/media/movies/file.mkv", None),
        ("/data/media/moviesx/T/f.mkv", None),
        ("/elsewhere/T/f.mkv", None),
    ],
)
def test_title_folder(dest: str, expected: Path | None) -> None:
    assert refresh.title_folder(dest, _cfg()) == expected


async def test_unconfigured_servers_make_no_request(requests) -> None:
    seen = requests(lambda r: httpx.Response(200))
    assert await refresh.refresh_media_servers(DeliveryConfig(), [Path("/x/y")]) == []
    assert await refresh.test_media_servers(DeliveryConfig()) == []
    assert seen == []


async def test_plex_targeted_success(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/library/sections":
            return httpx.Response(200, json=SECTIONS)
        return httpx.Response(200)

    seen = requests(handler)
    res = await refresh.refresh_media_servers(
        _plex_cfg(), [Path("/data/media/anime/Example Anime (2020)")]
    )
    assert [(x.server, x.ok, x.mode) for x in res] == [("plex", True, "targeted")]
    last = seen[-1]
    assert last.url.path == "/library/sections/2/refresh"
    assert last.url.params["path"] == "/media/anime/Example Anime (2020)"
    assert last.headers["X-Plex-Token"] == "test-plex-token"
    assert "test-plex-token" not in str(last.url)
    assert last.headers["User-Agent"]


async def test_plex_fetches_sections_once_per_burst(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/library/sections":
            return httpx.Response(200, json=SECTIONS)
        return httpx.Response(200)

    seen = requests(handler)
    res = await refresh.refresh_media_servers(
        _plex_cfg(),
        [Path("/data/media/anime/A (2020)"), Path("/data/media/anime/B (2021)")],
    )
    assert res[0].ok and res[0].mode == "targeted"
    assert [r.url.path for r in seen].count("/library/sections") == 1


async def test_refresh_logs_the_mode_that_ran(requests, caplog) -> None:
    requests(lambda r: httpx.Response(204))
    with caplog.at_level("INFO", logger=refresh.logger.name):
        await refresh.refresh_media_servers(_jf_cfg(), [Path("/data/media/anime/A")])
    assert "jellyfin library refresh: targeted (1 folder(s))" in caplog.text


async def test_test_results_carry_own_latency(requests) -> None:
    requests(lambda r: httpx.Response(200, json=SECTIONS))
    res = await refresh.test_media_servers(_plex_cfg())
    assert res[0].latency_ms >= 0


async def test_plex_no_matching_section_falls_back_to_full(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/library/sections":
            return httpx.Response(200, json=SECTIONS)
        return httpx.Response(200)

    seen = requests(handler)
    res = await refresh.refresh_media_servers(_plex_cfg(), [Path("/data/media/tv/X")])
    assert res[0].ok and res[0].mode == "full"
    refreshes = [r for r in seen if r.url.path.endswith("/refresh")]
    assert [r.url.path for r in refreshes] == ["/library/sections/all/refresh"]


async def test_plex_targeted_failure_falls_back_to_full(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/library/sections":
            return httpx.Response(200, json=SECTIONS)
        if r.url.path == "/library/sections/all/refresh":
            return httpx.Response(200)
        return httpx.Response(500)

    requests(handler)
    res = await refresh.refresh_media_servers(
        _plex_cfg(), [Path("/data/media/movies/M (2020)")]
    )
    assert res[0].ok and res[0].mode == "full"


async def test_plex_401_reports_short_detail(requests) -> None:
    requests(lambda r: httpx.Response(401))
    res = await refresh.refresh_media_servers(
        _plex_cfg(), [Path("/data/media/movies/M")]
    )
    assert not res[0].ok
    assert res[0].detail == "HTTP 401 Unauthorized"
    assert "test-plex-token" not in res[0].detail


async def test_plex_test_connection(requests) -> None:
    seen = requests(lambda r: httpx.Response(200, json=SECTIONS))
    res = await refresh.test_media_servers(_plex_cfg())
    assert [(x.server, x.ok, x.mode) for x in res] == [("plex", True, "test")]
    assert [r.url.path for r in seen] == ["/library/sections"]


async def test_jellyfin_single_updates_request(requests) -> None:
    seen = requests(lambda r: httpx.Response(204))
    res = await refresh.refresh_media_servers(
        _jf_cfg(),
        [Path("/data/media/anime/A"), Path("/data/media/movies/M (2020)")],
    )
    assert [(x.server, x.ok, x.mode) for x in res] == [("jellyfin", True, "targeted")]
    posts = [r for r in seen if r.method == "POST"]
    assert len(posts) == 1
    req = posts[0]
    assert (req.method, req.url.path) == ("POST", "/Library/Media/Updated")
    assert req.headers["Authorization"] == 'MediaBrowser Token="test-jf-key"'
    assert json.loads(req.content) == {
        "Updates": [
            {"Path": "/media/anime/A", "UpdateType": "Created"},
            {"Path": "/media/movies/M (2020)", "UpdateType": "Created"},
        ]
    }


async def test_jellyfin_falls_back_to_full_refresh(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/Library/Refresh":
            return httpx.Response(204)
        return httpx.Response(404)

    seen = requests(handler)
    res = await refresh.refresh_media_servers(_jf_cfg(), [Path("/data/media/anime/A")])
    assert res[0].ok and res[0].mode == "full"
    posts = [r.url.path for r in seen if r.method == "POST"]
    assert posts == ["/Library/Media/Updated", "/Library/Refresh"]


def _jf_library_handler(
    counts: dict[str, int], *, folders_status: int = 200
) -> Callable[[httpx.Request], httpx.Response]:
    """Jellyfin with libraries ``/media/<name>`` whose item counts are ``counts``."""

    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/Library/VirtualFolders":
            body = [
                {"ItemId": name, "Locations": [f"/media/{name}"]} for name in counts
            ]
            return httpx.Response(folders_status, json=body)
        if r.url.path == "/Items":
            total = counts[r.url.params["ParentId"]]
            return httpx.Response(200, json={"TotalRecordCount": total})
        return httpx.Response(204)

    return handler


def _posts(seen: list[httpx.Request]) -> list[str]:
    return [r.url.path for r in seen if r.method == "POST"]


async def test_jellyfin_empty_library_uses_full_refresh(requests) -> None:
    seen = requests(_jf_library_handler({"movies": 0, "anime": 5}))
    res = await refresh.refresh_media_servers(
        _jf_cfg(), [Path("/data/media/movies/M (2020)")]
    )
    assert [(x.server, x.ok, x.mode) for x in res] == [("jellyfin", True, "full")]
    assert _posts(seen) == ["/Library/Refresh"]  # targeted call would be ignored


async def test_jellyfin_populated_library_stays_targeted(requests) -> None:
    seen = requests(_jf_library_handler({"movies": 3, "anime": 0}))
    res = await refresh.refresh_media_servers(
        _jf_cfg(), [Path("/data/media/movies/M (2020)")]
    )
    assert res[0].mode == "targeted"  # the empty "anime" library is not involved
    assert _posts(seen) == ["/Library/Media/Updated"]


async def test_jellyfin_any_empty_target_library_forces_full_refresh(requests) -> None:
    seen = requests(_jf_library_handler({"movies": 3, "anime": 0}))
    res = await refresh.refresh_media_servers(
        _jf_cfg(),
        [Path("/data/media/movies/M (2020)"), Path("/data/media/anime/A")],
    )
    assert res[0].mode == "full"
    assert _posts(seen) == ["/Library/Refresh"]


async def test_jellyfin_empty_check_failure_stays_targeted(requests) -> None:
    seen = requests(_jf_library_handler({"movies": 0}, folders_status=403))
    res = await refresh.refresh_media_servers(
        _jf_cfg(), [Path("/data/media/movies/M (2020)")]
    )
    assert res[0].mode == "targeted"
    assert _posts(seen) == ["/Library/Media/Updated"]


async def test_jellyfin_timeout_never_raises(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=r)

    requests(handler)
    res = await refresh.refresh_media_servers(_jf_cfg(), [Path("/data/media/anime/A")])
    assert not res[0].ok
    assert res[0].detail == "timed out"


async def test_connection_failure_detail(requests) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom test-jf-key", request=r)

    requests(handler)
    res = await refresh.test_media_servers(_jf_cfg())
    assert not res[0].ok
    assert res[0].detail == "connection failed"


def _jf_folders(*paths: str) -> list[dict[str, list[str]]]:
    return [{"Locations": [p]} for p in paths]


def _jf_handler(folders: object, status: int = 200) -> Callable[..., httpx.Response]:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/Library/VirtualFolders":
            return httpx.Response(status, json=folders)
        return httpx.Response(200, json={})

    return handler


async def test_jellyfin_test_accepts_mapped_roots_inside_a_library(requests) -> None:
    requests(_jf_handler(_jf_folders("/media/movies", "/media/anime")))
    res = await refresh.test_media_servers(_jf_cfg())
    assert res[0].ok and res[0].detail == ""


async def test_jellyfin_test_flags_a_path_map_no_library_matches(requests) -> None:
    requests(_jf_handler(_jf_folders("/media/movies", "/media/anime")))
    res = await refresh.test_media_servers(_jf_cfg(scan_path_map_remote="/wrong"))
    assert not res[0].ok
    assert "No library contains /wrong/movies" in res[0].detail
    assert "path map" in res[0].detail


@pytest.mark.parametrize(("folders", "status"), [([], 200), (None, 403), ({}, 200)])
async def test_jellyfin_test_skips_the_library_check_when_unavailable(
    requests, folders: object, status: int
) -> None:
    requests(_jf_handler(folders, status))
    res = await refresh.test_media_servers(_jf_cfg(scan_path_map_remote="/wrong"))
    assert res[0].ok


async def test_plex_test_flags_a_path_map_no_section_matches(requests) -> None:
    requests(lambda r: httpx.Response(200, json=SECTIONS))
    res = await refresh.test_media_servers(_plex_cfg(scan_path_map_remote="/wrong"))
    assert not res[0].ok and "No library contains" in res[0].detail


async def test_jellyfin_test_connection(requests) -> None:
    seen = requests(lambda r: httpx.Response(200, json={}))
    res = await refresh.test_media_servers(_jf_cfg())
    assert res[0].ok and res[0].mode == "test"
    assert seen[0].url.path == "/System/Info"


async def test_only_the_selected_server_is_refreshed_and_tested(requests) -> None:
    seen = requests(lambda r: httpx.Response(200, json=SECTIONS))
    cfg = _plex_cfg(scan_jellyfin_url=JF, scan_jellyfin_api_key="test-jf-key")
    assert cfg.jellyfin_configured is False
    results = await refresh.test_media_servers(cfg)
    assert [r.server for r in results] == ["plex"]
    assert {r.url.host for r in seen} == {httpx.URL(PLEX).host}
