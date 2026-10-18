from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import select

from app.core.fake_detector import (
    extract_nzb_password,
    is_indexer_metadata_fake,
    is_nzb_content_fake,
)
from app.core.push_engine import _score_and_partition_candidates
from app.db.models import BlacklistedRelease
from app.services.downloaders.base import DownloaderFile
from tests.conftest import make_movie


@pytest.mark.parametrize(
    "raw_item,parsed_res,media_type,is_season_pack,expected_fake,expected_reason_substr",
    [
        # Password flagged
        (
            {"title": "Test", "size": 1000 * 1024 * 1024, "password": 1},
            None,
            "movie",
            False,
            True,
            "password",
        ),
        (
            {"title": "Test", "size": 1000 * 1024 * 1024, "password": "1"},
            None,
            "movie",
            False,
            True,
            "password",
        ),
        (
            {
                "title": "Test",
                "size": 1000 * 1024 * 1024,
                "passwordurl": "http://pass.fake",
            },
            None,
            "movie",
            False,
            True,
            "password URL",
        ),
        # Movie size bounds (< 300MB)
        (
            {"title": "Test Movie", "size": 250 * 1024 * 1024},
            None,
            "movie",
            False,
            True,
            "300MB",
        ),
        (
            {"title": "Test Movie", "size": 350 * 1024 * 1024},
            None,
            "movie",
            False,
            False,
            None,
        ),
        # 4K Movie size bounds (< 1.5GB for 2160p)
        (
            {"title": "Test 4K Movie", "size": 1200 * 1024 * 1024},
            "2160p",
            "movie",
            False,
            True,
            "1.5GB",
        ),
        (
            {"title": "Test 4K Movie", "size": 2000 * 1024 * 1024},
            "2160p",
            "movie",
            False,
            False,
            None,
        ),
        # 1080p Movie with 1GB passes (not 4K)
        (
            {"title": "Test 1080p Movie", "size": 1000 * 1024 * 1024},
            "1080p",
            "movie",
            False,
            False,
            None,
        ),
        # Episode bounds (< 50MB)
        (
            {"title": "Test Episode", "size": 40 * 1024 * 1024},
            None,
            "episode",
            False,
            True,
            "50MB",
        ),
        # Anime short (70MB) passes
        (
            {"title": "Test Anime Short", "size": 70 * 1024 * 1024},
            None,
            "anime",
            False,
            False,
            None,
        ),
        # Season Pack (200MB) passes episode bound even if under 300MB movie bound
        (
            {"title": "Test Season Pack", "size": 200 * 1024 * 1024},
            None,
            "show",
            True,
            False,
            None,
        ),
        # Huge UHD REMUX (85GB) passes (no upper ceiling)
        (
            {"title": "Huge UHD REMUX", "size": 85 * 1024 * 1024 * 1024},
            "2160p",
            "movie",
            False,
            False,
            None,
        ),
        # Missing or zero size skips size check
        ({"title": "Zero Size NZB", "size": 0}, None, "movie", False, False, None),
        ({"title": "No Size Field"}, None, "movie", False, False, None),
    ],
)
def test_is_indexer_metadata_fake(
    raw_item: dict[str, Any],
    parsed_res: str | None,
    media_type: str,
    is_season_pack: bool,
    expected_fake: bool,
    expected_reason_substr: str | None,
) -> None:
    is_fake, reason = is_indexer_metadata_fake(
        raw_item=raw_item,
        parsed_resolution=parsed_res,
        media_type=media_type,
        is_season_pack=is_season_pack,
    )
    assert is_fake is expected_fake
    if expected_fake and expected_reason_substr:
        assert reason is not None
        assert expected_reason_substr.lower() in reason.lower()
    elif not expected_fake:
        assert reason is None


@pytest.mark.asyncio
async def test_score_and_partition_candidates_drops_and_blacklists_layer1_fake(
    db_session,
) -> None:
    movie = await make_movie(db_session, title="Inception", year=2010)

    raw_results = [
        # Candidate 1: Password protected
        {
            "title": "Inception.2010.1080p.BluRay.x264-TEST",
            "guid": "guid-fake-pwd",
            "size": 5 * 1024 * 1024 * 1024,
            "password": 1,
        },
        # Candidate 2: Under-sized fake (150MB movie)
        {
            "title": "Inception.2010.720p.HDTV.x264-TINY",
            "guid": "guid-fake-tiny",
            "size": 150 * 1024 * 1024,
        },
        # Candidate 3: Valid 4GB movie release
        {
            "title": "Inception.2010.1080p.BluRay.DTS.x264-REAL",
            "guid": "guid-real-inception",
            "size": 4 * 1024 * 1024 * 1024,
        },
    ]

    blacklisted_guids: set[str] = set()
    blacklisted_titles: set[str] = set()

    partitioned = await _score_and_partition_candidates(
        raw_results=raw_results,
        blacklisted_guids=blacklisted_guids,
        blacklisted_titles=blacklisted_titles,
        expected_title=movie.title,
        expected_year=movie.year,
        expected_alt_title=None,
        expected_season=None,
        expected_episode=None,
        expected_season_title=None,
        runtime_minutes=movie.runtime_minutes,
        effective_cfg={"primary_language": "en", "video_quality_mode": "best"},
        media_type="movie",
        session=db_session,
        item_id=movie.id,
    )

    # Valid candidate should be returned
    assert len(partitioned["all_valid"]) == 1
    assert partitioned["all_valid"][0]["guid"] == "guid-real-inception"

    # In-memory blacklists should have recorded the dropped fakes
    assert "guid-fake-pwd" in blacklisted_guids
    assert "guid-fake-tiny" in blacklisted_guids

    # Database BlacklistedRelease table should persist records
    res = await db_session.execute(
        select(BlacklistedRelease).where(BlacklistedRelease.media_item_id == movie.id)
    )
    bl_rows = res.scalars().all()
    assert len(bl_rows) == 2
    bl_guids = {r.nzb_guid for r in bl_rows}
    assert "guid-fake-pwd" in bl_guids
    assert "guid-fake-tiny" in bl_guids
    for r in bl_rows:
        assert r.reason is not None and "Layer 1 Fake" in r.reason


@pytest.mark.asyncio
async def test_treasure_maps_extracts_password_and_passwordurl(monkeypatch) -> None:
    import httpx

    from app.services.indexers.newznab import _execute_search

    fake_xml_json = {
        "channel": {
            "item": [
                {
                    "title": "Password.Protected.Release.1080p",
                    "guid": "http://indexer.com/details/12345",
                    "attr": [
                        {"@attributes": {"name": "size", "value": "1500000000"}},
                        {"@attributes": {"name": "guid", "value": "hash123"}},
                        {"@attributes": {"name": "password", "value": "1"}},
                        {
                            "@attributes": {
                                "name": "passwordurl",
                                "value": "https://passwords-r-us.fake",
                            }
                        },
                    ],
                },
                {
                    "title": "Clean.Release.1080p",
                    "guid": "http://indexer.com/details/67890",
                    "attr": [
                        {"@attributes": {"name": "size", "value": "2500000000"}},
                        {"@attributes": {"name": "guid", "value": "hash456"}},
                    ],
                },
            ]
        }
    }

    class MockResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        def raise_for_status(self):
            pass

        def json(self):
            return fake_xml_json

    class MockAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None):
            return MockResponse()

    monkeypatch.setattr(httpx, "AsyncClient", MockAsyncClient)

    items = await _execute_search("http://fake-indexer.com/api", {})
    assert len(items) == 2

    # Item 1 has password and passwordurl extracted
    assert items[0]["password"] == 1
    assert items[0]["passwordurl"] == "https://passwords-r-us.fake"

    # Item 2 has defaults (0 and "")
    assert items[1]["password"] == 0
    assert items[1]["passwordurl"] == ""


@pytest.mark.asyncio
async def test_manual_search_drops_and_globally_blacklists_layer1_fake(
    async_client, db_session, monkeypatch
) -> None:
    from app.services.indexers import newznab as newznab_client

    mock_raw_results = [
        {
            "title": "Inception.2010.1080p.BluRay.x264-FAKEMOVIE",
            "guid": "guid-manual-fake-pwd",
            "link": "http://indexer.com/fake-pwd.nzb",
            "size": 3 * 1024 * 1024 * 1024,
            "password": 1,
        },
        {
            "title": "Inception.2010.1080p.BluRay.x264-VALID",
            "guid": "guid-manual-valid",
            "link": "http://indexer.com/valid.nzb",
            "size": 4 * 1024 * 1024 * 1024,
            "password": 0,
        },
    ]

    async def mock_search_movie(*args, **kwargs):
        return mock_raw_results

    monkeypatch.setattr(newznab_client, "search_movie", mock_search_movie)

    resp = await async_client.post(
        "/api/search/manual/releases",
        data={
            "scope": "item",
            "kind": "movie",
            "title": "Inception",
            "drawer_id": "drawer-tmdb-1",
        },
    )
    assert resp.status_code == 200
    html = resp.text

    # Valid release should appear in results
    assert "Inception.2010.1080p.BluRay.x264-VALID" in html
    # Fake release should NOT appear in results
    assert "Inception.2010.1080p.BluRay.x264-FAKEMOVIE" not in html

    # Global blacklist record should have been committed (media_item_id is None)
    res = await db_session.execute(
        select(BlacklistedRelease).where(
            BlacklistedRelease.nzb_guid == "guid-manual-fake-pwd"
        )
    )
    bl_row = res.scalars().first()
    assert bl_row is not None
    assert bl_row.media_item_id is None
    assert "Layer 1 Fake" in (bl_row.reason or "")


# ---------------------------------------------------------------------------
# Layer 2: NZB Content Inspection Tests
# ---------------------------------------------------------------------------


def test_layer2_password_meta_tag_extracted_not_rejected() -> None:

    # NZB with password meta tag in <head>
    nzb_with_pwd = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b"  <head>\n"
        b'    <meta type="password">SecretPass123</meta>\n'
        b"  </head>\n"
        b'  <file poster="p" date="1700000000" subject="Movie.2024.1080p.mkv">\n'
        b"    <groups><group>alt.binaries.movies</group></groups>\n"
        b'    <segments><segment bytes="500000000" number="1">seg1</segment></segments>\n'
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(nzb_with_pwd, media_type="movie")
    assert is_fake is False
    assert reason is None
    assert extract_nzb_password(nzb_with_pwd) == "SecretPass123"

    # NZB with empty password meta tag should not be flagged and returns None password
    nzb_empty_pwd = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b"  <head>\n"
        b'    <meta type="password"></meta>\n'
        b"  </head>\n"
        b'  <file poster="p" date="1700000000" subject="Movie.2024.1080p.mkv">\n'
        b"    <groups><group>alt.binaries.movies</group></groups>\n"
        b'    <segments><segment bytes="500000000" number="1">seg1</segment></segments>\n'
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(nzb_empty_pwd, media_type="movie")
    assert is_fake is False
    assert reason is None
    assert extract_nzb_password(nzb_empty_pwd) is None


@pytest.mark.parametrize(
    "total_bytes,media_type,is_season_pack,expected_fake,expected_reason_substr",
    [
        # Movie (< 30MB)
        (4 * 1024 * 1024, "movie", False, True, "below movie threshold (30MB)"),
        (29 * 1024 * 1024, "movie", False, True, "below movie threshold (30MB)"),
        (30 * 1024 * 1024, "movie", False, False, None),
        (50 * 1024 * 1024, "movie", False, False, None),
        # Episode (< 10MB)
        (4 * 1024 * 1024, "episode", False, True, "below episode threshold (10MB)"),
        (9 * 1024 * 1024, "episode", False, True, "below episode threshold (10MB)"),
        (10 * 1024 * 1024, "episode", False, False, None),
        (25 * 1024 * 1024, "episode", False, False, None),
        # Anime movie (< 30MB)
        (5 * 1024 * 1024, "anime-movie", False, True, "below movie threshold (30MB)"),
        (35 * 1024 * 1024, "anime-movie", False, False, None),
        # Season Pack (treated as Episode threshold: 10MB, even if media_type == 'show' or 'movie')
        (5 * 1024 * 1024, "show", True, True, "below episode threshold (10MB)"),
        (15 * 1024 * 1024, "show", True, False, None),
        (5 * 1024 * 1024, "movie", True, True, "below episode threshold (10MB)"),
        (15 * 1024 * 1024, "movie", True, False, None),
    ],
)
def test_layer2_segment_payload_size_thresholds(
    total_bytes: int,
    media_type: str,
    is_season_pack: bool,
    expected_fake: bool,
    expected_reason_substr: str | None,
) -> None:
    from app.core.fake_detector import is_nzb_content_fake

    # Construct NZB with specified segment byte size
    nzb = (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        f'  <file poster="p" date="1700000000" subject="Release.Title.1080p.mkv">\n'
        f"    <groups><group>alt.binaries.test</group></groups>\n"
        f'    <segments><segment bytes="{total_bytes}" number="1">seg1</segment></segments>\n'
        f"  </file>\n"
        f"</nzb>"
    ).encode()

    is_fake, reason = is_nzb_content_fake(
        nzb, media_type=media_type, is_season_pack=is_season_pack
    )
    assert is_fake is expected_fake
    if expected_fake and expected_reason_substr:
        assert reason is not None
        assert expected_reason_substr.lower() in reason.lower()
    elif not expected_fake:
        assert reason is None


def test_layer2_valid_multipart_and_obfuscated_releases_pass() -> None:
    from app.core.fake_detector import is_nzb_content_fake

    # Valid multi-part RAR archive with realistic segments (3 segments of 15MB = 45MB)
    multipart_rar_nzb = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b'  <file poster="p" date="1700000000" subject=\'[01/03] - "Movie.2024.1080p.part01.rar" yEnc (1/20)\'>\n'
        b"    <groups><group>alt.binaries.movies</group></groups>\n"
        b'    <segments><segment bytes="15000000" number="1">seg1</segment></segments>\n'
        b"  </file>\n"
        b'  <file poster="p" date="1700000000" subject=\'[02/03] - "Movie.2024.1080p.part02.rar" yEnc (1/20)\'>\n'
        b"    <groups><group>alt.binaries.movies</group></groups>\n"
        b'    <segments><segment bytes="15000000" number="1">seg2</segment></segments>\n'
        b"  </file>\n"
        b'  <file poster="p" date="1700000000" subject=\'[03/03] - "Movie.2024.1080p.par2" yEnc (1/20)\'>\n'
        b"    <groups><group>alt.binaries.movies</group></groups>\n"
        b'    <segments><segment bytes="15000000" number="1">seg3</segment></segments>\n'
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(multipart_rar_nzb, media_type="movie")
    assert is_fake is False
    assert reason is None

    # Valid obfuscated release (random alphanumeric subject, no video extension in subject, but has segments > 30MB)
    obfuscated_nzb = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b'  <file poster="p" date="1700000000" subject="abc123xyz890 (1/50)">\n'
        b"    <groups><group>alt.binaries.boneless</group></groups>\n"
        b"    <segments>\n"
        b'      <segment bytes="20000000" number="1">seg1</segment>\n'
        b'      <segment bytes="20000000" number="2">seg2</segment>\n'
        b"    </segments>\n"
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(obfuscated_nzb, media_type="movie")
    assert is_fake is False
    assert reason is None


# ---------------------------------------------------------------------------
# Layer 3: TorBox Completed Filelist Verification Tests
# ---------------------------------------------------------------------------


def test_layer3_executable_and_archive_rejections() -> None:
    from app.core.fake_detector import is_transfer_filelist_fake

    # Executable rejection (immediate hard reject, is_unextracted_archive is False)
    tb_exe = [
        {"name": "Movie.2024.1080p.mkv", "size": 2000000000},
        {"name": "setup.exe", "size": 500000},
    ]
    is_fake, reason, is_archive = is_transfer_filelist_fake(tb_exe, media_type="movie")
    assert is_fake is True
    assert reason is not None and "executable" in reason.lower()
    assert is_archive is False

    # Executable with archives present still immediately rejects as hard fake
    tb_exe_archive = [
        {"name": "Movie.2024.1080p.part01.rar", "size": 2000000000},
        {"name": "malicious.scr", "size": 50000},
    ]
    is_fake, reason, is_archive = is_transfer_filelist_fake(
        tb_exe_archive, media_type="movie"
    )
    assert is_fake is True
    assert reason is not None and "executable" in reason.lower()
    assert is_archive is False

    # Unextracted RAR-only archive discrimination
    tb_rar = [
        {"name": "Movie.2024.1080p.rar", "size": 2000000000},
        {"name": "Movie.2024.1080p.r00", "size": 2000000000},
    ]
    is_fake, reason, is_archive = is_transfer_filelist_fake(tb_rar, media_type="movie")
    assert is_fake is True
    assert reason == "Unextracted archive payload awaiting extraction"
    assert is_archive is True

    # Multi-part archive patterns (.part1.rar, .7z, .zip, .par2)
    for arch_name in [
        "Movie.part01.rar",
        "Movie.part1.rar",
        "Movie.r01",
        "Movie.7z",
        "Movie.zip",
        "Movie.vol01+02.par2",
    ]:
        is_fake, reason, is_archive = is_transfer_filelist_fake(
            [{"name": arch_name, "size": 100000000}], media_type="movie"
        )
        assert is_fake is True, f"Failed for {arch_name}"
        assert reason == "Unextracted archive payload awaiting extraction"
        assert is_archive is True

    # Unextracted archive with sample video clip
    tb_rar_sample = [
        {"name": "Movie.2024.1080p.sample.mkv", "size": 25000000},
        {"name": "Movie.2024.1080p.part01.rar", "size": 2000000000},
        {"name": "Movie.2024.1080p.part02.rar", "size": 2000000000},
    ]
    is_fake, reason, is_archive = is_transfer_filelist_fake(
        tb_rar_sample, media_type="movie"
    )
    assert is_fake is True
    assert reason == "Unextracted archive payload awaiting extraction"
    assert is_archive is True

    # Sample-only video rejection (no archives, no playable main video)
    tb_sample = [{"name": "Movie.2024.1080p.sample.mkv", "size": 25000000}]
    is_fake, reason, is_archive = is_transfer_filelist_fake(
        tb_sample, media_type="movie"
    )
    assert is_fake is True
    assert reason == "Downloaded payload contains only sample video file(s)"
    assert is_archive is False

    # Empty list
    is_fake, reason, is_archive = is_transfer_filelist_fake([])
    assert is_fake is True
    assert reason == "Downloader returned empty file list"
    assert is_archive is False


@pytest.mark.parametrize(
    "tb_files,media_type,is_season_pack,expected_fake,expected_reason_substr",
    [
        # 500KB fake video loop for movie (< 150MB)
        (
            [{"name": "Movie.2024.1080p.mkv", "size": 500 * 1024}],
            "movie",
            False,
            True,
            "unrealistically small",
        ),
        # 500KB fake video loop using 'bytes' attribute
        (
            [{"name": "Movie.2024.1080p.mkv", "bytes": 500 * 1024}],
            "movie",
            False,
            True,
            "unrealistically small",
        ),
        # 500KB fake video loop in nested folder structure
        (
            [
                {
                    "name": "ReleaseFolder",
                    "files": [{"name": "Movie.2024.1080p.mkv", "size": 500 * 1024}],
                }
            ],
            "movie",
            False,
            True,
            "unrealistically small",
        ),
        # 500KB fake video loop for episode (< 25MB)
        (
            [{"name": "Show.S01E01.1080p.mkv", "size": 500 * 1024}],
            "episode",
            False,
            True,
            "unrealistically small",
        ),
        # 140MB movie video (< 150MB) -> Fake
        (
            [{"name": "Movie.2024.720p.mkv", "size": 140 * 1024 * 1024}],
            "movie",
            False,
            True,
            "unrealistically small",
        ),
        # 150MB movie video (>= 150MB) -> Valid
        (
            [{"name": "Movie.2024.720p.mkv", "size": 150 * 1024 * 1024}],
            "movie",
            False,
            False,
            None,
        ),
        # 1.5GB movie video -> Valid
        (
            [{"name": "Movie.2024.1080p.mkv", "size": int(1.5 * 1024 * 1024 * 1024)}],
            "movie",
            False,
            False,
            None,
        ),
        # 20MB episode video (< 25MB) -> Fake
        (
            [{"name": "Show.S01E01.720p.mkv", "size": 20 * 1024 * 1024}],
            "episode",
            False,
            True,
            "unrealistically small",
        ),
        # 30MB anime short episode video (>= 25MB) -> Valid
        (
            [{"name": "AnimeShort.E01.1080p.mkv", "size": 30 * 1024 * 1024}],
            "anime",
            False,
            False,
            None,
        ),
        # Season pack with 30MB episode video (treated under episode 25MB threshold) -> Valid
        (
            [
                {
                    "name": "Show.S01.720p",
                    "files": [{"name": "Show.S01E01.mkv", "size": 30 * 1024 * 1024}],
                }
            ],
            "show",
            True,
            False,
            None,
        ),
        # Sample video (15MB) + Full movie video (2GB) -> Max size is 2GB -> Valid
        (
            [
                {"name": "sample.mkv", "size": 15 * 1024 * 1024},
                {"name": "Movie.2024.1080p.mkv", "size": 2 * 1024 * 1024 * 1024},
            ],
            "movie",
            False,
            False,
            None,
        ),
        # Missing size (size=0 or absent) preserves validity (doesn't falsely reject)
        ([{"name": "Movie.2024.1080p.mkv"}], "movie", False, False, None),
        ([{"name": "Movie.2024.1080p.mkv", "size": 0}], "movie", False, False, None),
    ],
)
def test_layer3_video_size_thresholds(
    tb_files: list[Any],
    media_type: str,
    is_season_pack: bool,
    expected_fake: bool,
    expected_reason_substr: str | None,
) -> None:
    from app.core.fake_detector import is_transfer_filelist_fake

    is_fake, reason, is_archive = is_transfer_filelist_fake(
        tb_files, media_type=media_type, is_season_pack=is_season_pack
    )
    assert is_fake is expected_fake
    assert is_archive is False
    if expected_fake and expected_reason_substr:
        assert reason is not None
        assert expected_reason_substr.lower() in reason.lower()
    elif not expected_fake:
        assert reason is None


@pytest.mark.asyncio
async def test_transfer_poller_rejects_layer3_undersized_video_and_replaces(
    db_session, monkeypatch
) -> None:
    """Integration test verifying that when a completed TorBox transfer contains an undersized video (< threshold),
    transfer_poller Tick-1 rejects it with reason 'Layer 3 Fake Detection: Downloaded video payload is unrealistically small (< threshold)',
    deletes it from TorBox, persists BlacklistedRelease, and auto-replaces.
    """
    from unittest.mock import AsyncMock, patch

    from app.core.transfer_poller import run_transfer_poller_tick
    from app.db.models import DownloadHistory, MediaStatus
    from tests.conftest import VALID_NZB_BYTES, make_movie

    movie = await make_movie(
        db_session, title="Oppenheimer", year=2023, status=MediaStatus.DOWNLOADING
    )

    history = DownloadHistory(
        media_item_id=movie.id,
        nzb_title="Oppenheimer.2023.1080p.BluRay.x264-FAKESTUB",
        nzb_guid="guid-opp-fake-stub",
        downloader_transfer_id=99001,
        score=7500.0,
        size_bytes=5 * 1024 * 1024 * 1024,
        status_detail="downloading",
        is_dismissed=False,
        push_mode="auto",
    )
    db_session.add(history)
    await db_session.commit()

    # TorBox snapshot reporting download_finished with a 500KB fake .mkv
    stub_snapshot = [
        {
            "id": 99001,
            "name": "Oppenheimer.2023.1080p.BluRay.x264-FAKESTUB",
            "download_state": "completed",
            "download_finished": True,
            "progress": 1.0,
            "download_speed": 0,
            "eta": 0,
            "files": [
                {
                    "name": "Oppenheimer.2023.1080p.BluRay.x264-FAKESTUB.mkv",
                    "size": 500 * 1024,  # 500KB fake stub video
                },
                {
                    "name": "instructions.nfo",
                    "size": 2048,
                },
            ],
        }
    ]

    replacement_candidates = [
        {
            "title": "Oppenheimer.2023.1080p.BluRay.DTS.x264-LEGIT",
            "guid": "guid-opp-legit",
            "link": "https://indexer.org/oppenheimer-legit.nzb",
            "size": 8 * 1024 * 1024 * 1024,
        }
    ]

    with (
        patch(
            "app.services.downloaders.torbox.delete_usenet_download",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock_del_tb,
        patch(
            "app.services.indexers.newznab.search_movie",
            new_callable=AsyncMock,
            return_value=replacement_candidates,
        ),
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new_callable=AsyncMock,
            return_value=(VALID_NZB_BYTES, "oppenheimer.nzb"),
        ),
        patch(
            "app.services.downloaders.torbox.send_nzb_file",
            new_callable=AsyncMock,
            return_value={"id": 99002, "hash": "hash-opp-legit"},
        ) as mock_send_repl,
    ):
        await run_transfer_poller_tick(db_session, pre_fetched_downloads=stub_snapshot)
        mock_del_tb.assert_awaited_once_with(99001, session=db_session)
        mock_send_repl.assert_awaited_once()

    # Blacklist record should have been committed with expected reason
    res = await db_session.execute(
        select(BlacklistedRelease).where(BlacklistedRelease.media_item_id == movie.id)
    )
    bl_rows = res.scalars().all()
    assert any(
        b.nzb_guid == "guid-opp-fake-stub"
        and b.reason
        == "Layer 3 Fake Detection: Downloaded video payload is unrealistically small (< threshold)"
        for b in bl_rows
    )


# ---------------------------------------------------------------------------
# Milestone 42: Password Extraction & Falsey Meta Tag Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_xml,expected_password",
    [
        # Falsey / No password tags
        (b'<nzb><head><meta type="password">0</meta></head></nzb>', None),
        (b'<nzb><head><meta type="password">no</meta></head></nzb>', None),
        (b'<nzb><head><meta type="password">none</meta></head></nzb>', None),
        (b'<nzb><head><meta type="password">false</meta></head></nzb>', None),
        (b'<nzb><head><meta type="password">n/a</meta></head></nzb>', None),
        (b'<nzb><head><meta type="password">   </meta></head></nzb>', None),
        (b"<nzb><head></head></nzb>", None),
        (b'<nzb><head><meta type="password"/></head></nzb>', None),
        # Valid passwords
        (
            b'<nzb><head><meta type="password">SecretPass123</meta></head></nzb>',
            "SecretPass123",
        ),
        (
            b"<nzb><head><meta type='password'>My Password With Spaces</meta></head></nzb>",
            "My Password With Spaces",
        ),
        (
            b'<nzb><head><meta type="password">\n  complex_pass_42\n  </meta></head></nzb>',
            "complex_pass_42",
        ),
    ],
)
def test_extract_nzb_password(raw_xml: bytes, expected_password: str | None):
    assert extract_nzb_password(raw_xml) == expected_password


def test_is_nzb_content_fake_allows_meta_password_0_and_real_password():
    # Valid NZB with <meta type="password">0</meta>
    nzb_zero = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b'  <head><meta type="password">0</meta></head>\n'
        b'  <file poster="test" date="1700000000" subject="Movie.1080p.mkv (1/1)">\n'
        b"    <groups><group>alt.binaries.test</group></groups>\n"
        b'    <segments><segment bytes="50000000" number="1">seg1</segment></segments>\n'
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(nzb_zero, media_type="movie")
    assert not is_fake
    assert reason is None

    # Valid NZB with real password in meta
    nzb_pass = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        b'  <head><meta type="password">actual_archive_password</meta></head>\n'
        b'  <file poster="test" date="1700000000" subject="Movie.1080p.mkv (1/1)">\n'
        b"    <groups><group>alt.binaries.test</group></groups>\n"
        b'    <segments><segment bytes="50000000" number="1">seg1</segment></segments>\n'
        b"  </file>\n"
        b"</nzb>"
    )
    is_fake, reason = is_nzb_content_fake(nzb_pass, media_type="movie")
    assert not is_fake
    assert reason is None


@pytest.mark.parametrize(
    "files,usable",
    [
        ([], False),
        (None, False),
        ([{"size": 123}, {"name": "  "}], False),
        ([DownloaderFile(name="", size_bytes=0, path=None)], False),
        (["", " "], False),
        ([{"name": "Movie.2024.1080p.mkv", "size": 1}], True),
        ([DownloaderFile(name="a.mkv", size_bytes=1)], True),
    ],
)
def test_has_usable_file_entries(files: Any, usable: bool) -> None:
    from app.core.fake_detector import has_usable_file_entries

    assert has_usable_file_entries(files) is usable


@pytest.mark.parametrize(
    "client_type,files,expected",
    [
        # Cloud client, nameless entries -> bounded unpack grace (is_archive=True)
        ("torbox", [{"size": 5}], (True, "TorBox returned empty file list", True)),
        (
            "torbox",
            [DownloaderFile(name="", size_bytes=0)],
            (True, "TorBox returned empty file list", True),
        ),
        ("torbox", [], (True, "TorBox returned empty file list", True)),
        # SABnzbd unpacks before Completed: empty = pass
        ("sabnzbd", [], (False, None, False)),
    ],
)
def test_layer3_verdict_grace_for_unnamed_cloud_listing(
    client_type: str, files: Any, expected: Any
) -> None:
    from types import SimpleNamespace

    from app.core.transfer_poller import _layer3_verdict

    verdict = _layer3_verdict(
        files,
        client=SimpleNamespace(provider_type=client_type),
        skip_layer3=False,
        display_name="TorBox",
        media_type="movie",
        is_season_pack=False,
        release_name="Movie.2024",
    )
    assert verdict == expected


def test_layer3_verdict_named_files_without_video_fail_immediately() -> None:
    from types import SimpleNamespace

    from app.core.transfer_poller import _layer3_verdict

    is_fake, reason, is_archive = _layer3_verdict(
        [{"name": "readme.txt", "size": 10}],
        client=SimpleNamespace(provider_type="torbox"),
        skip_layer3=False,
        display_name="TorBox",
        media_type="movie",
        is_season_pack=False,
        release_name="Movie.2024",
    )
    assert is_fake is True
    assert is_archive is False
    assert reason == "Downloaded payload lacks a valid playable video file"


def test_layer3_logs_raw_filelist(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    from app.core.fake_detector import is_transfer_filelist_fake

    with caplog.at_level(logging.DEBUG, logger="app.core.fake_detector"):
        is_transfer_filelist_fake([{"size": 7}], release_name="Rel")
    assert any(
        "Raw filelist" in r.message and "keys=['size']" in r.message
        for r in caplog.records
    )
