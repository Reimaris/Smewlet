"""Unified dispatch through the active downloader (m57-06)."""

import httpx
import pytest
from sqlalchemy import select

from app.core.push_engine import (
    _dispatch_candidate_list_to_downloader,
    dispatch_nzb_to_active_downloader,
)
from app.db.models import DownloadHistory, MediaType
from app.services.downloaders import sabnzbd
from app.services.downloaders.base import DownloaderError
from tests.conftest import VALID_NZB_BYTES, make_downloader_provider, make_movie

SAB_ADDFILE_OK = {"status": True, "nzo_ids": ["SABnzbd_nzo_x1y2z3"]}
SAB_CONFIG = (
    '{"category_movie":"movies","category_series":"tv","category_anime":"anime"}'
)


@pytest.fixture
def sab_requests(monkeypatch):
    """Capture SABnzbd multipart bodies; answer with SAB_ADDFILE_OK."""
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200, json=SAB_ADDFILE_OK)

    monkeypatch.setattr(
        sabnzbd,
        "_client_factory",
        lambda timeout: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=timeout
        ),
    )
    return bodies


async def seed_sab(session):
    return await make_downloader_provider(
        session,
        "sabnzbd",
        name="SABnzbd",
        api_url="http://sab:8080/sabnzbd/api",
        config_json=SAB_CONFIG,
    )


@pytest.mark.parametrize(
    ("media_type", "is_anime", "expected_cat"),
    [
        (MediaType.ANIME, True, b"anime"),
        (MediaType.MOVIE, False, b"movies"),
        (MediaType.SHOW, False, b"tv"),
    ],
)
async def test_sab_category_by_media(
    db_session, sab_requests, media_type, is_anime, expected_cat
):
    await seed_sab(db_session)
    await db_session.commit()
    client, result = await dispatch_nzb_to_active_downloader(
        db_session,
        VALID_NZB_BYTES,
        "Rel.nzb",
        password="pw",
        media_type=media_type,
        is_anime=is_anime,
    )
    assert client.provider_type == "sabnzbd"
    assert result.success and result.transfer_id == "SABnzbd_nzo_x1y2z3"
    body = sab_requests[0]
    assert b'name="cat"\r\n\r\n' + expected_cat + b"\r\n" in body
    assert b'name="nzbpassword"\r\n\r\npw' in body


async def test_no_active_downloader_raises(db_session):
    with pytest.raises(DownloaderError, match="No active downloader"):
        await dispatch_nzb_to_active_downloader(
            db_session,
            VALID_NZB_BYTES,
            "Rel.nzb",
            password=None,
            media_type=MediaType.MOVIE,
            is_anime=False,
        )


async def test_candidate_dispatch_stores_downloader_fields_for_sab(
    db_session, sab_requests
):
    await seed_sab(db_session)
    anime = await make_movie(db_session, media_type=MediaType.ANIME, title="Anime Film")
    await db_session.commit()
    cand = {
        "guid": "g1",
        "title": "Anime.Film.2024.1080p",
        "score": 1.0,
        "_prefetched_nzb": (VALID_NZB_BYTES, "Anime.Film.2024.1080p.nzb"),
    }
    history = await _dispatch_candidate_list_to_downloader(db_session, [cand], anime)
    assert isinstance(history, DownloadHistory)
    assert b'name="cat"\r\n\r\nanime\r\n' in sab_requests[0]
    row = (await db_session.execute(select(DownloadHistory))).scalars().one()
    assert row.downloader_type == "sabnzbd"
    assert row.downloader_transfer_id == "SABnzbd_nzo_x1y2z3"
    assert row.downloader_hash is None and row.dispatched_at is not None


async def test_candidate_dispatch_without_downloader_reports_error(db_session):
    movie = await make_movie(db_session)
    await db_session.commit()
    errors: list[str] = []
    cand = {
        "guid": "g1",
        "title": "Movie.2024.1080p",
        "_prefetched_nzb": (VALID_NZB_BYTES, "Movie.nzb"),
    }
    out = await _dispatch_candidate_list_to_downloader(
        db_session, [cand], movie, error_out=errors
    )
    assert out is None and "No active downloader" in errors[0]


@pytest.mark.parametrize(
    ("prior_delivery_status", "links"), [("delivered", True), ("none", False)]
)
async def test_manual_replace_links_delivered_prior(
    prior_delivery_status, links, db_session, sab_requests
):
    await seed_sab(db_session)
    movie = await make_movie(db_session, title="Replace Me")
    prior = DownloadHistory(
        media_item_id=movie.id,
        nzb_title="Replace.Me.2024.1080p.WEB.x264-OLD",
        nzb_guid="g-old",
        status_detail="completed",
        delivery_status=prior_delivery_status,
    )
    db_session.add(prior)
    await db_session.commit()
    cand = {
        "guid": "g-new",
        "title": "Replace.Me.2024.2160p.WEB.x265-NEW",
        "score": 1.0,
        "_prefetched_nzb": (VALID_NZB_BYTES, "Replace.Me.2024.2160p.nzb"),
    }

    history = await _dispatch_candidate_list_to_downloader(db_session, [cand], movie)

    assert isinstance(history, DownloadHistory)
    assert history.is_upgrade is False
    assert history.replaces_history_id == (prior.id if links else None)
