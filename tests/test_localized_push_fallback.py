"""Localized-title fallback re-query in the push engine scoring step (m82)."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.core.push_engine import _score_and_partition_candidates
from app.services import localized_titles
from app.services.indexers import newznab as newznab_client
from tests.conftest import make_indexer_provider, make_movie

ENGLISH = "The.Upside.2017.1080p.BluRay.x264-GRP"
GERMAN = "Mein.bester.und.ich.2017.German.DL.1080p.BluRay.x264-GRP"
DETAILS = {
    "translations": {
        "translations": [{"iso_639_1": "de", "data": {"title": "Mein bester und ich"}}]
    }
}
CFG: dict[str, Any] = {
    "primary_language": "de",
    "fallback_language": "en",
    "video_quality_mode": "best",
    "audio_quality_mode": "best",
    "custom_config": {},
}


def _raw(title: str) -> dict[str, Any]:
    return {"title": title, "guid": f"guid-{title}", "size": 8_000_000_000}


async def _score(db_session, item, raw) -> dict[str, Any]:
    return await _score_and_partition_candidates(
        raw_results=raw,
        blacklisted_guids=set(),
        blacklisted_titles=set(),
        expected_title=item.title,
        expected_year=2017,
        expected_alt_title=item.alt_title,
        expected_season=None,
        expected_episode=None,
        expected_season_title=None,
        runtime_minutes=120,
        effective_cfg=CFG,
        media_type="movie",
        session=db_session,
        item_id=item.id,
    )


async def _seed_movie(db_session, monkeypatch, **overrides):
    localized_titles.clear_localized_title_cache()
    monkeypatch.setattr(
        "app.services.tmdb.get_movie_details", AsyncMock(return_value=DETAILS)
    )
    await make_indexer_provider(db_session)
    return await make_movie(db_session, title="The Upside", tmdb_id=399057, **overrides)


@pytest.fixture
async def movie(db_session, monkeypatch):
    return await _seed_movie(db_session, monkeypatch)


async def test_fallback_requery_selects_german_dl(db_session, monkeypatch, movie):
    search = AsyncMock(return_value=[_raw(GERMAN)])
    monkeypatch.setattr(newznab_client, "search_movie", search)
    result = await _score(db_session, movie, [_raw(ENGLISH)])
    assert search.await_count == 1
    assert search.await_args.kwargs["title"] == "Mein bester und ich"
    assert search.await_args.kwargs["tmdb_id"] is None
    assert search.await_args.kwargs["imdb_id"] is None
    assert result["all_valid"][0]["title"] == GERMAN
    assert result["primary"]


async def test_no_requery_when_primary_candidate_exists(db_session, monkeypatch):
    movie = await _seed_movie(db_session, monkeypatch, alt_title="Mein bester und ich")
    search = AsyncMock(return_value=[])
    monkeypatch.setattr(newznab_client, "search_movie", search)
    result = await _score(db_session, movie, [_raw(GERMAN)])
    search.assert_not_awaited()
    assert result["all_valid"][0]["title"] == GERMAN


async def test_ampersand_title_matches_und_release(db_session, monkeypatch):
    monkeypatch.setattr(
        "app.services.tmdb.get_movie_details",
        AsyncMock(
            return_value={
                "translations": {
                    "translations": [
                        {"iso_639_1": "de", "data": {"title": "Mein bester & ich"}}
                    ]
                }
            }
        ),
    )
    localized_titles.clear_localized_title_cache()
    await make_indexer_provider(db_session)
    movie = await make_movie(db_session, title="The Upside", tmdb_id=440472)
    monkeypatch.setattr(
        newznab_client, "search_movie", AsyncMock(return_value=[_raw(GERMAN)])
    )
    result = await _score(db_session, movie, [_raw(ENGLISH)])
    assert result["all_valid"][0]["title"] == GERMAN
