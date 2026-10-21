import pytest

from app.db.models import Provider
from app.services.indexers.catalog import (
    DEFAULT_MOVIE_CATEGORIES,
    DEFAULT_TV_CATEGORIES,
    INDEXER_PRESETS,
    get_preset,
    is_indexer_type,
    normalize_category_csv,
    resolve_categories,
)


def test_catalog_integrity():
    assert len(INDEXER_PRESETS) == 11
    slugs = [p.type for p in INDEXER_PRESETS]
    assert len(set(slugs)) == 11
    assert slugs[0] == "treasuremaps" and slugs[-1] == "newznab"
    for p in INDEXER_PRESETS:
        if p.type != "newznab":
            assert p.api_url.startswith("https://") and p.api_url.endswith("/api")
            assert p.key_url.startswith("https://")
        assert is_indexer_type(p.type)


def test_get_preset_and_is_indexer_type():
    assert get_preset("NZBGeek").type == "nzbgeek"
    assert get_preset("bogus") is None
    assert not is_indexer_type("torbox")


@pytest.mark.parametrize(
    "raw,default,expected",
    [
        ("2000, 2040", DEFAULT_MOVIE_CATEGORIES, "2000,2040"),
        ("2040,2000,2040", DEFAULT_MOVIE_CATEGORIES, "2000,2040"),
        ("", DEFAULT_MOVIE_CATEGORIES, None),
        ("  ", DEFAULT_MOVIE_CATEGORIES, None),
        (None, DEFAULT_TV_CATEGORIES, None),
        ("2000,2040,2045,2050,2060", DEFAULT_MOVIE_CATEGORIES, None),
        ("5060,5000,5045,5030,5040", DEFAULT_TV_CATEGORIES, "5000,5030,5040,5045,5060"),
        ("999999", DEFAULT_MOVIE_CATEGORIES, "999999"),
    ],
)
def test_normalize_category_csv(raw, default, expected):
    assert normalize_category_csv(raw, default) == expected


@pytest.mark.parametrize("raw", ["abc", "2000,,2040", "0", "1000000", "-5", "20.5"])
def test_normalize_category_csv_invalid(raw):
    with pytest.raises(ValueError):
        normalize_category_csv(raw, DEFAULT_MOVIE_CATEGORIES)


@pytest.mark.parametrize(
    "config,kind,expected",
    [
        ('{"custom_tv_categories":"5000,5040"}', "series", "5000,5040"),
        ("{}", "movie", "2000,2040,2045,2050,2060"),
        ("{}", "series", "5000,5030,5040,5045"),
        ("{}", "anime", "5070"),
        ("{}", "anime-movie", "5070"),
        ('{"custom_tv_categories":"5070,5000"}', "anime", "5000,5070"),
        ('{"custom_movie_categories":"2040"}', "anime-movie", "2040"),
        ('{"custom_movie_categories":"2040"}', "anime", "5070"),
        ('{"custom_movie_categories":"2040"}', "movie", "2040"),
    ],
)
def test_resolve_categories(config, kind, expected):
    p = Provider(type="nzbgeek", config_json=config)
    assert resolve_categories(p, kind) == expected


@pytest.mark.parametrize(
    "config",
    [
        "not json",
        "[1]",
        '{"custom_movie_categories": 5, "custom_tv_categories": "abc"}',
    ],
)
def test_indexer_config_malformed_is_none(config):
    assert Provider(config_json=config).indexer_config == {
        "custom_movie_categories": None,
        "custom_tv_categories": None,
    }


def test_indexer_config_parses_values():
    p = Provider(config_json='{"custom_movie_categories":"2040, 2000"}')
    assert p.indexer_config == {
        "custom_movie_categories": "2000,2040",
        "custom_tv_categories": None,
    }


@pytest.mark.parametrize(
    "category,expected_t",
    [("2000,2040", "movie"), (2000, "movie"), ("2040,2000", "search")],
)
async def test_search_raw_category_csv_imdb_lookup(monkeypatch, category, expected_t):
    from app.services.indexers import newznab as newznab_client

    captured = {}

    async def fake_execute(url, params):
        captured.update(params)
        return []

    monkeypatch.setattr(newznab_client, "_execute_search", fake_execute)
    await newznab_client.search_raw(
        category=category,
        imdb_id="tt0111161",
        api_url="https://x.test/api",
        api_key="k",
    )
    assert captured["t"] == expected_t
    assert captured["cat"] == category
    assert captured["imdbid"] == "0111161"


@pytest.fixture
async def nzbgeek_indexer(db_session):
    async def _add(config_json: str = "{}") -> Provider:
        p = Provider(
            category="indexer",
            type="nzbgeek",
            name="NZBGeek",
            api_url="https://api.nzbgeek.info/api",
            api_key="k",
            config_json=config_json,
        )
        db_session.add(p)
        await db_session.commit()
        return p

    return _add


@pytest.mark.parametrize(
    "config,expected",
    [
        ('{"custom_movie_categories":"2040"}', "2040"),
        ("{}", "2000,2040,2045,2050,2060"),
    ],
)
async def test_push_movie_search_uses_indexer_categories(
    monkeypatch, db_session, nzbgeek_indexer, config, expected
):
    from app.core.push_engine import _query_movie_across_indexers
    from app.db.models import MediaItem
    from app.services.indexers import newznab as newznab_client

    await nzbgeek_indexer(config)
    calls = []

    async def fake_search_movie(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(newznab_client, "search_movie", fake_search_movie)
    await _query_movie_across_indexers(db_session, MediaItem(title="Heat"))
    assert [c["category"] for c in calls] == [expected]
    assert calls[0]["api_url"] == "https://api.nzbgeek.info/api"


@pytest.mark.parametrize(
    "kind,config,expected",
    [
        ("series", '{"custom_tv_categories":"5040"}', "5040"),
        ("series", "{}", "5000,5030,5040,5045"),
        ("anime", "{}", "5070"),
    ],
)
async def test_adhoc_show_search_uses_indexer_categories(
    monkeypatch, nzbgeek_indexer, kind, config, expected
):
    from app.core.adhoc_search import _fetch_raw_releases
    from app.services.indexers import newznab as newznab_client

    await nzbgeek_indexer(config)
    calls = []

    async def fake_search_show(**kwargs):
        calls.append(kwargs)
        return []

    async def fake_search_raw(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(newznab_client, "search_show", fake_search_show)
    monkeypatch.setattr(newznab_client, "search_raw", fake_search_raw)
    await _fetch_raw_releases(
        kind=kind,
        titles=["Show"],
        imdb_id=None,
        tmdb_id=None,
        tvdb_id=None,
        season=1,
        episode=None,
    )
    assert [c["category"] for c in calls] == [expected]
    assert calls[0]["api_key"] == "k"


async def _post_indexer(async_client, **overrides):
    form = {
        "name": "Geek",
        "provider_type": "nzbgeek",
        "api_key": "k",
        "is_active": "true",
    }
    return await async_client.post("/settings/provider", data={**form, **overrides})


async def test_save_indexer_normalizes_categories_and_fills_url(
    async_client, db_session
):
    import json

    from sqlalchemy import select

    resp = await _post_indexer(async_client, custom_movie_categories="2040,2000")
    assert resp.status_code == 200
    row = (await db_session.execute(select(Provider))).scalar_one()
    assert row.category == "indexer"
    assert row.api_url == "https://api.nzbgeek.info/api"
    assert json.loads(row.config_json)["custom_movie_categories"] == "2000,2040"


async def test_save_indexer_invalid_categories_rejected(async_client, db_session):
    from sqlalchemy import select

    resp = await _post_indexer(async_client, custom_tv_categories="abc")
    assert resp.status_code == 400
    assert "Invalid category list" in resp.text
    assert (await db_session.execute(select(Provider))).first() is None


async def test_diagnostics_label_for_preset_slug():
    from app.core.diagnostics import PROVIDER_TYPE_LABELS

    assert PROVIDER_TYPE_LABELS["nzbgeek"] == "NZBGeek"
    assert PROVIDER_TYPE_LABELS["newznab"] == "Newznab"


async def test_modal_new_shows_indexer_catalog(async_client):
    html = (await async_client.get("/settings/provider/new")).text
    for marker in (
        "indexer-card-nzbgeek",
        "indexer-card-newznab",
        'data-badge="NG"',
        "indexer-category-overrides",
        "indexer-key-link",
    ):
        assert marker in html


async def test_modal_edit_shows_stored_override(async_client, db_session):
    row = Provider(
        type="nzbgeek",
        category="indexer",
        name="G",
        api_url="https://x.example/api",
        config_json='{"custom_movie_categories": "2040"}',
    )
    db_session.add(row)
    await db_session.commit()
    html = (await async_client.get(f"/settings/provider/{row.id}/edit")).text
    assert 'name="custom_movie_categories" value="2040"' in html
    assert "https://x.example/api" in html


async def test_adhoc_movie_search_adds_localized_title(
    monkeypatch, nzbgeek_indexer, db_session
):
    from unittest.mock import AsyncMock

    from app.core.adhoc_search import AdHocFilters, search_and_score_releases
    from app.services import localized_titles
    from app.services.indexers import newznab as newznab_client

    await nzbgeek_indexer("{}")
    localized_titles.clear_localized_title_cache()
    details = {
        "translations": {
            "translations": [
                {"iso_639_1": "de", "data": {"title": "Mein bester & ich"}}
            ]
        }
    }
    monkeypatch.setattr(
        "app.services.tmdb.get_movie_details", AsyncMock(return_value=details)
    )
    calls = []

    async def fake_search_movie(**kwargs):
        calls.append(kwargs["title"])
        if kwargs["title"] == "Mein bester & ich":
            assert kwargs["tmdb_id"] is None and kwargs["imdb_id"] is None
        return []

    monkeypatch.setattr(newznab_client, "search_movie", fake_search_movie)
    for lang, expected in (
        ("de", ["The Upside", "Mein bester & ich"]),
        ("en", ["The Upside"]),
    ):
        calls.clear()
        await search_and_score_releases(
            kind="movie",
            scope="item",
            titles=["The Upside"],
            year=2017,
            imdb_id=None,
            tmdb_id=399057,
            tvdb_id=None,
            season=None,
            episode=None,
            filters=AdHocFilters(primary_language=lang),
            limit=10,
        )
        assert sorted(calls) == sorted(expected)


@pytest.mark.parametrize(
    "kind,config,languages,expected",
    [
        ("series", "{}", ("de", None), "5000,5030,5040,5045,5100,5130,5140,5145"),
        (
            "series",
            "{}",
            ("DE", "fr"),
            "5000,5030,5040,5045,5100,5130,5140,5145,5300,5330,5340,5345",
        ),
        ("series", "{}", ("en", None), "5000,5030,5040,5045"),
        ("series", "{}", (None, None), "5000,5030,5040,5045"),
        ("series", '{"custom_tv_categories":"5040"}', ("de", None), "5040"),
        (
            "movie",
            "{}",
            ("de", None),
            "2000,2040,2045,2050,2060,2100,2140,2145,2150,2160",
        ),
        ("anime", "{}", ("es", None), "5070,5270"),
    ],
)
def test_resolve_categories_adds_language_categories(kind, config, languages, expected):
    provider = Provider(config_json=config)
    assert resolve_categories(provider, kind, languages) == expected


async def test_show_search_uses_german_categories_for_german_preset(
    monkeypatch, db_session, nzbgeek_indexer
):
    from app.core.push_engine import _query_show_across_indexers
    from app.db.models import MediaItem
    from app.services.indexers import newznab as newznab_client

    await nzbgeek_indexer("{}")
    calls = []

    async def fake_search_show(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(newznab_client, "search_show", fake_search_show)
    cfg = {"primary_language": "de", "fallback_language": None}
    await _query_show_across_indexers(
        db_session, MediaItem(title="Show"), 1, 1, effective_cfg=cfg
    )
    assert calls[0]["category"] == "5000,5030,5040,5045,5100,5130,5140,5145"
