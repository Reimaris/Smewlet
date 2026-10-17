"""Provider capability registry (m70, ADR-153)."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from markupsafe import escape

from app.core.push_engine import (
    persist_sticky_preferences,
    resolve_effective_search_config,
)
from app.services.downloaders.base import DOWNLOADER_TYPES
from app.services.indexers.catalog import INDEXER_PRESETS
from app.services.provider_capabilities import (
    AUTO_ADVANCE_UNAVAILABLE_MESSAGE,
    CAPABILITIES,
    CATEGORY_ORDER,
    can_auto_advance,
    capabilities_by_category,
    get_capability,
    sources_support_auto_advance,
)
from app.services.provider_service import provider_category_for_type
from app.services.watchlists import WATCHLIST_TYPES
from tests.conftest import make_movie, make_series, make_source, make_watchlist_provider

ALL_SLUGS = (
    set(WATCHLIST_TYPES)
    | set(DOWNLOADER_TYPES)
    | {"tmdb", "newznab", "local_directory"}
    | {p.type for p in INDEXER_PRESETS}
)


@pytest.mark.parametrize("slug", sorted(ALL_SLUGS))
def test_registry_complete_and_category_matches(slug: str) -> None:
    cap = CAPABILITIES[slug]
    assert cap.provider_type == slug
    assert cap.category == provider_category_for_type(slug)


def test_registry_has_no_extra_entries() -> None:
    assert set(CAPABILITIES) == ALL_SLUGS


def test_indexer_display_names_from_presets() -> None:
    for p in INDEXER_PRESETS:
        assert CAPABILITIES[p.type].display_name == p.name


_ALL = ("movies", "series", "anime")
# slug: (category, coverage, progress, air_dates, kind, unpack, delete, cat_map, custom)
PINNED = {
    "simkl": ("watchlist", _ALL, True, True, "", False, False, False, False),
    "anilist": ("watchlist", ("anime",), True, True, "", False, False, False, False),
    "mal": ("watchlist", ("anime",), True, False, "", False, False, False, False),
    "plex": (
        "watchlist",
        ("movies", "series"),
        False,
        False,
        "",
        False,
        False,
        False,
        False,
    ),
    "jellyseerr": (
        "watchlist",
        ("movies", "series"),
        False,
        False,
        "",
        False,
        False,
        False,
        False,
    ),
    "overseerr": (
        "watchlist",
        ("movies", "series"),
        False,
        False,
        "",
        False,
        False,
        False,
        False,
    ),
    "tmdb": (
        "metadata",
        ("movies", "series"),
        False,
        False,
        "",
        False,
        False,
        False,
        False,
    ),
    "torbox": ("downloader", _ALL, False, False, "cloud", True, True, False, False),
    "premiumize": (
        "downloader",
        _ALL,
        False,
        False,
        "cloud",
        False,
        True,
        False,
        False,
    ),
    "sabnzbd": ("downloader", _ALL, False, False, "local", True, True, True, False),
    "newznab": ("indexer", _ALL, False, False, "", False, False, False, True),
    "local_directory": ("delivery", _ALL, False, False, "", False, False, False, False),
}
LIMITATIONS = {
    "plex": (
        "No watch progress (cannot trigger season auto-advance)",
        "No exact air dates (uses TMDB dates)",
    ),
    "mal": ("No exact air dates (uses AniList/TMDB dates)",),
    "premiumize": ("No unpack verification (completion is reported by Premiumize)",),
    "tmdb": ("No exact air dates for anime (AniList provides them)",),
    "simkl": ("Exact air dates for TV only",),
    "anilist": ("Exact air dates for anime only",),
}


@pytest.mark.parametrize("slug,expected", sorted(PINNED.items()))
def test_pinned_values(slug: str, expected: tuple) -> None:
    c = CAPABILITIES[slug]
    assert (
        c.category,
        c.media_coverage,
        c.supports_watch_progress,
        c.supports_exact_air_dates,
        c.downloader_kind,
        c.supports_unpack_verification,
        c.supports_remote_delete,
        c.supports_category_mapping,
        c.supports_custom_categories,
    ) == expected


@pytest.mark.parametrize("slug,expected", sorted(LIMITATIONS.items()))
def test_pinned_limitations(slug: str, expected: tuple[str, ...]) -> None:
    assert CAPABILITIES[slug].limitations == expected


def test_indexer_presets_share_indexer_values() -> None:
    for p in INDEXER_PRESETS:
        c = CAPABILITIES[p.type]
        assert (c.category, c.media_coverage, c.supports_custom_categories) == (
            "indexer",
            _ALL,
            True,
        )


def test_registry_immutable() -> None:
    with pytest.raises(TypeError):
        CAPABILITIES["plex"] = CAPABILITIES["mal"]  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        CAPABILITIES["plex"].supports_watch_progress = True  # type: ignore[misc]


def test_get_capability_case_insensitive() -> None:
    assert get_capability("  PLEX ") is CAPABILITIES["plex"]
    assert get_capability("bogus") is None
    assert get_capability("") is None


def test_capabilities_by_category_order() -> None:
    grouped = capabilities_by_category()
    assert tuple(grouped) == CATEGORY_ORDER
    assert all(grouped[c] for c in CATEGORY_ORDER)
    assert all(cap.category == c for c, caps in grouped.items() for cap in caps)


@pytest.mark.parametrize(
    "types,expected",
    [
        (["plex"], False),
        (["plex", "mal"], True),
        (["Simkl"], True),
        ([], False),
        (["bogus"], False),
        (["bogus", "anilist"], True),
    ],
)
def test_sources_support_auto_advance(types: list[str], expected: bool) -> None:
    assert sources_support_auto_advance(types) is expected


# --- can_auto_advance & dispatch gating -------------------------------------


@pytest.fixture
async def plex_item(db_session):
    plex = await make_watchlist_provider(db_session, "plex", name="Plex")
    item = await make_movie(db_session, provider_id=plex.id, auto_advance_seasons=True)
    await make_source(db_session, item, plex, remote_id="plex-1")
    return item


async def test_can_auto_advance_plex_only_false_then_mal_true(db_session, plex_item):
    assert await can_auto_advance(db_session, plex_item) is False
    mal = await make_watchlist_provider(db_session, "mal", name="MyAnimeList")
    await make_source(db_session, plex_item, mal, remote_id="mal-1")
    assert await can_auto_advance(db_session, plex_item) is True


async def test_can_auto_advance_simkl_origin_without_sources(db_session):
    simkl = await make_watchlist_provider(db_session, "simkl", name="Simkl")
    item = await make_movie(db_session, provider_id=simkl.id)
    assert await can_auto_advance(db_session, item) is True


async def test_gated_item_resolves_false_and_keeps_stored_preference(
    db_session, plex_item
):
    payload = {"auto_advance_seasons": True}
    cfg = await resolve_effective_search_config(db_session, plex_item, payload)
    assert cfg["auto_advance_seasons"] is False
    assert cfg["can_auto_advance"] is False

    await persist_sticky_preferences(
        db_session, plex_item, {"auto_advance_seasons": False}
    )
    await db_session.refresh(plex_item)
    assert plex_item.auto_advance_seasons is True


async def test_ungated_item_honours_payload(db_session):
    simkl = await make_watchlist_provider(db_session, "simkl", name="Simkl")
    item = await make_movie(db_session, provider_id=simkl.id)
    cfg = await resolve_effective_search_config(
        db_session, item, {"auto_advance_seasons": True}
    )
    assert cfg["auto_advance_seasons"] is True
    assert cfg["can_auto_advance"] is True


# --- push modal UI gating ---------------------------------------------------


async def test_push_modal_warns_for_plex_only_item(db_session, async_client):
    plex = await make_watchlist_provider(db_session, "plex", name="Plex")
    item, _, _ = await make_series(db_session, provider_id=plex.id)
    await make_source(db_session, item, plex, remote_id="plex-1")
    await db_session.commit()
    resp = await async_client.get(f"/api/items/{item.id}/push-modal")
    assert resp.status_code == 200
    assert 'data-testid="auto-advance-warning"' in resp.text
    assert str(escape(AUTO_ADVANCE_UNAVAILABLE_MESSAGE)) in resp.text
    assert 'x-model="autoAdvanceSeasons" class="sr-only peer" disabled' in resp.text
    assert "autoAdvanceSeasons: false" in resp.text


async def test_push_modal_no_warning_for_simkl_item(db_session, async_client):
    simkl = await make_watchlist_provider(db_session, "simkl", name="Simkl")
    item, _, _ = await make_series(db_session, provider_id=simkl.id)
    await db_session.commit()
    resp = await async_client.get(f"/api/items/{item.id}/push-modal")
    assert resp.status_code == 200
    assert "auto-advance-warning" not in resp.text
    assert 'x-model="autoAdvanceSeasons" class="sr-only peer" disabled' not in resp.text


# --- capabilities matrix modal & settings -----------------------------------


async def test_capabilities_modal_renders_tabs(async_client):
    resp = await async_client.get("/settings/capabilities")
    assert resp.status_code == 200
    for label in ("Watchlists", "Downloaders", "Metadata", "Indexers", "Delivery"):
        assert label in resp.text
    assert 'data-focused="true"' not in resp.text


async def test_capabilities_modal_focus_marks_row_and_tab(async_client):
    resp = await async_client.get("/settings/capabilities?focus=PLEX")
    assert resp.status_code == 200
    assert 'data-focused="true"' in resp.text
    assert resp.text.count('data-focused="true"') == 1
    resp = await async_client.get("/settings/capabilities?focus=torbox")
    assert 'tab: "downloader"' in resp.text


async def test_capabilities_modal_bogus_focus_and_configured_pill(
    db_session, async_client
):
    resp = await async_client.get("/settings/capabilities?focus=<bogus>")
    assert resp.status_code == 200
    assert 'data-focused="true"' not in resp.text
    assert "Configured" not in resp.text
    await make_watchlist_provider(db_session, "plex", name="Plex")
    await db_session.commit()
    resp = await async_client.get("/settings/capabilities")
    assert "Configured" in resp.text


async def test_settings_page_has_matrix_button_and_card_link(db_session, async_client):
    await make_watchlist_provider(db_session, "plex", name="Plex")
    await db_session.commit()
    resp = await async_client.get("/settings")
    assert resp.status_code == 200
    assert "Capabilities Matrix" in resp.text
    assert "/settings/capabilities?focus=plex" in resp.text


# --- provider modal pills & README gate -------------------------------------


async def test_provider_modal_shows_plex_limitation(async_client):
    resp = await async_client.get("/settings/provider/new")
    assert resp.status_code == 200
    assert CAPABILITIES["plex"].limitations[0] in resp.text
    assert "Watch progress" in resp.text


@pytest.mark.parametrize("slug", sorted(set(WATCHLIST_TYPES) | set(DOWNLOADER_TYPES)))
def test_readme_lists_every_provider(slug: str) -> None:
    readme = (Path(__file__).resolve().parent.parent / "README.md").read_text()
    assert CAPABILITIES[slug].display_name in readme


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?focus=simkl"])
async def test_capabilities_modal_alpine_state_is_valid_html(async_client, query):
    """Double quotes from tojson must not terminate the x-data attribute."""
    resp = await async_client.get(f"/settings/capabilities{query}")
    assert resp.status_code == 200
    assert "x-data='{ tab: " in resp.text
    assert 'x-data="{ tab: "' not in resp.text
