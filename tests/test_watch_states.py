"""Five-state watch model: constants, precedence and lifecycle (m69)."""

from __future__ import annotations

from datetime import datetime, timezone
from itertools import combinations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.automation import (
    classify_v3_status_tier,
    has_future_seasons,
    is_awaiting_seasons,
    latest_released_order,
)
from app.db.models import (
    ACTIVE_LIST_STATUSES,
    INACTIVE_LIST_STATUSES,
    LIST_STATUS_PRECEDENCE,
    MediaItem,
    Season,
    best_list_status,
)
from app.services.watchlist_ingest import (
    mark_absent_sources_completed,
    prune_inactive_media_items,
)
from tests.conftest import make_movie, make_series, make_source, make_watchlist_provider

ORDER = ("watching", "on_hold", "plantowatch", "completed", "dropped")


def test_constants():
    assert LIST_STATUS_PRECEDENCE == ORDER
    assert ACTIVE_LIST_STATUSES == {"plantowatch", "watching", "on_hold"}
    assert INACTIVE_LIST_STATUSES == {"completed", "dropped"}


@pytest.mark.parametrize(("a", "b"), list(combinations(ORDER, 2)))
def test_best_list_status_pairs(a, b):
    assert best_list_status([b, a]) == a
    assert best_list_status([a, b]) == a


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([], None),
        (["bogus"], None),
        (["bogus", "dropped"], "dropped"),
        (iter(["on_hold"]), "on_hold"),
    ],
)
def test_best_list_status_edges(statuses, expected):
    assert best_list_status(statuses) == expected


async def _load(session, item_id) -> MediaItem:
    return (
        await session.execute(
            select(MediaItem)
            .where(MediaItem.id == item_id)
            .options(selectinload(MediaItem.watchlist_sources))
            .execution_options(populate_existing=True)
        )
    ).scalar_one()


@pytest.mark.parametrize(
    ("statuses", "effective", "on_hold"),
    [
        ([], None, False),
        (["on_hold"], "on_hold", True),
        (["on_hold", "watching"], "watching", False),
        (["on_hold", "plantowatch"], "on_hold", True),
        (["completed", "dropped"], "completed", False),
    ],
)
async def test_item_effective_status(db_session, statuses, effective, on_hold):
    simkl = await make_watchlist_provider(db_session, "simkl")
    item = await make_movie(db_session, provider_id=simkl.id)
    for i, status in enumerate(statuses):
        prov = await make_watchlist_provider(db_session, "simkl", name=f"p{i}")
        await make_source(db_session, item, prov, status, f"r{i}")
    await db_session.commit()
    item = await _load(db_session, item.id)
    assert item.effective_list_status == effective
    assert item.is_list_on_hold is on_hold


async def test_absent_on_hold_source_completed_and_present_one_pruned_protected(
    db_session,
):
    simkl = await make_watchlist_provider(db_session, "simkl")
    held = await make_movie(db_session, provider_id=simkl.id, tmdb_id=1, simkl_id=1)
    absent = await make_movie(db_session, provider_id=simkl.id, tmdb_id=2, simkl_id=2)
    src_held = await make_source(db_session, held, simkl, "on_hold", "h")
    src_absent = await make_source(db_session, absent, simkl, "on_hold", "a")
    await db_session.commit()

    assert await mark_absent_sources_completed(db_session, simkl, {"h"}) == 1
    await db_session.refresh(src_held)
    await db_session.refresh(src_absent)
    assert (src_held.list_status, src_absent.list_status) == ("on_hold", "completed")

    assert await prune_inactive_media_items(db_session) == 1
    await db_session.commit()
    ids = set((await db_session.execute(select(MediaItem.id))).scalars().all())
    assert ids == {held.id}


async def test_simkl_get_watchlist_requests_all_five_statuses(monkeypatch):
    import httpx

    from app.services.watchlists import simkl

    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200, json={"shows": []})

    real = httpx.AsyncClient
    monkeypatch.setattr(
        simkl.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(handler), **kw),
    )
    monkeypatch.setattr(simkl._limiter, "wait", lambda: _noop())
    await simkl.get_watchlist("shows", "test-id", "test-token")
    assert requested == ["plantowatch", "watching", "hold", "completed", "dropped"]


async def _noop() -> None:
    return None


def test_simkl_hold_row_maps_to_on_hold():
    from app.services.watchlists.simkl_adapter import _simkl_list_status

    assert _simkl_list_status("hold") == "on_hold"


def _entry(remote_id: str, status: str, tmdb_id: int = 603):
    from app.services.watchlist_providers import WatchlistEntry

    return WatchlistEntry(
        remote_id=remote_id,
        media_kind="movie",
        title="Example Movie",
        list_status=status,  # type: ignore[arg-type]
        tmdb_id=tmdb_id,
    )


@pytest.mark.parametrize("status", ["completed", "dropped"])
async def test_inactive_unmatched_entry_creates_no_item(db_session, status):
    from app.services.watchlist_ingest import ingest_entries

    provider = await make_watchlist_provider(db_session, "simkl")
    result = await ingest_entries(db_session, provider, [_entry("x1", status)])
    assert result.added == 0
    assert (await db_session.execute(select(MediaItem.id))).first() is None


async def test_inactive_matched_entry_updates_source(db_session):
    from app.db.models import MediaItemWatchlistSource
    from app.services.watchlist_ingest import ingest_entries

    provider = await make_watchlist_provider(db_session, "simkl")
    item = await make_movie(db_session, provider_id=provider.id, tmdb_id=603)
    src = await make_source(db_session, item, provider, "plantowatch", "x1")
    await db_session.commit()

    await ingest_entries(db_session, provider, [_entry("x1", "completed")])
    src = (await db_session.execute(select(MediaItemWatchlistSource))).scalar_one()
    assert src.list_status == "completed"


@pytest.mark.parametrize(
    ("existing", "incoming", "expected"),
    [
        ("watching", "plantowatch", "watching"),
        ("on_hold", "completed", "on_hold"),
        ("plantowatch", "on_hold", "on_hold"),
        ("completed", "watching", "watching"),
    ],
)
async def test_consolidated_ids_never_regress_active_status(
    db_session, existing, incoming, expected
):
    from app.db.models import MediaItemWatchlistSource
    from app.services.watchlist_ingest import ingest_entries

    provider = await make_watchlist_provider(db_session, "simkl")
    item = await make_movie(db_session, provider_id=provider.id, tmdb_id=603)
    await make_source(db_session, item, provider, existing, "old-id")
    await db_session.commit()

    await ingest_entries(db_session, provider, [_entry("new-id", incoming)])
    src = (await db_session.execute(select(MediaItemWatchlistSource))).scalar_one()
    assert src.list_status == expected


# --- On-hold tier classification (m69-04) ---

_PAST = datetime(2020, 1, 1, tzinfo=timezone.utc)


async def _awaiting_series(
    session, *, s2: dict | None = None, watched: int = 1, s1_downloaded: bool = True
):
    from app.db.models import SeasonStatus

    provider = await make_watchlist_provider(session, "simkl")
    s2_cfg = {
        "season_number": 2,
        "watch_order": 2,
        "air_date": None,
        "status": SeasonStatus.FUTURE,
    }
    s2_cfg.update(s2 or {})
    item, _, _ = await make_series(
        session,
        seasons=[
            {
                "season_number": 1,
                "watch_order": 1,
                "air_date": _PAST,
                "status": SeasonStatus.DOWNLOADED
                if s1_downloaded
                else SeasonStatus.SEARCHING,
            },
            s2_cfg,
        ],
        provider_id=provider.id,
    )
    await make_source(session, item, provider, "watching", "s", watched)
    await session.commit()
    return await _load_full(session, item.id)


async def _load_full(session, item_id) -> MediaItem:
    return (
        await session.execute(
            select(MediaItem)
            .where(MediaItem.id == item_id)
            .options(
                selectinload(MediaItem.watchlist_sources),
                selectinload(MediaItem.seasons).selectinload(Season.episodes),
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one()


async def test_fully_watched_with_future_season_is_on_hold(db_session):
    item = await _awaiting_series(db_session)
    assert latest_released_order(item) == 1
    assert has_future_seasons(item)
    assert is_awaiting_seasons(item)
    assert classify_v3_status_tier(item) == "on_hold"


async def test_undownloaded_released_season_is_not_on_hold(db_session):
    item = await _awaiting_series(db_session, s1_downloaded=False)
    assert has_future_seasons(item)
    assert not is_awaiting_seasons(item)
    assert classify_v3_status_tier(item) == "ready_to_push"


async def test_released_second_season_is_not_on_hold(db_session):
    from app.db.models import SeasonStatus

    item = await _awaiting_series(
        db_session, s2={"air_date": _PAST, "status": SeasonStatus.SEARCHING}
    )
    assert not has_future_seasons(item)
    assert classify_v3_status_tier(item) != "on_hold"


async def test_unwatched_released_season_is_not_awaiting(db_session):
    item = await _awaiting_series(db_session, watched=0)
    assert not is_awaiting_seasons(item)
    assert classify_v3_status_tier(item) != "on_hold"


async def _with_s1_episodes(session, item, statuses, **s2):
    from app.db.models import Episode

    for n, st in enumerate(statuses, start=1):
        session.add(
            Episode(
                season_id=item.seasons[0].id,
                episode_number=n,
                monitored=True,
                status=st,
            )
        )
    await session.commit()
    return await _load_full(session, item.id)


def _eps(done: int, pending: int, future: int = 0, ignored: int = 0):
    from app.db.models import EpisodeStatus as E

    return (
        [E.DOWNLOADED] * done
        + [E.PENDING] * pending
        + [E.FUTURE] * future
        + [E.IGNORED] * ignored
    )


@pytest.mark.parametrize(
    ("statuses", "awaiting"),
    [
        (_eps(1, 23), False),
        (_eps(24, 0), True),
        (_eps(12, 0, future=12), True),
        (_eps(11, 1, future=12), False),
        (_eps(23, 0, ignored=1), True),
    ],
)
async def test_awaiting_requires_fully_downloaded_latest_season(
    db_session, statuses, awaiting
):
    item = await _awaiting_series(db_session)
    item = await _with_s1_episodes(db_session, item, statuses)
    assert is_awaiting_seasons(item) is awaiting
    assert (classify_v3_status_tier(item) == "on_hold") is awaiting


async def test_older_incomplete_season_is_ignored_for_awaiting(db_session):
    from app.db.models import SeasonStatus

    item = await _awaiting_series(
        db_session,
        s2={"air_date": _PAST, "status": SeasonStatus.DOWNLOADED},
        watched=2,
    )
    from app.db.models import Season as S

    s3 = S(
        media_item_id=item.id,
        season_number=3,
        watch_order=3,
        type_number=3,
        entry_type="season",
        title="Season 3",
        status=SeasonStatus.FUTURE,
        air_date=None,
    )
    db_session.add(s3)
    await db_session.commit()
    item = await _with_s1_episodes(db_session, item, _eps(1, 23))
    # S1 partial, S2 (latest released, no episode rows) complete via status fallback
    assert is_awaiting_seasons(item)


async def test_active_download_beats_on_hold(db_session):
    from app.db.models import SeasonStatus

    item = await _awaiting_series(db_session)
    item.seasons[0].status = SeasonStatus.DOWNLOADING
    assert classify_v3_status_tier(item) == "in_progress"


async def test_list_on_hold_source_classifies_on_hold(db_session):
    provider = await make_watchlist_provider(db_session, "simkl")
    item = await make_movie(db_session, provider_id=provider.id)
    await make_source(db_session, item, provider, "on_hold", "h")
    await db_session.commit()
    item = await _load_full(db_session, item.id)
    assert not is_awaiting_seasons(item)
    assert classify_v3_status_tier(item) == "on_hold"


async def test_watching_beats_on_hold_source(db_session):
    item = await _awaiting_series(db_session, watched=0)
    other = await make_watchlist_provider(db_session, "simkl", name="other")
    await make_source(db_session, item, other, "on_hold", "o")
    await db_session.commit()
    item = await _load_full(db_session, item.id)
    assert item.effective_list_status == "watching"
    assert classify_v3_status_tier(item) != "on_hold"


# --- Freeze automation & prune protection (m69-05) ---


async def _hold_series(session, *, s2_released: bool = False, status="watching"):
    from app.db.models import SeasonStatus

    item = await _awaiting_series(
        session,
        s2={"air_date": _PAST, "status": SeasonStatus.SEARCHING}
        if s2_released
        else None,
    )
    item.auto_advance_seasons = True
    item.watchlist_sources[0].list_status = status
    await session.commit()
    return await _load_full(session, item.id)


@pytest.mark.parametrize(
    ("status", "s2_released", "called"),
    [
        ("on_hold", True, False),  # list on hold
        ("watching", False, False),  # awaiting future season
        ("watching", True, True),  # nothing holds it
    ],
)
async def test_auto_advance_frozen_only_while_on_hold(
    db_session, status, s2_released, called
):
    from unittest.mock import AsyncMock, patch

    from app.core.push_engine import evaluate_watch_progress_and_advance

    item = await _hold_series(db_session, s2_released=s2_released, status=status)
    with patch(
        "app.core.push_engine.advance_season_buffer", new=AsyncMock(return_value=[])
    ) as adv:
        await evaluate_watch_progress_and_advance(db_session, item, {"status": "x"})
    assert item.last_watched_order == 1  # progress is still recorded
    assert (adv.await_count == 1) is called


async def test_air_push_skips_on_hold(db_session, monkeypatch):
    from unittest.mock import AsyncMock, patch

    from app.config import settings
    from app.core.air_push_engine import run_initial_air_push

    monkeypatch.setattr(settings, "air_delay_hours", 2)
    monkeypatch.setattr(settings, "upgrade_window_hours", 48)
    monkeypatch.setattr(settings, "sh_max_retries", 3)
    air = datetime(2026, 1, 1, tzinfo=timezone.utc)
    provider = await make_watchlist_provider(db_session, "simkl")
    held, _, eps = await make_series(
        db_session,
        episodes_per_season=1,
        simkl_id=1,
        provider_id=provider.id,
        auto_push_episodes=True,
    )
    eps[0].air_date = air
    await make_source(db_session, held, provider, "on_hold", "h")
    await db_session.commit()
    with patch(
        "app.core.air_push_engine.execute_auto_push",
        new=AsyncMock(return_value={"dispatched_count": 1}),
    ) as m:
        assert await run_initial_air_push(db_session, air.replace(hour=5)) == 0
    m.assert_not_awaited()


async def test_awaiting_item_survives_prune_then_is_pruned(db_session):
    from app.db.models import SeasonStatus
    from app.services.watchlist_ingest import prune_inactive_media_items

    item = await _hold_series(db_session, status="completed")
    assert await prune_inactive_media_items(db_session) == 0
    await db_session.commit()
    assert (await db_session.execute(select(MediaItem.id))).first() is not None

    item = await _load_full(db_session, item.id)
    item.seasons[1].air_date = _PAST
    item.seasons[1].status = SeasonStatus.SEARCHING
    await db_session.commit()
    assert await prune_inactive_media_items(db_session) == 1


# --- Local playback state mapping (m69-06) ---


@pytest.mark.parametrize(
    ("offset", "duration", "views", "continue_watching", "expected"),
    [
        (0, 100_000, 0, False, "plantowatch"),
        (1, 100_000, 0, False, "watching"),
        (94_999, 100_000, 0, False, "watching"),
        (95_000, 100_000, 0, False, "completed"),
        (100_000, 100_000, 1, False, "completed"),
        (0, 100_000, 1, False, "completed"),
        (0, 100_000, 1, True, "completed"),
        (0, 100_000, 0, True, "watching"),
        (0, 0, 0, False, "plantowatch"),
        (5, 0, 0, False, "watching"),
    ],
)
def test_playback_list_status(offset, duration, views, continue_watching, expected):
    from app.services.watchlist_providers import playback_list_status

    assert playback_list_status(offset, duration, views, continue_watching) == expected


# --- On Hold dashboard section (m69-07) ---


async def test_dashboard_renders_on_hold_section(db_session, async_client):
    from tests.conftest import seed_setup_ready

    await seed_setup_ready(db_session)
    provider = await make_watchlist_provider(db_session, "simkl", name="hold-src")
    item = await make_movie(db_session, provider_id=provider.id, title="Held Movie")
    await make_source(db_session, item, provider, "on_hold", "h")
    await db_session.commit()

    html = (await async_client.get("/")).text
    assert "Held Movie" in html
    body = html.split('id="video-table-body-on-hold"', 1)[1].split("</tbody>", 1)[0]
    assert "Held Movie" in body
    assert "On hold" in body
    assert "No on-hold or awaiting-season items in this category" in body
    upcoming = html.split('id="video-table-body-upcoming"', 1)[1].split("</tbody>", 1)[
        0
    ]
    assert "Held Movie" not in upcoming
