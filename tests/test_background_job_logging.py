"""m55: phase stats and Simkl periodic sync interval contract."""

from datetime import datetime, timezone

import pytest

from app.core.automation import (
    evaluate_periodic_watchlist_sync,
    normalize_sync_interval_minutes,
)
from app.db.models import Provider


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, 60),
        (0, 0),
        (-1, 0),
        (3, 5),
        (5, 5),
        (60, 60),
        (1440, 1440),
        (5000, 1440),
        ("15", 15),
        ("abc", 60),
        (True, 60),
        (False, 60),
    ],
)
def test_normalize_sync_interval_minutes(value, expected):
    assert normalize_sync_interval_minutes(value) == expected


def _provider(interval=60, last="2026-10-13T10:00:00+00:00", **kw) -> Provider:
    cfg = {"sync_interval_minutes": interval, "last_synced_at": last}
    return Provider(
        type="simkl",
        name="Simkl",
        category="watchlist",
        is_active=True,
        client_id="cid",
        access_token="tok",
        config_json=cfg,
        **kw,
    )


def _utc(h, m):
    return datetime(2026, 10, 13, h, m, tzinfo=timezone.utc)


def test_evaluator_not_due():
    st = evaluate_periodic_watchlist_sync([_provider()], _utc(10, 20))
    assert (
        st.state,
        st.interval_minutes,
        st.elapsed_minutes,
        st.remaining_minutes,
    ) == (
        "not_due",
        60,
        20,
        40,
    )


def test_evaluator_due_and_naive_now():
    st = evaluate_periodic_watchlist_sync([_provider()], _utc(11, 0))
    assert st.state == "due" and st.elapsed_minutes == 60 and st.remaining_minutes == 0
    naive = evaluate_periodic_watchlist_sync(
        [_provider()], datetime(2026, 10, 13, 10, 20)
    )
    assert naive.state == "not_due" and naive.elapsed_minutes == 20


def test_evaluator_never_synced_is_due():
    st = evaluate_periodic_watchlist_sync([_provider(last=None)], _utc(10, 0))
    assert st.state == "due" and st.elapsed_minutes is None


def test_evaluator_disabled_and_no_provider():
    assert (
        evaluate_periodic_watchlist_sync([_provider(interval=0)], _utc(10, 20)).state
        == "disabled"
    )
    assert evaluate_periodic_watchlist_sync([], _utc(10, 20)).state == "no_provider"


def test_evaluator_multiple_providers():
    fresh = _provider(interval=60, last="2026-10-13T10:10:00+00:00")
    soon = _provider(interval=30, last="2026-10-13T10:00:00+00:00")
    st = evaluate_periodic_watchlist_sync([fresh, soon], _utc(10, 20))
    assert st.state == "not_due" and st.remaining_minutes == 10
    stale = _provider(interval=5, last="2026-10-13T10:00:00+00:00")
    assert evaluate_periodic_watchlist_sync([fresh, stale], _utc(10, 20)).state == "due"


# ---------------------------------------------------------------- orchestrator tick
import logging  # noqa: E402
from unittest.mock import AsyncMock  # noqa: E402

from app.core import automation  # noqa: E402
from app.db.models import EpisodeStatus  # noqa: E402
from tests.conftest import make_series  # noqa: E402

SYNC_PREFIX = "[ORCHESTRATOR] [1/6] Periodic watchlist sync:"


def _orch_messages(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "app.core.automation" and "[ORCHESTRATOR]" in r.getMessage()
    ]


async def _tick(caplog, monkeypatch, now, level=logging.INFO):
    sync = AsyncMock()
    monkeypatch.setattr(automation, "sync_all_providers", sync)
    with caplog.at_level(level, logger="app.core.automation"):
        await automation.run_orchestrator_tick(now)
    return sync


async def test_idle_tick_info_lines(caplog, monkeypatch):
    await _tick(caplog, monkeypatch, _utc(10, 20), logging.DEBUG)
    all_msgs = _orch_messages(caplog)
    assert not any("[SYNC]" in m for m in all_msgs)
    msgs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and r.getMessage() in all_msgs
    ]
    assert msgs == [
        f"{SYNC_PREFIX} no active watchlist provider with credentials",
        "[ORCHESTRATOR] [2/6] Broadcast air date refresh: 0 refreshed (0 frozen, 0 paced)",
        "[ORCHESTRATOR] [3/6] Episode growth probe: 0 probed, 0 new episodes added",
        "[ORCHESTRATOR] [4/6] Air date rollover: 0 due episodes/seasons rolled over to searching",
        "[ORCHESTRATOR] [5/6] History autoclear: 0 entries cleared (pruned=0, aged=0)",
        "[ORCHESTRATOR] [6/6] Air-date push & upgrade: skipped (runs at :00 only)",
    ]
    banners = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith("=====")
    ]
    assert banners[0].startswith("===== [START] Orchestrator Tick 20")
    assert banners[-1].startswith("===== [END] Orchestrator Tick 20")
    assert " (took " in banners[-1]


async def test_tick_reports_rollover_count(db_session, caplog, monkeypatch):
    await make_series(
        db_session,
        episodes_per_season=1,
        episode_statuses=[EpisodeStatus.FUTURE],
    )
    from datetime import timedelta

    from sqlalchemy import update

    from app.db.models import Episode

    await db_session.execute(
        update(Episode).values(air_date=_utc(9, 0) - timedelta(days=1))
    )
    await db_session.commit()
    await _tick(caplog, monkeypatch, _utc(10, 20))
    # N = episodes_rolled_over + seasons_rolled_over (the FUTURE season rolls too)
    assert (
        "[ORCHESTRATOR] [4/6] Air date rollover: 2 due episodes/seasons rolled over to searching"
        in _orch_messages(caplog)
    )


@pytest.mark.parametrize(
    ("interval", "last", "now", "expected", "synced"),
    [
        (
            60,
            "2026-10-13T10:00:00+00:00",
            _utc(10, 20),
            "not due (20m elapsed, interval 60m; 40m remaining)",
            False,
        ),
        (
            60,
            "2026-10-13T10:00:00+00:00",
            _utc(11, 0),
            "due -> triggering provider sync",
            True,
        ),
        (60, None, _utc(10, 20), "due -> triggering provider sync", True),
        (0, "2026-10-13T10:00:00+00:00", _utc(10, 20), "disabled (interval 0)", False),
    ],
)
async def test_tick_step3_variants(
    db_session, caplog, monkeypatch, interval, last, now, expected, synced
):
    db_session.add(_provider(interval=interval, last=last))
    await db_session.commit()
    sync = await _tick(caplog, monkeypatch, now)
    assert f"{SYNC_PREFIX} {expected}" in _orch_messages(caplog)
    assert sync.await_count == (1 if synced else 0)


async def test_tick_due_but_sync_running(db_session, caplog, monkeypatch):
    db_session.add(_provider(last=None))
    await db_session.commit()
    await automation._sync_lock.acquire()
    try:
        sync = await _tick(caplog, monkeypatch, _utc(10, 20))
    finally:
        automation._sync_lock.release()
    assert (
        f"{SYNC_PREFIX} due but a sync is already running; skipping"
        in _orch_messages(caplog)
    )
    sync.assert_not_awaited()


# ---------------------------------------------------------------- air-push pass
from app.config import settings  # noqa: E402
from app.core import air_push_engine  # noqa: E402

AIR = datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc)
PUSH_NOW = datetime(2026, 10, 12, 13, 0, tzinfo=timezone.utc)


def _push_infos(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "app.core.air_push_engine" and r.levelno == logging.INFO
    ]


async def _run_push_pass(caplog):
    with caplog.at_level(logging.INFO, logger="app.core.air_push_engine"):
        await air_push_engine.run_air_date_push_and_upgrade_tick(PUSH_NOW)
    return _push_infos(caplog)


async def test_air_push_idle_pass(caplog):
    assert await _run_push_pass(caplog) == [
        "[AIR-PUSH] [1/2] Air-date auto-push: 0 eligible episodes found across active series",
        "[AIR-PUSH] [2/2] Quality upgrade pass: 0 eligible upgrade candidates found",
        "[AIR-PUSH] Hourly pass complete",
    ]


async def test_air_push_one_eligible_episode(db_session, caplog, monkeypatch):
    monkeypatch.setattr(settings, "air_delay_hours", 2)
    monkeypatch.setattr(settings, "upgrade_window_hours", 48)
    monkeypatch.setattr(settings, "sh_max_retries", 3)
    _, _, eps = await make_series(
        db_session, episodes_per_season=1, auto_push_episodes=True
    )
    eps[0].air_date = AIR
    await db_session.commit()
    monkeypatch.setattr(
        air_push_engine,
        "execute_auto_push",
        AsyncMock(return_value={"dispatched_count": 1}),
    )
    msgs = await _run_push_pass(caplog)
    assert msgs[0] == (
        "[AIR-PUSH] [1/2] Air-date auto-push: 1 eligible episodes found across active series"
    )


async def test_air_push_dry_run_suffix(caplog, monkeypatch):
    monkeypatch.setattr(settings, "dry_run", True)
    msgs = await _run_push_pass(caplog)
    assert msgs[1] == (
        "[AIR-PUSH] [2/2] Quality upgrade pass: 0 eligible upgrade candidates found (dry-run)"
    )


async def test_air_push_step_failure_logged(caplog, monkeypatch):
    monkeypatch.setattr(
        air_push_engine,
        "run_initial_air_push",
        AsyncMock(side_effect=RuntimeError("x")),
    )
    msgs = await _run_push_pass(caplog)
    assert msgs[0] == "[AIR-PUSH] [1/2] Air-date auto-push: failed (see error above)"
    assert msgs[-1] == "[AIR-PUSH] Hourly pass complete"


# --- Sync engine phases and maintenance (ticket 04, m61) ---


def _sync_infos(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "app.core.automation"
        and r.levelno == logging.INFO
        and r.getMessage().startswith("[SYNC] [")
    ]


async def _run_sync(
    db_session,
    caplog,
    monkeypatch,
    *,
    sync_anime=True,
    counts=(0, 0, 0, 0, 0),
    entries=None,
    ingest=None,
    config=None,
):
    from app.db.models import Provider
    from app.services import watchlist_ingest
    from app.services.watchlists.simkl_adapter import SimklWatchlistAdapter

    db_session.add(
        Provider(
            name="Simkl",
            type="simkl",
            category="watchlist",
            is_active=True,
            client_id="cid",
            access_token="tok",
            config_json={"sync_anime": sync_anime, **(config or {})},
        )
    )
    await db_session.commit()
    seq, anime, tba, dup, pruned = counts

    async def _tba(session, now=None, *, stats=None):
        stats.shows_evaluated, stats.seasons_discovered = tba
        return tba[1]

    monkeypatch.setattr(
        SimklWatchlistAdapter,
        "fetch_entries",
        AsyncMock(return_value=entries or []),
    )
    monkeypatch.setattr(
        watchlist_ingest,
        "ingest_entries",
        ingest or AsyncMock(return_value=watchlist_ingest.IngestResult()),
    )
    monkeypatch.setattr(
        automation, "consolidate_standalone_anime_sequels", AsyncMock(return_value=seq)
    )
    monkeypatch.setattr(
        automation, "refresh_due_future_anime_seasons", AsyncMock(return_value=anime)
    )
    monkeypatch.setattr(automation, "refresh_due_future_tv_seasons", _tba)
    monkeypatch.setattr(
        automation, "deduplicate_media_items", AsyncMock(return_value=dup)
    )
    monkeypatch.setattr(
        automation,
        "prune_inactive_media_items",
        AsyncMock(return_value=pruned),
    )
    with caplog.at_level(logging.DEBUG, logger="app.core.automation"):
        await automation.sync_all_providers()
    return _sync_infos(caplog)


async def test_sync_phases_simkl_and_maintenance_summaries(
    db_session, caplog, monkeypatch
):
    msgs = await _run_sync(db_session, caplog, monkeypatch, counts=(2, 3, (6, 1), 7, 8))
    assert msgs == [
        "[SYNC] [1/3] Simkl: fetched 0 entries (0 in enabled categories)",
        "[SYNC] [2/3] Simkl: ingested 0 (0 added, 0 linked, 0 status changes)",
        "[SYNC] [3/3] Simkl: 0 departed entries marked completed",
        "[SYNC] [MAINT] [1/4] 2 sequels consolidated, 3 TBA seasons updated",
        "[SYNC] [MAINT] [2/4] 1 TBA seasons discovered",
        "[SYNC] [MAINT] [3/4] 7 duplicates merged",
        "[SYNC] [MAINT] [4/4] 8 inactive titles pruned",
    ]
    # Air-date refresh and rollover belong to the orchestrator tick, not the sync.
    assert not any("Air date" in m or "Rollover" in m for m in msgs)


async def test_sync_phases_zero_and_anime_disabled(db_session, caplog, monkeypatch):
    msgs = await _run_sync(
        db_session,
        caplog,
        monkeypatch,
        sync_anime=False,
        counts=(9, 9, (0, 0), 0, 0),
    )
    msgs = msgs[3:]  # skip the three provider phases
    assert (
        msgs[0] == "[SYNC] [MAINT] [1/4] 0 sequels consolidated, 0 TBA seasons updated"
    )
    assert msgs[1] == "[SYNC] [MAINT] [2/4] 0 TBA seasons discovered"
    assert msgs[2] == "[SYNC] [MAINT] [3/4] 0 duplicates merged"
    assert msgs[3] == "[SYNC] [MAINT] [4/4] 0 inactive titles pruned"


async def test_sync_phases_1_2_fetch_and_ingest_counts(db_session, caplog, monkeypatch):
    from app.services.watchlist_ingest import IngestResult
    from app.services.watchlist_providers import WatchlistEntry

    entries = [
        WatchlistEntry(remote_id=str(i), media_kind=kind, title=f"T{i}")
        for i, kind in enumerate(["movie", "movie", "show"])
    ]
    ingest = AsyncMock(return_value=IngestResult(ingested=3, added=3, linked=1))
    msgs = await _run_sync(
        db_session, caplog, monkeypatch, entries=entries, ingest=ingest
    )
    assert msgs[:2] == [
        "[SYNC] [1/3] Simkl: fetched 3 entries (3 in enabled categories)",
        "[SYNC] [2/3] Simkl: ingested 3 (3 added, 1 linked, 0 status changes)",
    ]


async def test_sync_phase_1_disabled_categories_filtered(
    db_session, caplog, monkeypatch
):
    from app.services.watchlist_providers import WatchlistEntry

    entries = [
        WatchlistEntry(remote_id="1", media_kind="movie", title="M"),
        WatchlistEntry(remote_id="2", media_kind="show", title="S"),
        WatchlistEntry(remote_id="3", media_kind="anime", title="A"),
    ]
    msgs = await _run_sync(
        db_session,
        caplog,
        monkeypatch,
        sync_anime=False,
        config={"sync_movies": False, "sync_series": True},
        entries=entries,
    )
    assert msgs[0] == "[SYNC] [1/3] Simkl: fetched 3 entries (1 in enabled categories)"


async def test_sync_lines_inside_tick_carry_orchestrator_step_prefix(
    db_session, caplog, monkeypatch
):
    db_session.add(_provider(last=None))
    await db_session.commit()

    async def fake_sync(banner=True):
        automation.logger.info("[SYNC] [1/3] Fake: fetched 1 entries")

    monkeypatch.setattr(automation, "sync_all_providers", fake_sync)
    with caplog.at_level(logging.INFO, logger="app.core.automation"):
        await automation.run_orchestrator_tick(_utc(10, 20))
        automation.logger.info("[SYNC] outside tick")
    msgs = [r.getMessage() for r in caplog.records]
    assert "[ORCHESTRATOR] [1/6] [SYNC] [1/3] Fake: fetched 1 entries" in msgs
    assert "[SYNC] outside tick" in msgs
