import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, settings
from app.core.push_engine import populate_history_snapshot
from app.db.models import DownloadHistory, MediaItem, SystemSettings
from app.main import app

client = TestClient(app)


@pytest.mark.asyncio
async def test_schema_models_and_defaults(db_session: AsyncSession):
    """SystemSettings and DownloadHistory have correct column defaults for dry-run simulation."""
    db_settings = SystemSettings(id=1)
    db_session.add(db_settings)

    history = DownloadHistory(
        nzb_title="Test.Dry.Run.Release-GROUP",
        nzb_guid="test-dry-run-guid-1",
    )
    db_session.add(history)
    await db_session.commit()
    await db_session.refresh(db_settings)
    await db_session.refresh(history)

    assert db_settings.notify_on_dry_run is True
    assert history.is_dry_run is False


@pytest.mark.asyncio
async def test_migration_adds_columns(db_session: AsyncSession):
    """init_db verifies is_dry_run and notify_on_dry_run columns exist in sqlite tables."""
    dh_cols = (
        await db_session.execute(sa_text("PRAGMA table_info(download_history)"))
    ).fetchall()
    dh_col_names = {row[1] for row in dh_cols}
    assert "is_dry_run" in dh_col_names

    ss_cols = (
        await db_session.execute(sa_text("PRAGMA table_info(system_settings)"))
    ).fetchall()
    ss_col_names = {row[1] for row in ss_cols}
    assert "notify_on_dry_run" in ss_col_names


def test_config_settings_and_env(monkeypatch):
    """Settings loads notify_on_dry_run with environment variable support."""
    monkeypatch.delenv("NOTIFY_ON_DRY_RUN", raising=False)
    s1 = Settings()
    assert s1.notify_on_dry_run is True

    monkeypatch.setenv("NOTIFY_ON_DRY_RUN", "false")
    s2 = Settings()
    assert s2.notify_on_dry_run is False


@pytest.mark.asyncio
async def test_auto_save_notify_on_dry_run(db_session: AsyncSession):
    """POST /settings/auto-save parses and persists notify_on_dry_run via form and JSON."""
    db_settings = SystemSettings(id=1, notify_on_dry_run=True)
    db_session.add(db_settings)
    await db_session.commit()

    # 1. Update via form data to False
    resp1 = client.post(
        "/settings/auto-save",
        data={"notify_on_dry_run": "false"},
    )
    assert resp1.status_code == 200
    assert resp1.json() == {"status": "ok"}

    await db_session.refresh(db_settings)
    assert db_settings.notify_on_dry_run is False
    assert settings.notify_on_dry_run is False

    # 2. Update via JSON to True
    resp2 = client.post(
        "/settings/auto-save",
        json={"notify_on_dry_run": True},
    )
    assert resp2.status_code == 200
    assert resp2.json() == {"status": "ok"}

    await db_session.refresh(db_settings)
    assert db_settings.notify_on_dry_run is True
    assert settings.notify_on_dry_run is True


def test_populate_history_snapshot_preserves_or_defaults_is_dry_run():
    """populate_history_snapshot maintains or defaults is_dry_run to False."""
    item = MediaItem(
        title="Test Movie",
        year=2024,
        media_type="movie",
    )
    history = DownloadHistory(
        nzb_title="Test.Movie.2024.1080p-GROUP",
    )
    populate_history_snapshot(history, item)
    assert history.is_dry_run is False

    dry_history = DownloadHistory(
        nzb_title="Test.Movie.2024.1080p-GROUP",
        is_dry_run=True,
    )
    populate_history_snapshot(dry_history, item)
    assert dry_history.is_dry_run is True


@pytest.mark.asyncio
async def test_simulate_push_candidate_creates_and_persists_history(
    db_session: AsyncSession,
):
    """_simulate_push_candidate creates DownloadHistory with is_dry_run=True and status_detail='simulated'."""
    from app.core.push_engine import _simulate_push_candidate
    from app.core.transfer_poller import clear_dry_run_pushes, get_active_pushes
    from tests.conftest import make_movie

    clear_dry_run_pushes()
    movie = await make_movie(db_session, title="Test Simulation Movie", year=2024)

    cand = {
        "title": "Test.Simulation.Movie.2024.1080p.WEB-DL-GRP",
        "guid": "sim-cand-guid-1",
        "score": 9500.0,
        "size_bytes": 4 * 1024 * 1024 * 1024,
        "matched_language": "en",
    }

    hist = await _simulate_push_candidate(
        session=db_session,
        cand=cand,
        item=movie,
        push_mode="auto",
    )

    assert isinstance(hist, DownloadHistory)
    assert hist.is_dry_run is True
    assert hist.status_detail == "simulated"
    assert hist.is_dismissed is True
    assert hist.downloader_transfer_id is None
    assert hist.progress_pct == 100.0
    assert hist.completed_at is not None
    assert hist.nzb_title == cand["title"]
    assert hist.nzb_guid == cand["guid"]
    assert hist.score == cand["score"]
    assert hist.size_bytes == cand["size_bytes"]
    assert hist.media_title == movie.title
    assert hist.media_type_label == "movie"

    # In-memory registry for Card 5 contains record
    active = await get_active_pushes(db_session)
    assert len(active) == 1
    assert active[0]["is_dry_run"] is True


@pytest.mark.asyncio
async def test_full_pipeline_multi_episode_simulation(
    db_session: AsyncSession, monkeypatch
):
    """Auto-push simulates all missing episodes across seasons without premature termination."""
    from unittest.mock import AsyncMock, patch

    from app.core.push_engine import execute_auto_push
    from app.core.transfer_poller import clear_dry_run_pushes
    from app.db.models import EpisodeStatus, MediaStatus, SeasonStatus
    from tests.conftest import VALID_NZB_BYTES, make_series

    clear_dry_run_pushes()
    monkeypatch.setattr(settings, "dry_run", True)

    show, seasons, episodes = await make_series(
        db_session,
        title="Simulation Show",
        seasons=1,
        episodes_per_season=3,
        episode_statuses=[
            EpisodeStatus.SEARCHING,
            EpisodeStatus.SEARCHING,
            EpisodeStatus.SEARCHING,
        ],
    )

    def mock_indexer_search(
        season: int | None = None,
        ep: int | str | None = None,
        *args,
        **kwargs,
    ):
        ep_n = int(ep) if ep is not None else 1
        s_n = int(season) if season is not None else 1
        return [
            {
                "title": f"Simulation.Show.S{s_n:02d}E{ep_n:02d}.1080p.WEB-DL-GRP",
                "size": 1024 * 1024 * 1024,
                "guid": f"guid-s{s_n}-e{ep_n}",
            }
        ]

    with (
        patch(
            "app.services.indexers.newznab.search_show",
            new=AsyncMock(side_effect=mock_indexer_search),
        ),
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=AsyncMock(return_value=(VALID_NZB_BYTES, "test.nzb")),
        ),
        patch(
            "app.services.downloaders.torbox.send_nzb_file", new=AsyncMock()
        ) as mock_tb,
    ):
        res = await execute_auto_push(db_session, show.id)

    mock_tb.assert_not_awaited()
    assert res["status"] == "simulated"
    assert res["pushed"] is True
    assert res["simulated_count"] == 3
    assert res["dispatched_count"] == 3
    assert len(res["history_ids"]) == 3
    assert len(res["guids"]) == 3

    # Library entities MUST remain in SEARCHING status
    await db_session.refresh(show)
    assert show.status == MediaStatus.SEARCHING
    await db_session.refresh(seasons[0])
    assert seasons[0].status == SeasonStatus.SEARCHING
    for ep in episodes:
        await db_session.refresh(ep)
        assert ep.status == EpisodeStatus.SEARCHING

    # DownloadHistory rows persisted with is_dry_run=True
    dh_rows = (
        (
            await db_session.execute(
                sa_text("SELECT is_dry_run, status_detail FROM download_history")
            )
        )
        .mappings()
        .all()
    )
    assert len(dh_rows) == 3
    for row in dh_rows:
        assert row["is_dry_run"] == 1 or row["is_dry_run"] is True
        assert row["status_detail"] == "simulated"


@pytest.mark.asyncio
async def test_full_pipeline_auto_advance_buffer_simulation(
    db_session: AsyncSession, monkeypatch
):
    """Auto-advance buffer lookahead evaluates Season N+1 and Movie N+2 in dry-run mode."""
    from unittest.mock import AsyncMock, patch

    from app.core.push_engine import execute_auto_push
    from app.core.transfer_poller import clear_dry_run_pushes
    from app.db.models import Episode, EpisodeStatus, Season, SeasonStatus
    from tests.conftest import VALID_NZB_BYTES, make_series

    clear_dry_run_pushes()
    monkeypatch.setattr(settings, "dry_run", True)

    show, seasons, episodes = await make_series(
        db_session,
        title="Advancing Anime",
        seasons=1,
        episodes_per_season=1,
        auto_advance_seasons=True,
    )
    s1 = seasons[0]
    s1.watch_order = 1

    # Movie Bridge N+1 (watch_order=2)
    m_bridge = Season(
        media_item_id=show.id,
        season_number=0,
        type_number=1,
        watch_order=2,
        entry_type="movie",
        title="The Movie Bridge",
        monitored=True,
        status=SeasonStatus.SEARCHING,
    )
    db_session.add(m_bridge)

    # Season N+2 (watch_order=3)
    s2 = Season(
        media_item_id=show.id,
        season_number=2,
        type_number=2,
        watch_order=3,
        entry_type="season",
        monitored=True,
        status=SeasonStatus.SEARCHING,
    )
    db_session.add(s2)
    await db_session.flush()

    s2_ep = Episode(
        season_id=s2.id,
        episode_number=1,
        monitored=True,
        status=EpisodeStatus.SEARCHING,
    )
    db_session.add(s2_ep)
    await db_session.commit()

    def mock_indexer_search(
        season: int | None = None,
        ep: int | str | None = None,
        *args,
        **kwargs,
    ):
        s_num = int(season) if season is not None else 1
        ep_num = int(ep) if ep is not None else 1
        return [
            {
                "title": f"Advancing.Anime.S{s_num:02d}E{ep_num:02d}.1080p-GRP",
                "size": 1024 * 1024 * 1024,
                "guid": f"guid-{s_num}-{ep_num}",
            }
        ]

    def mock_movie_search(*args, **kwargs):
        return [
            {
                "title": "Advancing.Anime.The.Movie.Bridge.1080p-GRP",
                "size": 2048 * 1024 * 1024,
                "guid": "guid-movie-bridge",
            }
        ]

    with (
        patch(
            "app.services.indexers.newznab.search_show",
            new=AsyncMock(side_effect=mock_indexer_search),
        ),
        patch(
            "app.services.indexers.newznab.search_movie",
            new=AsyncMock(side_effect=mock_movie_search),
        ),
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=AsyncMock(return_value=(VALID_NZB_BYTES, "test.nzb")),
        ),
        patch(
            "app.services.downloaders.torbox.send_nzb_file", new=AsyncMock()
        ) as mock_tb,
    ):
        res = await execute_auto_push(
            db_session, show.id, payload={"auto_advance_seasons": True}
        )

    mock_tb.assert_not_awaited()
    assert res["status"] == "simulated"
    # S1 (1 ep) + Movie Bridge (1) + S2 (1 ep) = 3 simulated releases!
    assert res["simulated_count"] == 3
    assert len(res["history_ids"]) == 3

    # All remain untouched in SEARCHING
    await db_session.refresh(s1)
    await db_session.refresh(m_bridge)
    await db_session.refresh(s2)
    assert s1.status == SeasonStatus.SEARCHING
    assert m_bridge.status == SeasonStatus.SEARCHING
    assert s2.status == SeasonStatus.SEARCHING


@pytest.mark.asyncio
async def test_batch_notification_rollup_gated_by_notify_on_dry_run(
    db_session: AsyncSession, monkeypatch
):
    """Batch notification rollup formats simulated push summary and gates dispatch via notify_on_dry_run."""
    from unittest.mock import AsyncMock, patch

    from app.core.push_engine import execute_auto_push
    from app.core.transfer_poller import clear_dry_run_pushes
    from app.db.models import EpisodeStatus, SystemSettings
    from tests.conftest import VALID_NZB_BYTES, make_series

    clear_dry_run_pushes()
    monkeypatch.setattr(settings, "dry_run", True)

    db_settings = SystemSettings(
        id=1,
        notify_on_dry_run=True,
        dry_run=True,
        discord_enabled=True,
        discord_webhook_url="https://discord.com/api/webhooks/mock/123",
    )
    db_session.add(db_settings)

    show, seasons, episodes = await make_series(
        db_session,
        title="Notify Show",
        seasons=1,
        episodes_per_season=2,
        episode_statuses=[EpisodeStatus.SEARCHING, EpisodeStatus.SEARCHING],
    )

    def mock_indexer_search(
        season: int | None = None,
        ep: int | str | None = None,
        *args,
        **kwargs,
    ):
        ep_n = int(ep) if ep is not None else 1
        return [
            {
                "title": f"Notify.Show.S01E{ep_n:02d}.1080p.WEB-DL-GRP",
                "size": 1024 * 1024 * 1024,
                "guid": f"guid-notify-{ep_n}",
            }
        ]

    # Case 1: notify_on_dry_run = True -> dispatch_batch_notification_event is called
    with (
        patch(
            "app.services.indexers.newznab.search_show",
            new=AsyncMock(side_effect=mock_indexer_search),
        ),
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=AsyncMock(return_value=(VALID_NZB_BYTES, "test.nzb")),
        ),
        patch(
            "app.services.discord.dispatch_batch_notification_event",
            new=AsyncMock(return_value={"dispatched": True}),
        ) as mock_batch_dispatch,
    ):
        res1 = await execute_auto_push(db_session, show.id)

    assert res1["status"] == "simulated"
    assert res1["simulated_count"] == 2
    mock_batch_dispatch.assert_awaited_once()
    call_kwargs = mock_batch_dispatch.await_args.kwargs
    assert call_kwargs["event_type"] == "dry_run"
    assert len(call_kwargs["histories"]) == 2
    assert "2 release(s) simulated" in call_kwargs["reason"]

    # Case 2: Direct dispatch_batch_notification_event check when notify_on_dry_run = False
    from app.services import discord

    db_settings.notify_on_dry_run = False
    monkeypatch.setattr(settings, "notify_on_dry_run", False)
    await db_session.commit()

    with patch(
        "app.services.discord.send_discord_webhook", new=AsyncMock()
    ) as mock_webhook:
        notif_res = await discord.dispatch_batch_notification_event(
            db_session,
            event_type="dry_run",
            histories=call_kwargs["histories"],
            item=show,
            reason=call_kwargs["reason"],
            is_dry_run=True,
        )

    assert notif_res["dispatched"] is False
    assert "disabled" in notif_res["reason"].lower()
    mock_webhook.assert_not_awaited()


@pytest.mark.asyncio
async def test_purge_expired_dry_run_history(db_session: AsyncSession):
    """purge_expired_dry_run_history purges dry-run rows older than 1h, preserving recent dry-run and real history."""
    from datetime import datetime, timedelta, timezone

    from app.core.transfer_poller import purge_expired_dry_run_history

    now = datetime.now(timezone.utc)
    old_time = now - timedelta(hours=2)
    recent_time = now - timedelta(minutes=10)

    # 1. Expired dry-run row (older than 1h) -> should be deleted
    expired_dry = DownloadHistory(
        nzb_title="Expired.Dry.Run.Movie-GRP",
        is_dry_run=True,
        dispatched_at=old_time,
        created_at=old_time,
    )
    # 2. Recent dry-run row (10 min old) -> should NOT be deleted
    recent_dry = DownloadHistory(
        nzb_title="Recent.Dry.Run.Movie-GRP",
        is_dry_run=True,
        dispatched_at=recent_time,
        created_at=recent_time,
    )
    # 3. Old normal (non-dry-run) row -> should NOT be deleted
    old_normal = DownloadHistory(
        nzb_title="Old.Normal.Movie-GRP",
        is_dry_run=False,
        dispatched_at=old_time,
        created_at=old_time,
    )

    db_session.add_all([expired_dry, recent_dry, old_normal])
    await db_session.commit()

    deleted_count = await purge_expired_dry_run_history(db_session, ttl_hours=1)
    assert deleted_count == 1

    remaining = (
        (
            await db_session.execute(
                sa_text("SELECT nzb_title FROM download_history ORDER BY id ASC")
            )
        )
        .scalars()
        .all()
    )
    assert len(remaining) == 2
    assert "Expired.Dry.Run.Movie-GRP" not in remaining
    assert "Recent.Dry.Run.Movie-GRP" in remaining
    assert "Old.Normal.Movie-GRP" in remaining


@pytest.mark.asyncio
async def test_delete_dry_run_endpoint(db_session: AsyncSession):
    """POST /api/history/delete-dry-run purges all dry-run rows and returns refresh trigger."""
    from app.db.models import DownloadHistory

    # Seed 2 dry-run rows and 1 real history row
    h1 = DownloadHistory(nzb_title="Dry.Run.1-GRP", is_dry_run=True)
    h2 = DownloadHistory(nzb_title="Dry.Run.2-GRP", is_dry_run=True)
    h3 = DownloadHistory(nzb_title="Real.Download-GRP", is_dry_run=False)
    db_session.add_all([h1, h2, h3])
    await db_session.commit()

    # 1. JSON Request
    resp = client.post(
        "/api/history/delete-dry-run", headers={"Content-Type": "application/json"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["deleted_count"] == 2
    assert resp.headers.get("HX-Trigger") == "refreshDashboard"

    remaining = (
        (await db_session.execute(sa_text("SELECT nzb_title FROM download_history")))
        .scalars()
        .all()
    )
    assert remaining == ["Real.Download-GRP"]

    # 2. HTMX Request
    h4 = DownloadHistory(nzb_title="Dry.Run.3-GRP", is_dry_run=True)
    db_session.add(h4)
    await db_session.commit()

    resp_htmx = client.post(
        "/api/history/delete-dry-run", headers={"HX-Request": "true"}
    )
    assert resp_htmx.status_code == 200
    assert resp_htmx.headers.get("HX-Trigger") == "refreshDashboard"

    remaining2 = (
        (await db_session.execute(sa_text("SELECT nzb_title FROM download_history")))
        .scalars()
        .all()
    )
    assert remaining2 == ["Real.Download-GRP"]


@pytest.mark.asyncio
async def test_dashboard_page_is_read_only_and_exposes_count(
    db_session: AsyncSession,
):
    """Dashboard load leaves expired dry-run rows to the orchestrator/poller purge."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    old_time = now - timedelta(hours=2)
    recent_time = now - timedelta(minutes=5)

    expired = DownloadHistory(
        nzb_title="Expired.Sim.1-GRP",
        is_dry_run=True,
        dispatched_at=old_time,
        created_at=old_time,
    )
    active_dry = DownloadHistory(
        nzb_title="Active.Sim.2-GRP",
        is_dry_run=True,
        dispatched_at=recent_time,
        created_at=recent_time,
    )
    real_dh = DownloadHistory(
        nzb_title="Real.DH.3-GRP",
        is_dry_run=False,
    )
    db_session.add_all([expired, active_dry, real_dh])
    await db_session.commit()

    resp = client.get("/")
    assert resp.status_code == 200

    # Page loads never write (they contended for the SQLite write lock)
    remaining = (
        (
            await db_session.execute(
                sa_text("SELECT nzb_title FROM download_history ORDER BY id ASC")
            )
        )
        .scalars()
        .all()
    )
    assert remaining == ["Expired.Sim.1-GRP", "Active.Sim.2-GRP", "Real.DH.3-GRP"]


@pytest.mark.asyncio
async def test_poller_tick_triggers_dry_run_purge(db_session: AsyncSession):
    """run_transfer_poller_tick executes purge_expired_dry_run_history."""
    from datetime import datetime, timedelta, timezone

    from app.core.transfer_poller import run_transfer_poller_tick

    now = datetime.now(timezone.utc)
    old_time = now - timedelta(hours=2)

    expired = DownloadHistory(
        nzb_title="Tick.Expired.Sim-GRP",
        is_dry_run=True,
        dispatched_at=old_time,
        created_at=old_time,
    )
    db_session.add(expired)
    await db_session.commit()

    await run_transfer_poller_tick(session=db_session)

    remaining = (
        (
            await db_session.execute(
                sa_text("SELECT nzb_title FROM download_history WHERE is_dry_run = 1")
            )
        )
        .scalars()
        .all()
    )
    assert "Tick.Expired.Sim-GRP" not in remaining


@pytest.mark.asyncio
async def test_dashboard_renders_delete_dry_run_button_when_count_positive(
    db_session: AsyncSession,
):
    """Card 4 toolbar renders Delete Dry-Run button when dry-run history rows exist."""
    # When count is 0, button should not appear
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Delete Dry-Run" not in resp.text
    assert 'hx-post="/api/history/delete-dry-run"' not in resp.text

    # Seed dry-run rows
    sim1 = DownloadHistory(
        nzb_title="Sim.One.2026-GRP",
        is_dry_run=True,
    )
    sim2 = DownloadHistory(
        nzb_title="Sim.Two.2026-GRP",
        is_dry_run=True,
    )
    db_session.add_all([sim1, sim2])
    await db_session.commit()

    resp = client.get("/")
    assert resp.status_code == 200
    assert 'id="history-toolbar-actions"' in resp.text
    assert "'history-toolbar-container'" in resp.text
    assert "Delete Dry-Run (2)" in resp.text
    assert 'hx-post="/api/history/delete-dry-run"' in resp.text
    assert (
        'hx-confirm="Are you sure you want to delete all simulated dry-run history entries?"'
        in resp.text
    )


@pytest.mark.asyncio
async def test_dashboard_amber_row_styling_and_badges(
    db_session: AsyncSession,
):
    """Card 4 applies amber row accents, 🧪 target prefix, and SIMULATED badge for dry-run rows."""
    sim = DownloadHistory(
        nzb_title="Amber.Release.1080p-GRP",
        media_title="Amber Movie",
        target_label="Movie",
        is_dry_run=True,
        status_detail="simulated",
    )
    real = DownloadHistory(
        nzb_title="Real.Release.1080p-GRP",
        media_title="Real Movie",
        target_label="Movie",
        is_dry_run=False,
        status_detail="completed",
    )
    db_session.add_all([sim, real])
    await db_session.commit()

    resp = client.get("/")
    assert resp.status_code == 200
    # Amber row styling present
    assert (
        "bg-amber-500/5 hover:bg-amber-500/10 border-l-2 border-l-amber-500/60"
        in resp.text
    )
    # Target pill beaker svg prefix and label
    assert "M19.428 15.428" in resp.text
    assert "Movie</span>" in resp.text
    # Simulated status badge
    assert "SIMULATED (DRY RUN)" in resp.text
    assert "bg-amber-500/20 text-amber-300 border-amber-500/40" in resp.text
    # Real row uses READY ON TORBOX
    assert "READY ON TORBOX" in resp.text


@pytest.mark.asyncio
async def test_settings_renders_notify_on_dry_run_checkbox_and_autosaves(
    db_session: AsyncSession,
):
    """Slice 3 renders notify_on_dry_run auto-saving checkbox and saves state."""
    resp = client.get("/settings")
    assert resp.status_code == 200
    assert 'name="notify_on_dry_run"' in resp.text
    assert "Dry-Run Simulation Alerts (Batch Summary)" in resp.text
    assert 'hx-post="/settings/auto-save"' in resp.text
    assert (
        'js:{"notify_on_dry_run": event.target.checked ? "true" : "false"}' in resp.text
    )

    # Test auto-saving via endpoint
    post_resp = client.post(
        "/settings/auto-save",
        data={"notify_on_dry_run": "false"},
        headers={"HX-Request": "true"},
    )
    assert post_resp.status_code == 200

    res = (
        await db_session.execute(
            sa_text("SELECT notify_on_dry_run FROM system_settings WHERE id = 1")
        )
    ).scalar()
    assert res == 0 or res is False
