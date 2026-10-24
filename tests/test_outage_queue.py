"""Outage-aware push engine: queue, backoff, terminal failure, auth fail-fast (ADR-158..160)."""

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app.core.adhoc_search import push_adhoc_release
from app.core.outage import clear_outages, has_active_outage, record_outage
from app.core.push_engine import execute_auto_push
from app.core.rate_limit_queue import resume_rate_limited_pushes
from app.db.models import BlacklistedRelease, DownloadHistory, FailureLog, MediaStatus
from app.services.downloaders import torbox
from app.services.downloaders.base import DownloaderUploadResult, outage_upload_result
from app.services.indexers import newznab
from app.services.outage_classifier import OutageVerdict
from tests.conftest import VALID_NZB_BYTES, make_indexer_provider, make_movie

INDEXER_URL = "https://indexer.example.com/api"
RESULT = [
    {
        "title": "Test.Movie.2024.1080p.BluRay.x264-GRP",
        "guid": "guid-tm",
        "size_bytes": 8 * 1024**3,
        "indexer_url": INDEXER_URL,
    }
]


def _client():
    return type("C", (), {"display_name": "TorBox", "provider_type": "torbox"})()


def _fetch_raising(kind: str, label: str):
    cls = newznab.IndexerAuthError if kind == "auth" else newznab.IndexerOutageError
    return AsyncMock(
        side_effect=cls("boom", INDEXER_URL, OutageVerdict(kind, label))  # type: ignore[arg-type]
    )


def _patches(fetch=None, upload=None):
    return (
        patch(
            "app.services.indexers.newznab.search_movie",
            new=AsyncMock(return_value=[dict(r) for r in RESULT]),
        ),
        patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=fetch or AsyncMock(return_value=(VALID_NZB_BYTES, "t.nzb")),
        ),
        patch(
            "app.core.push_engine.dispatch_nzb_to_active_downloader",
            new=upload
            or AsyncMock(
                return_value=(
                    _client(),
                    DownloaderUploadResult(
                        success=True, transfer_id="tb1", raw={"hash": "h"}
                    ),
                )
            ),
        ),
    )


async def _push(session, item, **kw):
    p1, p2, p3 = _patches(**kw)
    with p1, p2, p3:
        await execute_auto_push(session, item.id, {})
    await session.commit()


async def _live(session, item):
    return (
        (
            await session.execute(
                select(DownloadHistory)
                .where(
                    DownloadHistory.media_item_id == item.id,
                    DownloadHistory.is_dismissed.is_(False),
                )
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )


async def test_indexer_outage_queues_then_backs_off_then_fails_terminally(db_session):
    item = await make_movie(db_session)
    expected = [600, 1200, 1800, 1800, 1800]
    for attempt, seconds in enumerate(expected, start=1):
        await _push(db_session, item, fetch=_fetch_raising("outage", "HTTP 503"))
        (row,) = await _live(db_session, item)
        assert row.status_detail == "paused: indexer outage (HTTP 503)"
        assert row.outage_attempts == attempt
        assert item.fail_count == 0
        assert newznab.get_indexer_cooldown_remaining(INDEXER_URL) == pytest.approx(
            seconds, abs=5
        )
        newznab.set_indexer_cooldown(INDEXER_URL, 0)
        clear_outages()
    await db_session.refresh(item)
    assert item.status == MediaStatus.RATE_LIMITED

    await _push(db_session, item, fetch=_fetch_raising("outage", "HTTP 503"))
    (row,) = await _live(db_session, item)
    assert row.status_detail == "failed: indexer outage (5 attempts failed)"
    assert row.failed_at is not None
    await db_session.refresh(item)
    assert item.status == MediaStatus.FAILED


async def test_downloader_outage_keeps_candidate_and_does_not_blacklist(db_session):
    item = await make_movie(db_session)
    client = _client()
    upload = AsyncMock(
        return_value=(
            client,
            outage_upload_result(OutageVerdict("outage", "HTTP 502"), "x"),
        )
    )
    await _push(db_session, item, upload=upload)
    (row,) = await _live(db_session, item)
    assert row.status_detail == "paused: downloader outage (HTTP 502)"
    assert item.fail_count == 0
    assert not (await db_session.execute(select(BlacklistedRelease))).scalars().all()


async def test_indexer_auth_error_fails_immediately_without_cooldown(db_session):
    item = await make_movie(db_session)
    await _push(db_session, item, fetch=_fetch_raising("auth", "HTTP 401"))
    (row,) = await _live(db_session, item)
    assert row.status_detail == "failed: indexer authentication error (HTTP 401)"
    assert row.outage_attempts == 0
    assert newznab.get_indexer_cooldown_remaining(INDEXER_URL) == 0
    logs = (await db_session.execute(select(FailureLog))).scalars().all()
    assert logs


async def test_resume_redispatches_after_outage_cleared(db_session):
    item = await make_movie(db_session)
    await _push(db_session, item, fetch=_fetch_raising("outage", "HTTP 503"))
    assert await resume_rate_limited_pushes() == 0  # still cooling down
    newznab.set_indexer_cooldown(INDEXER_URL, 0)
    clear_outages()
    p1, p2, p3 = _patches()
    with p1, p2, p3:
        assert await resume_rate_limited_pushes() == 1
    (row,) = await _live(db_session, item)
    assert row.downloader_transfer_id == "tb1"


async def test_429_flow_unchanged(db_session):
    item = await make_movie(db_session)
    torbox.set_cooldown(600.0)
    p1, p2, p3 = _patches()
    with p1, p2, p3:
        await execute_auto_push(db_session, item.id, {})
    await db_session.commit()
    (row,) = await _live(db_session, item)
    assert row.status_detail == "rate_limited"
    assert row.outage_attempts == 0


@pytest.fixture
def alert_spy(monkeypatch):
    """Capture dispatch_alert_message calls; episode settle delay is skipped."""
    from app.core import rate_limit_queue as rlq

    async def no_delay():
        return None

    monkeypatch.setattr(rlq, "_reached_settle_delay", no_delay)
    rlq.outage_episode.reset()
    rlq.rate_limit_episode.reset()
    calls: list[tuple[str, str]] = []

    async def fake(_session, flag, title, body, **_kw):
        calls.append((flag, title))
        return {}

    monkeypatch.setattr("app.services.notifications.dispatch_alert_message", fake)
    return calls


async def _outage_pause_two_items(session):
    from app.core.rate_limit_queue import outage_episode

    for i in range(2):
        item = await make_movie(session, simkl_id=500 + i)
        await _push(session, item, fetch=_fetch_raising("outage", "HTTP 503"))
    if outage_episode._task is not None:
        await outage_episode._task


async def test_outage_pause_and_resume_alert_once_each(db_session, alert_spy):
    await _outage_pause_two_items(db_session)
    newznab.set_indexer_cooldown(INDEXER_URL, 0)
    clear_outages()
    p1, p2, p3 = _patches()
    with p1, p2, p3:
        await resume_rate_limited_pushes()
    assert alert_spy == [
        ("notify_on_outages", "⏸️ Service Outage — Pushes Paused"),
        ("notify_on_outages", "▶️ Service Recovered — Queue Resumed"),
    ]


async def test_outage_alerts_gated_by_notify_on_outages(db_session, alert_spy):
    from app.db.models import SystemSettings
    from app.services import discord

    db_session.add(
        SystemSettings(
            id=1,
            discord_webhook_url="https://discord.example.com/api/webhooks/1/x",
            discord_enabled=True,
            notify_on_outages=False,
            notify_on_errors=True,
        )
    )
    await db_session.commit()
    real = discord.dispatch_alert_message
    with patch(
        "app.services.discord.send_discord_webhook", new_callable=AsyncMock
    ) as hook:
        res = await real(db_session, "notify_on_outages", "t", "b")
    assert res["dispatched"] is False
    hook.assert_not_awaited()


async def test_terminal_outage_failure_alerts_via_failure_event(db_session, alert_spy):
    item = await make_movie(db_session)
    with patch(
        "app.services.notifications.dispatch_notification_event",
        new_callable=AsyncMock,
    ) as event:
        for _ in range(6):
            await _push(db_session, item, fetch=_fetch_raising("outage", "HTTP 503"))
            newznab.set_indexer_cooldown(INDEXER_URL, 0)
            clear_outages()
    event.assert_awaited_once()
    assert event.await_args.kwargs["event_type"] == "failure"
    assert "5 attempts failed" in event.await_args.kwargs["status_reason"]


@pytest.fixture
def nzbgeek_outage():
    newznab._indexer_names["indexer.example.com"] = "NZBgeek"
    record_outage("indexer", "indexer.example.com", "NZBgeek", "HTTP 503", 1)
    yield
    newznab._indexer_names.pop("indexer.example.com", None)
    clear_outages()


async def test_adhoc_push_during_outage_queues_with_toast(db_session, nzbgeek_outage):
    await make_indexer_provider(db_session, api_url=INDEXER_URL)
    await db_session.commit()
    outcome = await push_adhoc_release(
        db_session,
        release_link=f"{INDEXER_URL}?t=get&id=1",
        nzb_title=RESULT[0]["title"],
    )
    assert outcome.ok
    assert (
        outcome.message
        == "Push queued — will start automatically once NZBgeek recovers."
    )
    rows = (await db_session.execute(select(DownloadHistory))).scalars().all()
    assert [r.status_detail for r in rows] == ["paused: indexer outage (HTTP 503)"]


async def test_manual_push_outage_returns_503_and_auth_400(async_client):
    outage_exc = newznab.IndexerOutageError(
        "boom", INDEXER_URL, OutageVerdict("outage", "HTTP 503")
    )
    newznab._indexer_names["indexer.example.com"] = "NZBgeek"
    try:
        with patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=AsyncMock(side_effect=outage_exc),
        ):
            res = await async_client.post(
                "/api/torbox/add", data={"magnet": INDEXER_URL}
            )
        assert res.status_code == 503
        assert "Service outage: NZBgeek returned HTTP 503" in res.text

        auth_exc = newznab.IndexerAuthError(
            "<bad>", INDEXER_URL, OutageVerdict("auth", "HTTP 401")
        )
        with patch(
            "app.services.indexers.newznab.fetch_nzb_bytes",
            new=AsyncMock(side_effect=auth_exc),
        ):
            res = await async_client.post(
                "/api/torbox/add", data={"magnet": INDEXER_URL}
            )
        assert res.status_code == 400
        assert "<bad>" not in res.text
    finally:
        newznab._indexer_names.pop("indexer.example.com", None)


async def test_resume_endpoint_clears_outages(async_client, nzbgeek_outage):
    assert has_active_outage()
    res = await async_client.post("/api/pushes/resume-rate-limited")
    assert res.status_code == 200
    assert not has_active_outage()


async def _queue_adhoc(db_session, *, episode: int = 3):
    await make_indexer_provider(db_session, api_url=INDEXER_URL)
    await db_session.commit()
    record_outage("downloader", "torbox", "TorBox", "HTTP 500", 1)
    return await push_adhoc_release(
        db_session,
        release_link=f"{INDEXER_URL}?t=get&id=1",
        nzb_title="Show.S03E03.2160p.WEB-GRP",
        nzb_guid="g3",
        media_title="Show",
        media_kind="tv",
        tmdb_id=1,
        season=3,
        episode=episode,
        deliver=True,
    )


async def test_adhoc_outage_queue_flags_outcome_and_keeps_link(db_session):
    outcome = await _queue_adhoc(db_session)
    assert outcome.ok and outcome.queued
    (row,) = (await db_session.execute(select(DownloadHistory))).scalars().all()
    assert row.release_link == f"{INDEXER_URL}?t=get&id=1"
    assert row.deliver_enabled is True


async def test_resume_redispatches_adhoc_row(db_session):
    await _queue_adhoc(db_session)
    assert await resume_rate_limited_pushes() == 0  # still cooling down
    clear_outages()
    torbox.clear_cooldown()
    p1, p2, p3 = _patches()
    with p1, p2, p3:
        assert await resume_rate_limited_pushes() == 1
    db_session.expire_all()
    (row,) = (await db_session.execute(select(DownloadHistory))).scalars().all()
    assert row.status_detail == "downloading"
    assert row.downloader_transfer_id == "tb1"
    assert row.is_ad_hoc and row.target_label == "S03E03"
    assert row.deliver_enabled is True


async def test_resume_adhoc_hard_failure_marks_row_failed(db_session):
    await _queue_adhoc(db_session)
    clear_outages()
    torbox.clear_cooldown()
    upload = AsyncMock(
        return_value=(
            _client(),
            DownloaderUploadResult(success=False, detail="bad nzb"),
        )
    )
    p1, p2, p3 = _patches(upload=upload)
    with p1, p2, p3:
        await resume_rate_limited_pushes()
    db_session.expire_all()
    (row,) = (await db_session.execute(select(DownloadHistory))).scalars().all()
    assert (row.status_detail or "").startswith("failed")
