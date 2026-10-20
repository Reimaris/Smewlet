"""Active Pushes: section assignment, queue position, ordering, retention."""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from app.core.transfer_poller import assign_section, get_active_pushes
from app.db.models import MediaStatus
from app.services.delivery import offline, worker
from tests.conftest import make_history, make_movie

_ids = itertools.count(1)


def _ago(minutes: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=minutes)


async def _row(session: Any, **overrides: Any) -> Any:
    n = next(_ids)
    item = await make_movie(
        session,
        title=f"Movie {n}",
        simkl_id=40000 + n,
        tmdb_id=50000 + n,
        status=MediaStatus.DOWNLOADED,
    )
    fields: dict[str, Any] = dict(
        is_dismissed=False,
        completed_at=_ago(10),
        deliver_enabled=True,
        nzb_guid=f"guid-sec-{n}",
    )
    fields.update(overrides)
    row = await make_history(session, item=item, **fields)
    await session.commit()
    return row


@pytest.mark.parametrize(
    ("overrides", "section"),
    [
        # failed download: no time limit
        (
            dict(status_detail="failed: boom", failed_at=_ago(60), completed_at=None),
            "downloads",
        ),
        # completed, not delivered: hidden after 5 min, shown before
        (dict(delivery_status="none", completed_at=_ago(10)), None),
        (dict(delivery_status="none", completed_at=_ago(1)), "downloads"),
        # delivered: 5 minutes after delivered_at
        (dict(delivery_status="delivered", delivered_at=_ago(1)), "deliveries"),
        (dict(delivery_status="delivered", delivered_at=_ago(10)), None),
        # failed / attention pauses persist
        (dict(delivery_status="failed", delivery_error="X"), "deliveries"),
        (
            dict(delivery_status="paused", delivery_error="INSUFFICIENT_STORAGE"),
            "deliveries",
        ),
        # aborted delivery: leaves Deliveries, downloads only inside the window
        (
            dict(delivery_status="none", deliver_enabled=False, completed_at=_ago(1)),
            "downloads",
        ),
        (
            dict(delivery_status="none", deliver_enabled=False, completed_at=_ago(10)),
            None,
        ),
    ],
)
async def test_retention_and_section(db_session, overrides, section) -> None:
    await _row(db_session, **overrides)

    rows = await get_active_pushes(db_session)

    assert [r["section"] for r in rows] == ([section] if section else [])


async def test_rate_limited_row_stays_in_downloads(db_session) -> None:
    await _row(
        db_session,
        status_detail="Indexer rate limit reached",
        completed_at=None,
        delivery_status="none",
    )
    (row,) = await get_active_pushes(db_session)
    assert (row["section"], row["is_rate_limited"]) == ("downloads", True)


async def test_queue_positions_and_ordering(db_session, monkeypatch) -> None:
    done = await _row(db_session, delivery_status="delivered", delivered_at=_ago(2))
    failed = await _row(db_session, delivery_status="failed", delivery_error="X")
    q3 = await _row(db_session, delivery_status="pending")
    q1 = await _row(db_session, delivery_status="pending")
    q2 = await _row(db_session, delivery_status="pending")
    active = await _row(db_session, delivery_status="delivering")
    dl = await _row(db_session, delivery_status="none", completed_at=_ago(1))
    queue = [q1.id, q2.id, q3.id]
    monkeypatch.setattr(worker, "_queued_ids", list(queue))

    rows = await get_active_pushes(db_session)

    assert [r["history_id"] for r in rows] == [
        dl.id,
        active.id,
        q1.id,
        q2.id,
        q3.id,
        failed.id,
        done.id,
    ]
    queued = {r["history_id"]: r for r in rows if r["delivery_status"] == "pending"}
    assert [
        (queued[i]["queue_position"], queued[i]["queue_total"], queued[i]["status"])
        for i in queue
    ] == [(n, 3, f"Delivery queued ({n} of 3)") for n in (1, 2, 3)]
    assert rows[1]["queue_position"] is None


async def test_storage_offline_row_badge_and_flag(db_session, monkeypatch) -> None:
    await _row(db_session, delivery_status="paused", delivery_error="STORAGE_OFFLINE")
    monkeypatch.setattr(offline, "_offline", True)
    monkeypatch.setattr(
        offline, "_next_check", datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    )

    (row,) = await get_active_pushes(db_session)

    assert row["is_delivery_paused"] is True
    assert row["next_check_label"] and len(row["next_check_label"]) == 5
    assert row["status"] == (
        f"Delivery paused: storage offline (next check {row['next_check_label']})"
    )


def test_assign_section_plain_rows() -> None:
    assert assign_section({"delivery_status": "none"}) == "downloads"
    assert assign_section({"delivery_status": "pending"}) == "deliveries"
    assert assign_section({"delivery_status": "delivered"}) == "downloads"


async def test_detached_ad_hoc_delivery_row_is_listed(db_session, monkeypatch) -> None:
    row = await make_history(
        db_session,
        is_ad_hoc=True,
        media_title="Ad Hoc Movie",
        media_year=2026,
        media_type_label="movie",
        target_label="Movie",
        is_dismissed=False,
        completed_at=_ago(1),
        deliver_enabled=True,
        delivery_status="pending",
        nzb_guid="guid-adhoc-1",
    )
    await db_session.commit()
    monkeypatch.setattr(worker, "_queued_ids", [row.id])

    (r,) = await get_active_pushes(db_session)

    assert r["section"] == "deliveries"
    assert r["title_and_target"] == "Ad Hoc Movie (2026)"
    assert r["queue_position"] == 1
    assert r["media_item_id"] is None
