"""Deliver-to-Library toggle on push modal, drawer and push endpoints (m78)."""

from __future__ import annotations

import itertools
from datetime import datetime, timezone
from typing import Any

import pytest

from app.core.transfer_poller import get_active_pushes, get_push_history_ledger
from app.db.models import DeliveredFile
from app.services.delivery.hooks import deliver_override, parse_deliver
from tests.conftest import (
    make_delivery_provider,
    make_history,
    make_movie,
    seed_setup_ready,
)
from tests.test_adhoc_multilayer_search import (  # noqa: F401 - fixtures reused
    NEWZNAB_RELEASE,
    _auto_form,
    _history_rows,
    _push_form,
    _release_form,
    _scored,
    push_env,
    releases_spy,
    scored_results,
)

CHECKBOX = 'name="deliver"'


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        ("true", True),
        ("on", True),
        ("False", False),
        ("0", False),
        ("", None),
        (None, None),
        ("maybe", None),
    ],
)
def test_parse_deliver(value: Any, expected: bool | None) -> None:
    assert parse_deliver(value) is expected


async def _modal(client: Any, item_id: int) -> str:
    return (await client.get(f"/api/items/{item_id}/push-modal")).text


@pytest.mark.parametrize(
    "provider,visible,checked",
    [
        ("absent", False, None),
        ("inactive", True, "false"),
        ("active", True, "true"),
    ],
)
async def test_push_modal_deliver_toggle_states(
    async_client: Any,
    db_session: Any,
    provider: str,
    visible: bool,
    checked: str | None,
) -> None:
    item = await make_movie(db_session)
    if provider != "absent":
        p = await make_delivery_provider(db_session)
        p.is_active = provider == "active"
    await db_session.commit()
    html = await _modal(async_client, item.id)
    assert (CHECKBOX in html) is visible
    assert ("Deliver to Library" in html) is visible
    if checked is not None:
        assert f"deliver: {checked}," in html
    assert "deliver: this.deliver" in html  # sent with auto-push and manual grab


async def test_drawer_deliver_checkbox_states(
    async_client: Any,
    db_session: Any,
    releases_spy: dict[str, Any],  # noqa: F811
) -> None:
    releases_spy["primary"] = [
        {**_scored(NEWZNAB_RELEASE["title"], 9000.0), "feature_pills": []}
    ]
    html = (
        await async_client.post("/api/search/manual/releases", data=_release_form())
    ).text
    assert "Deliver to Library" not in html

    provider = await make_delivery_provider(db_session)
    await db_session.commit()
    html = (
        await async_client.post("/api/search/manual/releases", data=_release_form())
    ).text
    assert "Deliver to Library" in html
    assert 'name="deliver" value="true" checked' in html
    assert 'hx-include="#drawer-tmdb-1396-s1-e1-deliver"' in html

    provider.is_active = False
    await db_session.commit()
    html = (
        await async_client.post("/api/search/manual/releases", data=_release_form())
    ).text
    assert 'name="deliver" value="true" checked' not in html


@pytest.mark.parametrize(
    "form_extra,active,expected",
    [
        ({"deliver": "false"}, True, False),
        ({"deliver": "true"}, False, True),
        ({}, True, True),
        ({}, False, False),
    ],
)
@pytest.mark.needs_downloader
async def test_adhoc_push_stores_deliver_flag(
    async_client: Any,
    db_session: Any,
    push_env: dict[str, Any],  # noqa: F811
    form_extra: dict[str, str],
    active: bool,
    expected: bool,
) -> None:
    provider = await make_delivery_provider(db_session)
    provider.is_active = active
    await db_session.commit()
    resp = await async_client.post(
        "/api/search/manual/push", data=_push_form(**form_extra)
    )
    assert resp.status_code == 200
    db_session.expire_all()
    (row,) = await _history_rows(db_session)
    assert row.deliver_enabled is expected


@pytest.mark.needs_downloader
async def test_adhoc_auto_push_honours_deliver_false(
    async_client: Any,
    db_session: Any,
    push_env: dict[str, Any],  # noqa: F811
    scored_results: dict[str, Any],  # noqa: F811
) -> None:
    await make_delivery_provider(db_session)
    await db_session.commit()
    scored_results["primary"] = [_scored("Best.Primary", 9000)]
    resp = await async_client.post(
        "/api/search/manual/auto-push", data=_auto_form(deliver="false")
    )
    assert "Pushed best" in resp.text
    db_session.expire_all()
    (row,) = await _history_rows(db_session)
    assert row.deliver_enabled is False


async def test_deliver_override_reaches_resolver(db_session: Any) -> None:
    from app.services.delivery.hooks import resolve_deliver_enabled

    await make_delivery_provider(db_session)
    await db_session.commit()
    assert await resolve_deliver_enabled(db_session, None) is True
    token = deliver_override.set(False)
    try:
        assert await resolve_deliver_enabled(db_session, None) is False
        assert await resolve_deliver_enabled(db_session, True) is True
    finally:
        deliver_override.reset(token)


# --- Card 4 / Card 5 delivery UI, retry & deliver-now (m78-15) -----------------


@pytest.fixture
def no_enqueue(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    queued: list[int] = []

    async def fake(history_id: int) -> None:
        queued.append(history_id)

    monkeypatch.setattr("app.services.delivery.worker.enqueue_delivery", fake)
    return queued


_ids = itertools.count(1)


async def _delivery_row(session: Any, **overrides: Any) -> Any:
    n = next(_ids)
    item = await make_movie(
        session, title=f"Movie {n}", simkl_id=20000 + n, tmdb_id=30000 + n
    )
    row = await make_history(
        session,
        item=item,
        is_dismissed=False,
        completed_at=datetime.now(timezone.utc),
        deliver_enabled=True,
        **overrides,
    )
    await session.commit()
    await session.refresh(row)
    return row


async def _active_html(client: Any) -> str:
    return (await client.get("/api/pushes/active", headers={"HX-Request": "true"})).text


@pytest.mark.parametrize(
    "status,downloader,expected,absent",
    [
        ("delivering", "torbox", "DELIVERING", "Retry Delivery"),
        ("delivering", "sabnzbd", "Filing...", "Retry Delivery"),
        ("pending", "torbox", "Delivery queued", "Retry Delivery"),
        ("failed", "torbox", "Retry Delivery", "Dismiss-none"),
        ("paused", "torbox", "Retry Delivery", "Dismiss-none"),
    ],
)
async def test_card5_delivery_states(
    async_client: Any,
    db_session: Any,
    status: str,
    downloader: str,
    expected: str,
    absent: str,
) -> None:
    row = await _delivery_row(
        db_session,
        delivery_status=status,
        downloader_type=downloader,
        progress_pct=40.0,
        # completed long ago: delivery rows outlive the 5-minute retention
        failed_at=None,
    )
    row.completed_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    await db_session.commit()
    await db_session.refresh(row)
    html = await _active_html(async_client)
    assert f'data-delivery-status="{status}"' in html
    assert expected in html
    assert (absent in html) is False
    if status in ("delivering", "pending"):
        assert f"/api/pushes/{row.id}/dismiss" not in html  # waits until settled
        assert "/api/pushes/{}/cancel".format(row.id) not in html
    else:
        assert f"/api/history/{row.id}/retry-delivery" in html


async def test_card5_delivering_exposes_contract_keys(db_session: Any) -> None:
    row = await _delivery_row(
        db_session, delivery_status="delivering", progress_pct=55.0
    )
    (push,) = await get_active_pushes(db_session)
    assert push["history_id"] == row.id
    assert push["status"] == "DELIVERING"
    assert push["progress_pct"] == 55.0
    assert push["deliver_enabled"] is True
    assert push["delivery_status"] == "delivering"
    assert push["can_retry_delivery"] is False
    assert push["delivered_paths"] == []


async def test_card5_settled_old_row_not_listed(db_session: Any) -> None:
    row = await _delivery_row(db_session, delivery_status="delivered")
    row.completed_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    await db_session.commit()
    assert await get_active_pushes(db_session) == []


async def test_dismiss_all_keeps_in_flight_delivery(
    async_client: Any, db_session: Any
) -> None:
    busy = await _delivery_row(db_session, delivery_status="delivering")
    done = await _delivery_row(db_session, delivery_status="delivered", nzb_guid="g2")
    resp = await async_client.post("/api/pushes/dismiss-all")
    assert resp.status_code == 200
    db_session.expire_all()
    await db_session.refresh(busy)
    await db_session.refresh(done)
    assert busy.is_dismissed is False
    assert done.is_dismissed is True


async def test_card4_ledger_delivery_contract(db_session: Any) -> None:
    await make_delivery_provider(db_session)
    delivered = await _delivery_row(db_session, delivery_status="delivered")
    delivered.delivered_at = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    db_session.add(
        DeliveredFile(
            history_id=delivered.id,
            source_name="a.mkv",
            dest_path="/media/movies/A (2024)/A (2024).mkv",
            kind="video",
            status="delivered",
        )
    )
    failed = await _delivery_row(
        db_session,
        delivery_status="failed",
        delivery_error="TARGET_EXISTS",
        nzb_guid="g2",
    )
    plain = await _delivery_row(db_session, nzb_guid="g3")
    await db_session.commit()
    by_id = {r["id"]: r for r in await get_push_history_ledger(db_session)}
    d = by_id[delivered.id]
    assert d["delivered_paths"] == ["/media/movies/A (2024)/A (2024).mkv"]
    assert d["delivered_at"] is not None
    assert (d["can_retry_delivery"], d["can_deliver_now"]) == (False, False)
    f = by_id[failed.id]
    assert (f["can_retry_delivery"], f["delivery_error"]) == (True, "TARGET_EXISTS")
    assert by_id[plain.id]["can_deliver_now"] is True  # provider active, status none


async def test_card4_renders_badge_and_buttons(
    async_client: Any, db_session: Any
) -> None:
    await seed_setup_ready(db_session)
    await make_delivery_provider(db_session)
    delivered = await _delivery_row(db_session, delivery_status="delivered")
    db_session.add(
        DeliveredFile(
            history_id=delivered.id,
            source_name="a.mkv",
            dest_path="/media/movies/Tooltip Path/a.mkv",
            kind="video",
            status="delivered",
        )
    )
    failed = await _delivery_row(db_session, delivery_status="failed", nzb_guid="g2")
    plain = await _delivery_row(db_session, nzb_guid="g3")
    await db_session.commit()
    for r in (delivered, failed, plain):
        await db_session.refresh(r)
    html = (await async_client.get("/")).text
    assert 'data-delivery-badge="delivered"' in html
    assert "/media/movies/Tooltip Path/a.mkv" in html
    assert f"/api/history/{failed.id}/retry-delivery" in html
    assert f"/api/history/{plain.id}/deliver" in html
    assert f"/api/history/{delivered.id}/deliver" not in html
    assert f"/api/history/{failed.id}/deliver" not in html  # retry takes precedence


async def test_card4_no_deliver_button_without_active_provider(
    async_client: Any, db_session: Any
) -> None:
    await seed_setup_ready(db_session)
    plain = await _delivery_row(db_session)
    html = (await async_client.get("/")).text
    assert f"/api/history/{plain.id}/deliver" not in html


@pytest.mark.parametrize("status", ["none", "pending", "delivering", "delivered"])
async def test_retry_delivery_409_for_illegal_state(
    async_client: Any, db_session: Any, no_enqueue: list[int], status: str
) -> None:
    row = await _delivery_row(db_session, delivery_status=status)
    resp = await async_client.post(f"/api/history/{row.id}/retry-delivery")
    assert resp.status_code == 409
    assert no_enqueue == []


async def test_retry_delivery_requeues_failed(
    async_client: Any, db_session: Any, no_enqueue: list[int]
) -> None:
    row = await _delivery_row(
        db_session, delivery_status="failed", delivery_error="LINK_EXPIRED"
    )
    resp = await async_client.post(f"/api/history/{row.id}/retry-delivery")
    assert resp.status_code == 200
    assert "refreshDashboard" in resp.headers["HX-Trigger"]
    assert no_enqueue == [row.id]
    db_session.expire_all()
    await db_session.refresh(row)
    assert (row.delivery_status, row.delivery_error) == ("pending", None)


async def test_deliver_now_flow_and_denials(
    async_client: Any, db_session: Any, no_enqueue: list[int]
) -> None:
    ok = await _delivery_row(db_session)
    sab = await _delivery_row(db_session, downloader_type="sabnzbd", nzb_guid="g2")
    deleted = await _delivery_row(db_session, status_detail="deleted", nzb_guid="g3")
    for denied in (sab, deleted, ok.id + 999):
        hid = denied if isinstance(denied, int) else denied.id
        resp = await async_client.post(f"/api/history/{hid}/deliver")
        assert resp.status_code == 409
    resp = await async_client.post(f"/api/history/{ok.id}/deliver")
    assert resp.status_code == 200
    assert "refreshDashboard" in resp.headers["HX-Trigger"]
    assert no_enqueue == [ok.id]
    db_session.expire_all()
    await db_session.refresh(ok)
    assert (ok.delivery_status, ok.deliver_enabled) == ("pending", True)
