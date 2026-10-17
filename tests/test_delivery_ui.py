"""Deliver-to-Library toggle on push modal, drawer and push endpoints (m78)."""

from __future__ import annotations

from typing import Any

import pytest

from app.services.delivery.hooks import deliver_override, parse_deliver
from tests.conftest import make_delivery_provider, make_movie
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
