"""Schema and migration tests for local directory delivery (m78)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import close_db, get_engine, init_db
from app.db.models import (
    DeliveredFile,
    DeliveryStatus,
    DownloadHistory,
    Provider,
    ProviderCategory,
    SystemSettings,
)
from app.services import provider_service
from app.services.delivery.config import DeliveryConfig, validate_templates
from tests.conftest import make_delivery_provider, make_history, make_movie

HISTORY_COLUMNS = (
    "deliver_enabled",
    "delivery_status",
    "delivered_at",
    "delivery_error",
)
SETTINGS_COLUMNS = ("notify_on_delivery_failed", "notify_on_delivered")


def test_enum_values() -> None:
    assert ProviderCategory.DELIVERY.value == "delivery"
    assert {s.value for s in DeliveryStatus} == {
        "none",
        "pending",
        "delivering",
        "delivered",
        "failed",
        "paused",
    }


async def test_history_delivery_defaults(db_session: AsyncSession) -> None:
    item = await make_movie(db_session)
    history = await make_history(db_session, item=item)
    await db_session.commit()
    await db_session.refresh(history)
    assert history.deliver_enabled is False
    assert history.delivery_status == "none"
    assert history.delivered_at is None
    assert history.delivery_error is None


async def test_settings_notification_defaults(db_session: AsyncSession) -> None:
    db_session.add(SystemSettings())
    await db_session.commit()
    settings = (await db_session.execute(select(SystemSettings))).scalar_one()
    assert settings.notify_on_delivery_failed is True
    assert settings.notify_on_delivered is False


async def test_make_delivery_provider(db_session: AsyncSession) -> None:
    provider = await make_delivery_provider(db_session, movies_path="/media/movies")
    assert provider.category == "delivery"
    assert provider.type == "local_directory"
    assert provider.is_active is True
    assert '"movies_path"' in provider.config_json


async def test_delivered_files_cascade_on_history_delete(
    db_session: AsyncSession,
) -> None:
    item = await make_movie(db_session)
    history = await make_history(db_session, item=item)
    db_session.add(
        DeliveredFile(history_id=history.id, source_name="a.mkv", kind="video")
    )
    await db_session.commit()
    row = (await db_session.execute(select(DeliveredFile))).scalar_one()
    assert row.status == "pending"
    assert row.dest_path is None

    await db_session.delete(await db_session.get(DownloadHistory, history.id))
    await db_session.commit()
    assert (await db_session.execute(select(DeliveredFile))).scalars().all() == []


async def _columns(table: str) -> set[str]:
    async with get_engine().connect() as conn:
        rows = await conn.execute(text(f'PRAGMA table_info("{table}")'))
        return {r[1] for r in rows}


async def test_migration_adds_columns_and_is_idempotent(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'legacy.db'}"
    await close_db()
    await init_db(url)
    async with get_engine().begin() as conn:
        for col in HISTORY_COLUMNS:
            await conn.execute(text(f"ALTER TABLE download_history DROP COLUMN {col}"))
        for col in SETTINGS_COLUMNS:
            await conn.execute(text(f"ALTER TABLE system_settings DROP COLUMN {col}"))
        await conn.execute(text("DROP TABLE delivered_files"))
    await close_db()

    for _ in range(2):
        await init_db(url)
        assert set(HISTORY_COLUMNS) <= await _columns("download_history")
        assert set(SETTINGS_COLUMNS) <= await _columns("system_settings")
        assert "dest_path" in await _columns("delivered_files")
        await close_db()
    await init_db(url)


def _cfg(raw: str) -> DeliveryConfig:
    return DeliveryConfig.from_provider(Provider(config_json=raw))


def test_config_defaults_for_empty_json() -> None:
    cfg = _cfg("{}")
    assert cfg == DeliveryConfig()
    assert cfg.movies_path == ""
    assert cfg.min_free_buffer_bytes == 5368709120
    assert cfg.delete_cloud_after_delivery is False
    assert cfg.delivery_max_parallel == 1
    assert validate_templates(cfg) == []


@pytest.mark.parametrize("raw", ["not json", "[]", "null", ""])
def test_config_malformed_json_uses_defaults(raw: str) -> None:
    assert _cfg(raw) == DeliveryConfig()


def test_config_wrong_types_coerce_and_valid_values_kept() -> None:
    cfg = _cfg(
        '{"movies_path": 5, "shows_path": "/tv", "min_free_buffer_bytes": "x",'
        ' "delete_cloud_after_delivery": 1, "anime_template": null}'
    )
    assert cfg.movies_path == ""
    assert cfg.shows_path == "/tv"
    assert cfg.min_free_buffer_bytes == 5368709120
    assert cfg.delete_cloud_after_delivery is False
    assert cfg.anime_template == DeliveryConfig().anime_template


def test_config_max_parallel_pinned_to_one() -> None:
    assert _cfg('{"delivery_max_parallel": 5}').delivery_max_parallel == 1


def test_validate_templates_reports_unknown_placeholders() -> None:
    cfg = _cfg(
        '{"movie_template": "{foo}/{title}.{ext}", "show_template": "{bar:02d}{foo}"}'
    )
    assert validate_templates(cfg) == ["bar", "foo"]


async def test_second_delivery_provider_rejected(db_session: AsyncSession) -> None:
    first = await provider_service.create_delivery_provider(db_session)
    assert first.is_active is False
    with pytest.raises(
        provider_service.DeliveryProviderExistsError, match="already exists"
    ):
        await provider_service.create_delivery_provider(db_session)


async def test_inactive_delivery_provider_lookup(db_session: AsyncSession) -> None:
    assert await provider_service.get_delivery_provider(db_session) is None
    provider = await provider_service.create_delivery_provider(db_session)
    assert await provider_service.get_delivery_provider(db_session) is provider
    assert await provider_service.get_active_delivery_provider(db_session) is None
    provider.is_active = True
    assert await provider_service.get_active_delivery_provider(db_session) is provider


# --- Settings routes (m78-12) -------------------------------------------------


async def _save_delivery(client, **data):  # type: ignore[no-untyped-def]
    base = {"name": "Local directory", "provider_type": "local_directory"}
    return await client.post("/settings/provider", data={**base, **data})


async def _delivery_row(session: AsyncSession) -> Provider:
    session.expire_all()
    provider = await provider_service.get_delivery_provider(session)
    assert provider is not None
    return provider


async def test_create_delivery_provider_inactive_and_persists_config(
    async_client, db_session: AsyncSession, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    movies = tmp_path / "movies"
    resp = await _save_delivery(
        async_client,
        is_active="true",  # creation forces inactive
        movies_path=str(movies),
        min_free_buffer_gb="2",
        delete_cloud_after_delivery="true",
        sab_path_map_remote="/downloads",
        sab_path_map_local="/mnt/dl",
    )
    assert resp.status_code == 200
    provider = await _delivery_row(db_session)
    assert provider.is_active is False
    assert provider.category == "delivery"
    cfg = DeliveryConfig.from_provider(provider)
    assert cfg.movies_path == str(movies)
    assert cfg.min_free_buffer_bytes == 2 * 1024**3
    assert cfg.delete_cloud_after_delivery is True
    assert (cfg.sab_path_map_remote, cfg.sab_path_map_local) == (
        "/downloads",
        "/mnt/dl",
    )
    assert cfg.show_template == DeliveryConfig().show_template


async def test_edit_delivery_provider_can_activate(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    provider = await make_delivery_provider(db_session, movies_path="/old")
    provider.is_active = False
    await db_session.commit()
    resp = await async_client.post(
        f"/settings/provider/{provider.id}",
        data={
            "name": "Local directory",
            "provider_type": "local_directory",
            "is_active": "true",
            "movies_path": "/new",
        },
    )
    assert resp.status_code == 200
    row = await _delivery_row(db_session)
    assert row.is_active is True
    assert DeliveryConfig.from_provider(row).movies_path == "/new"


async def test_second_delivery_provider_returns_409(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    await make_delivery_provider(db_session)
    await db_session.commit()
    resp = await _save_delivery(async_client)
    assert resp.status_code == 409
    assert "A delivery provider already exists" in resp.text


async def test_unknown_placeholder_returns_422(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    resp = await _save_delivery(async_client, movie_template="{foo}/{title}.{ext}")
    assert resp.status_code == 422
    assert "foo" in resp.text
    assert await provider_service.get_delivery_provider(db_session) is None


@pytest.mark.parametrize("field", ["movies_path", "shows_path", "anime_path"])
async def test_relative_path_returns_422(async_client, field: str) -> None:  # type: ignore[no-untyped-def]
    resp = await _save_delivery(async_client, **{field: "relative/dir"})
    assert resp.status_code == 422
    assert "absolute" in resp.text


async def test_test_endpoint_reports_per_path(async_client, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    good = tmp_path / "ok"
    good.mkdir()
    missing = tmp_path / "missing"
    resp = await async_client.post(
        "/api/providers/test",
        data={
            "provider_type": "local_directory",
            "movies_path": str(good),
            "shows_path": str(missing),
        },
    )
    body = resp.json()
    assert body["ok"] is False
    paths = {p["field"]: p for p in body["paths"]}
    assert paths["movies_path"]["ok"] is True
    assert paths["shows_path"]["ok"] is False
    assert list(good.iterdir()) == []  # temp probe file removed
    assert "anime_path" not in paths  # unconfigured paths are skipped


async def test_test_endpoint_without_paths_fails(async_client) -> None:  # type: ignore[no-untyped-def]
    resp = await async_client.post(
        "/api/providers/test", data={"provider_type": "local_directory"}
    )
    assert resp.json()["ok"] is False


async def test_delivery_notification_toggles_persist(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    resp = await async_client.post(
        "/settings/auto-save",
        data={"notify_on_delivery_failed": "false", "notify_on_delivered": "true"},
    )
    assert resp.status_code == 200
    db_session.expire_all()
    settings = (await db_session.execute(select(SystemSettings))).scalars().one()
    assert settings.notify_on_delivery_failed is False
    assert settings.notify_on_delivered is True


async def test_settings_page_and_modal_expose_delivery(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    modal = (await async_client.get("/settings/provider/new")).text
    assert "selectProvider('local_directory', 'delivery'" in modal
    await make_delivery_provider(db_session, movies_path="/m")
    await db_session.commit()
    page = (await async_client.get("/settings")).text
    assert "Delivery" in page
    assert 'name="notify_on_delivered"' in page
    assert 'name="notify_on_delivery_failed"' in page


async def test_edit_modal_prefills_delivery_config(
    async_client, db_session: AsyncSession
) -> None:  # type: ignore[no-untyped-def]
    provider = await make_delivery_provider(
        db_session, movies_path="/data/films", min_free_buffer_bytes=2 * 1024**3
    )
    await db_session.commit()
    html = (await async_client.get(f"/settings/provider/{provider.id}/edit")).text
    assert 'value="/data/films"' in html
    assert 'name="min_free_buffer_gb" value="2"' in html
