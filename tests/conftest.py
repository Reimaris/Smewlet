"""
Shared Test Fixtures & Domain Factories
=======================================
Provides a centralized in-memory SQLite database, session factory, ASGI HTTP test
client, state resets, and reusable domain entity fixtures for all tests.

Domain Factory Helpers (importable async functions)
----------------------------------------------------
- VALID_NZB_BYTES          : Minimal NZB XML bytes that pass Layer 2 inspection.
- make_movie(session, **overrides) -> MediaItem
- make_series(session, *, seasons, episodes_per_season, episode_statuses, **overrides)
      -> tuple[MediaItem, list[Season], list[Episode]]
- make_history(session, *, item, season, episode, **overrides) -> DownloadHistory
- make_watchlist_provider(session, type, **overrides) -> Provider
- make_indexer_provider(session, **overrides) -> Provider
- make_downloader_provider(session, type="torbox", **overrides) -> Provider
- make_source(session, item, provider, list_status, remote_id, last_watched_order)
      -> MediaItemWatchlistSource
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.db.database import close_db, get_engine, get_session_factory, init_db
from app.db.models import (
    Base,
    DownloadHistory,
    Episode,
    EpisodeStatus,
    MediaItem,
    MediaItemWatchlistSource,
    MediaStatus,
    MediaType,
    Provider,
    SearchPreset,
    Season,
    SeasonStatus,
)
from app.main import app
from app.services.delivery import engines as delivery_engines
from app.services.delivery import worker as delivery_worker
from app.services.downloaders import torbox
from app.services.downloaders.base import (
    DownloaderFile,
    DownloaderTransfer,
    DownloadLink,
    TransferState,
)
from app.services.watchlists import simkl

# Override DB URL for tests
settings.database_url = "sqlite+aiosqlite:///:memory:"

# ---------------------------------------------------------------------------
# Shared constant: minimal NZB XML that passes Layer 2 fake detection
# ---------------------------------------------------------------------------
VALID_NZB_BYTES: bytes = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">'
    b'  <file poster="test" date="1700000000"'
    b'        subject="Test.Release.1080p.WEB-DL.mkv (1/1)">'
    b"    <groups><group>alt.binaries.test</group></groups>"
    b"    <segments>"
    b'      <segment bytes="500000000" number="1">seg1@test</segment>'
    b"    </segments>"
    b"  </file>"
    b"</nzb>"
)


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_setup_guard():
    """Setup guard is opt-in per test (m64); restore the previous value afterwards."""
    from app.main import app

    previous = app.state.setup_guard_enabled
    app.state.setup_guard_enabled = False
    yield
    app.state.setup_guard_enabled = previous


@pytest.fixture(autouse=True)
def disable_job_guard():
    """Background-job setup guard is opt-in per test (m64); restore afterwards."""
    from app.core import prerequisites

    previous = prerequisites.JOB_GUARD_ENABLED
    prerequisites.JOB_GUARD_ENABLED = False
    prerequisites._last_blocked.clear()
    yield
    prerequisites.JOB_GUARD_ENABLED = previous
    prerequisites._last_blocked.clear()


@pytest.fixture(autouse=True)
def restore_app_logger_level():
    """reload_settings_from_db toggles the `app` logger level; undo it per test."""
    app_logger = logging.getLogger("app")
    level = app_logger.level
    yield
    app_logger.setLevel(level)


@pytest.fixture(autouse=True)
def reset_downloader_state():
    from app.core.transfer_poller import clear_dry_run_pushes, transfer_poller
    from app.services import provider_service

    provider_service._remember_active(None)
    torbox.set_cooldown(0.0)
    torbox.clear_usenet_cache()
    simkl._SIMKL_DEVICE_SESSIONS.clear()
    clear_dry_run_pushes()
    transfer_poller.in_flight_pushes.clear()
    transfer_poller.set_scheduler(None)
    transfer_poller.sleep(0)
    from app.core.outage import clear_outages
    from app.core.rate_limit_queue import rate_limit_episode
    from app.services.indexers.newznab import clear_all_indexer_cooldowns

    clear_all_indexer_cooldowns()
    clear_outages()
    rate_limit_episode.reset()
    yield
    clear_all_indexer_cooldowns()
    clear_outages()
    torbox.set_cooldown(0.0)
    torbox.clear_usenet_cache()
    simkl._SIMKL_DEVICE_SESSIONS.clear()
    clear_dry_run_pushes()
    transfer_poller.in_flight_pushes.clear()
    transfer_poller.set_scheduler(None)
    transfer_poller.sleep(0)


@pytest.fixture(autouse=True)
async def setup_db():
    await init_db(settings.database_url)
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA foreign_keys = OFF;")
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.exec_driver_sql("PRAGMA foreign_keys = ON;")
    yield
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA foreign_keys = OFF;")
        await conn.run_sync(Base.metadata.drop_all)
    await close_db()


_DISPATCHING_MODULES = frozenset(
    {
        "test_push_modal_and_dispatch",
        "test_adhoc_multilayer_search",
        "test_episode_upgrades",
        "test_unified_push_modal_and_v3_contract",
        "test_season_pack_matrix",
        "test_rate_limit_queue",
        "test_season_expansion",
        "test_simkl_pruning_and_history_snapshots",
        "test_discord_and_notifications",
        "test_poller_mutex_and_batch_notifications",
        "test_transfer_poller_and_recovery",
        "test_debug_and_dry_run",
        "test_dry_run_simulation",
        "test_fake_detector",
        "test_transfer_poller",
    }
)


@pytest.fixture(autouse=True)
async def seeded_torbox_downloader(setup_db, request):
    """Seed an active TorBox downloader for suites that dispatch NZBs (m57-06).

    Other suites keep an empty provider table; opt in elsewhere with
    ``@pytest.mark.needs_downloader``.
    """
    module = request.module.__name__.rsplit(".", 1)[-1]
    if module not in _DISPATCHING_MODULES and not request.node.get_closest_marker(
        "needs_downloader"
    ):
        yield
        return
    async with get_session_factory()() as session:
        await make_downloader_provider(
            session, "torbox", name="TorBox", api_key="seed-torbox-key"
        )
        await session.commit()
    yield


@pytest.fixture
def session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the shared async session factory bound to the in-memory test database."""
    return get_session_factory()


@pytest.fixture
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Yield a managed AsyncSession bound to the shared in-memory test database."""
    async with session_factory() as session:
        yield session


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Yield a shared ASGI HTTPX AsyncClient targeting the FastAPI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def watchlist_provider(db_session: AsyncSession) -> Provider:
    """Seed and return an active Simkl watchlist Provider."""
    provider = Provider(
        name="Simkl",
        type="simkl",
        category="watchlist",
        is_active=True,
    )
    db_session.add(provider)
    await db_session.commit()
    await db_session.refresh(provider)
    return provider


@pytest.fixture
async def default_search_preset(db_session: AsyncSession) -> SearchPreset:
    """Seed and return the default 'Default (Best)' SearchPreset."""
    preset = SearchPreset(
        name="Default (Best)",
        is_default=True,
        primary_language="en",
        fallback_language=None,
        video_quality_mode="best",
        audio_quality_mode="best",
        allow_season_packs=False,
        prefer_season_packs=False,
        custom_config_json="{}",
    )
    db_session.add(preset)
    await db_session.commit()
    await db_session.refresh(preset)
    return preset


# ---------------------------------------------------------------------------
# Domain Entity Factory Helpers
# ---------------------------------------------------------------------------


async def _ensure_watchlist_provider(
    session: AsyncSession, provider_id: int | None
) -> int:
    """Return *provider_id* unchanged, or seed and return a default watchlist Provider's id."""
    if provider_id is not None:
        return provider_id
    from sqlalchemy import select

    existing = (
        (
            await session.execute(
                select(Provider).where(Provider.category == "watchlist").limit(1)
            )
        )
        .scalars()
        .first()
    )
    if existing is not None:
        return existing.id
    provider = Provider(
        name="Simkl", type="simkl", category="watchlist", is_active=True
    )
    session.add(provider)
    await session.flush()
    return provider.id  # type: ignore[return-value]


async def make_movie(session: AsyncSession, **overrides: Any) -> MediaItem:
    """Create, flush, and return a ``MediaItem`` with ``media_type=MOVIE``.

    Sensible defaults (all overridable via keyword arguments):

    .. code-block:: python

        title="Test Movie"
        year=2024
        status=MediaStatus.SEARCHING
        simkl_id=10001
        provider_id=<auto-seeded watchlist provider>
    """
    provider_id = await _ensure_watchlist_provider(
        session, overrides.pop("provider_id", None)
    )
    defaults: dict[str, Any] = dict(
        provider_id=provider_id,
        simkl_id=10001,
        title="Test Movie",
        year=2024,
        media_type=MediaType.MOVIE,
        status=MediaStatus.SEARCHING,
    )
    defaults.update(overrides)
    item = MediaItem(**defaults)
    session.add(item)
    await session.flush()
    return item


async def make_series(
    session: AsyncSession,
    *,
    seasons: list[dict[str, Any]] | int = 1,
    episodes_per_season: int = 0,
    episode_statuses: list[EpisodeStatus] | None = None,
    **overrides: Any,
) -> tuple[MediaItem, list[Season], list[Episode]]:
    """Create, flush, and return a ``MediaItem`` (SHOW or ANIME), its Season rows,
    and any requested Episode rows.

    Parameters
    ----------
    seasons:
        • ``int`` — create *N* seasons with sequential ``season_number``,
          ``watch_order``, and ``type_number`` (1-based), all ``monitored=True``,
          ``status=SeasonStatus.SEARCHING``.
        • ``list[dict]`` — each dict is passed directly as keyword arguments to
          ``Season(...)``.  Missing keys fall back to the per-season defaults above.
    episodes_per_season:
        How many ``Episode`` rows to create per season (skipped when 0).
    episode_statuses:
        Optional list of ``EpisodeStatus`` values used round-robin across all
        episodes when *episodes_per_season* > 0.  Defaults to ``SEARCHING``.
    **overrides:
        Forwarded to ``MediaItem(...)`` after applying defaults.

    Returns
    -------
    tuple[MediaItem, list[Season], list[Episode]]
        The parent item, a list of ``Season`` objects (in creation order), and a
        flat list of all ``Episode`` objects (all seasons, in creation order).
    """
    provider_id = await _ensure_watchlist_provider(
        session, overrides.pop("provider_id", None)
    )
    item_defaults: dict[str, Any] = dict(
        provider_id=provider_id,
        simkl_id=20001,
        title="Test Series",
        year=2024,
        media_type=MediaType.SHOW,
        status=MediaStatus.SEARCHING,
    )
    item_defaults.update(overrides)
    item = MediaItem(**item_defaults)
    session.add(item)
    await session.flush()

    # Normalise *seasons* argument
    season_specs: list[dict[str, Any]]
    if isinstance(seasons, int):
        season_specs = [{"season_number": n} for n in range(1, seasons + 1)]
    else:
        season_specs = list(seasons)

    season_objs: list[Season] = []
    episode_objs: list[Episode] = []
    statuses = episode_statuses or [EpisodeStatus.SEARCHING]

    for idx, spec in enumerate(season_specs, start=1):
        season_defaults: dict[str, Any] = dict(
            media_item_id=item.id,
            season_number=idx,
            watch_order=idx,
            type_number=idx,
            entry_type="season",
            title=f"Season {idx}",
            monitored=True,
            status=SeasonStatus.SEARCHING,
            episode_count=episodes_per_season if episodes_per_season else None,
        )
        season_defaults.update(spec)
        # Ensure media_item_id is always correct even when caller passes season_number
        season_defaults["media_item_id"] = item.id
        s = Season(**season_defaults)
        session.add(s)
        await session.flush()
        season_objs.append(s)

        for ep_num in range(1, episodes_per_season + 1):
            ep_status = statuses[(ep_num - 1) % len(statuses)]
            ep = Episode(
                season_id=s.id,
                episode_number=ep_num,
                monitored=True,
                status=ep_status,
            )
            session.add(ep)
            episode_objs.append(ep)

    if episode_objs:
        await session.flush()

    return item, season_objs, episode_objs


async def make_watchlist_provider(
    session: AsyncSession, type: str, **overrides: Any
) -> Provider:
    """Create, flush, and return an active watchlist ``Provider`` of *type*."""
    fields: dict[str, Any] = {
        "name": type,
        "type": type,
        "category": "watchlist",
        "is_active": True,
    }
    fields.update(overrides)
    provider = Provider(**fields)
    session.add(provider)
    await session.flush()
    return provider


async def make_downloader_provider(
    session: AsyncSession, type: str = "torbox", **overrides: Any
) -> Provider:
    """Create, flush, and return an active ``category="downloader"`` ``Provider``."""
    fields: dict[str, Any] = {
        "name": type,
        "type": type,
        "category": "downloader",
        "api_key": "test-key",
        "is_active": True,
    }
    fields.update(overrides)
    provider = Provider(**fields)
    session.add(provider)
    await session.flush()
    return provider


async def make_delivery_provider(
    session: AsyncSession, **config_overrides: Any
) -> Provider:
    """Create, flush, and return an active ``category="delivery"`` ``local_directory`` ``Provider``."""
    provider = Provider(
        name="local_directory",
        type="local_directory",
        category="delivery",
        is_active=True,
        config_json=json.dumps(config_overrides),
    )
    session.add(provider)
    await session.flush()
    return provider


async def make_indexer_provider(session: AsyncSession, **overrides: Any) -> Provider:
    """Create, flush, and return an active, credentialed ``category="indexer"`` ``Provider``."""
    fields: dict[str, Any] = {
        "name": "indexer",
        "type": "newznab",
        "category": "indexer",
        "api_key": "test-key",
        "is_active": True,
    }
    fields.update(overrides)
    provider = Provider(**fields)
    session.add(provider)
    await session.flush()
    return provider


async def seed_core_prerequisites(session: AsyncSession) -> None:
    """Seed TMDB key, an indexer and a downloader so ``SetupStatus.core_ready`` holds."""
    session.add(
        Provider(
            name="tmdb", type="tmdb", category="metadata", api_key="k", is_active=True
        )
    )
    await make_indexer_provider(session)
    await make_downloader_provider(session)


async def seed_ready_setup(session: AsyncSession) -> None:
    """Make ``SetupStatus.watchlist_ready`` hold: core prerequisites + a connected Simkl watchlist."""
    from sqlalchemy import select

    await seed_core_prerequisites(session)
    existing = (
        (await session.execute(select(Provider).where(Provider.type == "simkl")))
        .scalars()
        .first()
    )
    if existing is None:
        existing = await make_watchlist_provider(session, "simkl")
    existing.client_id, existing.access_token = "cid", "tok"
    await session.flush()


async def make_source(
    session: AsyncSession,
    item: MediaItem,
    provider: Provider,
    list_status: str = "plantowatch",
    remote_id: str = "1",
    last_watched_order: int = 0,
) -> MediaItemWatchlistSource:
    """Create, flush, and return a ``MediaItemWatchlistSource`` linking *item* to *provider*."""
    source = MediaItemWatchlistSource(
        media_item_id=item.id,
        provider_id=provider.id,
        remote_id=remote_id,
        list_status=list_status,
        last_watched_order=last_watched_order,
    )
    session.add(source)
    await session.flush()
    return source


async def make_history(
    session: AsyncSession,
    *,
    item: MediaItem | None = None,
    season: Season | None = None,
    episode: Episode | None = None,
    **overrides: Any,
) -> DownloadHistory:
    """Create, flush, and return a ``DownloadHistory`` row.

    At least one of *item*, *season*, or *episode* should be provided so the
    history row is linked to a real entity.  All fields accept ``**overrides``.

    Sensible defaults:

    .. code-block:: python

        nzb_title="Test.Release.2024.1080p.WEB-DL.H264-GRP"
        nzb_guid="guid-hist-default"
        downloader_transfer_id="tb-9000"
        score=8500.0
        size_bytes=8 * 1024 ** 3  # 8 GB
        status_detail="completed"
        is_dismissed=True
        push_mode="auto"
    """
    defaults: dict[str, Any] = dict(
        media_item_id=item.id if item is not None else None,
        season_id=season.id if season is not None else None,
        episode_id=episode.id if episode is not None else None,
        nzb_title="Test.Release.2024.1080p.WEB-DL.H264-GRP",
        nzb_guid="guid-hist-default",
        downloader_type="torbox",
        downloader_transfer_id="tb-9000",
        score=8500.0,
        size_bytes=8 * 1024**3,
        status_detail="completed",
        is_dismissed=True,
        push_mode="auto",
    )
    defaults.update(overrides)
    hist = DownloadHistory(**defaults)
    session.add(hist)
    await session.flush()
    return hist


async def ingest_simkl_payloads(
    session: AsyncSession,
    provider: Provider,
    payloads: list[dict[str, Any]],
    media_type: Any,
    *,
    maintain: bool = True,
) -> Any:
    """Normalize Simkl-shaped list payloads via the Simkl adapter, ingest them, then run maintenance.

    ``media_type`` is a ``MediaType`` selecting the Simkl category the payloads belong to.
    Returns the ``IngestResult``.
    """
    from unittest.mock import AsyncMock, patch

    from app.db.models import MediaType
    from app.services.library_maintenance import run_library_maintenance
    from app.services.watchlist_ingest import ingest_entries
    from app.services.watchlists.simkl_adapter import SimklWatchlistAdapter

    category = {
        MediaType.MOVIE: "movies",
        MediaType.SHOW: "shows",
        MediaType.ANIME: "anime",
    }[media_type]

    async def fake_fetch(_s, _p, cat, cid, tok):
        return (payloads if cat == category else []), cid, tok

    await session.commit()
    with (
        patch(
            "app.services.watchlists.simkl_adapter.simkl.ensure_valid_simkl_token",
            new=AsyncMock(return_value=("cid", "tok")),
        ),
        patch(
            "app.services.watchlists.simkl_adapter.simkl._fetch_simkl_category_with_retry",
            new=fake_fetch,
        ),
    ):
        entries = await SimklWatchlistAdapter().fetch_entries(provider)
    result = await ingest_entries(session, provider, entries)
    if maintain:
        await run_library_maintenance(session)
    await session.commit()
    return result


async def seed_setup_ready(session: AsyncSession) -> None:
    """Seed TMDB key, indexer, downloader and watchlist so the dashboard renders its item tables."""
    session.add(
        Provider(
            name="tmdb",
            type="tmdb",
            category="metadata",
            api_key="test-key",
            is_active=True,
        )
    )
    await make_indexer_provider(session)
    await make_downloader_provider(session)
    await make_watchlist_provider(session, "simkl", client_id="cid", access_token="tok")
    await session.flush()


# ---------------------------------------------------------------------------
# Delivery (m78) shared fakes
# ---------------------------------------------------------------------------

NAME = "Example.Show.S01E01.1080p.WEB-DL.mkv"
PAYLOAD = b"x" * 3000
URL = "https://example.com/dl/file.mkv"


class FakeDownloaderClient:
    def __init__(self, size: int = len(PAYLOAD), folder: str | None = None) -> None:
        self.size, self.folder = size, folder

    async def get_transfer_files(self, _tid: str) -> list[DownloaderFile]:
        return [DownloaderFile(name=NAME, size_bytes=300 * 1024 * 1024)]

    async def get_download_links(self, _tid: str) -> list[DownloadLink]:
        return [DownloadLink(name=NAME, url=URL, size_bytes=self.size)]

    async def get_transfers(self) -> list[DownloaderTransfer]:
        return [
            DownloaderTransfer(
                transfer_id="tb-9000",
                name="x",
                state=TransferState.COMPLETED,
                progress_pct=100.0,
                folder_id=self.folder,
            )
        ]


@pytest.fixture(autouse=True)
def fresh_delivery_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(delivery_worker, "_queue", None)


@pytest.fixture
def downloader(monkeypatch: pytest.MonkeyPatch):
    def _set(client: FakeDownloaderClient) -> None:
        async def get(_s: Any, _t: str) -> FakeDownloaderClient:
            return client

        for mod in (delivery_worker, delivery_engines):
            monkeypatch.setattr(mod, "get_downloader_client_for_type", get)
        monkeypatch.setattr(
            delivery_engines,
            "_client_factory",
            lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(200, content=PAYLOAD)
                )
            ),
        )

    return _set
