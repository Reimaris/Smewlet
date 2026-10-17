"""
Smewlet Main Application Entrypoint
====================================
FastAPI application with lifespan for database initialization and
APScheduler background jobs. Contains HTML routes for the dashboard
and API endpoints for HTMX interactions.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncGenerator, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import (
    BackgroundTasks,
    FastAPI,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import reload_settings_from_db, settings
from app.core.anime_titles import (
    anime_season_title_for,
    anime_title_for,
    is_anime_scope,
    normalize_anime_title_language,
    search_names,
)
from app.core.outage import has_active_outage, outage_banner_context
from app.core.time_utils import format_local_datetime
from app.db.database import (
    async_session_factory,
    close_db,
    get_session_factory,
    init_db,
)
from app.version import APP_NAME, APP_VERSION

if TYPE_CHECKING:
    from app.db.models import MediaItem

# Configure logging
config_dir = Path("/app/config")
if not config_dir.exists():
    config_dir = Path("config")  # Fallback for local testing
log_dir = config_dir / "logs"
log_file = log_dir / "smewlet.log"

formatter = logging.Formatter(
    "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler: TimedRotatingFileHandler | None = None
try:
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        filename=log_file, when="midnight", interval=1, backupCount=5, encoding="utf-8"
    )
    file_handler.suffix = "%Y-%m-%d.log"
    file_handler.setFormatter(formatter)
except PermissionError:
    file_handler = None
    import sys

    print(
        f"WARNING: Permission denied creating log file {log_file}. Falling back to console logging.",
        file=sys.stderr,
    )

root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(console_handler)
if file_handler:
    root_logger.addHandler(file_handler)

# Mask API keys and other secrets on every handler (ADR-097) and forward
# ERROR+ records to Telegram/Discord (ADR-096)
from app.core.error_forwarder import ErrorForwarderHandler  # noqa: E402
from app.core.log_redaction import (  # noqa: E402
    install_model_listeners,
    install_redaction,
    refresh_secrets,
)

error_forwarder = ErrorForwarderHandler()
root_logger.addHandler(error_forwarder)
install_redaction([console_handler, file_handler, error_forwarder])
install_model_listeners()

logger = logging.getLogger(__name__)

# Silence noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)


logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Scheduler instance
scheduler = AsyncIOScheduler()

# Templates setup
_ROOT = Path(__file__).parent.parent
templates_dir = _ROOT / "templates"
static_dir = Path(__file__).parent / "static"

# Ensure directories exist
templates_dir.mkdir(exist_ok=True)
static_dir.mkdir(exist_ok=True)


def _setup_context(request: Request) -> dict[str, Any]:
    """Expose the per-request SetupStatus (set by ``setup_guard``) to every template."""
    setup = getattr(request.state, "setup", None)
    return {"setup": setup} if setup is not None else {}


templates = Jinja2Templates(
    directory=str(templates_dir), context_processors=[_setup_context]
)


def relative_date(dt: datetime | None) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        today = datetime.now().date()
    else:
        today = datetime.now(dt.tzinfo).date()
    target_date = dt.date()
    days = (target_date - today).days
    if days == 0:
        return "Today"
    elif days == 1:
        return "in 1 day"
    elif days > 1:
        return f"in {days} days"
    elif days == -1:
        return "1 day ago"
    else:
        return f"{abs(days)} days ago"


templates.env.filters["relative_date"] = relative_date
templates.env.filters["local_datetime"] = format_local_datetime
templates.env.globals["settings"] = settings
templates.env.globals["app_version"] = APP_VERSION
templates.env.globals["app_name"] = APP_NAME
from app.services.provider_service import ActiveDownloaderName  # noqa: E402

templates.env.globals["downloader_name"] = ActiveDownloaderName()
from app.services.downloaders.base import (  # noqa: E402
    DOWNLOADER_DISPLAY_NAMES as _DL_NAMES,
)
from app.services.downloaders.base import DOWNLOADER_TYPES  # noqa: E402
from app.services.watchlists import (  # noqa: E402
    WATCHLIST_DISPLAY_NAMES,
    WATCHLIST_TYPES,
)

templates.env.globals["downloader_display_names"] = _DL_NAMES


def _current_rate_limit_source() -> str:
    from app.core.rate_limit_queue import (
        active_rate_limit_sources,
        rate_limit_source_label,
    )

    return rate_limit_source_label(active_rate_limit_sources())


templates.env.globals["rate_limit_source"] = _current_rate_limit_source
templates.env.globals["outage_banner"] = outage_banner_context
templates.env.globals["outage_active"] = has_active_outage
from app.services.indexers.catalog import (  # noqa: E402
    INDEXER_PRESETS,
    get_preset,
    is_indexer_type,
)

templates.env.globals["indexer_preset"] = get_preset

from app.core.diagnostics import GITHUB_BUG_REPORT_URL  # noqa: E402
from app.core.experimental import experimental_types, is_experimental  # noqa: E402
from app.services.provider_capabilities import (  # noqa: E402
    capabilities_by_category,
    get_capability,
)

templates.env.globals["is_experimental"] = is_experimental
templates.env.globals["get_capability"] = get_capability
templates.env.globals["capabilities_by_category"] = capabilities_by_category
templates.env.globals["anime_title_for"] = anime_title_for
templates.env.globals["anime_season_title_for"] = anime_season_title_for
templates.env.globals["bug_report_url"] = GITHUB_BUG_REPORT_URL


async def run_orchestrator_tick(now: datetime | None = None) -> None:
    """Execute scheduled periodic watchlist sync and air date rollover aligned to wall-clock intervals."""
    from app.core.automation import (
        run_orchestrator_tick as core_run_orchestrator_tick,
    )

    await core_run_orchestrator_tick(now)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifecycle manager."""
    # 1. Initialize Database
    await init_db(settings.database_url)

    # 1.5 Load settings from DB into memory
    async with async_session_factory() as session:
        await reload_settings_from_db(session)
        await refresh_secrets(session)

    import asyncio as _asyncio

    error_forwarder.set_loop(_asyncio.get_running_loop())

    # 2. Setup and Start APScheduler
    import asyncio

    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    from app.core.transfer_poller import (
        count_downloading_entities,
        run_transfer_poller_tick,
        transfer_poller,
    )

    scheduler.add_job(
        run_orchestrator_tick,
        CronTrigger(minute="0,15,30,45"),
        id="orchestrator_job",
        replace_existing=True,
        misfire_grace_time=60,
        coalesce=True,
    )
    scheduler.add_job(
        run_transfer_poller_tick,
        IntervalTrigger(seconds=20),
        id="transfer_poller_job",
        replace_existing=True,
    )
    from app.core.rate_limit_queue import (
        RESUME_JOB_ID,
        count_rate_limited_pushes,
        rate_limit_queue,
        run_rate_limit_resume_tick,
    )

    scheduler.add_job(
        run_rate_limit_resume_tick,
        IntervalTrigger(seconds=15),
        id=RESUME_JOB_ID,
        replace_existing=True,
    )
    scheduler.start()
    transfer_poller.set_scheduler(scheduler)
    rate_limit_queue.set_scheduler(scheduler)

    # Rebuild the rate-limit queue state from the database (survives restarts)
    async with async_session_factory() as session:
        if await count_rate_limited_pushes(session) == 0:
            rate_limit_queue.sleep()
        else:
            from sqlalchemy import select

            from app.core.rate_limit_queue import outage_episode, rate_limit_episode
            from app.db.models import DownloadHistory

            rate_limit_episode.adopt_existing_queue()
            if (
                await session.execute(
                    select(DownloadHistory.id)
                    .where(
                        DownloadHistory.status_detail.like("paused: %"),
                        DownloadHistory.is_dismissed.is_(False),
                    )
                    .limit(1)
                )
            ).first():
                outage_episode.adopt_existing_queue()

    async with async_session_factory() as session:
        active_dl_count = await count_downloading_entities(session)
        if active_dl_count > 0:
            transfer_poller.wake()
            asyncio.create_task(run_transfer_poller_tick())
        else:
            transfer_poller.sleep(0)

    logger.info(
        "APScheduler started with quarter-hour cron trigger and 20s auto-wake transfer poller."
    )

    # Anime title variant backfill runs off the startup path; failures never propagate.
    async def _run_title_backfill() -> None:
        from app.core.prerequisites import setup_blocks
        from app.services.anime_title_backfill import run_anime_title_backfill

        try:
            if await setup_blocks("anime title backfill"):
                return
            await run_anime_title_backfill(get_session_factory())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[BACKFILL] anime title backfill crashed")

    backfill_task = asyncio.create_task(_run_title_backfill())

    # Local directory delivery: single background worker + crash recovery
    from app.services.delivery.worker import (
        recover_interrupted_deliveries,
        start_delivery_worker,
        stop_delivery_worker,
    )

    await start_delivery_worker()
    async with async_session_factory() as session:
        await recover_interrupted_deliveries(session)

    # Yield control to the FastAPI application
    yield

    # 3. Shutdown gracefully
    await stop_delivery_worker()
    if not backfill_task.done():
        backfill_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await backfill_task
    scheduler.shutdown(wait=False)
    logger.info("APScheduler shut down.")
    await close_db()


# FastAPI App
app = FastAPI(
    title=APP_NAME,
    description="Autonomous Media Processing System",
    version=APP_VERSION,
    lifespan=lifespan,
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

HISTORY_STATUS_KEYS: tuple[str, ...] = (
    "failed",
    "canceled",
    "replaced",
    "deleted",
    "ready",
)


def count_history_by_status(ledger: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Count ledger rows per pinned status key (lowercased downloader_status); unknown values ignored."""
    counts = dict.fromkeys(HISTORY_STATUS_KEYS, 0)
    for row in ledger:
        key = (row.get("downloader_status") or "").lower()
        if key in counts:
            counts[key] += 1
    return counts


HISTORY_FILTER_KEYS: tuple[str, ...] = (
    "failed",
    "canceled",
    "replaced",
    "deleted",
    "ready",
    "simulated",
)


def count_history_filter_buckets(ledger: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Count ledger rows per HISTORY_FILTER_KEYS bucket; dry-run rows count only under 'simulated'; unknown statuses ignored."""
    counts = dict.fromkeys(HISTORY_FILTER_KEYS, 0)
    for row in ledger:
        key = (
            "simulated"
            if row.get("is_dry_run")
            else (row.get("downloader_status") or "").lower()
        )
        if key in counts:
            counts[key] += 1
    return counts


SETUP_LABELS: dict[str, str] = {
    "tmdb": "TMDB API key",
    "indexer": "Indexer",
    "downloader": "Downloader",
}

app.state.setup_guard_enabled = True


@app.middleware("http")
async def setup_guard(request: Request, call_next):
    """Refuse non-open requests while core prerequisites (TMDB, indexer, downloader) are missing."""
    from app.core.prerequisites import get_setup_status
    from app.core.route_policy import classify_path

    if not request.app.state.setup_guard_enabled:
        return await call_next(request)
    if request.url.path.startswith("/static"):
        return await call_next(request)

    async with async_session_factory() as session:
        status = await get_setup_status(session)
    request.state.setup = status
    path_class = classify_path(request.url.path, request.method)
    if path_class == "open":
        return await call_next(request)
    is_htmx = request.headers.get("HX-Request") == "true"
    accepts_json = "application/json" in request.headers.get("accept", "")

    def refuse(missing: list[str], message: str) -> Response:
        if is_htmx:
            # X-Status-Span makes base.html swap the 4xx body so the message is visible.
            msg = html.escape(message)
            return HTMLResponse(
                content=(
                    '<span class="text-red-400 font-medium text-xs px-2 py-1.5 '
                    f'bg-red-500/10 border border-red-500/20 rounded-md">{msg}</span>'
                ),
                status_code=403,
                headers={"X-Status-Span": "1"},
            )
        return JSONResponse(
            {"detail": "setup_required", "missing": missing}, status_code=403
        )

    if status.core_ready:
        if status.has_watchlist or path_class != "watchlist":
            return await call_next(request)
        return refuse(
            ["watchlist"], "Connect a watchlist provider in Settings to use this."
        )

    missing_labels = [SETUP_LABELS[m] for m in status.missing_core]
    is_page = (
        request.method == "GET"
        and not is_htmx
        and not accepts_json
        and not request.url.path.startswith("/api")
    )
    if is_page:
        return templates.TemplateResponse(
            request=request,
            name="setup_required.html",
            context={"setup": status, "missing_labels": missing_labels},
        )
    return refuse(
        list(status.missing_core), "Setup required: " + ", ".join(missing_labels)
    )


# ---------------------------------------------------------------------------
# Routes (HTML & HTMX)
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, background_tasks: BackgroundTasks):
    """Main dashboard displaying the 6-Card Interactive Top Deck and 3-Tier Status Tables."""
    from sqlalchemy import func, select
    from sqlalchemy.orm import selectinload

    from app.core.automation import (
        classify_v3_status_tier,
        consolidate_standalone_anime_sequels,
        deduplicate_media_items,
    )
    from app.core.prerequisites import get_setup_status
    from app.core.transfer_poller import (
        count_downloading_entities,
        get_active_pushes,
        transfer_poller,
    )
    from app.db.database import async_session_factory
    from app.db.models import (
        DownloadHistory,
        MediaItem,
        MediaType,
        Season,
        SystemSettings,
    )

    async with async_session_factory() as session:
        # Auto-consolidate any legacy standalone sequels and deduplicate items before rendering
        await consolidate_standalone_anime_sequels(session)
        if await deduplicate_media_items(session) > 0:
            await session.commit()

        # Fetch items
        stmt = (
            select(MediaItem)
            .order_by(MediaItem.created_at.desc())
            .options(
                selectinload(MediaItem.seasons).selectinload(Season.episodes),
                selectinload(MediaItem.seasons).selectinload(Season.download_history),
                selectinload(MediaItem.failure_logs),
                selectinload(MediaItem.download_history),
                selectinload(MediaItem.provider),
            )
        )
        from app.db.models import MediaStatus

        result = await session.execute(stmt)
        all_items = result.scalars().all()
        items = [i for i in all_items if i.status != MediaStatus.IGNORED]

        for item in items:
            item.status_tier = classify_v3_status_tier(item)

        in_progress_items = [i for i in items if i.status_tier == "in_progress"]
        ready_to_push_items = [i for i in items if i.status_tier == "ready_to_push"]
        upcoming_items = [i for i in items if i.status_tier == "upcoming"]
        on_hold_items = [i for i in items if i.status_tier == "on_hold"]

        from app.core.push_engine import get_anime_title_language

        title_language = await get_anime_title_language(session)
        search_index: dict[int, dict[str, Any]] = {
            item.id: {
                "names": search_names(
                    item.title,
                    item.alt_title,
                    *(
                        (item.title_english, item.title_romaji, item.title_native)
                        if is_anime_scope(item)
                        else ()
                    ),
                ),
                "children": _search_children(item, title_language),
            }
            for item in items
        }

        items_payload = [
            {
                "category": "anime"
                if (
                    i.media_type.value == "movie"
                    and getattr(i, "is_anime_movie", False)
                )
                else i.media_type.value,
                "status_tier": getattr(i, "status_tier", "ready_to_push"),
                "title": (i.title or "").lower(),
                "alt_title": (i.alt_title or "").lower(),
                "ids": f"{i.tvdb_id or ''} {i.tmdb_id or ''} {i.imdb_id or ''} {i.simkl_id or ''} {i.anilist_id or ''}".lower(),
                "names": search_index[i.id]["names"],
                "children": search_index[i.id]["children"],
            }
            for i in items
        ]

        # Active category collections
        movie_items = [
            i
            for i in items
            if i.media_type == MediaType.MOVIE
            and not getattr(i, "is_anime_movie", False)
        ]
        series_items = [i for i in items if i.media_type == MediaType.SHOW]
        anime_items = [
            i
            for i in items
            if i.media_type == MediaType.ANIME or getattr(i, "is_anime_movie", False)
        ]

        def _build_cat_stats(cat_items: list[MediaItem]) -> dict[str, int]:
            return {
                "total": len(cat_items),
                "in_progress": sum(
                    1 for i in cat_items if i.status_tier == "in_progress"
                ),
                "ready_to_push": sum(
                    1 for i in cat_items if i.status_tier == "ready_to_push"
                ),
                "upcoming": sum(1 for i in cat_items if i.status_tier == "upcoming"),
                "on_hold": sum(1 for i in cat_items if i.status_tier == "on_hold"),
                "wanted": sum(
                    1 for i in cat_items if i.status in ("searching", "pending")
                ),
                "ignored": sum(
                    1 for i in cat_items if i.status in ("ignored", "canceled")
                ),
            }

        movie_stats = _build_cat_stats(movie_items)
        series_stats = _build_cat_stats(series_items)
        anime_stats = _build_cat_stats(anime_items)

        from app.core.transfer_poller import (
            autoclear_history_ledger,
            count_downloading_entities,
            get_active_pushes,
            get_push_history_ledger,
            purge_expired_dry_run_history,
            transfer_poller,
        )

        # Auto-purge expired dry-run history on page load
        await purge_expired_dry_run_history(session)
        await autoclear_history_ledger(session)

        # Card 4: Push History Ledger
        push_history_ledger = await get_push_history_ledger(session)
        dry_run_history_count = (
            await session.execute(
                select(func.count(DownloadHistory.id)).where(
                    DownloadHistory.is_dry_run.is_(True)
                )
            )
        ).scalar() or 0

        # Card 5: Active Pushes
        active_pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)
        in_flight_titles = transfer_poller.in_flight_titles
        poller_awake = (
            transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
        )

        # Card 6: Manual Search defaults & Search Presets
        from app.services.preset_service import list_presets

        presets = await list_presets(session)
        presets_payload = [p.to_dict() for p in presets]
        db_settings = (
            (
                await session.execute(
                    select(SystemSettings).where(SystemSettings.id == 1)
                )
            )
            .scalars()
            .first()
        )
        defaults: dict[str, Any] = {}
        if db_settings and db_settings.scoring_settings:
            defaults = db_settings.scoring_settings.get("manual_search_defaults", {})
        setup = getattr(request.state, "setup", None) or await get_setup_status(session)

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "items": items,
            "items_payload": items_payload,
            "search_index": search_index,
            "in_progress_items": in_progress_items,
            "ready_to_push_items": ready_to_push_items,
            "upcoming_items": upcoming_items,
            "on_hold_items": on_hold_items,
            "movie_items": movie_items,
            "series_items": series_items,
            "anime_items": anime_items,
            "movie_stats": movie_stats,
            "series_stats": series_stats,
            "anime_stats": anime_stats,
            "movies_wanted": movie_stats["wanted"],
            "series_wanted": series_stats["wanted"],
            "anime_wanted": anime_stats["wanted"],
            "push_history_ledger": push_history_ledger,
            "history_total": len(push_history_ledger),
            "dry_run_history_count": dry_run_history_count,
            "history_status_counts": count_history_by_status(push_history_ledger),
            "history_filter_counts": count_history_filter_buckets(push_history_ledger),
            "active_pushes": active_pushes,
            "active_pushes_count": len(active_pushes),
            "downloading_count": downloading_count,
            "in_flight_titles": in_flight_titles,
            "poller_awake": poller_awake,
            "presets": presets,
            "presets_payload": presets_payload,
            "defaults": defaults,
            "setup": setup,
            "anime_title_language": normalize_anime_title_language(
                db_settings.anime_title_language if db_settings else None
            ),
            "show_onboarding": (db_settings is None)
            or (not db_settings.onboarding_completed),
        },
    )


def _search_children(item: MediaItem, language: str) -> list[dict[str, str]]:
    """Searchable consolidated children (seasons/movies) of an anime item."""
    if not is_anime_scope(item):
        return []
    children: list[dict[str, str]] = []
    for s in sorted(item.seasons, key=lambda s: (s.watch_order, s.season_number)):
        names = search_names(s.title, s.title_english, s.title_romaji, s.title_native)
        if not names:
            continue
        kind = "Movie" if s.entry_type == "movie" else "Season"
        children.append(
            {
                "label": f"{kind} {s.type_number}",
                "title": anime_season_title_for(item, s, language),
                "names": names,
            }
        )
    return children


def _hit_query_string(hit: dict[str, Any]) -> str:
    """Encode the parent-hit snapshot carried by Tier 2 lazy-load URLs."""
    from urllib.parse import urlencode

    params = {
        "title": hit.get("title") or "",
        "alt_titles": "\n".join(hit.get("alt_titles") or []),
        "year": hit.get("year"),
        "imdb_id": hit.get("imdb_id"),
        "tmdb_id": hit.get("tmdb_id"),
        "tvdb_id": hit.get("tvdb_id"),
        "poster_url": hit.get("poster_url"),
    }
    return urlencode({k: v for k, v in params.items() if v not in (None, "")})


templates.env.filters["hit_qs"] = _hit_query_string


@app.post("/api/search/manual", response_class=HTMLResponse)
async def manual_search(
    request: Request,
    query: str = Form(""),
    category: str = Form("any"),
    language: str = Form("any"),
    primary_language: str = Form("any"),
    fallback_language: str = Form("none"),
    resolution: str = Form("any"),
    source: str = Form("any"),
    hdr: str = Form("any"),
    video_codec: str = Form("any"),
    audio_tier: str = Form("any"),
    audio_channels: str = Form("any"),
    limit: int = Form(10),
    save_defaults: bool = Form(False),
    imdb_id: str = Form(""),
    tmdb_id: str = Form(""),
    tvdb_id: str = Form(""),
    force_title: bool = Form(False),
):
    """Tier 1: smart-box media search returning expandable media hits."""
    import copy

    from sqlalchemy import select

    from app.db.database import async_session_factory
    from app.db.models import SystemSettings

    # Save defaults
    if save_defaults:
        async with async_session_factory() as session:
            stmt = select(SystemSettings).where(SystemSettings.id == 1)
            db_settings = (await session.execute(stmt)).scalars().first()
            if db_settings:
                new_defaults = {
                    "category": category,
                    "language": language,
                    "primary_language": primary_language,
                    "fallback_language": fallback_language,
                    "resolution": resolution,
                    "source": source,
                    "hdr": hdr,
                    "video_codec": video_codec,
                    "audio_tier": audio_tier,
                    "audio_channels": audio_channels,
                    "limit": limit,
                }
                current = (
                    copy.deepcopy(db_settings.scoring_settings)
                    if db_settings.scoring_settings
                    else {}
                )
                current["manual_search_defaults"] = new_defaults
                db_settings.scoring_settings = current
                await session.commit()

    import re

    from app.services import media_search, tmdb

    clean_category = media_search.normalize_category(category)
    clean_query = query.strip()
    clean_imdb = imdb_id.strip().lower()
    imdb_val = clean_imdb if re.fullmatch(r"tt\d+", clean_imdb) else None
    tmdb_val = _opt_int(tmdb_id)
    tvdb_val = _opt_int(tvdb_id)

    def _render(hits: list[Any], error_message: str | None = None):
        return templates.TemplateResponse(
            request=request,
            name="partials/search_media_hits.html",
            context={
                "hits": hits,
                "error_message": error_message,
                "category": clean_category,
                "query": clean_query,
                "force_title": force_title,
                "numeric_query": media_search.classify_query(clean_query).mode
                == "numeric",
            },
        )

    if not (clean_query or imdb_val or tmdb_val or tvdb_val):
        return _render([], "Please enter a title or ID.")
    if tvdb_val and not (clean_query or imdb_val or tmdb_val):
        return _render(
            [],
            "TVDb IDs cannot be resolved to a title. Add a title or an IMDb/TMDb ID.",
        )

    async with async_session_factory() as session:
        hits = await media_search.search_media(
            clean_query,
            clean_category,
            imdb_id=imdb_val,
            tmdb_id=tmdb_val,
            tvdb_id=tvdb_val,
            session=session,
            force_title=force_title,
        )
        tmdb_key = await tmdb.resolve_api_key(session)

    error_message = None
    if not hits and not tmdb_key and clean_category in {"any", "movie", "series"}:
        error_message = "TMDB API key is not configured (Settings → Metadata)."
    return _render(list(hits), error_message)


def _media_provider_or_404(provider: str) -> str:
    if provider not in {"tmdb", "anilist"}:
        raise HTTPException(status_code=404, detail="Unknown metadata provider")
    return provider


def _hit_snapshot(
    provider: str,
    ext_id: int,
    kind: str,
    *,
    title: str,
    alt_titles: str,
    year: str,
    imdb_id: str,
    tmdb_id: str,
    tvdb_id: str,
    poster_url: str,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "ext_id": ext_id,
        "kind": kind,
        "title": title,
        "alt_titles": [t for t in alt_titles.split("\n") if t],
        "year": _opt_int(year),
        "imdb_id": imdb_id or None,
        "tmdb_id": _opt_int(tmdb_id) or (ext_id if provider == "tmdb" else None),
        "tvdb_id": _opt_int(tvdb_id),
        "anilist_id": ext_id if provider == "anilist" else None,
        "poster_url": poster_url or None,
    }


@app.get(
    "/api/search/manual/media/{provider}/{ext_id}/seasons",
    response_class=HTMLResponse,
)
async def manual_search_seasons(
    request: Request,
    provider: str,
    ext_id: int,
    kind: str = "series",
    title: str = "",
    alt_titles: str = "",
    year: str = "",
    imdb_id: str = "",
    tmdb_id: str = "",
    tvdb_id: str = "",
    poster_url: str = "",
):
    """Tier 2a: lazily load the season rows of a series/anime hit."""
    from app.services import media_search

    _media_provider_or_404(provider)
    async with async_session_factory() as session:
        seasons = await media_search.list_seasons(
            provider,
            ext_id,
            kind,  # type: ignore[arg-type]
            session,
        )
    return templates.TemplateResponse(
        request=request,
        name="partials/search_seasons.html",
        context={
            "seasons": seasons,
            "hit": _hit_snapshot(
                provider,
                ext_id,
                kind,
                title=title,
                alt_titles=alt_titles,
                year=year,
                imdb_id=imdb_id,
                tmdb_id=tmdb_id,
                tvdb_id=tvdb_id,
                poster_url=poster_url,
            ),
            "error_message": None,
        },
    )


@app.get(
    "/api/search/manual/media/{provider}/{ext_id}/seasons/{season_number}/episodes",
    response_class=HTMLResponse,
)
async def manual_search_episodes(
    request: Request,
    provider: str,
    ext_id: int,
    season_number: int,
    kind: str = "series",
    title: str = "",
    alt_titles: str = "",
    year: str = "",
    imdb_id: str = "",
    tmdb_id: str = "",
    tvdb_id: str = "",
    poster_url: str = "",
):
    """Tier 2b: lazily load the episode rows of one season."""
    from app.services import media_search

    _media_provider_or_404(provider)
    async with async_session_factory() as session:
        episodes = await media_search.list_episodes(
            provider,
            ext_id,
            kind,  # type: ignore[arg-type]
            season_number,
            session,
        )
    return templates.TemplateResponse(
        request=request,
        name="partials/search_episodes.html",
        context={
            "episodes": episodes,
            "season_number": season_number,
            "hit": _hit_snapshot(
                provider,
                ext_id,
                kind,
                title=title,
                alt_titles=alt_titles,
                year=year,
                imdb_id=imdb_id,
                tmdb_id=tmdb_id,
                tvdb_id=tvdb_id,
                poster_url=poster_url,
            ),
            "error_message": None,
        },
    )


@app.post("/api/search/manual/releases", response_class=HTMLResponse)
async def manual_search_releases(
    request: Request,
    scope: str = Form(""),
    kind: str = Form(""),
    title: str = Form(""),
    alt_titles: str = Form(""),
    year: str = Form(""),
    imdb_id: str = Form(""),
    tmdb_id: str = Form(""),
    tvdb_id: str = Form(""),
    anilist_id: str = Form(""),
    poster_url: str = Form(""),
    season: str = Form(""),
    episode: str = Form(""),
    drawer_id: str = Form("drawer"),
    language: str = Form("any"),
    primary_language: str = Form("any"),
    fallback_language: str = Form("none"),
    resolution: str = Form("any"),
    source: str = Form("any"),
    hdr: str = Form("any"),
    video_codec: str = Form("any"),
    audio_tier: str = Form("any"),
    audio_channels: str = Form("any"),
    limit: int = Form(10),
):
    """Tier 3: scoped NZB search + scoring rendered into an inline drawer."""
    from app.core.adhoc_search import AdHocFilters, search_and_score_releases

    title = title.strip()
    season_n, episode_n = _opt_int(season), _opt_int(episode)
    if scope not in ("item", "season", "episode") or not title:
        error: str | None = "Invalid search scope or missing title."
    elif scope != "item" and season_n is None:
        error = "A season number is required."
    elif scope == "episode" and episode_n is None:
        error = "An episode number is required."
    else:
        error = None

    if scope == "item":
        season_n = episode_n = None
    elif scope == "season":
        episode_n = None

    hit = _hit_snapshot(
        "anilist" if anilist_id.strip() else "tmdb",
        _opt_int(anilist_id) or _opt_int(tmdb_id) or 0,
        kind,
        title=title,
        alt_titles=alt_titles,
        year=year,
        imdb_id=imdb_id.strip(),
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        poster_url=poster_url,
    )
    primary: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    mismatched: list[dict[str, Any]] = []
    search_outage: dict[str, str] | None = None
    if error is None:
        titles = list(
            dict.fromkeys(
                [title, *(t.strip() for t in alt_titles.split("\n") if t.strip())]
            )
        )
        result = await search_and_score_releases(
            kind=kind,
            scope=scope,
            titles=titles,
            year=_opt_int(year),
            imdb_id=imdb_id.strip() or None,
            tmdb_id=_opt_int(tmdb_id),
            tvdb_id=_opt_int(tvdb_id),
            season=season_n,
            episode=episode_n,
            filters=AdHocFilters(
                primary_language=primary_language,
                fallback_language=fallback_language,
                language=language,
                resolution=resolution,
                source=source,
                hdr=hdr,
                video_codec=video_codec,
                audio_tier=audio_tier,
                audio_channels=audio_channels,
            ),
            limit=limit,
        )
        error = result.error_message
        search_outage = result.outage
        primary = result.primary_results
        fallback = result.fallback_results
        mismatched = result.mismatched_results

    return templates.TemplateResponse(
        request=request,
        name="partials/search_release_drawer.html",
        context={
            "drawer_id": drawer_id,
            "scope": scope,
            "primary_results": primary,
            "fallback_results": fallback,
            "mismatched_results": mismatched,
            "error_message": error,
            "search_outage": search_outage,
            "retry_vals": {
                "scope": scope,
                "kind": kind,
                "title": title,
                "alt_titles": alt_titles,
                "year": year,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "tvdb_id": tvdb_id,
                "anilist_id": anilist_id,
                "poster_url": poster_url,
                "season": season_n if season_n is not None else "",
                "episode": episode_n if episode_n is not None else "",
                "drawer_id": drawer_id,
            },
            "hit": hit,
            "season_number": season_n,
            "episode_number": episode_n,
            "auto_push_vals": {
                "scope": scope,
                "kind": kind,
                "title": title,
                "alt_titles": alt_titles,
                "year": year,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "tvdb_id": tvdb_id,
                "poster_url": poster_url,
                "season": season_n if season_n is not None else "",
                "episode": episode_n if episode_n is not None else "",
            },
            "push_context": {
                "media_title": title,
                "media_year": year,
                "media_kind": kind,
                "poster_url": poster_url,
                "tmdb_id": tmdb_id,
                "imdb_id": imdb_id,
                "anilist_id": anilist_id,
                "season": season_n if season_n is not None else "",
                "episode": episode_n if episode_n is not None else "",
            },
        },
    )


@app.post("/items/{item_id}/reset-metadata")
async def reset_anime_metadata_endpoint(request: Request, item_id: int):
    """Rebuilds anime metadata cleanly from AniList, purges phantom episodes, and resets backoff counters."""
    from app.core.automation import reset_anime_metadata
    from app.core.push_engine import get_push_modal_context
    from app.db.database import async_session_factory
    from app.db.models import MediaItem, MediaType

    async with async_session_factory() as session:
        item = await session.get(MediaItem, item_id)
        if not item or (
            item.media_type != MediaType.ANIME
            and not getattr(item, "is_anime_movie", False)
        ):
            return HTMLResponse(content="Invalid Item", status_code=400)

        await reset_anime_metadata(session, item_id)
        if request.headers.get("HX-Request") == "true":
            ctx = await get_push_modal_context(session, item_id)
            if ctx is not None:
                return templates.TemplateResponse(
                    request=request,
                    name="modals/push_modal.html",
                    context=ctx,
                )

    return HTMLResponse(
        content="<script>window.location.reload();</script>",
        headers={"HX-Refresh": "true"},
    )


@app.get("/api/outages/banner", response_class=HTMLResponse)
async def outage_banner_partial(request: Request) -> HTMLResponse:
    """Outage banner partial (empty when no outage is active)."""
    return templates.TemplateResponse(request, "partials/outage_banner.html", {})


def _outage_span(provider: str, label: str) -> HTMLResponse:
    msg = html.escape(f"Service outage: {provider} returned {label}. Try again later.")
    return HTMLResponse(
        content=f'<span class="text-red-400 font-medium text-xs px-2 py-1.5 bg-red-500/10 border border-red-500/20 rounded-md" title="{msg}">{msg}</span>',
        status_code=503,
    )


# legacy alias (m59)
@app.post("/api/torbox/add", response_class=HTMLResponse)
async def manual_push_to_downloader(magnet: str = Form(...)):
    """Push a search result directly to the active downloader."""
    import logging

    from app.services.downloaders.base import (
        DOWNLOADER_DISPLAY_NAMES,
        DownloaderError,
        DownloaderOutageError,
    )
    from app.services.downloaders.factory import downloader_class
    from app.services.indexers import newznab as newznab_client

    logger = logging.getLogger(__name__)
    sent_to = DOWNLOADER_DISPLAY_NAMES["torbox"]

    try:
        if magnet.startswith("magnet:"):
            try:
                result = await downloader_class("torbox").send_magnet(
                    magnet, is_manual=True
                )
            except DownloaderOutageError as e:
                return _outage_span(
                    sent_to, e.verdict.label if e.verdict else "an error"
                )
            except DownloaderError as e:
                result = {"error": str(e)}
        else:
            try:
                from app.core.fake_detector import extract_nzb_password

                nzb_bytes, filename = await newznab_client.fetch_nzb_bytes(magnet)
                nzb_pass = extract_nzb_password(nzb_bytes)
                from app.core.push_engine import dispatch_nzb_to_active_downloader
                from app.db.database import async_session_factory

                async with async_session_factory() as dispatch_session:
                    client, upload = await dispatch_nzb_to_active_downloader(
                        dispatch_session,
                        nzb_bytes,
                        filename,
                        password=nzb_pass,
                        media_type="",
                        is_anime=False,
                    )
                sent_to = client.display_name
                if upload.is_outage:
                    return _outage_span(sent_to, upload.outage_label or "an error")
                result = (
                    {**upload.raw, "id": upload.transfer_id}
                    if upload.success
                    else {"error": upload.detail}
                )
            except newznab_client.IndexerOutageError as e:
                return _outage_span(
                    newznab_client.indexer_display_name(e.indexer_url),
                    e.verdict.label if e.verdict else "an error",
                )
            except newznab_client.IndexerError as e:
                result = {"error": f"NZB Download Error: {e}"}
            except DownloaderOutageError as e:
                return _outage_span(
                    sent_to, e.verdict.label if e.verdict else "an error"
                )
            except DownloaderError as e:
                result = {"error": str(e)}

        if result and (result.get("hash") or result.get("id")):
            return HTMLResponse(
                content=f'<span class="text-emerald-400 font-medium text-xs px-2 py-1.5 bg-emerald-500/10 border border-emerald-500/20 rounded-md">Sent to {sent_to}!</span>'
            )
        else:
            err_msg = html.escape(
                str(
                    result.get("error", "Failed to send")
                    if result
                    else "Failed to send"
                )
            )
            # Return 400 so the frontend can display the error without triggering a generic 500
            return HTMLResponse(
                content=f'<span class="text-red-400 font-medium text-xs px-2 py-1.5 bg-red-500/10 border border-red-500/20 rounded-md" title="{err_msg}">{err_msg}</span>',
                status_code=400,
            )
    except Exception as e:
        logger.error(f"Error in manual_push_to_downloader: {e}")
        return HTMLResponse(
            content='<span class="text-red-400 font-medium text-xs px-2 py-1.5 bg-red-500/10 border border-red-500/20 rounded-md">Error</span>',
            status_code=500,
        )


def _status_span(message: str, ok: bool, status_code: int = 200) -> HTMLResponse:
    from html import escape

    msg = escape(message)
    if ok:
        cls = "text-emerald-400 font-medium text-xs px-2 py-1.5 bg-emerald-500/10 border border-emerald-500/20 rounded-md"
        return HTMLResponse(
            f'<span class="{cls}">{msg}</span>',
            status_code=status_code,
            headers={"X-Status-Span": "1"},
        )
    cls = "text-red-400 font-medium text-xs px-2 py-1.5 bg-red-500/10 border border-red-500/20 rounded-md"
    return HTMLResponse(
        f'<span class="{cls}" title="{msg}">{msg}</span>',
        status_code=status_code,
        headers={"X-Status-Span": "1"},
    )


def _opt_int(value: str) -> int | None:
    value = (value or "").strip()
    return int(value) if value.isdigit() else None


@app.post("/api/search/manual/push", response_class=HTMLResponse)
async def manual_push_adhoc(
    release_link: str = Form(...),
    nzb_title: str = Form(...),
    nzb_guid: str = Form(""),
    score: float = Form(0.0),
    size_bytes: int = Form(0),
    is_fallback: bool = Form(False),
    matched_language: str = Form(""),
    media_title: str = Form(""),
    media_year: str = Form(""),
    media_kind: str = Form(""),
    poster_url: str = Form(""),
    tmdb_id: str = Form(""),
    imdb_id: str = Form(""),
    anilist_id: str = Form(""),
    season: str = Form(""),
    episode: str = Form(""),
):
    """Dispatch one ad-hoc release to the active downloader and record a detached history row."""
    from app.core.adhoc_search import push_adhoc_release

    async with async_session_factory() as session:
        outcome = await push_adhoc_release(
            session,
            release_link=release_link.strip(),
            nzb_title=nzb_title,
            nzb_guid=nzb_guid or None,
            score=score,
            size_bytes=size_bytes,
            is_fallback=is_fallback,
            matched_language=matched_language or None,
            media_title=media_title or None,
            media_year=_opt_int(media_year),
            media_kind=media_kind or None,
            poster_url=poster_url or None,
            tmdb_id=_opt_int(tmdb_id),
            imdb_id=imdb_id or None,
            anilist_id=_opt_int(anilist_id),
            season=_opt_int(season),
            episode=_opt_int(episode),
        )
    return _status_span(outcome.message, outcome.ok, outcome.status_code)


@app.post("/api/search/manual/auto-push", response_class=HTMLResponse)
async def manual_auto_push(
    scope: str = Form(...),
    kind: str = Form(""),
    title: str = Form(""),
    alt_titles: str = Form(""),
    year: str = Form(""),
    imdb_id: str = Form(""),
    tmdb_id: str = Form(""),
    tvdb_id: str = Form(""),
    season: str = Form(""),
    episode: str = Form(""),
    poster_url: str = Form(""),
    language: str = Form("any"),
    primary_language: str = Form("any"),
    fallback_language: str = Form("none"),
    resolution: str = Form("any"),
    source: str = Form("any"),
    hdr: str = Form("any"),
    video_codec: str = Form("any"),
    audio_tier: str = Form("any"),
    audio_channels: str = Form("any"),
    limit: int = Form(10),
):
    """Search one scope and push the best primary (else fallback) release."""
    from app.core.adhoc_search import (
        AdHocFilters,
        push_adhoc_release,
        search_and_score_releases,
    )

    title = title.strip()
    season_n, episode_n = _opt_int(season), _opt_int(episode)
    if scope not in ("item", "season", "episode") or not title:
        return _status_span("Invalid search scope or missing title.", False)
    if scope != "item" and season_n is None:
        return _status_span("A season number is required.", False)
    if scope == "episode" and episode_n is None:
        return _status_span("An episode number is required.", False)

    titles = list(
        dict.fromkeys(
            [title, *(t.strip() for t in alt_titles.split("\n") if t.strip())]
        )
    )
    result = await search_and_score_releases(
        kind=kind,
        scope=scope,
        titles=titles,
        year=_opt_int(year),
        imdb_id=imdb_id or None,
        tmdb_id=_opt_int(tmdb_id),
        tvdb_id=_opt_int(tvdb_id),
        season=season_n if scope != "item" else None,
        episode=episode_n if scope == "episode" else None,
        filters=AdHocFilters(
            primary_language=primary_language,
            fallback_language=fallback_language,
            language=language,
            resolution=resolution,
            source=source,
            hdr=hdr,
            video_codec=video_codec,
            audio_tier=audio_tier,
            audio_channels=audio_channels,
        ),
        limit=limit,
    )
    if result.error_message:
        return _status_span(result.error_message, False)
    candidates = result.primary_results or result.fallback_results
    if not candidates:
        return _status_span("No suitable release found.", False)
    best = candidates[0]

    async with async_session_factory() as session:
        outcome = await push_adhoc_release(
            session,
            release_link=str(best.get("link") or best.get("guid") or ""),
            nzb_title=best["title"],
            nzb_guid=best.get("guid") or None,
            score=best.get("score"),
            size_bytes=int(best.get("size") or 0),
            is_fallback=bool(best.get("is_fallback")),
            matched_language=best.get("matched_language"),
            media_title=title,
            media_year=_opt_int(year),
            media_kind=kind or None,
            poster_url=poster_url or None,
            tmdb_id=_opt_int(tmdb_id),
            imdb_id=imdb_id or None,
            season=season_n if scope != "item" else None,
            episode=episode_n if scope == "episode" else None,
        )
    if not outcome.ok:
        return _status_span(outcome.message, False, outcome.status_code)
    return _status_span(f"Pushed best: {best['title']}", True)


@app.post("/api/items/{item_id}/change-type")
async def change_type_endpoint(
    request: Request,
    item_id: int,
    background_tasks: BackgroundTasks,
    target_type: str = Form(...),
):

    from sqlalchemy.orm import selectinload

    from app.core.push_engine import get_push_modal_context
    from app.db.database import async_session_factory
    from app.db.models import MediaItem, MediaType, Season

    async with async_session_factory() as session:
        item = await session.get(
            MediaItem,
            item_id,
            options=[
                selectinload(MediaItem.seasons),
                selectinload(MediaItem.download_history),
            ],
        )
        if not item:
            return Response("Item not found", status_code=404)

        if target_type not in ("movie", "series", "anime", "anime-movie"):
            return Response("Invalid target_type", status_code=400)

        currently_is_series = item.media_type in (MediaType.SHOW, MediaType.ANIME)
        target_is_series = target_type in ("series", "anime")

        # 1. Structural Reshaping
        if not currently_is_series and target_is_series:
            season_1 = next((s for s in item.seasons if s.season_number == 1), None)
            if not season_1:
                season_1 = Season(
                    media_item_id=item.id,
                    season_number=1,
                    monitored=True,
                    status=item.status,
                )
                session.add(season_1)
                await session.flush()

            for dh in item.download_history:
                dh.season_id = season_1.id

        elif currently_is_series and not target_is_series:
            from sqlalchemy import delete, update

            from app.db.models import DownloadHistory, Season

            session.expunge(item)

            # Unlink DH
            await session.execute(
                update(DownloadHistory)
                .where(DownloadHistory.media_item_id == item.id)
                .values(season_id=None, episode_id=None)
            )

            await session.execute(delete(Season).where(Season.media_item_id == item.id))

            # Reload item
            item = await session.get(
                MediaItem, item_id, options=[selectinload(MediaItem.seasons)]
            )

        if item is not None:
            # 2. Field updates
            if target_type == "movie":
                item.media_type = MediaType.MOVIE
                item.is_anime_movie = False
            elif target_type == "anime-movie":
                item.media_type = MediaType.MOVIE
                item.is_anime_movie = True
            elif target_type == "series":
                item.media_type = MediaType.SHOW
                item.is_anime_movie = False
            elif target_type == "anime":
                item.media_type = MediaType.ANIME
                item.is_anime_movie = False

            # 3. Search Resets
            item.last_searched_at = None
            if target_is_series:
                for s in item.seasons:
                    s.last_searched_at = None

            await session.commit()

        # 4. Dispatch background task
        if target_type in ("anime", "anime-movie") and background_tasks:

            async def _bg_enrich():
                from app.core.automation import enrich_anime_metadata
                from app.db.database import async_session_factory

                async with async_session_factory() as bg_session:
                    bg_item = await bg_session.get(MediaItem, item_id)
                    if bg_item and bg_item.anilist_id:
                        await enrich_anime_metadata(bg_session, bg_item)
                        await bg_session.commit()

            background_tasks.add_task(_bg_enrich)

        if request.headers.get("HX-Request") == "true":
            ctx = await get_push_modal_context(session, item_id)
            if ctx is not None:
                return templates.TemplateResponse(
                    request=request,
                    name="modals/push_modal.html",
                    context=ctx,
                )

    return RedirectResponse(url=request.headers.get("referer", "/"), status_code=303)


@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    """Delete an item from the watchlist while preserving detached DownloadHistory snapshots."""
    from app.core.push_engine import detach_item_download_history
    from app.db.database import async_session_factory
    from app.db.models import MediaItem

    async with async_session_factory() as session:
        item = await session.get(MediaItem, item_id)
        if item:
            await detach_item_download_history(session, item)
            await session.delete(item)
            await session.commit()
            return HTMLResponse(
                content=""
            )  # Empty response means success (HTMX can remove element)
    return HTMLResponse(content="Error", status_code=400)


# ---------------------------------------------------------------------------
# Settings & Configuration Routes
# ---------------------------------------------------------------------------


@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    """Render the settings form."""
    from sqlalchemy import select

    from app.db.models import (
        BlacklistedRelease,
        NotificationChannel,
        Provider,
        SystemSettings,
    )

    async with async_session_factory() as session:
        stmt = select(SystemSettings).where(SystemSettings.id == 1)
        db_settings = (await session.execute(stmt)).scalars().first()

        raw_providers = (await session.execute(select(Provider))).scalars().all()
        downloader_name = await _active_downloader_name(session)

        category_order = {
            "watchlist": 0,
            "metadata": 1,
            "downloader": 2,
            "indexer": 3,
            "delivery": 4,
        }

        def provider_sort_key(p: Provider) -> tuple[int, int, str]:
            cat = p.category.value if hasattr(p.category, "value") else str(p.category)
            cat_rank = category_order.get(cat.lower(), 99)
            # Priority ascending, nulls last (rank infinity or 999999)
            pri = p.priority if p.priority is not None else 999999
            return (cat_rank, pri, p.name or "")

        providers = sorted(raw_providers, key=provider_sort_key)
        total_providers_count = len(providers)
        active_providers_count = sum(1 for p in providers if p.is_active)
        notifications = (
            (await session.execute(select(NotificationChannel))).scalars().all()
        )

        from app.services.preset_service import list_presets

        presets = await list_presets(session)
        default_preset = next((p for p in presets if p.is_default), None)

        blacklisted_releases = (
            (
                await session.execute(
                    select(BlacklistedRelease).order_by(
                        BlacklistedRelease.created_at.desc()
                    )
                )
            )
            .scalars()
            .all()
        )

        # Flatten scoring logic for the UI if needed
        scoring = (
            db_settings.scoring_settings
            if db_settings and db_settings.scoring_settings
            else {}
        )

    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context={
            "db_settings": db_settings,
            "scoring": scoring,
            "providers": providers,
            "total_providers_count": total_providers_count,
            "active_providers_count": active_providers_count,
            "presets": presets,
            "default_preset": default_preset,
            "notifications": notifications,
            "blacklisted_releases": blacklisted_releases,
            "downloader_name": downloader_name,
            "delivery_provider": next(
                (p for p in providers if p.category == "delivery"), None
            ),
        },
    )


@app.post("/settings")
@app.post("/settings/global")
@app.post("/settings/system")
@app.post("/settings/auto-save")
async def save_global_settings(request: Request):
    import copy

    from sqlalchemy import select

    from app.core.default_scoring import DEFAULT_SCORING_CONFIG
    from app.db.models import SystemSettings

    payload = await _parse_request_payload(request)

    def _to_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ("1", "true", "on", "yes")

    async with async_session_factory() as session:
        stmt = select(SystemSettings).where(SystemSettings.id == 1)
        db_settings = (await session.execute(stmt)).scalars().first()

        if not db_settings:
            db_settings = SystemSettings(id=1)
            session.add(db_settings)

        if "download_timeout_minutes" in payload:
            try:
                val = int(str(payload["download_timeout_minutes"]))
                if val in (15, 30, 60, 120, 240):
                    db_settings.download_timeout_minutes = val
                else:
                    db_settings.download_timeout_minutes = 60
            except (ValueError, TypeError):
                db_settings.download_timeout_minutes = 60

        if "anime_title_language" in payload:
            db_settings.anime_title_language = normalize_anime_title_language(
                payload["anime_title_language"]
            )

        for field, allowed, default in (
            ("air_delay_hours", (0, 1, 2, 4, 6, 12), 2),
            ("upgrade_window_hours", (24, 48, 72, 168), 48),
            ("max_upgrade_checks", (1, 2, 3, 5), 3),
            ("history_retention_days", (0, 7, 14, 30, 60, 90), 30),
        ):
            if field in payload:
                try:
                    val = int(str(payload[field]))
                except (ValueError, TypeError):
                    val = default
                setattr(db_settings, field, val if val in allowed else default)

        if "sh_max_retries" in payload:
            try:
                val = int(str(payload["sh_max_retries"]))
                if val >= 0:
                    db_settings.sh_max_retries = val
            except (ValueError, TypeError):
                pass

        if "discord_webhook_url" in payload:
            raw_url = str(payload.get("discord_webhook_url") or "").strip()
            db_settings.discord_webhook_url = raw_url or None

        if "discord_enabled" in payload:
            db_settings.discord_enabled = _to_bool(payload.get("discord_enabled"))

        if "notify_on_push_initiated" in payload:
            db_settings.notify_on_push_initiated = _to_bool(
                payload.get("notify_on_push_initiated")
            )

        if "notify_on_completed" in payload:
            db_settings.notify_on_completed = _to_bool(
                payload.get("notify_on_completed")
            )

        if "notify_on_failure" in payload:
            db_settings.notify_on_failure = _to_bool(payload.get("notify_on_failure"))

        if "notify_on_auto_advance" in payload:
            db_settings.notify_on_auto_advance = _to_bool(
                payload.get("notify_on_auto_advance")
            )

        if "notify_on_errors" in payload:
            db_settings.notify_on_errors = _to_bool(payload.get("notify_on_errors"))

        if "notify_on_dry_run" in payload:
            db_settings.notify_on_dry_run = _to_bool(payload.get("notify_on_dry_run"))

        if "notify_on_outages" in payload:
            db_settings.notify_on_outages = _to_bool(payload.get("notify_on_outages"))

        for flag in ("notify_on_delivery_failed", "notify_on_delivered"):
            if flag in payload:
                setattr(db_settings, flag, _to_bool(payload.get(flag)))

        if "debug_mode" in payload:
            db_settings.debug_mode = _to_bool(payload.get("debug_mode"))

        if "dry_run" in payload:
            db_settings.dry_run = _to_bool(payload.get("dry_run"))

        if "history_autoclear_pruned" in payload:
            db_settings.history_autoclear_pruned = _to_bool(
                payload.get("history_autoclear_pruned")
            )

        sc: dict[str, Any] = copy.deepcopy(DEFAULT_SCORING_CONFIG)
        if (
            isinstance(db_settings.scoring_settings, dict)
            and "manual_search_defaults" in db_settings.scoring_settings
        ):
            sc["manual_search_defaults"] = copy.deepcopy(
                db_settings.scoring_settings["manual_search_defaults"]
            )
        db_settings.scoring_settings = sc

        await session.commit()
        await reload_settings_from_db(session)

    # If requested via /settings/auto-save or caller requests JSON, return JSONResponse with HX-Trigger
    if (
        request.url.path == "/settings/auto-save"
        or request.headers.get("accept") == "application/json"
    ):
        return JSONResponse(
            {"status": "ok"},
            headers={"HX-Trigger": '{"showAutoSaveToast": "Saved"}'},
        )

    return HTMLResponse(
        content='<div class="p-4 mb-4 bg-[#d40060] text-white rounded">Settings saved successfully!</div>',
        headers={"HX-Trigger": '{"showAutoSaveToast": "Saved"}'},
    )


_WATCHLIST_TYPES = WATCHLIST_TYPES


@app.get("/settings/capabilities", response_class=HTMLResponse)
async def capabilities_modal(
    request: Request, focus: str = Query("", max_length=32)
) -> HTMLResponse:
    from sqlalchemy import select

    from app.db.models import Provider
    from app.services.provider_capabilities import capabilities_by_category

    async with async_session_factory() as session:
        configured_types = set(
            (await session.execute(select(Provider.type))).scalars().all()
        )
    focus_cap = get_capability(focus)
    return templates.TemplateResponse(
        request=request,
        name="modals/capabilities_modal.html",
        context={
            "categories": capabilities_by_category(),
            "focus": focus_cap.provider_type if focus_cap else None,
            "focus_category": focus_cap.category if focus_cap else None,
            "configured_types": configured_types,
        },
    )


@app.get("/settings/provider/new", response_class=HTMLResponse)
@app.get("/settings/provider/{provider_id}/edit", response_class=HTMLResponse)
async def provider_modal(
    request: Request,
    provider_id: int | None = None,
    type: str = Query("", max_length=32),
):
    from sqlalchemy import select

    from app.db.models import NotificationChannel, Provider
    from app.services import provider_service
    from app.services.indexers.catalog import INDEXER_PRESETS

    async with async_session_factory() as session:
        provider = None
        if provider_id:
            provider = await session.get(Provider, provider_id)

        notifications = (
            (await session.execute(select(NotificationChannel))).scalars().all()
        )
        downloader_name = await _active_downloader_name(session)
        delivery_provider = await provider_service.get_delivery_provider(session)

    delivery_config = None
    if provider is not None and provider.type == "local_directory":
        delivery_config = _delivery_form_values(provider)
    new_type = type.strip().lower() if type.strip().lower() in _WATCHLIST_TYPES else ""
    return templates.TemplateResponse(
        request=request,
        name="modals/provider.html",
        context={
            "provider": provider,
            "downloader_name": downloader_name,
            "notifications": notifications,
            "new_type": new_type if provider is None else "",
            "mal_redirect_uri": str(request.base_url).rstrip("/") + _MAL_CALLBACK_PATH,
            "indexer_presets": INDEXER_PRESETS,
            "experimental_types": experimental_types(),
            "indexer_cfg": provider.indexer_config if provider else {},
            "delivery_provider": delivery_provider,
            "delivery_config": delivery_config,
        },
    )


_DOWNLOADER_TYPES = DOWNLOADER_TYPES


async def _active_downloader_name(session: Any) -> str:
    """Display name of the active downloader; 'TorBox' when none is active."""
    from app.services import provider_service
    from app.services.downloaders import DOWNLOADER_DISPLAY_NAMES

    active = await provider_service.get_active_downloader(session)
    return DOWNLOADER_DISPLAY_NAMES.get(active.type, "TorBox") if active else "TorBox"


def _validate_downloader_form(provider_type: str, api_key: str, api_url: str) -> str:
    """Return an error message for an invalid downloader form, '' when valid (spec §4.10)."""
    if provider_type not in _DOWNLOADER_TYPES:
        return f"Unsupported downloader type: {provider_type}"
    if not api_key.strip():
        return "API key is required."
    if provider_type == "sabnzbd" and not api_url.strip().lower().startswith(
        ("http://", "https://")
    ):
        return "SABnzbd URL is required and must start with http:// or https://."
    return ""


async def _sab_categories_from_form(request: Request) -> dict[str, str]:
    """SABnzbd categories from the raw form; an omitted field gets its default,
    an empty string is preserved (FastAPI Form() would collapse '' to a default)."""
    form = await request.form()

    def pick(key: str, default: str) -> str:
        value = form.get(key)
        return default if not isinstance(value, str) else value.strip()

    return {
        "category_movie": pick("category_movie", "movies"),
        "category_series": pick("category_series", "tv"),
        "category_anime": pick("category_anime", "anime"),
    }


def _form_error(message: str, status_code: int = 422) -> HTMLResponse:
    return HTMLResponse(
        content=f'<p class="text-sm text-red-400">{html.escape(message)}</p>',
        status_code=status_code,
    )


_seerr_form_error = _form_error


def _reload_response(flash: str = "") -> HTMLResponse:
    alert = f"alert({json.dumps(flash)});" if flash else ""
    return HTMLResponse(content=f"<script>{alert}window.location.reload();</script>")


async def _foreign_transfer_flash(session: Any, new_type: str) -> str:
    """Non-blocking notice about in-flight transfers staying on another downloader."""
    from app.services import provider_service
    from app.services.downloaders import DOWNLOADER_DISPLAY_NAMES

    counts = await provider_service.count_foreign_downloading(session, new_type)
    return " ".join(
        f"{n} running transfer(s) stay on "
        f"{DOWNLOADER_DISPLAY_NAMES.get(t, 'TorBox')} and keep being tracked."
        for t, n in sorted(counts.items())
    )


_DELIVERY_PATH_FIELDS = ("movies_path", "shows_path", "anime_path", "anime_movies_path")
_DELIVERY_TEMPLATE_FIELDS = (
    "movie_template",
    "show_template",
    "anime_template",
    "anime_movie_template",
)


def _delivery_form_values(provider: Any) -> dict[str, Any]:
    """Stored delivery config shaped for the edit form (buffer in GB)."""
    from dataclasses import asdict

    from app.services.delivery.config import DeliveryConfig

    values = asdict(DeliveryConfig.from_provider(provider))
    gb = values["min_free_buffer_bytes"] / 1024**3
    values["min_free_buffer_gb"] = int(gb) if gb == int(gb) else round(gb, 2)
    return values


def _delivery_config_from_form(form: Any) -> tuple[dict[str, Any], str]:
    """Build the delivery ``config_json`` dict from a raw form; returns ``(cfg, error)``."""
    from app.services.delivery.config import DeliveryConfig, validate_templates

    def text(key: str) -> str:
        value = form.get(key)
        return value.strip() if isinstance(value, str) else ""

    cfg: dict[str, Any] = {}
    for key in _DELIVERY_PATH_FIELDS:
        value = text(key)
        if value and not value.startswith("/"):
            return {}, f"{key} must be an absolute path."
        cfg[key] = value
    for key in _DELIVERY_TEMPLATE_FIELDS:
        value = text(key)
        if value:
            cfg[key] = value
    raw_gb = text("min_free_buffer_gb")
    if raw_gb:
        try:
            gb = float(raw_gb)
        except ValueError:
            gb = -1.0
        if gb < 0:
            return (
                {},
                "Free-space buffer must be a number of GB (0 disables the guard).",
            )
        cfg["min_free_buffer_bytes"] = int(gb * 1024**3)
    cfg["delete_cloud_after_delivery"] = (
        form.get("delete_cloud_after_delivery") is not None
    )
    cfg["sab_path_map_remote"] = text("sab_path_map_remote")
    cfg["sab_path_map_local"] = text("sab_path_map_local")
    unknown = validate_templates(DeliveryConfig(**cfg))
    if unknown:
        return {}, "Unknown template placeholder(s): " + ", ".join(unknown)
    return cfg, ""


async def _save_delivery_provider(
    request: Request, provider_id: int | None, name: str, is_active: bool
) -> HTMLResponse:
    from app.db.models import Provider
    from app.services import provider_service

    cfg, error = _delivery_config_from_form(await request.form())
    if error:
        return _form_error(error)
    async with async_session_factory() as session:
        if provider_id:
            provider = await session.get(Provider, provider_id)
            if provider is None or provider.type != "local_directory":
                raise HTTPException(status_code=404, detail="Provider not found")
            provider.name = name
            provider.is_active = is_active
            provider.config_json = json.dumps(cfg)
        else:
            try:
                await provider_service.create_delivery_provider(
                    session, name=name, config_json=json.dumps(cfg)
                )
            except provider_service.DeliveryProviderExistsError as exc:
                return _form_error(str(exc), status_code=409)
        await session.commit()
    return _reload_response()


def _probe_directory(path: str) -> tuple[bool, str]:
    """Check that ``path`` is an existing writable directory (create + delete a temp file)."""
    import tempfile

    if not path.startswith("/"):
        return False, "Not an absolute path"
    if not os.path.isdir(path):
        return False, "Directory does not exist"
    try:
        fd, probe = tempfile.mkstemp(prefix=".smewlet-probe-", dir=path)
        os.close(fd)
        os.unlink(probe)
    except OSError as exc:
        return False, f"Not writable: {exc.strerror or exc}"
    return True, "Writable"


async def _test_delivery_paths(request: Request) -> JSONResponse:
    form = await request.form()
    paths: list[dict[str, Any]] = []
    for field in _DELIVERY_PATH_FIELDS:
        value = form.get(field)
        value = value.strip() if isinstance(value, str) else ""
        if not value:
            continue
        ok, detail = await asyncio.to_thread(_probe_directory, value)
        paths.append({"field": field, "path": value, "ok": ok, "detail": detail})
    if not paths:
        return JSONResponse(
            {"ok": False, "message": "Configure at least one path.", "paths": []}
        )
    failed = sum(1 for p in paths if not p["ok"])
    message = (
        "All paths are writable."
        if not failed
        else f"{failed} of {len(paths)} path(s) failed."
    )
    return JSONResponse({"ok": not failed, "message": message, "paths": paths})


@app.post("/api/providers/test")
async def test_downloader_provider(
    request: Request,
    provider_type: str = Form(""),
    api_key: str = Form(""),
    api_url: str = Form(""),
) -> JSONResponse:
    """Test connectivity of an unsaved downloader form."""
    from app.db.models import Provider, ProviderCategory
    from app.services.downloaders.factory import get_downloader_client

    ptype = provider_type.strip().lower()
    if ptype == "local_directory":
        return await _test_delivery_paths(request)
    error = _validate_downloader_form(ptype, api_key, api_url)
    if error:
        return JSONResponse({"ok": False, "message": error}, status_code=422)
    provider = Provider(
        type=ptype,
        category=ProviderCategory.DOWNLOADER.value,
        name=ptype,
        api_key=api_key.strip(),
        api_url=api_url.strip(),
        config_json=json.dumps(await _sab_categories_from_form(request)),
    )
    try:
        ok, message = await get_downloader_client(provider).test_connection()
    except Exception as exc:  # noqa: BLE001 - surface any client failure inline
        return JSONResponse({"ok": False, "message": f"Connection failed: {exc}"})
    return JSONResponse({"ok": ok, "message": message})


@app.post("/settings/provider", response_class=HTMLResponse)
@app.post("/settings/provider/{provider_id}", response_class=HTMLResponse)
async def save_provider(
    request: Request,
    provider_id: int | None = None,
    name: str = Form(...),
    # provider-specific code (allowlisted)
    simkl_username: str = Form(""),
    username: str = Form(""),
    access_token: str = Form(""),
    refresh_token: str = Form(""),
    token_expires_at: str = Form(""),
    client_id: str = Form(""),
    api_key: str = Form(""),
    api_url: str = Form(""),
    priority: int = Form(1),
    is_active: bool = Form(False),
    category: str = Form(""),
    enable_movies: bool = Form(False),
    enable_series: bool = Form(False),
    enable_anime: bool = Form(False),
    provider_type: str = Form(...),
    sync_interval_minutes: int = Form(0),
    sync_movies: bool | None = Form(None),
    sync_series: bool | None = Form(None),
    sync_anime: bool | None = Form(None),
    custom_movie_categories: str = Form(""),
    custom_tv_categories: str = Form(""),
):
    from app.db.models import Provider, ProviderCategory
    from app.services import provider_service
    from app.services.indexers.catalog import (
        DEFAULT_MOVIE_CATEGORIES,
        DEFAULT_TV_CATEGORIES,
        get_preset,
        is_indexer_type,
        normalize_category_csv,
    )

    # provider-specific code (allowlisted)
    from app.services.watchlists import simkl

    try:
        type_category = provider_service.provider_category_for_type(provider_type)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    resolved_category = category or type_category

    if type_category == ProviderCategory.DELIVERY.value:
        return await _save_delivery_provider(
            request, provider_id, name, is_active and provider_id is not None
        )

    indexer_categories: dict[str, str | None] = {}
    if is_indexer_type(provider_type):
        try:
            indexer_categories = {
                "custom_movie_categories": normalize_category_csv(
                    custom_movie_categories, DEFAULT_MOVIE_CATEGORIES
                ),
                "custom_tv_categories": normalize_category_csv(
                    custom_tv_categories, DEFAULT_TV_CATEGORIES
                ),
            }
        except ValueError:
            return HTMLResponse(
                content=(
                    '<p class="text-sm text-red-400">Invalid category list: use '
                    "comma-separated numeric IDs (e.g. 2000,2040).</p>"
                ),
                status_code=400,
            )

    from app.core.automation import normalize_sync_interval_minutes

    if resolved_category == ProviderCategory.DOWNLOADER.value:
        error = _validate_downloader_form(provider_type.lower(), api_key, api_url)
        if error:
            return HTMLResponse(
                content=f'<p class="text-sm text-red-400">{html.escape(error)}</p>',
                status_code=422,
            )

    flash = ""
    async with async_session_factory() as session:
        old_client_id = ""
        old_access_token = ""
        old_refresh_token = ""

        if provider_id:
            provider = await session.get(Provider, provider_id)
            if not provider:
                provider = Provider(type=provider_type, category=resolved_category)
                session.add(provider)
            else:
                old_cfg = provider.watchlist_config
                old_client_id = str(
                    provider.client_id or old_cfg.get("client_id") or ""
                ).strip()
                old_access_token = str(
                    provider.access_token or old_cfg.get("access_token") or ""
                ).strip()
                old_refresh_token = str(old_cfg.get("refresh_token") or "").strip()
                provider.type = provider_type
                provider.category = resolved_category
        else:
            provider = Provider(type=provider_type, category=resolved_category)
            session.add(provider)

        is_new_watchlist = (
            resolved_category == ProviderCategory.WATCHLIST.value
            and provider_type.lower() != "simkl"
        )
        # Secrets are never echoed into forms: a blank token keeps the stored one.
        keep_old_token = old_access_token and (
            access_token.strip() == "***"
            or (is_new_watchlist and not access_token.strip())
        )
        effective_access_token = (
            old_access_token if keep_old_token else access_token.strip()
        )

        provider.name = name
        if not (is_new_watchlist and provider_type.lower() in ("plex", "mal")):
            # provider-specific code (allowlisted)
            provider.username = simkl_username or username
        provider.access_token = effective_access_token
        provider.client_id = client_id.strip()
        provider.api_key = api_key
        provider.api_url = api_url
        if is_indexer_type(provider_type) and not api_url.strip():
            preset = get_preset(provider_type)
            provider.api_url = preset.api_url if preset else api_url
        provider.priority = priority
        provider.is_active = is_active

        if provider.type.lower() == "simkl":
            existing_cfg = provider.watchlist_config
            # If user manually replaced an existing token with a different one, best-effort revoke old token
            if (
                old_access_token
                and effective_access_token
                and effective_access_token != old_access_token
            ):
                tok_to_revoke = old_refresh_token or old_access_token
                if (
                    tok_to_revoke
                    and tok_to_revoke != refresh_token.strip()
                    and tok_to_revoke != effective_access_token
                ):
                    # provider-specific code (allowlisted)
                    await simkl.revoke_token(
                        old_client_id or client_id.strip(), tok_to_revoke
                    )

            existing_cfg["sync_interval_minutes"] = normalize_sync_interval_minutes(
                sync_interval_minutes
            )
            existing_cfg["sync_movies"] = (
                bool(sync_movies)
                if sync_movies is not None
                else bool(enable_movies or existing_cfg.get("sync_movies", True))
            )
            existing_cfg["sync_series"] = (
                bool(sync_series)
                if sync_series is not None
                else bool(enable_series or existing_cfg.get("sync_series", True))
            )
            existing_cfg["sync_anime"] = (
                bool(sync_anime)
                if sync_anime is not None
                else bool(enable_anime or existing_cfg.get("sync_anime", True))
            )
            existing_cfg["client_id"] = client_id.strip()
            existing_cfg["access_token"] = effective_access_token

            if refresh_token.strip():
                existing_cfg["refresh_token"] = refresh_token.strip()
            elif effective_access_token == old_access_token and old_refresh_token:
                existing_cfg["refresh_token"] = old_refresh_token
            elif not effective_access_token or (
                old_access_token and effective_access_token != old_access_token
            ):
                existing_cfg["refresh_token"] = None

            if token_expires_at.strip():
                existing_cfg["token_expires_at"] = token_expires_at.strip()
            elif effective_access_token == old_access_token and existing_cfg.get(
                "token_expires_at"
            ):
                pass
            elif not existing_cfg.get("refresh_token"):
                existing_cfg["token_expires_at"] = None

            provider.config_json = json.dumps(existing_cfg)
        elif is_new_watchlist:
            try:
                raw_cfg = json.loads(provider.config_json or "{}")
            except ValueError:
                raw_cfg = {}
            raw_cfg["sync_interval_minutes"] = normalize_sync_interval_minutes(
                sync_interval_minutes
            )
            if provider_type.lower() in ("jellyseerr", "overseerr"):
                form = await request.form()
                server_url = str(form.get("server_url") or "").strip().rstrip("/")
                if not re.fullmatch(r"https?://\S+", server_url, re.IGNORECASE):
                    return _seerr_form_error(
                        "Server URL is required and must start with http:// or https://."
                    )
                # A blank key on edit keeps the stored one; it is never echoed into forms.
                new_key = api_key.strip() or str(raw_cfg.get("api_key") or "").strip()
                if not new_key:
                    return _seerr_form_error("API key is required.")
                raw_cfg.update(
                    server_url=server_url,
                    api_key=new_key,
                    sync_admin_watchlist=form.get("sync_admin_watchlist") is not None,
                    notify_available=form.get("notify_available") is not None,
                    sync_movies=bool(sync_movies),
                    sync_series=bool(sync_series),
                )
                provider.api_key = ""
            provider.config_json = json.dumps(raw_cfg)
        elif provider_type.lower() == "sabnzbd":
            provider.config_json = json.dumps(await _sab_categories_from_form(request))
        elif is_indexer_type(provider_type):
            try:
                idx_cfg = json.loads(provider.config_json or "{}")
            except ValueError:
                idx_cfg = {}
            if not isinstance(idx_cfg, dict):
                idx_cfg = {}
            for key, value in indexer_categories.items():
                if value is None:
                    idx_cfg.pop(key, None)
                else:
                    idx_cfg[key] = value
            provider.config_json = json.dumps(idx_cfg)

        await session.flush()  # get ID

        # If Downloader and active, enforce single-active downloader rule
        if (
            provider.category == ProviderCategory.DOWNLOADER.value
            and provider.is_active
        ):
            flash = await _foreign_transfer_flash(session, provider.type)
            await provider_service.set_active_downloader(session, provider.id)

        await session.commit()

    # Reload page
    return _reload_response(flash)


@app.post("/settings/provider/{provider_id}/toggle-active", response_class=HTMLResponse)
async def toggle_provider_active(provider_id: int) -> HTMLResponse:
    from app.db.models import Provider, ProviderCategory
    from app.services import provider_service

    flash = ""
    async with async_session_factory() as session:
        provider = await session.get(Provider, provider_id)
        if provider:
            is_downloader = (
                provider.category == ProviderCategory.DOWNLOADER.value
                or provider.category == ProviderCategory.DOWNLOADER
            )
            if is_downloader:
                if not provider.is_active:
                    flash = await _foreign_transfer_flash(session, provider.type)
                    await provider_service.set_active_downloader(session, provider.id)
                else:
                    provider.is_active = False
                    await session.commit()
            else:
                provider.is_active = not provider.is_active
                await session.commit()

    return _reload_response(flash)


@app.delete("/settings/provider/{provider_id}", response_class=HTMLResponse)
async def delete_provider(provider_id: int) -> HTMLResponse:
    from app.db.models import Provider

    # provider-specific code (allowlisted)
    from app.services.watchlists import simkl

    async with async_session_factory() as session:
        provider = await session.get(Provider, provider_id)
        if provider:
            if (provider.type or "").lower() == "simkl" or (
                provider.name or ""
            ).lower() == "simkl":
                cfg = provider.watchlist_config
                cid = str(provider.client_id or cfg.get("client_id") or "").strip()
                tok = str(
                    cfg.get("refresh_token")
                    or provider.access_token
                    or cfg.get("access_token")
                    or ""
                ).strip()
                if cid and tok:
                    # provider-specific code (allowlisted)
                    await simkl.revoke_token(cid, tok)
            await _rehome_or_detach_provider_items(session, provider)
            await session.delete(provider)
            await session.commit()
    return HTMLResponse(content="<script>window.location.reload();</script>")


async def _rehome_or_detach_provider_items(session: Any, provider: Any) -> None:
    """Re-home items still listed by another provider; detach history of the rest."""
    from sqlalchemy import select

    from app.core.push_engine import detach_item_download_history
    from app.db.models import MediaItem

    items = (
        (
            await session.execute(
                select(MediaItem).where(MediaItem.provider_id == provider.id)
            )
        )
        .scalars()
        .all()
    )
    for item in items:
        other = next(
            (s for s in item.watchlist_sources if s.provider_id != provider.id), None
        )
        if other is not None:
            item.provider_id = other.provider_id
        else:
            await detach_item_download_history(session, item)
    await session.flush()


@app.get("/settings/notification/new", response_class=HTMLResponse)
@app.get("/settings/notification/{notification_id}/edit", response_class=HTMLResponse)
async def notification_modal(
    request: Request, notification_id: int | None = None
) -> HTMLResponse:
    from app.db.models import NotificationChannel

    async with async_session_factory() as session:
        notification = None
        if notification_id:
            notification = await session.get(NotificationChannel, notification_id)

    return templates.TemplateResponse(
        request=request,
        name="modals/notification.html",
        context={"notification": notification},
    )


@app.post("/settings/notification", response_class=HTMLResponse)
@app.post("/settings/notification/{notification_id}", response_class=HTMLResponse)
async def save_notification(
    request: Request,
    notification_id: int | None = None,
    name: str = Form(...),
    bot_token: str = Form(""),
    chat_id: str = Form(""),
) -> HTMLResponse:
    from app.db.models import NotificationChannel

    async with async_session_factory() as session:
        if notification_id:
            notification = await session.get(NotificationChannel, notification_id)
            if not notification:
                notification = NotificationChannel(type="telegram")
                session.add(notification)
        else:
            notification = NotificationChannel(type="telegram")
            session.add(notification)

        notification.name = name
        notification.bot_token = bot_token
        notification.chat_id = chat_id
        await session.commit()

    return HTMLResponse(content="<script>window.location.reload();</script>")


@app.delete("/settings/notification/{notification_id}", response_class=HTMLResponse)
async def delete_notification(notification_id: int) -> HTMLResponse:
    from app.db.models import NotificationChannel

    async with async_session_factory() as session:
        notification = await session.get(NotificationChannel, notification_id)
        if notification:
            await session.delete(notification)
            await session.commit()
    return HTMLResponse(content="<script>window.location.reload();</script>")


@app.post("/api/settings/discord/test")
@app.post("/api/notifications/test/discord")
@app.post("/settings/notification/discord/test")
async def test_discord_notification(request: Request) -> Response:
    """Send a test rich Discord Embed to verify the configured or provided Discord Webhook URL."""
    from sqlalchemy import select

    from app.config import reload_settings_from_db
    from app.db.models import SystemSettings
    from app.services import discord

    payload = await _parse_request_payload(request)
    provided_url = str(payload.get("discord_webhook_url") or "").strip()
    webhook_url = provided_url

    async with async_session_factory() as session:
        stmt = select(SystemSettings).where(SystemSettings.id == 1)
        db_settings = (await session.execute(stmt)).scalars().first()
        if not webhook_url:
            if db_settings and db_settings.discord_webhook_url:
                webhook_url = db_settings.discord_webhook_url.strip()
            elif settings.discord_webhook_url:
                webhook_url = settings.discord_webhook_url.strip()

        if not webhook_url:
            if request.headers.get("HX-Request") == "true":
                return HTMLResponse(
                    content='<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-xs text-red-300 font-medium">Please enter a Discord Webhook URL first.</div>',
                    status_code=200,
                )
            return JSONResponse(
                {"ok": False, "error": "Discord Webhook URL is not configured"},
                status_code=400,
            )

        embed = discord.build_discord_embed(
            event_type="completed",
            media_title=f"{APP_NAME} Test Notification",
            media_year=2026,
            target_label="Test Connection",
            release_name="Smewlet.v3.0.0.2160p.WEB-DL.DDP5.1.Atmos.H.265",
            resolution="2160p",
            source="WEB-DL",
            video_codec="H.265",
            audio_codec="Atmos",
            language="en",
            status_reason="Discord Webhook integration verified!",
        )
        ok = await discord.send_discord_webhook(webhook_url, embed=embed)
        if ok and provided_url:
            if not db_settings:
                db_settings = SystemSettings(id=1)
                session.add(db_settings)
            db_settings.discord_webhook_url = provided_url
            db_settings.discord_enabled = True
            await session.commit()
            await reload_settings_from_db(session)

    if request.headers.get("HX-Request") == "true":
        if ok:
            return HTMLResponse(
                content='<div class="p-3 bg-emerald-900/30 border border-emerald-500/40 rounded-lg text-xs text-emerald-300 font-medium">Discord test notification sent &amp; webhook saved!</div>',
                status_code=200,
            )
        return HTMLResponse(
            content='<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-xs text-red-300 font-medium">Discord webhook delivery failed — please check the Webhook URL.</div>',
            status_code=200,
        )
    return JSONResponse(
        {"ok": ok, "channel": "discord"},
        status_code=200 if ok else 400,
    )


@app.post("/api/notifications/test/telegram")
@app.post("/settings/notification/telegram/test")
@app.post("/settings/notification/{notification_id}/test")
async def test_telegram_notification(
    request: Request, notification_id: int | None = None
) -> Response:
    """Send a test Telegram message to verify a specific or configured Telegram channel."""
    from sqlalchemy import select

    from app.db.models import NotificationChannel
    from app.services import telegram

    payload = await _parse_request_payload(request)
    bot_token = str(payload.get("bot_token") or "").strip()
    chat_id = str(payload.get("chat_id") or "").strip()
    target_id = notification_id
    if target_id is None and payload.get("notification_id"):
        try:
            target_id = int(payload["notification_id"])
        except (TypeError, ValueError):
            target_id = None

    channels_to_test: list[tuple[str, str]] = []
    if bot_token and chat_id:
        channels_to_test.append((bot_token, chat_id))
    else:
        async with async_session_factory() as session:
            if target_id is not None:
                ch = await session.get(NotificationChannel, target_id)
                if ch and ch.bot_token and ch.chat_id:
                    channels_to_test.append((ch.bot_token, ch.chat_id))
            else:
                stmt = select(NotificationChannel).where(
                    NotificationChannel.type == "telegram"
                )
                rows = (await session.execute(stmt)).scalars().all()
                for ch in rows:
                    if ch.bot_token and ch.chat_id:
                        channels_to_test.append((ch.bot_token, ch.chat_id))

    if not channels_to_test:
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                content='<span class="text-xs text-red-400">No Telegram credentials configured.</span>',
                status_code=400,
            )
        return JSONResponse(
            {"ok": False, "error": "No Telegram channel configured"},
            status_code=400,
        )

    test_msg = f"<b>🔔 {APP_NAME} Test Notification</b>\nTelegram notification channel verified!"
    sent_count = 0
    for token, cid in channels_to_test:
        if await telegram.send_notification(test_msg, token, cid):
            sent_count += 1

    ok = sent_count > 0
    if request.headers.get("HX-Request") == "true":
        if ok:
            return HTMLResponse(
                content='<span class="text-xs text-emerald-400">Telegram test notification sent!</span>'
            )
        return HTMLResponse(
            content='<span class="text-xs text-red-400">Telegram delivery failed.</span>',
            status_code=400,
        )
    return JSONResponse(
        {"ok": ok, "channel": "telegram", "sent_count": sent_count},
        status_code=200 if ok else 400,
    )


@app.post("/api/notifications/settings")
async def save_notification_settings(request: Request) -> Response:
    """Save Discord webhook URL, discord_enabled, and per-event trigger checkboxes on SystemSettings."""
    from sqlalchemy import select

    from app.config import reload_settings_from_db
    from app.db.models import SystemSettings

    payload = await _parse_request_payload(request)

    def _to_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ("1", "true", "on", "yes")

    async with async_session_factory() as session:
        stmt = select(SystemSettings).where(SystemSettings.id == 1)
        db_settings = (await session.execute(stmt)).scalars().first()
        if not db_settings:
            db_settings = SystemSettings(id=1)
            session.add(db_settings)

        if "discord_webhook_url" in payload:
            raw_url = str(payload.get("discord_webhook_url") or "").strip()
            db_settings.discord_webhook_url = raw_url or None

        db_settings.discord_enabled = _to_bool(payload.get("discord_enabled", False))
        db_settings.notify_on_push_initiated = _to_bool(
            payload.get("notify_on_push_initiated", False)
        )
        db_settings.notify_on_completed = _to_bool(
            payload.get("notify_on_completed", False)
        )
        db_settings.notify_on_failure = _to_bool(
            payload.get("notify_on_failure", False)
        )
        db_settings.notify_on_auto_advance = _to_bool(
            payload.get("notify_on_auto_advance", False)
        )
        db_settings.notify_on_errors = _to_bool(
            payload.get("notify_on_errors", db_settings.notify_on_errors)
        )
        db_settings.notify_on_outages = _to_bool(
            payload.get("notify_on_outages", db_settings.notify_on_outages)
        )

        await session.commit()
        await reload_settings_from_db(session)

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content='<div class="p-3 bg-emerald-900/40 border border-emerald-500/40 text-emerald-200 rounded-lg text-xs">Notification settings saved!</div>'
        )
    return JSONResponse({"ok": True})


# provider-specific code (allowlisted)
def _render_simkl_device_card(auth_session: dict[str, Any]) -> str:
    """Render the HTMX RFC 8628 Device Authorization card without exposing device_code."""
    session_id = auth_session["session_id"]
    user_code = auth_session["user_code"]
    verification_uri = auth_session.get("verification_uri", "https://simkl.com/pin")
    verification_uri_complete = auth_session.get(
        "verification_uri_complete", f"{verification_uri}?user_code={user_code}"
    )
    interval = int(auth_session.get("interval", 5) or 5)
    return f"""
    <div id="simkl-poll-status" hx-get="/simkl/auth/poll?session_id={session_id}" hx-trigger="every {interval}s" hx-swap="outerHTML" class="bg-[#111115] border border-[#d40060]/60 rounded-xl p-5 text-center mt-3 space-y-3">
        <h4 class="text-white font-bold text-sm">Simkl AUTH V2 Device Authorization</h4>
        <p class="text-[#a1a1aa] text-xs">Click the button below or visit <a href="{verification_uri}" target="_blank" rel="noopener noreferrer" class="text-[#d40060] hover:underline font-semibold">{verification_uri}</a> and confirm your 8-character code:</p>
        <div class="text-2xl font-mono font-bold text-[#d40060] tracking-widest bg-[#1e1e24] border border-[#3f3f46] rounded-lg py-2.5 px-4 inline-block">{user_code}</div>
        <div>
            <a href="{verification_uri_complete}" target="_blank" rel="noopener noreferrer" class="inline-flex items-center gap-2 px-4 py-2 bg-[#d40060] hover:bg-[#a3004a] text-white rounded-lg text-xs font-bold transition-colors shadow-lg shadow-[#d40060]/20">
                Open Simkl &amp; Authorize ({user_code})
            </a>
        </div>
        <div class="text-xs text-amber-400 animate-pulse pt-1">
            Waiting for authorization on Simkl (polling every {interval}s)...
        </div>
    </div>
    """


# provider-specific code (allowlisted)
@app.post("/simkl/auth/start")
async def start_simkl_auth(
    client_id: str = Form(""),
    provider_id: str | None = Form(None),
) -> HTMLResponse:
    """Start the Simkl AUTH V2 RFC 8628 Device Authorization Flow."""
    from app.services.watchlists import simkl

    clean_client_id = (client_id or "").strip()
    if not clean_client_id:
        return HTMLResponse(
            content='<div class="p-3 bg-amber-900/30 border border-amber-500/40 rounded-lg text-amber-200 text-xs mt-2">Please enter your Simkl V2 Client ID first.</div>'
        )

    try:
        data = await simkl.request_device_code(clean_client_id, scope="media:read")
    except simkl.SimklOAuthError as e:
        if e.status_code in (401, 412) or e.error in (
            "invalid_client",
            "client_id_failed",
        ):
            return HTMLResponse(
                content=(
                    '<div class="p-4 bg-red-900/30 border border-red-500/40 rounded-xl text-red-200 text-xs mt-2 space-y-1.5">'
                    '<div class="font-bold text-red-300">Simkl AUTH V2 Client ID Rejected ('
                    f"{e.status_code} {e.error})</div>"
                    "<p>Simkl AUTH V2 Device Flow requires a public client application. Please register or update your app at "
                    '<a href="https://simkl.com/settings/developer/" target="_blank" rel="noopener noreferrer" class="underline font-semibold text-white">simkl.com/settings/developer</a> '
                    "with <strong>Application Type</strong> set to <strong>TV, devices &amp; command line</strong> or "
                    "<strong>Mobile, desktop &amp; browser apps</strong>.</p>"
                    "</div>"
                )
            )
        return HTMLResponse(
            content=f'<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-red-300 text-xs mt-2">Error requesting PIN: {e}</div>'
        )
    except Exception as e:
        return HTMLResponse(
            content=f'<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-red-300 text-xs mt-2">Error requesting PIN: {e}</div>'
        )

    if not data.get("user_code") or not data.get("device_code"):
        return HTMLResponse(
            content='<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-red-300 text-xs mt-2">Invalid device authorization response from Simkl.</div>'
        )

    prov_id_int = (
        int(provider_id) if provider_id and str(provider_id).isdigit() else None
    )
    auth_session = simkl.create_device_auth_session(
        clean_client_id, data, provider_id=prov_id_int
    )
    return HTMLResponse(content=_render_simkl_device_card(auth_session))


_PLEX_ERR = (
    '<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg '
    'text-red-300 text-xs mt-2">{}</div>'
)


def _render_plex_pin_card(sess: dict[str, Any]) -> str:
    import html

    from app.services.watchlists import plex

    url = html.escape(
        plex.build_auth_url(sess["client_identifier"], sess["code"]), quote=True
    )
    poll = html.escape(
        f"/settings/provider/plex/auth/poll?session_id={sess['session_id']}"
    )
    return (
        f'<div class="p-3 bg-slate-800 rounded-lg text-xs mt-2" hx-get="{poll}" '
        'hx-trigger="every 3s" hx-swap="outerHTML">'
        f'Your PIN: <strong class="font-mono text-base">{html.escape(sess["code"])}</strong><br>'
        f'<a href="{url}" target="_blank" rel="noopener" class="text-sky-400 underline">'
        "Open Plex to authorize</a></div>"
    )


@app.post("/settings/provider/plex/auth/start")
async def start_plex_auth(provider_id: str = Form("")) -> HTMLResponse:
    """Request a Plex PIN for an already-saved Plex provider row."""
    import uuid

    from app.db.models import Provider
    from app.services.watchlists import plex

    missing = HTMLResponse(
        content=_PLEX_ERR.format("Save the Plex provider first."), status_code=400
    )
    if not provider_id.strip().isdigit():
        return missing
    async with async_session_factory() as session:
        provider = await session.get(Provider, int(provider_id))
        if provider is None or provider.type != "plex":
            return missing
        cfg = provider.watchlist_config
        client_identifier = str(cfg.get("plex_client_identifier") or "")
        if not client_identifier:
            client_identifier = str(uuid.uuid4())
            try:
                raw = json.loads(provider.config_json or "{}")
            except ValueError:
                raw = {}
            raw["plex_client_identifier"] = client_identifier
            provider.config_json = json.dumps(raw)
            await session.commit()
    try:
        pin = await plex.create_pin(client_identifier)
    except Exception as e:
        return HTMLResponse(
            content=_PLEX_ERR.format(f"Error requesting Plex PIN: {type(e).__name__}")
        )
    sess = plex.create_pin_session(pin, client_identifier, int(provider_id))
    return HTMLResponse(content=_render_plex_pin_card(sess))


@app.get("/settings/provider/plex/auth/poll")
async def poll_plex_auth(session_id: str = "") -> HTMLResponse:
    """Poll plex.tv for the PIN claim; on success persist the token."""
    from app.db.models import Provider
    from app.services.watchlists import plex

    sess = plex.get_pin_session(session_id)
    if sess is None:
        return HTMLResponse(
            content='<div class="p-3 bg-amber-900/30 border border-amber-500/40 rounded-lg '
            'text-amber-200 text-xs mt-2">Expired PIN — please click Get PIN again.</div>'
        )
    try:
        token = await plex.poll_pin(
            sess["pin_id"], sess["code"], sess["client_identifier"]
        )
    except Exception as e:
        plex.pop_pin_session(session_id)
        return HTMLResponse(
            content=_PLEX_ERR.format(f"Error polling Plex: {type(e).__name__}")
        )
    if not token:
        return HTMLResponse(content=_render_plex_pin_card(sess))
    plex.pop_pin_session(session_id)
    async with async_session_factory() as session:
        provider = await session.get(Provider, int(sess["provider_id"]))
        if provider is None:
            return HTMLResponse(
                content=_PLEX_ERR.format("Save the Plex provider first."),
                status_code=400,
            )
        provider.access_token = token
        await session.commit()
    return HTMLResponse(
        content='<div class="p-3 bg-emerald-900/30 border border-emerald-500/40 '
        'rounded-lg text-emerald-300 text-xs mt-2">Connected</div>'
    )


_MAL_CALLBACK_PATH = "/settings/provider/mal/auth/callback"


def _mal_status_fragment(provider_id: int, connected: bool) -> str:
    if connected:
        return '<div class="text-emerald-300 text-xs mt-2">✓ Connected</div>'
    return (
        '<div class="text-slate-400 text-xs mt-2" '
        f'hx-get="/settings/provider/mal/auth/status?provider_id={provider_id}" '
        'hx-trigger="every 3s" hx-swap="outerHTML">Waiting for authorization…</div>'
    )


@app.post("/settings/provider/mal/auth/start")
async def start_mal_auth(
    request: Request, client_id: str = Form(""), provider_id: str = Form("")
) -> HTMLResponse:
    """Build the MAL PKCE authorize link for an already-saved MAL provider."""
    import html

    from app.db.models import Provider
    from app.services.watchlists import mal

    def err(msg: str) -> HTMLResponse:
        return HTMLResponse(content=_PLEX_ERR.format(msg), status_code=400)

    clean_client_id = client_id.strip()
    if not provider_id.strip().isdigit():
        return err("Save the MyAnimeList provider first.")
    async with async_session_factory() as session:
        provider = await session.get(Provider, int(provider_id))
        if provider is None or provider.type != "mal":
            return err("Save the MyAnimeList provider first.")
        if not clean_client_id:
            clean_client_id = str(provider.client_id or "").strip()
        if not clean_client_id:
            return err("Enter your MyAnimeList client ID first.")
        provider.client_id = clean_client_id
        await session.commit()
    redirect_uri = str(request.base_url).rstrip("/") + _MAL_CALLBACK_PATH
    verifier, challenge = mal.generate_pkce_pair()
    state = mal.create_auth_state(verifier, int(provider_id))
    url = mal.build_authorize_url(clean_client_id, challenge, state, redirect_uri)
    return HTMLResponse(
        content=(
            '<div class="p-3 bg-slate-800 rounded-lg text-xs mt-2">'
            f'<a href="{html.escape(url, quote=True)}" target="_blank" '
            'rel="noopener noreferrer" class="text-sky-400 underline">'
            "Authorize on MyAnimeList</a>"
            '<div class="mt-2 text-slate-400">Register this redirect URI in your MAL app: '
            f'<code class="font-mono">{html.escape(redirect_uri)}</code></div>'
            f"{_mal_status_fragment(int(provider_id), False)}</div>"
        )
    )


@app.get("/settings/provider/mal/auth/callback")
async def mal_auth_callback(
    request: Request, code: str = "", state: str = ""
) -> HTMLResponse:
    """OAuth redirect target: verify state, exchange the code, store tokens."""
    from app.db.models import Provider
    from app.services.watchlists import mal

    pending = mal.pop_auth_state(state) if state else None
    if pending is None or not code:
        return HTMLResponse(
            content="Invalid or expired authorization request. Please start again.",
            status_code=400,
        )
    verifier, provider_id = pending
    redirect_uri = str(request.base_url).rstrip("/") + _MAL_CALLBACK_PATH
    async with async_session_factory() as session:
        provider = await session.get(Provider, provider_id)
        if provider is None:
            return HTMLResponse(content="Provider not found.", status_code=400)
        try:
            data = await mal.exchange_code(
                str(provider.client_id or ""), code, verifier, redirect_uri
            )
        except Exception as e:
            logger.warning("MAL token exchange failed: %s", type(e).__name__)
            return HTMLResponse(
                content="MyAnimeList authorization failed. Please try again.",
                status_code=400,
            )
        try:
            raw = json.loads(provider.config_json or "{}")
        except ValueError:
            raw = {}
        raw["refresh_token"] = data.get("refresh_token")
        raw["token_expires_at"] = data["token_expires_at"]
        provider.access_token = str(data.get("access_token") or "")
        provider.config_json = json.dumps(raw)
        await session.commit()
    return HTMLResponse(
        content=(
            "<!doctype html><html><body><p>Connected — you may close this tab.</p>"
            '<script>setTimeout(function(){location.href="/settings"},1500)</script>'
            "</body></html>"
        )
    )


@app.get("/settings/provider/mal/auth/status")
async def mal_auth_status(provider_id: str = "") -> HTMLResponse:
    """Polled by the modal/wizard while the OAuth flow runs in another tab."""
    from app.db.models import Provider

    pid = int(provider_id) if provider_id.strip().isdigit() else 0
    async with async_session_factory() as session:
        provider = await session.get(Provider, pid) if pid else None
        connected = (
            provider is not None and provider.watchlist_connection_status == "connected"
        )
    return HTMLResponse(content=_mal_status_fragment(pid, connected))


# provider-specific code (allowlisted)
@app.get("/simkl/auth/poll")
async def poll_simkl_auth(session_id: str = "") -> HTMLResponse:
    """Poll Simkl AUTH V2 token endpoint using server-side session_id without exposing device_code."""
    import json

    from sqlalchemy import select

    from app.db.models import Provider
    from app.services.watchlists import simkl

    auth_session = simkl.get_device_auth_session(session_id)
    if auth_session is None:
        return HTMLResponse(
            content='<div class="p-3 bg-amber-900/30 border border-amber-500/40 rounded-lg text-amber-200 text-xs mt-2">Expired PIN — please click Get PIN &amp; Connect again.</div>'
        )

    try:
        token_data = await simkl.poll_device_token(
            auth_session["client_id"], auth_session["device_code"]
        )
    except simkl.SimklOAuthError as e:
        if e.error == "authorization_pending":
            return HTMLResponse(content=_render_simkl_device_card(auth_session))
        if e.error == "slow_down":
            auth_session["interval"] = int(auth_session.get("interval", 5) or 5) + 5
            return HTMLResponse(content=_render_simkl_device_card(auth_session))
        simkl.pop_device_auth_session(session_id)
        if e.error in ("expired_token", "access_denied"):
            return HTMLResponse(
                content=f'<div class="p-3 bg-amber-900/30 border border-amber-500/40 rounded-lg text-amber-200 text-xs mt-2">Simkl authorization ended ({e.error}). Please click Get PIN &amp; Connect to try again.</div>'
            )
        return HTMLResponse(
            content=f'<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-red-300 text-xs mt-2">Error polling Simkl status: {e}</div>'
        )
    except Exception as e:
        simkl.pop_device_auth_session(session_id)
        return HTMLResponse(
            content=f'<div class="p-3 bg-red-900/30 border border-red-500/40 rounded-lg text-red-300 text-xs mt-2">Error polling status: {e}</div>'
        )

    simkl.pop_device_auth_session(session_id)
    client_id = str(auth_session["client_id"])
    access_token = str(token_data.get("access_token") or "")
    refresh_token = str(token_data.get("refresh_token") or "")
    token_expires_at = str(token_data.get("token_expires_at") or "")
    target_provider_id = auth_session.get("provider_id")

    # Persist to existing Simkl Provider row in SQLite if present, and revoke old token best-effort
    async with async_session_factory() as session:
        provider: Provider | None = None
        if target_provider_id:
            provider = await session.get(Provider, int(target_provider_id))
        if provider is None:
            stmt = select(Provider).where(Provider.type == "simkl")
            provider = (await session.execute(stmt)).scalars().first()

        if provider is not None:
            old_cfg = provider.watchlist_config
            old_cid = str(
                provider.client_id or old_cfg.get("client_id") or client_id
            ).strip()
            old_tok = str(
                old_cfg.get("refresh_token")
                or provider.access_token
                or old_cfg.get("access_token")
                or ""
            ).strip()
            if old_tok and old_tok not in (access_token, refresh_token):
                await simkl.revoke_token(old_cid, old_tok)

            provider.client_id = client_id
            provider.access_token = access_token
            old_cfg["client_id"] = client_id
            old_cfg["access_token"] = access_token
            old_cfg["refresh_token"] = refresh_token
            old_cfg["token_expires_at"] = token_expires_at
            provider.config_json = json.dumps(old_cfg)
            await session.commit()

    return HTMLResponse(
        content=f"""
        <div class="p-3 bg-emerald-900/30 border border-emerald-500/40 rounded-lg text-emerald-300 text-xs font-bold mt-2 flex items-center justify-between">
            <span>Connected · AUTH V2 (Auto-Refresh Enabled)</span>
        </div>
        <script>
            (function() {{
                const atInput = document.getElementById('access_token') || document.getElementById('simkl-access-token');
                if (atInput) atInput.value = "{access_token}";
                const rtInput = document.getElementById('simkl-refresh-token');
                if (rtInput) rtInput.value = "{refresh_token}";
                const expInput = document.getElementById('simkl-token-expires-at');
                if (expInput) expInput.value = "{token_expires_at}";
                const badge = document.getElementById('simkl-modal-status-badge');
                if (badge) {{
                    badge.textContent = "Connected · AUTH V2 (Auto-Refresh)";
                    badge.className = "px-2.5 py-1 rounded-full text-xs font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30";
                }}
            }})();
        </script>
        """
    )


@app.get("/system/check/modal", response_class=HTMLResponse)
async def system_check_modal(request: Request):
    """Render the system check modal."""
    return templates.TemplateResponse(
        request=request, name="modals/system_check.html", context={}
    )


@app.get("/system/export-sanitized-db")
async def export_sanitized_db() -> Response:
    """Download a credential-free snapshot of the database (m71, ADR-154)."""
    import os
    import tempfile

    from starlette.background import BackgroundTask

    from app.db.database import get_engine
    from app.services.db_export import (
        create_sanitized_snapshot,
        sanitized_export_filename,
    )

    fd, path = tempfile.mkstemp(prefix="smewlet_sanitized_", suffix=".db")
    os.close(fd)
    Path(path).unlink()
    try:
        await create_sanitized_snapshot(get_engine(), path)
    except Exception:
        logger.exception("Sanitized database export failed")
        Path(path).unlink(missing_ok=True)
        return JSONResponse(
            status_code=500, content={"detail": "Sanitized export failed"}
        )
    return FileResponse(
        path,
        media_type="application/vnd.sqlite3",
        filename=sanitized_export_filename(),
        headers={"Cache-Control": "no-store"},
        background=BackgroundTask(_unlink_quietly, path),
    )


def _unlink_quietly(path: str) -> None:
    Path(path).unlink(missing_ok=True)


@app.get("/feedback/modal", response_class=HTMLResponse)
async def feedback_modal(
    request: Request, type: Literal["bug", "feature", "question"] = "bug"
) -> Response:
    """Render the feedback launcher modal with privacy-safe diagnostics."""
    from app.core.diagnostics import (
        GITHUB_BUG_REPORT_URL,
        GITHUB_FEATURE_REQUEST_URL,
        GITHUB_QUESTION_URL,
        GITHUB_SECURITY_ADVISORY_URL,
        build_diagnostics_table_markdown,
        collect_feedback_diagnostics,
    )

    async with async_session_factory() as session:
        diag = await collect_feedback_diagnostics(session)
    return templates.TemplateResponse(
        request=request,
        name="modals/feedback_modal.html",
        context={
            "initial_tab": type,
            "diagnostics": diag,
            "diagnostics_markdown": build_diagnostics_table_markdown(diag),
            "issue_form_urls": {
                "bug": GITHUB_BUG_REPORT_URL,
                "feature": GITHUB_FEATURE_REQUEST_URL,
                "question": GITHUB_QUESTION_URL,
            },
            "security_advisory_url": GITHUB_SECURITY_ADVISORY_URL,
        },
    )


def _resolve_onboarding_type(
    payload: dict[str, Any],
    key: str,
    valid: frozenset[str],
    *,
    label: str,
    legacy: str = "",
) -> str:
    """Selected type slug for an onboarding Step 1 section; "" = None/Skip.

    A present key wins (blank skips, unknown raises ``ValueError``); an absent key
    falls back to the caller-computed ``legacy`` inference.
    """
    if key not in payload:
        return legacy
    value = str(payload.get(key) or "").strip().lower()
    if value and value not in valid:
        raise ValueError(f"Unknown {label} type.")
    return value


_ONBOARDING_WATCHLIST_TYPES = WATCHLIST_TYPES
_ONBOARDING_SEERR_TYPES = ("jellyseerr", "overseerr")


async def _existing_seerr_key(ptype: str) -> bool:
    """True when a Jellyseerr/Overseerr provider of ``ptype`` already stores an API key."""
    from sqlalchemy import select

    from app.db.models import Provider

    async with async_session_factory() as session:
        prov = (
            (
                await session.execute(
                    select(Provider).where(Provider.type == ptype).order_by(Provider.id)
                )
            )
            .scalars()
            .first()
        )
    return _seerr_onboarding_context(prov)["has_key"]


def _seerr_onboarding_context(provider: Any) -> dict[str, Any]:
    """Prefill values for a Jellyseerr/Overseerr onboarding panel (the key is never echoed)."""
    try:
        cfg = json.loads(provider.config_json or "{}") if provider else {}
    except ValueError:
        cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    return {
        "server_url": str(cfg.get("server_url") or ""),
        "has_key": bool(cfg.get("api_key")),
    }


_ONBOARDING_DOWNLOADER_TYPES = DOWNLOADER_TYPES


@app.get("/setup/status")
async def setup_status(request: Request) -> dict[str, Any]:
    """Polled by the Setup required page to reload once the prerequisites are met."""
    from app.core.prerequisites import get_setup_status

    status = getattr(request.state, "setup", None)
    if status is None:  # guard disabled (tests)
        async with async_session_factory() as session:
            status = await get_setup_status(session)
    return {"ready": status.core_ready, "missing": list(status.missing_core)}


# provider-specific code (allowlisted)
@app.get("/onboarding/modal", response_class=HTMLResponse)
async def onboarding_modal(request: Request) -> Response:
    """Render the first-run Setup Wizard, pre-populated from existing records."""
    from sqlalchemy import select

    from app.db.models import NotificationChannel, Provider, SystemSettings

    async def _first_provider(session: Any, ptype: str) -> Provider | None:
        return (
            (
                await session.execute(
                    select(Provider).where(Provider.type == ptype).order_by(Provider.id)
                )
            )
            .scalars()
            .first()
        )

    async with async_session_factory() as session:
        simkl_p = await _first_provider(session, "simkl")
        anilist_p = await _first_provider(session, "anilist")
        plex_p = await _first_provider(session, "plex")
        mal_p = await _first_provider(session, "mal")
        seerr_ps = {
            t: await _first_provider(session, t) for t in _ONBOARDING_SEERR_TYPES
        }
        active_wl = (
            (
                await session.execute(
                    select(Provider)
                    .where(
                        Provider.category == "watchlist",
                        Provider.is_active.is_(True),
                        Provider.type.in_(_ONBOARDING_WATCHLIST_TYPES),
                    )
                    .order_by(Provider.priority, Provider.id)
                )
            )
            .scalars()
            .first()
        )
        tmdb_p = await _first_provider(session, "tmdb")
        torbox_p = await _first_provider(session, "torbox")
        sab_p = await _first_provider(session, "sabnzbd")
        pmz_p = await _first_provider(session, "premiumize")
        active_dl = (
            (
                await session.execute(
                    select(Provider)
                    .where(
                        Provider.category == "downloader",
                        Provider.is_active.is_(True),
                    )
                    .order_by(Provider.id)
                )
            )
            .scalars()
            .first()
        )
        indexer_p = (
            (
                await session.execute(
                    select(Provider)
                    .where(Provider.category == "indexer")
                    .order_by(Provider.id)
                )
            )
            .scalars()
            .first()
        )
        sys_settings = await session.get(SystemSettings, 1)
        telegram_c = (
            (
                await session.execute(
                    select(NotificationChannel)
                    .where(NotificationChannel.type == "telegram")
                    .order_by(NotificationChannel.id)
                )
            )
            .scalars()
            .first()
        )

    from app.services.downloaders.sabnzbd import parse_sabnzbd_api_url

    sab_host, sab_port, sab_ssl = parse_sabnzbd_api_url(
        sab_p.api_url if sab_p else None
    )
    context: dict[str, Any] = {
        "simkl": {
            "client_id": (simkl_p.client_id if simkl_p else None) or "",
            "username": (simkl_p.username if simkl_p else None) or "",
            "provider_id": str(simkl_p.id) if simkl_p else "",
            "connected": "1" if simkl_p and simkl_p.access_token else "",
        },
        "watchlist_type": active_wl.type if active_wl else "",
        "anilist": {
            "username": (anilist_p.username if anilist_p else None) or "",
            "has_token": bool(anilist_p and anilist_p.access_token),
        },
        "plex": {
            "provider_id": str(plex_p.id) if plex_p else "",
            "connected": "1"
            if plex_p and plex_p.watchlist_connection_status == "connected"
            else "",
            "has_token": bool(plex_p and plex_p.access_token),
        },
        "mal": {
            "client_id": (mal_p.client_id if mal_p else None) or "",
            "provider_id": str(mal_p.id) if mal_p else "",
            "connected": "1"
            if mal_p and mal_p.watchlist_connection_status == "connected"
            else "",
            "redirect_uri": str(request.base_url).rstrip("/") + _MAL_CALLBACK_PATH,
        },
        "seerr": {t: _seerr_onboarding_context(p) for t, p in seerr_ps.items()},
        "tmdb": {"api_key": (tmdb_p.api_key if tmdb_p else None) or ""},
        "torbox": {
            "api_key": (torbox_p.api_key if torbox_p else None) or "",
            "api_url": (torbox_p.api_url if torbox_p else None)
            or "https://api.torbox.app/v1/api",
        },
        "downloader_type": (
            active_dl.type
            if active_dl and active_dl.type in _ONBOARDING_DOWNLOADER_TYPES
            else ""
        ),
        "sabnzbd": {
            "host": sab_host,
            "port": str(sab_port),
            "ssl": sab_ssl,
            "api_key": (sab_p.api_key if sab_p else None) or "",
        },
        "premiumize": {"api_key": (pmz_p.api_key if pmz_p else None) or ""},
        "indexer_type": (
            indexer_p.type if indexer_p and is_indexer_type(indexer_p.type) else ""
        ),
        "indexer_presets": INDEXER_PRESETS,
        "indexer_preset_map": {
            p.type: {"name": p.name, "api_url": p.api_url, "key_url": p.key_url}
            for p in INDEXER_PRESETS
        },
        "indexer": {
            "name": (indexer_p.name if indexer_p else None) or "",
            "api_url": (indexer_p.api_url if indexer_p else None) or "",
            "api_key": (indexer_p.api_key if indexer_p else None) or "",
        },
        "discord": {
            "webhook_url": (sys_settings.discord_webhook_url if sys_settings else None)
            or "",
            "enabled": bool(sys_settings and sys_settings.discord_enabled),
        },
        "telegram": {
            "name": (telegram_c.name if telegram_c else None) or "",
            "bot_token": (telegram_c.bot_token if telegram_c else None) or "",
            "chat_id": (telegram_c.chat_id if telegram_c else None) or "",
        },
    }
    return templates.TemplateResponse(
        request=request, name="modals/onboarding.html", context=context
    )


# provider-specific code (allowlisted)
@app.post("/api/onboarding/step1")
async def onboarding_step1(request: Request) -> Response:
    """Atomically upsert Simkl/TMDB/TorBox/indexer providers from optional sections."""
    from sqlalchemy import select

    from app.db.models import Provider, ProviderCategory
    from app.services import provider_service
    from app.services.downloaders.sabnzbd import build_sabnzbd_api_url

    payload = await _parse_request_payload(request)

    def _f(key: str) -> str:
        return str(payload.get(key) or "").strip()

    simkl_client_id, simkl_username = _f("simkl_client_id"), _f("simkl_username")
    tmdb_api_key = _f("tmdb_api_key")
    torbox_api_key, torbox_api_url = _f("torbox_api_key"), _f("torbox_api_url")
    indexer_name, indexer_url, indexer_key = (
        _f("indexer_name"),
        _f("indexer_url"),
        _f("indexer_api_key"),
    )

    try:
        watchlist_type = _resolve_onboarding_type(
            payload,
            "watchlist_type",
            _ONBOARDING_WATCHLIST_TYPES,
            label="watchlist",
            legacy="simkl" if simkl_client_id or simkl_username else "",
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    try:
        downloader_type = _resolve_onboarding_type(
            payload,
            "downloader_type",
            _ONBOARDING_DOWNLOADER_TYPES,
            label="downloader",
            legacy="torbox" if torbox_api_key else "",
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    sab_host, sab_key = _f("sabnzbd_host"), _f("sabnzbd_api_key")
    sab_port = 8080
    sab_ssl = _f("sabnzbd_ssl").lower() in ("1", "true", "on", "yes")
    pmz_key = _f("premiumize_api_key")
    dl_provided = {
        "torbox": bool(torbox_api_key),
        "sabnzbd": bool(sab_host or sab_key),
        "premiumize": bool(pmz_key),
    }.get(downloader_type, False)
    if downloader_type == "sabnzbd" and dl_provided:
        error = ""
        if not (sab_host and sab_key):
            error = "SABnzbd requires host and API key."
        elif "://" in sab_host or "/" in sab_host:
            error = "SABnzbd host must be a hostname or IP address."
        else:
            port_raw = _f("sabnzbd_port")
            try:
                sab_port = int(port_raw) if port_raw else 8080
            except ValueError:
                sab_port = 0
            if not 1 <= sab_port <= 65535:
                error = "SABnzbd port must be between 1 and 65535."
        if error:
            return JSONResponse({"ok": False, "error": error}, status_code=400)

    anilist_username, anilist_token = _f("anilist_username"), _f("anilist_access_token")
    plex_token = _f("plex_auth_token")
    plex_prepare = _f("watchlist_prepare").lower() in ("1", "true", "on", "yes")
    mal_client_id = _f("mal_client_id")
    seerr_url = _f("seerr_server_url").rstrip("/")
    seerr_key = _f("seerr_api_key")
    wl_provided = {
        "simkl": bool(simkl_client_id or simkl_username),
        "anilist": bool(anilist_username or anilist_token),
        "plex": bool(plex_token or plex_prepare),
        "mal": bool(mal_client_id),
        "jellyseerr": bool(seerr_url or seerr_key),
        "overseerr": bool(seerr_url or seerr_key),
    }.get(watchlist_type, False)
    if watchlist_type == "anilist" and wl_provided and not anilist_username:
        return JSONResponse(
            {"ok": False, "error": "AniList requires a username."}, status_code=400
        )
    if watchlist_type in _ONBOARDING_SEERR_TYPES and wl_provided:
        seerr_error = ""
        if not re.fullmatch(r"https?://\S+", seerr_url, re.IGNORECASE):
            seerr_error = (
                "Server URL is required and must start with http:// or https://."
            )
        elif not seerr_key and not (await _existing_seerr_key(watchlist_type)):
            seerr_error = "API key is required."
        if seerr_error:
            return JSONResponse({"ok": False, "error": seerr_error}, status_code=400)

    try:
        indexer_type = _resolve_onboarding_type(
            payload,
            "indexer_type",
            frozenset(p.type for p in INDEXER_PRESETS),
            label="indexer",
            legacy="newznab" if indexer_url or indexer_key else "",
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    indexer_preset = get_preset(indexer_type)
    indexer_provided = bool(indexer_type and (indexer_url or indexer_key))
    if (
        indexer_provided
        and not indexer_url
        and "indexer_type" in payload
        and indexer_preset is not None
    ):
        indexer_url = indexer_preset.api_url
    if indexer_provided and not (indexer_url and indexer_key):
        return JSONResponse(
            {"ok": False, "error": "Indexer requires both URL and API key."},
            status_code=400,
        )

    async with async_session_factory() as session:

        async def _first(ptype: str) -> Provider | None:
            return (
                (
                    await session.execute(
                        select(Provider)
                        .where(Provider.type == ptype)
                        .order_by(Provider.id)
                    )
                )
                .scalars()
                .first()
            )

        async def _upsert(
            ptype: str, category: ProviderCategory, default_name: str
        ) -> Provider:
            prov = await _first(ptype)
            if prov is None:
                prov = Provider(
                    type=ptype,
                    category=category.value,
                    name=default_name,
                    is_active=True,
                    priority=1,
                )
                session.add(prov)
            return prov

        providers: dict[str, Provider | None] = {
            "simkl": await _first("simkl"),
            "tmdb": await _first("tmdb"),
            "torbox": await _first("torbox"),
            "downloader": None,
            "indexer": (
                (
                    await session.execute(
                        select(Provider)
                        .where(Provider.category == "indexer")
                        .order_by(Provider.id)
                    )
                )
                .scalars()
                .first()
            ),
        }

        if watchlist_type == "simkl" and wl_provided:
            is_new = providers["simkl"] is None
            simkl = await _upsert("simkl", ProviderCategory.WATCHLIST, "Simkl")
            simkl.client_id = simkl_client_id
            simkl.username = simkl_username
            try:
                cfg = json.loads(simkl.config_json or "{}")
            except ValueError:
                cfg = {}
            if not isinstance(cfg, dict):
                cfg = {}
            cfg["client_id"] = simkl_client_id
            if is_new:
                cfg.update(
                    sync_movies=True,
                    sync_series=True,
                    sync_anime=True,
                    sync_interval_minutes=0,
                )
            simkl.config_json = json.dumps(cfg)
            providers["simkl"] = simkl

        selected_wl: Provider | None = None
        if watchlist_type == "simkl":
            selected_wl = providers["simkl"]
        elif watchlist_type:
            names = WATCHLIST_DISPLAY_NAMES
            selected_wl = await _first(watchlist_type)
            if wl_provided:
                if selected_wl is None:
                    selected_wl = await _upsert(
                        watchlist_type,
                        ProviderCategory.WATCHLIST,
                        names[watchlist_type],
                    )
                    selected_wl.config_json = json.dumps({"sync_interval_minutes": 0})
                if watchlist_type == "anilist":
                    selected_wl.username = anilist_username
                    if anilist_token:
                        selected_wl.access_token = anilist_token
                elif watchlist_type in _ONBOARDING_SEERR_TYPES:
                    try:
                        seerr_cfg = json.loads(selected_wl.config_json or "{}")
                    except ValueError:
                        seerr_cfg = {}
                    if not isinstance(seerr_cfg, dict):
                        seerr_cfg = {}
                    seerr_cfg.setdefault("sync_admin_watchlist", False)
                    seerr_cfg.setdefault("notify_available", True)
                    seerr_cfg.setdefault("sync_movies", True)
                    seerr_cfg.setdefault("sync_series", True)
                    seerr_cfg["server_url"] = seerr_url
                    if seerr_key:
                        seerr_cfg["api_key"] = seerr_key
                    selected_wl.api_key = ""
                    selected_wl.config_json = json.dumps(seerr_cfg)
                elif watchlist_type == "plex":
                    if plex_token:
                        selected_wl.access_token = plex_token
                    try:
                        plex_cfg = json.loads(selected_wl.config_json or "{}")
                    except ValueError:
                        plex_cfg = {}
                    if not isinstance(plex_cfg, dict):
                        plex_cfg = {}
                    if not plex_cfg.get("plex_client_identifier"):
                        plex_cfg["plex_client_identifier"] = str(uuid.uuid4())
                    selected_wl.config_json = json.dumps(plex_cfg)
                else:
                    selected_wl.client_id = mal_client_id

        if tmdb_api_key:
            tmdb = await _upsert("tmdb", ProviderCategory.METADATA, "TMDB")
            tmdb.api_key = tmdb_api_key
            providers["tmdb"] = tmdb

        selected_dl: Provider | None = None
        if dl_provided:
            if downloader_type == "torbox":
                selected_dl = await _upsert(
                    "torbox", ProviderCategory.DOWNLOADER, "TorBox"
                )
                selected_dl.api_key = torbox_api_key
                selected_dl.api_url = torbox_api_url or "https://api.torbox.app/v1/api"
                providers["torbox"] = selected_dl
            elif downloader_type == "sabnzbd":
                sab_is_new = await _first("sabnzbd") is None
                selected_dl = await _upsert(
                    "sabnzbd", ProviderCategory.DOWNLOADER, "SABnzbd"
                )
                if sab_is_new:
                    selected_dl.config_json = json.dumps(
                        {
                            "category_movie": "movies",
                            "category_series": "tv",
                            "category_anime": "anime",
                        }
                    )
                selected_dl.api_key = sab_key
                selected_dl.api_url = build_sabnzbd_api_url(sab_host, sab_port, sab_ssl)
            else:
                selected_dl = await _upsert(
                    "premiumize", ProviderCategory.DOWNLOADER, "Premiumize"
                )
                selected_dl.api_key = pmz_key

        if indexer_provided and indexer_preset is not None:
            indexer = await _upsert(
                indexer_type, ProviderCategory.INDEXER, indexer_preset.name
            )
            if indexer_name:
                indexer.name = indexer_name
            indexer.api_url = indexer_url
            indexer.api_key = indexer_key
            providers["indexer"] = indexer

        await session.flush()
        if selected_dl is not None:
            # set_active_downloader commits the whole transaction.
            await provider_service.set_active_downloader(session, selected_dl.id)
        else:
            await session.commit()

        provider_ids = {
            key: (prov.id if prov is not None else None)
            for key, prov in providers.items()
        }
        provider_ids["downloader"] = selected_dl.id if selected_dl is not None else None
        provider_ids["watchlist"] = selected_wl.id if selected_wl is not None else None
    return JSONResponse({"ok": True, "provider_ids": provider_ids})


@app.post("/api/onboarding/step2")
async def onboarding_step2(request: Request) -> Response:
    """Atomically save optional Discord (SystemSettings) and Telegram (NotificationChannel)."""
    from sqlalchemy import select

    from app.config import reload_settings_from_db
    from app.db.models import NotificationChannel, SystemSettings

    payload = await _parse_request_payload(request)

    def _f(key: str) -> str:
        return str(payload.get(key) or "").strip()

    discord_url = _f("discord_webhook_url")
    discord_enabled = _f("discord_enabled").lower() in ("1", "true", "on", "yes")
    tg_name, tg_token, tg_chat = (
        _f("telegram_name"),
        _f("telegram_bot_token"),
        _f("telegram_chat_id"),
    )

    telegram_provided = bool(tg_token or tg_chat)
    if telegram_provided and not (tg_token and tg_chat):
        return JSONResponse(
            {"ok": False, "error": "Telegram requires both Bot Token and Chat ID."},
            status_code=400,
        )

    async with async_session_factory() as session:
        if discord_url:
            row = await session.get(SystemSettings, 1)
            if row is None:
                row = SystemSettings(id=1)
                session.add(row)
            row.discord_webhook_url = discord_url
            row.discord_enabled = discord_enabled

        if telegram_provided:
            channel = (
                (
                    await session.execute(
                        select(NotificationChannel)
                        .where(NotificationChannel.type == "telegram")
                        .order_by(NotificationChannel.id)
                    )
                )
                .scalars()
                .first()
            )
            if channel is None:
                channel = NotificationChannel(type="telegram", name="Telegram")
                session.add(channel)
            channel.name = tg_name or "Telegram"
            channel.bot_token = tg_token
            channel.chat_id = tg_chat

        await session.commit()
        await reload_settings_from_db(session)
    return JSONResponse({"ok": True})


@app.post("/api/onboarding/complete")
async def onboarding_complete() -> Response:
    """Permanently mark first-run onboarding as completed/skipped (idempotent)."""
    from app.db.models import SystemSettings

    async with async_session_factory() as session:
        row = await session.get(SystemSettings, 1)
        if row is None:
            row = SystemSettings(id=1)
            session.add(row)
        row.onboarding_completed = True
        await session.commit()
    return HTMLResponse(
        content="",
        headers={
            "HX-Trigger": '{"onboardingCompleted": true, "refreshDashboard": true}'
        },
    )


@app.get("/system/check/run", response_class=HTMLResponse)
async def run_system_check(request: Request):
    """Run concurrent diagnostic tests for configured services and core infrastructure."""
    import asyncio
    import html as html_escape
    import os
    import shutil
    import time

    import httpx
    from sqlalchemy import select, text

    from app.config import DEFAULT_USER_AGENT
    from app.db.models import NotificationChannel, Provider, SystemSettings
    from app.services import anilist

    # provider-specific code (allowlisted)
    from app.services.watchlists import (
        mal,
        plex,
        simkl,
    )

    async with async_session_factory() as session:
        providers = (await session.execute(select(Provider))).scalars().all()
        notifications = (
            (await session.execute(select(NotificationChannel))).scalars().all()
        )
        sys_settings = (
            (
                await session.execute(
                    select(SystemSettings).where(SystemSettings.id == 1)
                )
            )
            .scalars()
            .first()
        )

    # Active providers & notification channels
    active_providers = [p for p in providers if p.is_active]
    active_indexers = [
        p
        for p in active_providers
        if p.category == "indexer" or is_indexer_type(p.type)
    ]
    active_downloaders = [
        p
        for p in active_providers
        if p.category == "downloader" or p.type in ("torbox", "sabnzbd", "premiumize")
    ]
    active_watchlists = [p for p in active_providers if p.category == "watchlist"]
    active_tmdb = next(
        (p for p in active_providers if p.type == "tmdb"),
        None,
    )

    # Vital role warning banners
    vital_warnings: list[str] = []
    if not active_downloaders:
        vital_warnings.append(
            "No Usenet Downloader active. Downloads cannot be dispatched."
        )
    if not active_indexers:
        vital_warnings.append(
            "No Usenet Indexer active. Automated searches will yield no releases."
        )

    # Define individual probe coroutines
    async def probe_database() -> dict[str, Any]:
        start = time.perf_counter()
        try:
            async with async_session_factory() as s:
                await asyncio.wait_for(s.execute(text("SELECT 1")), timeout=5.0)
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "SQLite Database",
                "category": "Core Infrastructure",
                "ok": True,
                "msg": "Operational (SELECT 1 passed)",
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "SQLite Database",
                "category": "Core Infrastructure",
                "ok": False,
                "msg": f"Query failed: {e}",
                "latency_ms": latency,
            }

    async def probe_storage() -> dict[str, Any]:
        start = time.perf_counter()
        try:
            # Check write permissions and disk space in /app/data, fallback to ./config or .
            candidate_paths = ["/app/data", "/app/config", "./config", "."]
            target_path = next((p for p in candidate_paths if os.path.exists(p)), ".")

            # Test write access
            test_file = os.path.join(target_path, ".diag_write_test")
            try:
                with open(test_file, "w") as f:
                    f.write("ok")
                if os.path.exists(test_file):
                    os.remove(test_file)
            except Exception as write_err:
                latency = int((time.perf_counter() - start) * 1000)
                return {
                    "name": "Persistent Storage",
                    "category": "Core Infrastructure",
                    "ok": False,
                    "msg": f"Read-only filesystem: {write_err}",
                    "latency_ms": latency,
                }

            total, used, free = shutil.disk_usage(target_path)
            free_gb = free / (1024**3)
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "Persistent Storage",
                "category": "Core Infrastructure",
                "ok": True,
                "msg": f"Writable ({free_gb:.1f} GB free)",
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "Persistent Storage",
                "category": "Core Infrastructure",
                "ok": False,
                "msg": f"Storage probe failed: {e}",
                "latency_ms": latency,
            }

    async def probe_anilist() -> dict[str, Any]:
        start = time.perf_counter()
        try:
            query = {"query": "{ Page(perPage: 1) { media(type: ANIME) { id } } }"}
            async with httpx.AsyncClient(
                timeout=5.0, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as client:
                r = await client.post(anilist.ANILIST_URL, json=query)
                latency = int((time.perf_counter() - start) * 1000)
                ok = r.status_code == 200
                msg = "GraphQL API reachable" if ok else f"HTTP {r.status_code}"
                return {
                    "name": "AniList API",
                    "category": "Core Infrastructure",
                    "ok": ok,
                    "msg": msg,
                    "latency_ms": latency,
                }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "AniList API",
                "category": "Core Infrastructure",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    # provider-specific code (allowlisted)
    async def probe_simkl(p: Provider) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            async with async_session_factory() as s:
                db_prov = await s.get(Provider, p.id)
                if not db_prov:
                    raise ValueError("Provider not found")
                cid, tok = await asyncio.wait_for(
                    simkl.ensure_valid_simkl_token(s, db_prov),
                    timeout=5.0,
                )
                r = await asyncio.wait_for(
                    simkl.check_simkl_user_settings(cid, tok),
                    timeout=5.0,
                )
                if r.status_code == 401 and db_prov.watchlist_config.get(
                    "refresh_token"
                ):
                    cid, tok = await asyncio.wait_for(
                        simkl.refresh_provider_simkl_token(s, db_prov),
                        timeout=5.0,
                    )
                    r = await asyncio.wait_for(
                        simkl.check_simkl_user_settings(cid, tok),
                        timeout=5.0,
                    )
            latency = int((time.perf_counter() - start) * 1000)
            ok = r.status_code == 200
            user_label = p.username or "Authenticated"
            if ok:
                try:
                    data = r.json()
                    user_label = (data.get("user") or {}).get("name") or user_label
                except Exception:
                    pass
            msg = f"User: {user_label}" if ok else f"HTTP {r.status_code}"
            return {
                "name": f"{p.name or 'Simkl'} (Watchlist)",
                "category": "Watchlist Provider",
                "ok": ok,
                "msg": msg,
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": f"{p.name or 'Simkl'} (Watchlist)",
                "category": "Watchlist Provider",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    async def probe_watchlist(p: Provider) -> dict[str, Any]:
        start = time.perf_counter()
        ok, msg = False, ""
        try:
            if p.type == "anilist":
                ok, msg = await asyncio.wait_for(
                    anilist.check_user(p.username or "", p.access_token or None),
                    timeout=5.0,
                )
            elif p.type == "plex":
                ok, msg = await asyncio.wait_for(
                    plex.check_account(
                        p.access_token or "",
                        str(p.watchlist_config.get("plex_client_identifier") or ""),
                    ),
                    timeout=5.0,
                )
            elif p.type == "mal":
                async with async_session_factory() as s:
                    db_prov = await s.get(Provider, p.id)
                    if not db_prov:
                        raise ValueError("Provider not found")
                    token = await asyncio.wait_for(
                        mal.ensure_valid_mal_token(s, db_prov), timeout=5.0
                    )
                ok, msg = await asyncio.wait_for(mal.check_account(token), timeout=5.0)
        except Exception as e:
            ok, msg = False, str(e) or type(e).__name__
        return {
            "name": f"{p.name or p.type} (Watchlist)",
            "category": "Watchlist Provider",
            "ok": ok,
            "msg": msg,
            "latency_ms": int((time.perf_counter() - start) * 1000),
        }

    async def probe_downloader(p: Provider) -> dict[str, Any]:
        from app.services.downloaders.factory import get_downloader_client

        start = time.perf_counter()
        result: dict[str, Any] = {
            "name": f"{p.name or p.type} (Downloader)",
            "category": "Usenet Downloader",
            "ok": False,
            "msg": "",
            "latency_ms": 0,
        }
        if p.type == "sabnzbd" and not (p.api_url or "").strip():
            result["msg"] = "Host missing"
            return result
        if not (p.api_key or "").strip():
            result["msg"] = "API Key missing"
            return result
        try:
            ok, msg = await asyncio.wait_for(
                get_downloader_client(p).test_connection(), timeout=10.0
            )
            result.update(ok=ok, msg=msg)
        except Exception as e:
            result["msg"] = str(e) or type(e).__name__
        result["latency_ms"] = int((time.perf_counter() - start) * 1000)
        return result

    async def probe_tmdb(p: Provider) -> dict[str, Any]:
        start = time.perf_counter()
        tmdb_key = (p.api_key or "").strip()
        if not tmdb_key:
            return {
                "name": f"{p.name or 'TheMovieDB'} (Metadata)",
                "category": "Metadata Provider",
                "ok": False,
                "msg": "API Key missing",
                "latency_ms": 0,
            }
        try:
            headers = {"User-Agent": DEFAULT_USER_AGENT}
            params = {}
            if tmdb_key.startswith("ey"):
                headers["Authorization"] = f"Bearer {tmdb_key}"
            else:
                params["api_key"] = tmdb_key
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(
                    "https://api.themoviedb.org/3/configuration",
                    params=params,
                    headers=headers,
                )
            latency = int((time.perf_counter() - start) * 1000)
            ok = r.status_code == 200
            msg = "API configuration accessible" if ok else f"HTTP {r.status_code}"
            return {
                "name": f"{p.name or 'TheMovieDB'} (Metadata)",
                "category": "Metadata Provider",
                "ok": ok,
                "msg": msg,
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": f"{p.name or 'TheMovieDB'} (Metadata)",
                "category": "Metadata Provider",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    async def probe_indexer(p: Provider) -> dict[str, Any]:
        start = time.perf_counter()
        key = (p.api_key or "").strip()
        url = (p.api_url or "").strip()
        if not url:
            preset = get_preset(p.type)
            url = preset.api_url if preset else ""
        if not url:
            return {
                "name": p.name or "Usenet Indexer",
                "category": "Usenet Indexer",
                "ok": False,
                "msg": "Missing API URL",
                "latency_ms": 0,
            }
        try:
            params = {"t": "caps"}
            if key:
                params["apikey"] = key
            async with httpx.AsyncClient(
                timeout=5.0, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as client:
                r = await client.get(url, params=params)
            latency = int((time.perf_counter() - start) * 1000)
            body = r.text.lower()
            ok = r.status_code == 200 and (
                "<caps" in body or "<server" in body or "<categories" in body
            )
            msg = (
                "Caps XML verified"
                if ok
                else (
                    f"HTTP {r.status_code}"
                    if r.status_code != 200
                    else "Invalid caps XML"
                )
            )
            return {
                "name": p.name or "Usenet Indexer",
                "category": "Usenet Indexer",
                "ok": ok,
                "msg": msg,
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": p.name or "Usenet Indexer",
                "category": "Usenet Indexer",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    async def probe_discord(webhook_url: str) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            async with httpx.AsyncClient(
                timeout=5.0, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as client:
                r = await client.get(webhook_url)
            latency = int((time.perf_counter() - start) * 1000)
            ok = r.status_code == 200
            detail = "Webhook verified"
            if ok:
                try:
                    data = r.json()
                    name = data.get("name")
                    if name:
                        detail = f"Webhook: {name}"
                except Exception:
                    pass
            msg = detail if ok else f"HTTP {r.status_code}"
            return {
                "name": "Discord Webhook",
                "category": "Notifications",
                "ok": ok,
                "msg": msg,
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": "Discord Webhook",
                "category": "Notifications",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    async def probe_telegram(ch: NotificationChannel) -> dict[str, Any]:
        start = time.perf_counter()
        bot_token = (ch.bot_token or "").strip()
        try:
            async with httpx.AsyncClient(
                timeout=5.0, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as client:
                r = await client.get(f"https://api.telegram.org/bot{bot_token}/getMe")
            latency = int((time.perf_counter() - start) * 1000)
            ok = r.status_code == 200
            detail = "Bot verified"
            if ok:
                try:
                    data = r.json()
                    user = (data.get("result") or {}).get("username")
                    if user:
                        detail = f"Bot @{user}"
                except Exception:
                    pass
            msg = detail if ok else f"HTTP {r.status_code}"
            return {
                "name": f"Telegram Bot ({ch.name})",
                "category": "Notifications",
                "ok": ok,
                "msg": msg,
                "latency_ms": latency,
            }
        except Exception as e:
            latency = int((time.perf_counter() - start) * 1000)
            return {
                "name": f"Telegram Bot ({ch.name})",
                "category": "Notifications",
                "ok": False,
                "msg": str(e),
                "latency_ms": latency,
            }

    async def probe_delivery(provider: Provider) -> list[dict[str, Any]]:
        from app.services.delivery.config import DeliveryConfig

        cfg = DeliveryConfig.from_provider(provider)
        rows: list[dict[str, Any]] = []
        for field in _DELIVERY_PATH_FIELDS:
            path = getattr(cfg, field)
            if not path:  # unconfigured paths are never reported failed
                continue
            start = time.perf_counter()
            ok, detail = await asyncio.to_thread(_probe_directory, path)
            rows.append(
                {
                    "name": f"Delivery: {field.removesuffix('_path').replace('_', ' ')}",
                    "category": "Delivery",
                    "ok": ok,
                    "msg": f"{path} — {detail}",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                }
            )
        return rows

    # Build probe coroutine list
    probes: list[Coroutine[Any, Any, Any]] = [
        probe_database(),
        probe_storage(),
        probe_anilist(),
    ]

    for wl in active_watchlists:
        if wl.type == "simkl":
            if (
                wl.client_id
                or wl.access_token
                or wl.watchlist_config.get("client_id")
                or wl.watchlist_config.get("access_token")
            ):
                # provider-specific code (allowlisted)
                probes.append(probe_simkl(wl))
        elif (
            wl.type in ("anilist", "plex", "mal")
            and wl.watchlist_connection_status != "not_connected"
        ):
            probes.append(probe_watchlist(wl))

    for dl in active_downloaders:
        probes.append(probe_downloader(dl))

    if active_tmdb and active_tmdb.api_key:
        probes.append(probe_tmdb(active_tmdb))

    for idx in active_indexers:
        probes.append(probe_indexer(idx))

    # Enabled notifications
    if (
        sys_settings
        and sys_settings.discord_enabled
        and sys_settings.discord_webhook_url
    ):
        probes.append(probe_discord(sys_settings.discord_webhook_url))

    for tg in notifications:
        if tg.type == "telegram" and tg.bot_token:
            probes.append(probe_telegram(tg))

    delivery = next((p for p in active_providers if p.category == "delivery"), None)
    if delivery is not None:
        probes.append(probe_delivery(delivery))

    # Run all probes concurrently
    probe_outputs = await asyncio.gather(*probes, return_exceptions=True)

    results: list[dict[str, Any]] = []
    for out in probe_outputs:
        if isinstance(out, dict):
            results.append(out)
        elif isinstance(out, list):
            results.extend(out)
        elif isinstance(out, Exception):
            results.append(
                {
                    "name": "Diagnostic Task",
                    "category": "Unknown",
                    "ok": False,
                    "msg": f"Task error: {out}",
                    "latency_ms": 0,
                }
            )

    # Render results HTML fragment
    total_count = len(results)
    passed_count = sum(1 for r in results if r["ok"])
    failed_count = total_count - passed_count

    html_parts: list[str] = ['<div class="space-y-4">']

    # Render vital role warning banners
    for w in vital_warnings:
        html_parts.append(f"""
        <div class="flex items-start gap-3 p-3 bg-amber-950/40 border border-amber-600/40 text-amber-200 rounded-lg text-xs leading-relaxed animate-fade-in">
            <svg class="w-4 h-4 text-amber-400 shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
            </svg>
            <div>
                <span class="font-bold block text-amber-300">Vital Role Inactive</span>
                <span>{html_escape.escape(w)}</span>
            </div>
        </div>
        """)

    # Render service cards
    html_parts.append('<div class="space-y-2.5">')
    for item in results:
        ok = item["ok"]
        icon = "✅" if ok else "❌"
        status_color = "text-emerald-400" if ok else "text-rose-400"
        bg_color = (
            "bg-[#141418] border-[#2d2d38]"
            if ok
            else "bg-rose-950/10 border-rose-900/30"
        )
        latency = item.get("latency_ms", 0)

        # Latency badge styling
        if latency < 200:
            lat_color = "bg-emerald-950/50 text-emerald-300 border-emerald-800/40"
        elif latency <= 800:
            lat_color = "bg-amber-950/50 text-amber-300 border-amber-800/40"
        else:
            lat_color = "bg-rose-950/50 text-rose-300 border-rose-800/40"

        html_parts.append(f"""
        <div class="flex items-center justify-between p-3 {bg_color} border rounded-lg transition-colors">
            <div class="flex items-center gap-3 min-w-0 pr-2">
                <span class="text-base shrink-0">{icon}</span>
                <div class="min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="font-semibold text-white text-xs truncate">{html_escape.escape(item["name"])}</span>
                        <span class="text-[10px] text-[#71717a] border border-[#3f3f46]/50 rounded px-1.5 py-0.2 shrink-0">{html_escape.escape(item["category"])}</span>
                    </div>
                    <div class="text-[11px] {status_color} mt-0.5 truncate">{html_escape.escape(item["msg"])}</div>
                </div>
            </div>
            <div class="shrink-0">
                <span class="text-[10px] font-mono px-2 py-0.5 rounded border {lat_color}">{latency}ms</span>
            </div>
        </div>
        """)
    html_parts.append("</div>")

    # Summary bar
    summary_bg = "bg-[#18181f] border-[#2d2d38]"
    html_parts.append(f"""
    <div class="flex items-center justify-between px-3 py-2 {summary_bg} border rounded-lg text-xs text-[#a1a1aa] mt-2">
        <div class="flex items-center gap-2">
            <span>Probes: <strong class="text-white font-mono">{total_count}</strong></span>
            <span>•</span>
            <span class="text-emerald-400">Passed: <strong class="font-mono">{passed_count}</strong></span>
            {f'<span>•</span><span class="text-rose-400">Failed: <strong class="font-mono">{failed_count}</strong></span>' if failed_count > 0 else ""}
        </div>
        <div class="text-[10px] text-[#71717a] font-mono">
            Concurrent engine
        </div>
    </div>
    """)

    html_parts.append("</div>")
    return HTMLResponse(content="\n".join(html_parts))


@app.post("/sync", response_class=HTMLResponse)
async def manual_sync() -> HTMLResponse:
    """Trigger manual sync for all configured providers — synchronous, blocks until complete."""
    from app.core.automation import sync_all_providers

    errors = await sync_all_providers()
    msg = "Watchlist sync complete."
    if isinstance(errors, int) and errors > 0:
        msg = f"Watchlist sync complete ({errors} provider error(s))."

    return HTMLResponse(
        content=f"""
        <div class="bg-[#3f3f46] text-white px-4 py-3 rounded-md shadow-lg border border-[#52525b] flex items-center justify-between animate-fade-in-down mb-4">
            <div class="flex items-center gap-3">
                <svg class="w-5 h-5 shrink-0 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                <span class="text-sm font-medium">{msg}</span>
            </div>
            <button onclick="this.parentElement.remove()" class="p-1 text-gray-400 hover:text-white hover:bg-black/20 rounded transition-colors focus:outline-none">
                <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
        </div>
    """,
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.post("/settings/provider/{provider_id}/sync", response_class=HTMLResponse)
async def sync_provider(provider_id: int) -> HTMLResponse:
    """Sync a single watchlist provider and return a toast fragment."""
    import html

    from app.core.automation import sync_single_provider

    result = await sync_single_provider(provider_id)
    status = result.get("status")
    if status == "busy":
        msg, icon = "A sync is already running.", "text-amber-400"
    elif status == "error":
        err = html.escape(str(result.get("error") or "unknown error"))
        msg, icon = f"Sync failed: {err}", "text-red-400"
    else:
        msg = (
            f"Sync complete ({result.get('ingested', 0)} ingested, "
            f"{result.get('added', 0)} added, {result.get('pruned', 0)} pruned)."
        )
        icon = "text-emerald-400"
    return HTMLResponse(
        content=f"""
        <div class="bg-[#3f3f46] text-white px-4 py-3 rounded-md shadow-lg border border-[#52525b] flex items-center justify-between animate-fade-in-down mb-4">
            <div class="flex items-center gap-3">
                <svg class="w-5 h-5 shrink-0 {icon}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                <span class="text-sm font-medium">{msg}</span>
            </div>
            <button onclick="this.parentElement.remove()" class="p-1 text-gray-400 hover:text-white hover:bg-black/20 rounded transition-colors focus:outline-none">
                <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
        </div>
    """,
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.get("/api/blacklist")
async def get_blacklist():
    """Retrieve all blacklisted releases."""
    from sqlalchemy import select

    from app.db.database import async_session_factory
    from app.db.models import BlacklistedRelease

    async with async_session_factory() as session:
        items = (
            (
                await session.execute(
                    select(BlacklistedRelease).order_by(
                        BlacklistedRelease.created_at.desc()
                    )
                )
            )
            .scalars()
            .all()
        )
        return [
            {
                "id": i.id,
                "media_item_id": i.media_item_id,
                "scope_label": (
                    f"Item #{i.media_item_id}"
                    if i.media_item_id is not None
                    else "Global"
                ),
                "nzb_guid": i.nzb_guid,
                "nzb_title": i.nzb_title,
                "reason": i.reason,
                "created_at": i.created_at.isoformat(),
            }
            for i in items
        ]


@app.post("/api/blacklist")
async def api_create_blacklisted_release(request: Request) -> Response:
    """Manually blacklist a release (item-scoped or global)."""
    from app.core.transfer_poller import ensure_blacklisted_release
    from app.db.database import async_session_factory

    payload = await _parse_request_payload(request)
    raw_item_id = payload.get("media_item_id")
    media_item_id: int | None = None
    if raw_item_id not in (None, "", "null", "None"):
        try:
            media_item_id = int(raw_item_id)
        except (TypeError, ValueError):
            media_item_id = None

    nzb_guid = str(payload.get("nzb_guid") or payload.get("guid") or "").strip() or None
    nzb_title = str(payload.get("nzb_title") or payload.get("title") or "").strip()
    reason = (
        str(payload.get("reason") or "Manually blacklisted").strip()
        or "Manually blacklisted"
    )

    if not nzb_title and not nzb_guid:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "nzb_title or nzb_guid is required"},
        )

    async with async_session_factory() as session:
        entry, created = await ensure_blacklisted_release(
            session,
            media_item_id=media_item_id,
            nzb_guid=nzb_guid,
            nzb_title=nzb_title or (nzb_guid or "Unknown Release"),
            reason=reason,
        )
        await session.commit()
        entry_data = {
            "ok": True,
            "created": created,
            "id": entry.id,
            "media_item_id": entry.media_item_id,
            "nzb_guid": entry.nzb_guid,
            "nzb_title": entry.nzb_title,
            "reason": entry.reason,
        }

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content='<span class="px-2.5 py-1 text-xs font-semibold rounded bg-zinc-800 text-zinc-400 border border-zinc-700">Blacklisted</span>',
            status_code=200,
        )

    return JSONResponse(status_code=201 if created else 200, content=entry_data)


@app.get("/settings/blacklist/clear-modal")
async def settings_blacklist_clear_modal(request: Request) -> Response:
    """Render confirmation modal for clearing all blacklisted releases."""
    from sqlalchemy import func, select

    from app.db.database import async_session_factory
    from app.db.models import BlacklistedRelease

    async with async_session_factory() as session:
        count = (
            await session.execute(select(func.count()).select_from(BlacklistedRelease))
        ).scalar_one()

    return templates.TemplateResponse(
        request=request,
        name="modals/clear_blacklist.html",
        context={"total_count": count},
    )


@app.post("/api/blacklist/clear")
async def clear_all_blacklisted_releases(request: Request) -> Response:
    """Delete all blacklisted releases."""
    from typing import cast

    from sqlalchemy import CursorResult, delete

    from app.db.database import async_session_factory
    from app.db.models import BlacklistedRelease

    async with async_session_factory() as session:
        result = cast(
            CursorResult[Any], await session.execute(delete(BlacklistedRelease))
        )
        deleted_count = result.rowcount or 0
        await session.commit()

    is_htmx = request.headers.get("HX-Request") == "true"
    accept_header = request.headers.get("Accept", "")

    if is_htmx or "application/json" not in accept_header:
        if is_htmx:
            resp = templates.TemplateResponse(
                request=request,
                name="partials/blacklist_slice.html",
                context={"blacklisted_releases": []},
            )
            resp.headers["HX-Trigger"] = json.dumps({"closeModal": True})
            return resp

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "deleted_count": deleted_count,
            "message": "All blacklisted releases cleared successfully.",
        },
    )


@app.delete("/api/blacklist/{blacklist_id}")
async def delete_blacklisted_release(blacklist_id: int):
    """Delete a blacklisted release so it can be grabbed again."""
    from app.db.database import async_session_factory
    from app.db.models import BlacklistedRelease

    async with async_session_factory() as session:
        item = await session.get(BlacklistedRelease, blacklist_id)
        if item:
            await session.delete(item)
            await session.commit()

    # Return empty string for HTMX to clear the row
    return Response(content="", status_code=200)


# ---------------------------------------------------------------------------
# Search Presets & Sticky Item Config API (v3.0.0)
# ---------------------------------------------------------------------------


async def _parse_request_payload(request: Request) -> dict[str, Any]:
    """Parse either JSON body or Form data into a dictionary."""
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            data = await request.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    try:
        form = await request.form()
        payload: dict[str, Any] = {}
        for k in form.keys():
            vals = form.getlist(k)
            if k.endswith("[]"):
                payload[k[:-2]] = vals
            elif len(vals) > 1:
                payload[k] = vals
            else:
                payload[k] = form.get(k)
        return payload
    except Exception:
        return {}


@app.get("/settings/preset/new/modal")
async def settings_preset_new_modal(request: Request) -> Response:
    """Render empty Search Preset creation dialog modal."""
    return templates.TemplateResponse(
        request=request,
        name="modals/preset_modal.html",
        context={
            "preset": None,
            "custom_config": {},
        },
    )


@app.get("/settings/preset/{preset_id}/edit/modal")
async def settings_preset_edit_modal(preset_id: int, request: Request) -> Response:
    """Render pre-filled Search Preset edit dialog modal."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import get_preset

    async with async_session_factory() as session:
        preset = await get_preset(session, preset_id)
        if preset is None:
            return JSONResponse(status_code=404, content={"error": "Preset not found"})
        return templates.TemplateResponse(
            request=request,
            name="modals/preset_modal.html",
            context={
                "preset": preset,
                "custom_config": preset.custom_config,
            },
        )


async def _render_presets_list_or_json(
    session: Any, request: Request, json_content: Any, status_code: int = 200
) -> Response:
    """Helper to return presets_list.html partial for HTMX requests or JSON response."""
    from fastapi.responses import JSONResponse

    from app.services.preset_service import list_presets

    is_htmx = request.headers.get("HX-Request", "").strip().lower() == "true"
    if is_htmx:
        presets = await list_presets(session)
        response = templates.TemplateResponse(
            request=request,
            name="partials/presets_list.html",
            context={"presets": presets},
            status_code=status_code,
        )
        # Close any open modal container via HX-Trigger
        response.headers["HX-Trigger"] = '{"closePresetModal": true}'
        return response
    return JSONResponse(status_code=status_code, content=json_content)


@app.get("/api/presets")
async def api_list_presets() -> list[dict[str, Any]]:
    """Return all saved SearchPreset rows ordered with the default preset first."""
    from app.db.database import async_session_factory
    from app.services.preset_service import list_presets

    async with async_session_factory() as session:
        presets = await list_presets(session)
        return [p.to_dict() for p in presets]


@app.get("/api/presets/{preset_id}")
async def api_get_preset(preset_id: int) -> Response:
    """Return a single SearchPreset by ID."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import get_preset

    async with async_session_factory() as session:
        preset = await get_preset(session, preset_id)
        if preset is None:
            return JSONResponse(status_code=404, content={"error": "Preset not found"})
        return JSONResponse(status_code=200, content=preset.to_dict())


@app.post("/api/presets")
async def api_create_preset(request: Request) -> Response:
    """Create a new SearchPreset (or update if same name) and enforce single default."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import create_preset

    payload = await _parse_request_payload(request)
    async with async_session_factory() as session:
        try:
            preset = await create_preset(session, payload)
            await session.commit()
            await session.refresh(preset)
            return await _render_presets_list_or_json(
                session, request, preset.to_dict(), status_code=201
            )
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})


@app.post("/api/presets/save-inline")
async def api_save_inline_preset(request: Request) -> Response:
    """Save the current Push Modal configuration as a named SearchPreset without leaving the modal."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import save_inline_preset

    payload = await _parse_request_payload(request)
    async with async_session_factory() as session:
        try:
            preset = await save_inline_preset(session, payload)
            await session.commit()
            await session.refresh(preset)
            return JSONResponse(status_code=201, content=preset.to_dict())
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})


@app.put("/api/presets/{preset_id}")
@app.post("/api/presets/{preset_id}")
async def api_update_preset(preset_id: int, request: Request) -> Response:
    """Update an existing SearchPreset by ID."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import update_preset

    payload = await _parse_request_payload(request)
    async with async_session_factory() as session:
        preset = await update_preset(session, preset_id, payload)
        if preset is None:
            return JSONResponse(status_code=404, content={"error": "Preset not found"})
        await session.commit()
        await session.refresh(preset)
        return await _render_presets_list_or_json(
            session, request, preset.to_dict(), status_code=200
        )


@app.post("/api/presets/{preset_id}/default")
@app.post("/api/presets/{preset_id}/set-default")
async def api_set_default_preset(preset_id: int, request: Request) -> Response:
    """Mark a SearchPreset as the single default preset."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import set_default_preset

    async with async_session_factory() as session:
        preset = await set_default_preset(session, preset_id)
        if preset is None:
            return JSONResponse(status_code=404, content={"error": "Preset not found"})
        await session.commit()
        await session.refresh(preset)
        return await _render_presets_list_or_json(
            session, request, preset.to_dict(), status_code=200
        )


@app.delete("/api/presets/{preset_id}")
async def api_delete_preset(preset_id: int, request: Request) -> Response:
    """Delete a SearchPreset and promote another preset to default if needed."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import delete_preset

    async with async_session_factory() as session:
        deleted = await delete_preset(session, preset_id)
        if not deleted:
            return JSONResponse(status_code=404, content={"error": "Preset not found"})
        await session.commit()
        return await _render_presets_list_or_json(
            session, request, {"deleted": True, "id": preset_id}, status_code=200
        )


@app.post("/api/items/{item_id}/search-config")
@app.put("/api/items/{item_id}/search-config")
async def api_update_item_search_config(item_id: int, request: Request) -> Response:
    """Persist sticky search configuration on a MediaItem."""
    from fastapi.responses import JSONResponse

    from app.db.database import async_session_factory
    from app.services.preset_service import update_item_sticky_search_config

    payload = await _parse_request_payload(request)
    async with async_session_factory() as session:
        item = await update_item_sticky_search_config(session, item_id, payload)
        if item is None:
            return JSONResponse(
                status_code=404, content={"error": "MediaItem not found"}
            )
        await session.commit()
        await session.refresh(item)
        return JSONResponse(
            status_code=200,
            content={
                "id": item.id,
                "preset_id": item.preset_id,
                "custom_search_config_json": item.custom_search_config_json,
                "prefer_season_packs": item.prefer_season_packs,
                "auto_advance_seasons": item.auto_advance_seasons,
            },
        )


# ---------------------------------------------------------------------------
# Unified Push Modal & On-Demand Push Engine API (v3.0.0)
# ---------------------------------------------------------------------------


@app.post("/api/items/{item_id}/toggle-auto-push")
async def api_toggle_auto_push(item_id: int) -> JSONResponse:
    """Flip `MediaItem.auto_push_episodes` for a Series/Anime."""
    from app.db.database import async_session_factory
    from app.db.models import MediaItem, MediaType

    async with async_session_factory() as session:
        item = await session.get(MediaItem, item_id)
        if item is None:
            return JSONResponse(
                status_code=404, content={"error": "MediaItem not found"}
            )
        if item.media_type == MediaType.MOVIE:
            return JSONResponse(
                status_code=400,
                content={"error": "Auto-push is not available for movies"},
            )
        item.auto_push_episodes = not item.auto_push_episodes
        await session.commit()
        enabled = bool(item.auto_push_episodes)

    return JSONResponse(
        {"auto_push_episodes": enabled},
        headers={"HX-Trigger": '{"showAutoSaveToast": "Saved"}'},
    )


@app.get("/api/items/{item_id}/push-modal")
async def api_get_push_modal(item_id: int, request: Request) -> Response:
    """Return the Unified Push Modal context (JSON or HTML partial)."""
    from fastapi.responses import JSONResponse

    from app.core.push_engine import get_push_modal_context
    from app.db.database import async_session_factory

    async with async_session_factory() as session:
        ctx = await get_push_modal_context(session, item_id)
        if ctx is None:
            return JSONResponse(
                status_code=404, content={"error": "MediaItem not found"}
            )

        accept = (request.headers.get("accept") or "").lower()
        modal_tpl = Path("templates/modals/push_modal.html")
        if "application/json" not in accept and modal_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="modals/push_modal.html",
                context=ctx,
            )

        item = ctx["item"]
        entries = ctx["entries"]
        presets = ctx["presets"]
        return JSONResponse(
            status_code=200,
            content={
                "item": {
                    "id": item.id,
                    "title": item.title,
                    "display_title": ctx["display_title"],
                    "year": item.year,
                    "media_type": str(item.media_type),
                    "preset_id": item.preset_id,
                    "prefer_season_packs": item.prefer_season_packs,
                    "auto_advance_seasons": item.auto_advance_seasons,
                },
                "entries": [
                    {
                        "id": s.id,
                        "season_number": s.season_number,
                        "watch_order": s.watch_order,
                        "type_number": s.type_number,
                        "entry_type": s.entry_type,
                        "title": s.title,
                        "display_title": s.display_title,
                        "status": str(s.status),
                        "episodes": [
                            {
                                "id": ep.id,
                                "episode_number": ep.episode_number,
                                "status": str(ep.status),
                            }
                            for ep in sorted(s.episodes, key=lambda e: e.episode_number)
                        ],
                    }
                    for s in entries
                ],
                "presets": [p.to_dict() for p in presets],
                "effective_config": ctx["effective_config"],
            },
        )


_background_push_tasks: set[asyncio.Task[Any]] = set()


async def wait_for_background_pushes() -> None:
    """Await any in-flight background Auto-Push tasks (used by tests and graceful shutdown)."""
    while _background_push_tasks:
        pending = list(_background_push_tasks)
        await asyncio.gather(*pending, return_exceptions=True)


async def cancel_in_flight_push_tasks(item_id: int | None = None) -> int:
    """Cancel running in-flight background Auto-Push tasks, release their locks, and unregister them.

    If item_id is specified, only tasks and locks matching that item_id are cancelled.
    Otherwise, all in-flight tasks and locks are cancelled.
    Returns the count of cancelled tasks.
    """
    from app.core.search_lock import item_lock_manager
    from app.core.transfer_poller import transfer_poller

    if item_id is not None:
        target_tasks = [
            t
            for t in list(_background_push_tasks)
            if getattr(t, "item_id", None) == item_id and not t.done()
        ]
        target_item_ids = [item_id]
    else:
        target_tasks = [t for t in list(_background_push_tasks) if not t.done()]
        target_item_ids = list(transfer_poller.in_flight_pushes.keys())

    canceled_count = len(target_tasks)
    for task in target_tasks:
        task.cancel()

    if target_tasks:
        await asyncio.gather(*target_tasks, return_exceptions=True)

    for iid in target_item_ids:
        transfer_poller.unregister_in_flight_push(iid)
        item_lock_manager.release(iid)

    return canceled_count


async def _run_background_auto_push(item_id: int, payload: dict[str, Any]) -> None:
    """Execute Auto-Push Best asynchronously in the background while retaining lock & in-flight state."""
    from app.core.push_engine import execute_auto_push
    from app.core.search_lock import item_lock_manager
    from app.core.transfer_poller import count_downloading_entities, transfer_poller
    from app.db.database import async_session_factory

    try:
        async with async_session_factory() as session:
            await execute_auto_push(session, item_id, payload)
    except Exception as exc:
        logger.exception("Background Auto-Push failed for item %s: %s", item_id, exc)
    finally:
        transfer_poller.unregister_in_flight_push(item_id)
        item_lock_manager.release(item_id)
        try:
            async with async_session_factory() as session:
                remaining = await count_downloading_entities(session)
                if remaining == 0 and not transfer_poller.has_in_flight_pushes:
                    transfer_poller.sleep(0)
        except (Exception, asyncio.CancelledError) as exc:
            logger.debug("Post-push poller sleep check skipped: %s", exc)


@app.post("/api/items/{item_id}/push/auto")
async def api_push_item_auto(item_id: int, request: Request) -> Response:
    """Execute Auto-Push Best (`push_mode = 'auto'`) in the background protected by MediaItemLockManager."""
    from fastapi.responses import JSONResponse

    from app.core.push_engine import persist_sticky_preferences
    from app.core.search_lock import create_conflict_response, item_lock_manager
    from app.core.transfer_poller import transfer_poller, wake_transfer_poller
    from app.db.database import async_session_factory
    from app.db.models import MediaItem

    if not item_lock_manager.try_acquire(item_id, owner="manual"):
        owner = item_lock_manager.get_lock_owner(item_id)
        msg = (
            "Background automation is currently searching this item. Please wait."
            if owner == "background"
            else "A search or push is already in progress for this item. Please wait."
        )
        return create_conflict_response(msg)

    try:
        payload = await _parse_request_payload(request)
        async with async_session_factory() as session:
            item = await session.get(MediaItem, item_id)
            if item is None:
                item_lock_manager.release(item_id)
                return JSONResponse(
                    status_code=404,
                    content={
                        "pushed": False,
                        "error": f"MediaItem {item_id} not found",
                    },
                )
            await persist_sticky_preferences(session, item, payload)
            await session.commit()
            item_title = item.title
    except Exception:
        item_lock_manager.release(item_id)
        raise

    transfer_poller.register_in_flight_push(item_id, item_title)
    wake_transfer_poller()
    task = asyncio.create_task(_run_background_auto_push(item_id, payload))
    setattr(task, "item_id", item_id)
    _background_push_tasks.add(task)
    task.add_done_callback(_background_push_tasks.discard)

    return JSONResponse(
        status_code=200,
        content={
            "status": "started",
            "pushed": True,
            "push_mode": "auto",
            "item_id": item_id,
            "title": item_title,
            "message": f"Auto-Push started for '{item_title}'.",
        },
    )


@app.post("/api/items/{item_id}/push/manual-search")
async def api_push_item_manual_search(item_id: int, request: Request) -> Response:
    """Search indexers and return scored releases partitioned into Season Packs/Movies and Episode Accordions."""
    from fastapi.responses import JSONResponse

    from app.core.push_engine import execute_manual_search, persist_sticky_preferences
    from app.core.search_lock import create_conflict_response, item_lock_manager
    from app.db.database import async_session_factory
    from app.db.models import MediaItem

    if not item_lock_manager.try_acquire(item_id, owner="manual"):
        return create_conflict_response(
            "A search or push is already in progress for this item. Please wait."
        )

    try:
        payload = await _parse_request_payload(request)
        async with async_session_factory() as session:
            item = await session.get(MediaItem, item_id)
            if item is not None:
                await persist_sticky_preferences(session, item, payload)
                await session.commit()
            result = await execute_manual_search(session, item_id, payload)
        status_code = int(result.pop("status_code", 200))
        return JSONResponse(status_code=status_code, content=result)
    finally:
        item_lock_manager.release(item_id)


@app.post("/api/items/{item_id}/push/manual-grab")
async def api_push_item_manual_grab(item_id: int, request: Request) -> Response:
    """Dispatch a manually chosen release to the active downloader with `DownloadHistory.push_mode = 'manual'`."""
    from fastapi.responses import JSONResponse

    from app.core.push_engine import execute_manual_grab
    from app.core.search_lock import create_conflict_response, item_lock_manager
    from app.db.database import async_session_factory

    if not item_lock_manager.try_acquire(item_id, owner="manual"):
        return create_conflict_response(
            "A search or push is already in progress for this item. Please wait."
        )

    try:
        payload = await _parse_request_payload(request)
        async with async_session_factory() as session:
            result = await execute_manual_grab(session, item_id, payload)
        status_code = int(result.pop("status_code", 200))
        return JSONResponse(status_code=status_code, content=result)
    finally:
        item_lock_manager.release(item_id)


# ---------------------------------------------------------------------------
# Card 5: Active Pushes & Split Failure Recovery API (v3.0.0)
# ---------------------------------------------------------------------------


def _build_poller_state_header(
    *,
    poller_awake: bool,
    active_pushes_count: int,
    downloading_count: int,
    in_flight_count: int,
    refresh_dashboard: bool = False,
    refresh_active_pushes: bool = False,
) -> dict[str, str]:
    payload: dict[str, Any] = {
        "smewlet:poller-state": {
            "pollerAwake": poller_awake,
            "activePushesCount": active_pushes_count,
            "downloadingCount": downloading_count,
            "inFlightCount": in_flight_count,
        }
    }
    if refresh_dashboard:
        payload["refreshDashboard"] = True
    if refresh_active_pushes:
        payload["refreshActivePushes"] = True
    return {"HX-Trigger": json.dumps(payload)}


@app.get("/api/pushes/active")
async def api_get_active_pushes(request: Request) -> Response:
    """Return active transfers (`DOWNLOADING`), recently completed transfers (`Ready on <downloader>`), and unacknowledged failed pushes (`FAILED`)."""
    from app.core.transfer_poller import (
        count_downloading_entities,
        get_active_pushes,
        run_transfer_poller_tick,
        transfer_poller,
    )
    from app.services.downloaders.factory import downloader_class

    force = (request.query_params.get("force") or "").lower() in ("1", "true", "yes")
    if force:
        downloader_class("torbox").clear_transfer_cache()

    async with async_session_factory() as session:
        downloading_count = await count_downloading_entities(session)
        if downloading_count > 0:
            try:
                live_downloads = await downloader_class("torbox").fetch_raw_transfers(
                    bypass_cache=False, session=session
                )
                await run_transfer_poller_tick(
                    session, pre_fetched_downloads=live_downloads
                )
                downloading_count = await count_downloading_entities(session)
            except Exception as exc:
                logger.debug("Live active pushes refresh skipped: %s", exc)
        pushes = await get_active_pushes(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
    )

    rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))

    accept = (request.headers.get("accept") or "").lower()
    is_hx = request.headers.get("HX-Request") == "true"
    partial_tpl = Path("templates/partials/active_pushes_table.html")
    if is_hx and "application/json" not in accept and partial_tpl.exists():
        return templates.TemplateResponse(
            request=request,
            name="partials/active_pushes_table.html",
            context={
                "active_pushes": pushes,
                "rate_limited_count": rate_limited_count,
                "downloading_count": downloading_count,
                "in_flight_titles": in_flight_titles,
                "poller_awake": poller_awake,
            },
            headers=headers,
        )

    return JSONResponse(
        status_code=200,
        content={
            "pushes": pushes,
            "active_count": len(pushes),
            "rate_limited_count": rate_limited_count,
            "downloading_count": downloading_count,
            "in_flight_titles": in_flight_titles,
            "poller_awake": poller_awake,
        },
        headers=headers,
    )


@app.post("/api/pushes/{push_id}/dismiss")
async def api_dismiss_failed_push(push_id: str, request: Request) -> Response:
    """Dismiss a failed, completed, or synthetic dry-run push row from Active Pushes."""
    from app.core.transfer_poller import (
        count_downloading_entities,
        dismiss_dry_run_push,
        dismiss_failed_push,
        get_active_pushes,
        transfer_poller,
    )

    if push_id.startswith("dry-run-") or not push_id.isdigit():
        ok = dismiss_dry_run_push(push_id)
        if not ok:
            return JSONResponse(
                status_code=404, content={"ok": False, "error": "Push row not found"}
            )
        async with async_session_factory() as session:
            pushes = await get_active_pushes(session)
            downloading_count = await count_downloading_entities(session)
    else:
        history_id = int(push_id)
        async with async_session_factory() as session:
            ok = await dismiss_failed_push(session, history_id)
            if not ok:
                return JSONResponse(
                    status_code=404,
                    content={"ok": False, "error": "Push row not found"},
                )
            pushes = await get_active_pushes(session)
            downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
        refresh_active_pushes=True,
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={"ok": True, "push_id": push_id, "remaining_pushes": len(pushes)},
        headers=headers,
    )


@app.post("/api/pushes/dismiss-all")
async def api_dismiss_all_active_pushes(request: Request) -> Response:
    """Dismiss all non-active rows (completed, failed, dry-run) from Card 5 Active Pushes."""
    from app.core.transfer_poller import (
        count_downloading_entities,
        dismiss_all_active_pushes,
        get_active_pushes,
        transfer_poller,
    )

    async with async_session_factory() as session:
        dismissed_count = await dismiss_all_active_pushes(session)
        pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
        refresh_active_pushes=True,
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "dismissed_count": dismissed_count,
            "remaining_pushes": len(pushes),
        },
        headers=headers,
    )


@app.post("/api/pushes/{history_id}/delete-transfer")
# legacy alias (m59)
@app.post(
    "/api/pushes/{history_id}/delete-torbox",
    include_in_schema=False,
)  # legacy alias (m59) — remove after one release
async def api_active_push_delete_transfer(
    history_id: int, request: Request
) -> Response:
    """Delete a completed transfer from the downloader via Active Pushes, mark row 'deleted', and reset target to SEARCHING."""
    from app.core.transfer_poller import (
        count_downloading_entities,
        get_active_pushes,
        manual_delete_transfer,
        transfer_poller,
    )

    async with async_session_factory() as session:
        res = await manual_delete_transfer(session, history_id=history_id)
        if res.get("status") == "not_found":
            return JSONResponse(
                status_code=404,
                content={
                    "ok": False,
                    "status": "not_found",
                    "error": "Push row not found",
                },
            )
        pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
        refresh_dashboard=True,
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "status": "ok",
            "history_id": history_id,
            "deleted_transfer_ids": res.get("deleted_transfer_ids", []),
            "remaining_pushes": len(pushes),
        },
        headers=headers,
    )


@app.post("/api/pushes/{history_id}/cancel")
@app.delete("/api/pushes/{history_id}")
async def api_cancel_active_push(history_id: int, request: Request) -> Response:
    """Cancel and delete an active transfer on the downloader and remove it from Active Pushes."""
    from app.core.transfer_poller import (
        cancel_active_push,
        count_downloading_entities,
        get_active_pushes,
        transfer_poller,
    )

    async with async_session_factory() as session:
        ok = await cancel_active_push(session, history_id)
        if not ok:
            return JSONResponse(
                status_code=404, content={"ok": False, "error": "Push row not found"}
            )
        pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={"ok": True, "history_id": history_id, "remaining_pushes": len(pushes)},
        headers=headers,
    )


@app.post("/api/pushes/{history_id}/blacklist")
async def api_blacklist_active_push(history_id: int, request: Request) -> Response:
    """Blacklist release from Active Pushes, cancel/delete on the downloader or dismiss failed row, and reset target to SEARCHING."""
    from app.core.transfer_poller import (
        blacklist_from_active_pushes,
        count_downloading_entities,
        get_active_pushes,
        transfer_poller,
    )

    async with async_session_factory() as session:
        ok = await blacklist_from_active_pushes(session, history_id)
        if not ok:
            return JSONResponse(
                status_code=404, content={"ok": False, "error": "Push row not found"}
            )
        pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
        refresh_dashboard=True,
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={"ok": True, "history_id": history_id, "remaining_pushes": len(pushes)},
        headers=headers,
    )


@app.post("/api/pushes/abort-all")
async def api_abort_all_transfers(request: Request) -> Response:
    """Abort all active downloading and rate-limited transfers across the downloader and database."""
    from app.core.transfer_poller import (
        abort_all_active_transfers,
        count_downloading_entities,
        get_active_pushes,
        transfer_poller,
    )

    async with async_session_factory() as session:
        res = await abort_all_active_transfers(session)
        pushes = await get_active_pushes(session)
        downloading_count = await count_downloading_entities(session)

    in_flight_titles = transfer_poller.in_flight_titles
    poller_awake = (
        transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
    )
    headers = _build_poller_state_header(
        poller_awake=poller_awake,
        active_pushes_count=len(pushes),
        downloading_count=downloading_count,
        in_flight_count=len(in_flight_titles),
        refresh_dashboard=True,
    )

    if request.headers.get("HX-Request") == "true":
        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content=res,
        headers=headers,
    )


@app.post("/api/pushes/resume-rate-limited")
async def api_resume_rate_limited_pushes(request: Request) -> Response:
    """Clear all downloader and indexer cooldowns, wake the queue, and trigger an immediate resume tick."""
    from app.core.rate_limit_queue import (
        clear_all_rate_limit_cooldowns,
        rate_limit_queue,
        run_rate_limit_resume_tick,
    )
    from app.core.transfer_poller import (
        count_downloading_entities,
        get_active_pushes,
        transfer_poller,
    )

    clear_all_rate_limit_cooldowns()
    rate_limit_queue.wake()
    await run_rate_limit_resume_tick()

    if request.headers.get("HX-Request") == "true":
        async with async_session_factory() as session:
            pushes = await get_active_pushes(session)
            downloading_count = await count_downloading_entities(session)

        rate_limited_count = sum(1 for p in pushes if p.get("is_rate_limited"))
        in_flight_titles = transfer_poller.in_flight_titles
        poller_awake = (
            transfer_poller.is_awake or downloading_count > 0 or bool(in_flight_titles)
        )
        headers = _build_poller_state_header(
            poller_awake=poller_awake,
            active_pushes_count=len(pushes),
            downloading_count=downloading_count,
            in_flight_count=len(in_flight_titles),
            refresh_dashboard=True,
        )

        partial_tpl = Path("templates/partials/active_pushes_table.html")
        if partial_tpl.exists():
            return templates.TemplateResponse(
                request=request,
                name="partials/active_pushes_table.html",
                context={
                    "active_pushes": pushes,
                    "rate_limited_count": rate_limited_count,
                    "downloading_count": downloading_count,
                    "in_flight_titles": in_flight_titles,
                    "poller_awake": poller_awake,
                },
                headers=headers,
            )
        return HTMLResponse(content="", status_code=200, headers=headers)

    return JSONResponse(
        status_code=200,
        content={"status": "resumed", "cooldowns_cleared": True},
    )


@app.post("/api/pushes/poll")
async def api_trigger_transfer_poller_tick() -> Response:
    """Trigger an immediate transfer poller tick."""
    from app.core.transfer_poller import run_transfer_poller_tick

    async with async_session_factory() as session:
        res = await run_transfer_poller_tick(session)
    return JSONResponse(status_code=200, content=res)


# ---------------------------------------------------------------------------
# Card 4: Push History Ledger & Dual-Mode Transfer Deletion API (v3.0.0)
# ---------------------------------------------------------------------------


@app.get("/api/history")
async def api_get_push_history(request: Request) -> Response:
    """Return the Push History Ledger (DownloadHistory rows) ordered by most recent push."""
    from app.core.transfer_poller import (
        autoclear_history_ledger,
        get_push_history_ledger,
    )

    async with async_session_factory() as session:
        await autoclear_history_ledger(session)
        ledger = await get_push_history_ledger(session)

    return JSONResponse(
        status_code=200,
        content={"history": ledger, "total": len(ledger)},
    )


@app.post("/api/history/{history_id}/delete-transfer")
# legacy alias (m59)
@app.post(
    "/api/history/{history_id}/delete-torbox",
    include_in_schema=False,
)  # legacy alias (m59) — remove after one release
async def api_history_delete_transfer(history_id: int, request: Request) -> Response:
    """Delete transfer from the downloader via Push History Ledger row, mark row 'deleted', reset target to SEARCHING, and recalculate parent status."""
    from app.core.transfer_poller import manual_delete_transfer

    async with async_session_factory() as session:
        res = await manual_delete_transfer(session, history_id=history_id)
        if res.get("status") == "not_found":
            return JSONResponse(
                status_code=404,
                content={"status": "not_found", "error": "History entry not found"},
            )

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content='<span class="px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider rounded bg-zinc-500/15 text-zinc-400 border border-zinc-500/30">DELETED</span>',
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(status_code=200, content=res)


@app.post("/api/history/{history_id}/blacklist")
async def api_history_blacklist(history_id: int, request: Request) -> Response:
    """Blacklist release from Push History Ledger row, delete from the downloader if present, mark row 'deleted', and reset target to SEARCHING."""
    from app.core.transfer_poller import blacklist_from_history

    async with async_session_factory() as session:
        res = await blacklist_from_history(session, history_id=history_id)
        if res.get("status") == "not_found":
            return JSONResponse(
                status_code=404,
                content={"status": "not_found", "error": "History entry not found"},
            )

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content='<span class="px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider rounded bg-zinc-500/15 text-zinc-400 border border-zinc-500/30">DELETED</span>',
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(status_code=200, content=res)


@app.delete("/api/history/{history_id}")
async def api_delete_history_entry(history_id: int, request: Request) -> Response:
    """Delete a DownloadHistory ledger row from SQLite and reconcile parent status if zero transfers remain."""
    from app.core.transfer_poller import _recalculate_parent_status
    from app.db.models import DownloadHistory, MediaItem

    async with async_session_factory() as session:
        row = await session.get(DownloadHistory, history_id)
        if row is None:
            return JSONResponse(
                status_code=404,
                content={"deleted": False, "error": "History entry not found"},
            )
        media_item_id = row.media_item_id
        await session.delete(row)
        await session.flush()

        if media_item_id is not None:
            item = await session.get(MediaItem, media_item_id)
            if item is not None:
                await _recalculate_parent_status(session, item)

        await session.commit()

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content="",
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(
        status_code=200,
        content={"deleted": True, "id": history_id},
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.post("/api/history/batch-delete")
async def api_batch_delete_history_entries(request: Request) -> Response:
    """Batch delete DownloadHistory ledger rows from SQLite and reconcile parent statuses."""
    from app.core.transfer_poller import batch_delete_history_entries

    raw_ids: list[Any] = []
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            body = await request.json()
            if isinstance(body, dict):
                raw_ids = body.get("history_ids", [])
            elif isinstance(body, list):
                raw_ids = body
        except Exception:
            raw_ids = []
    else:
        try:
            form = await request.form()
            raw_ids = form.getlist("history_ids")
            if not raw_ids and "history_ids" in form:
                val = form.get("history_ids")
                if val:
                    raw_ids = [val]
        except Exception:
            raw_ids = []

    history_ids: list[int] = []
    for item in raw_ids:
        try:
            history_ids.append(int(item))
        except (ValueError, TypeError):
            continue

    async with async_session_factory() as session:
        res = await batch_delete_history_entries(session, history_ids)

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content="",
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(
        status_code=200,
        content=res,
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.post("/api/history/clear")
async def api_clear_history_ledger(request: Request) -> Response:
    """Clear all DownloadHistory ledger rows from SQLite and reconcile parent statuses."""
    from app.core.transfer_poller import clear_history_ledger

    async with async_session_factory() as session:
        res = await clear_history_ledger(session)

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content="",
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(
        status_code=200,
        content=res,
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.post("/api/history/delete-dry-run")
async def api_delete_dry_run_history(request: Request) -> Response:
    """Delete all simulated dry-run DownloadHistory ledger rows from SQLite."""
    from app.core.transfer_poller import delete_dry_run_history

    async with async_session_factory() as session:
        res = await delete_dry_run_history(session)

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            content="",
            status_code=200,
            headers={"HX-Trigger": "refreshDashboard"},
        )

    return JSONResponse(
        status_code=200,
        content=res,
        headers={"HX-Trigger": "refreshDashboard"},
    )


@app.post("/api/items/{item_id}/delete-transfer")
# legacy alias (m59)
@app.post(
    "/api/items/{item_id}/delete-torbox",
    include_in_schema=False,
)  # legacy alias (m59) — remove after one release
async def api_item_delete_transfer(
    item_id: int,
    request: Request,
    season_id: int | None = None,
    episode_id: int | None = None,
) -> Response:
    """Delete transfer from the downloader for a Movie, Season/Franchise Movie, or Episode from the Push Modal."""
    from app.core.push_engine import get_push_modal_context
    from app.core.transfer_poller import manual_delete_transfer

    payload = await _parse_request_payload(request)
    if season_id is None and payload.get("season_id") is not None:
        try:
            season_id = int(payload["season_id"])
        except (TypeError, ValueError):
            season_id = None
    if episode_id is None and payload.get("episode_id") is not None:
        try:
            episode_id = int(payload["episode_id"])
        except (TypeError, ValueError):
            episode_id = None

    async with async_session_factory() as session:
        res = await manual_delete_transfer(
            session,
            item_id=item_id,
            season_id=season_id,
            episode_id=episode_id,
        )
        if request.headers.get("HX-Request") == "true":
            ctx = await get_push_modal_context(session, item_id)
            if ctx is not None:
                return templates.TemplateResponse(
                    request=request,
                    name="modals/push_modal.html",
                    context=ctx,
                    headers={"HX-Trigger": "refreshDashboard"},
                )
            return HTMLResponse(
                content="",
                status_code=200,
                headers={"HX-Trigger": "refreshDashboard"},
            )

    return JSONResponse(status_code=200, content=res)


@app.post("/api/items/{item_id}/abort-transfer")
async def api_item_abort_transfer(
    item_id: int,
    request: Request,
    season_id: int | None = None,
    episode_id: int | None = None,
) -> Response:
    """Abort an active or rate-limited transfer for a Movie, Season, or Episode from the Push Modal."""
    from app.core.push_engine import get_push_modal_context
    from app.core.transfer_poller import abort_item_transfer

    payload = await _parse_request_payload(request)
    if season_id is None and payload.get("season_id") is not None:
        try:
            season_id = int(payload["season_id"])
        except (TypeError, ValueError):
            season_id = None
    if episode_id is None and payload.get("episode_id") is not None:
        try:
            episode_id = int(payload["episode_id"])
        except (TypeError, ValueError):
            episode_id = None

    async with async_session_factory() as session:
        res = await abort_item_transfer(
            session,
            item_id=item_id,
            season_id=season_id,
            episode_id=episode_id,
        )
        if request.headers.get("HX-Request") == "true":
            ctx = await get_push_modal_context(session, item_id)
            if ctx is not None:
                return templates.TemplateResponse(
                    request=request,
                    name="modals/push_modal.html",
                    context=ctx,
                    headers={"HX-Trigger": "refreshDashboard"},
                )
            return HTMLResponse(
                content="",
                status_code=200,
                headers={"HX-Trigger": "refreshDashboard"},
            )

    return JSONResponse(
        status_code=200,
        content=res,
        headers={"HX-Trigger": "refreshDashboard"},
    )
