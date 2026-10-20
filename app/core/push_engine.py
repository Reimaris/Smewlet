"""
On-Demand Search & Push Engine (v3.0.0)
=======================================
Powers the Unified Push Modal with two execution modes:
1. Auto-Push Best (`push_mode = "auto"`):
   - Sticky search preference persistence on MediaItem
   - Multi-indexer search with Preset / Custom Scorer evaluation
   - Layer 1 & Layer 2 fake detection
   - Automatic Pack-to-Episode fallback when `prefer_season_packs` is ON
   - Franchise Movie category routing (`entry_type == "movie"`)
   - Season Expansion & Movie Bridge Lookahead when `auto_advance_seasons` is ON
2. Search & Pick Manually (`push_mode = "manual"`):
   - Returns scored candidates partitioned into Season Packs / Movie Releases
     and Episode Accordions with 3-tier language grouping (`primary`, `fallback`, `mismatched`)
   - 1-click Manual Grab dispatch to the downloader with `push_mode = "manual"`
"""

from __future__ import annotations

import json
import logging
import re
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.core.anime_titles import (
    anime_season_title_for,
    anime_title_for,
    normalize_anime_title_language,
)
from app.core.failure_logger import log_failure
from app.core.fake_detector import (
    extract_nzb_password,
    is_indexer_metadata_fake,
    is_nzb_content_fake,
)
from app.core.outage import (
    MAX_OUTAGE_RETRIES,
    OutageMarker,
    has_active_outage,
    make_outage_marker,
    parse_outage_marker,
    paused_detail,
    record_outage_from_marker,
    terminal_detail,
)
from app.core.parser import build_release_feature_pills, parse_release_name
from app.core.rate_limit_queue import (
    RATE_LIMITED_DETAIL,
    active_rate_limit_sources,
    downloader_rate_limit_detail,
    get_downloader_cooldown_remaining,
    is_indexer_rate_limit_error,
    is_rate_limit_error,
    longest_downloader_cooldown,
    outage_episode,
    rate_limit_episode,
    rate_limit_queue,
    rate_limit_source_label,
    resume_time_label,
)
from app.core.scorer import score_release
from app.db.models import (
    BlacklistedRelease,
    DownloadHistory,
    Episode,
    EpisodeStatus,
    MediaItem,
    MediaStatus,
    MediaType,
    Season,
    SeasonStatus,
)
from app.services.delivery.hooks import (
    delivery_toggle_context,
    log_dry_run_delivery_plan,
    resolve_deliver_enabled,
)
from app.services.downloaders.base import (
    BaseDownloader,
    DownloaderError,
    DownloaderUploadResult,
)
from app.services.indexers import newznab as newznab_client
from app.services.indexers.catalog import (
    ANIME_CATEGORY_FALLBACK,
    DEFAULT_MOVIE_CATEGORIES,
    DEFAULT_TV_CATEGORIES,
    resolve_categories,
)
from app.services.preset_service import (
    CUSTOM_CONFIG_KEYS,
    extract_custom_config_json,
    get_default_preset,
    get_preset,
    list_presets,
    normalize_season_pack_flags,
    update_item_sticky_search_config,
)
from app.services.provider_capabilities import (
    AUTO_ADVANCE_UNAVAILABLE_MESSAGE,
    can_auto_advance,
)
from app.services.provider_service import get_active_indexers

logger = logging.getLogger(__name__)


def _parse_int_list(val: Any) -> list[int]:
    if val is None:
        return []
    if isinstance(val, int):
        return [val]
    if isinstance(val, str):
        out: list[int] = []
        for part in val.split(","):
            p = part.strip()
            if p:
                try:
                    out.append(int(p))
                except ValueError:
                    pass
        return out
    if isinstance(val, (list, tuple, set)):
        res: list[int] = []
        for x in val:
            try:
                res.append(int(x))
            except (TypeError, ValueError):
                pass
        return res
    return []


def build_history_media_type_label(item: MediaItem) -> str:
    """Return normalized media_type_label ('movie', 'series', 'anime', or 'anime-movie') for DownloadHistory."""
    if item.media_type == MediaType.MOVIE:
        return (
            "anime-movie" if bool(getattr(item, "is_anime_movie", False)) else "movie"
        )
    if item.media_type == MediaType.ANIME:
        return "anime"
    return "series"


def build_history_target_label(
    item: MediaItem | None,
    season: Season | None = None,
    episode: Episode | None = None,
) -> str:
    """Return human-readable target_label ('Movie', 'Season X Pack', 'Movie X' / season title, or 'SXXEYY')."""
    if episode is not None and season is not None:
        s_num = int(season.type_number or season.season_number or 1)
        return f"S{s_num:02d}E{int(episode.episode_number):02d}"
    if episode is not None:
        return f"E{int(episode.episode_number):02d}"
    if season is not None:
        entry_type = getattr(season, "entry_type", "season") or "season"
        s_num = int(season.type_number or season.season_number or 1)
        if entry_type == "movie":
            return season.title or f"Movie {s_num}"
        return f"Season {s_num} Pack"
    return "Movie"


def populate_history_snapshot(
    history: DownloadHistory,
    item: MediaItem,
    season: Season | None = None,
    episode: Episode | None = None,
    *,
    only_missing: bool = False,
) -> None:
    """Populate denormalized media snapshot fields on a DownloadHistory instance."""
    m_type_label = build_history_media_type_label(item)
    t_label = build_history_target_label(item, season, episode)
    season_ext_id = getattr(season, "simkl_id", None) if season is not None else None
    s_anilist = getattr(season, "anilist_id", None) if season is not None else None

    if not only_missing or not history.media_title:
        history.media_title = item.title
    if not only_missing or history.media_year is None:
        history.media_year = item.year
    if not only_missing or not history.media_type_label:
        history.media_type_label = m_type_label
    if not only_missing or not history.target_label:
        history.target_label = t_label
    if not only_missing or not history.poster_url:
        history.poster_url = item.poster_url
    if season_ext_id is not None:
        history.simkl_id = season_ext_id
    elif not only_missing or history.simkl_id is None:
        history.simkl_id = item.simkl_id
    if not only_missing or history.tmdb_id is None:
        history.tmdb_id = item.tmdb_id
    if not only_missing or not history.imdb_id:
        history.imdb_id = item.imdb_id
    if s_anilist is not None:
        history.anilist_id = s_anilist
    elif not only_missing or history.anilist_id is None:
        history.anilist_id = item.anilist_id
    if getattr(history, "is_dry_run", None) is None:
        history.is_dry_run = False


async def detach_item_download_history(session: AsyncSession, item: MediaItem) -> None:
    """Snapshot and detach all DownloadHistory rows linked to a MediaItem before deleting the item."""
    stmt = (
        select(DownloadHistory)
        .where(DownloadHistory.media_item_id == item.id)
        .options(
            selectinload(DownloadHistory.season),
            selectinload(DownloadHistory.episode),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    for dh in rows:
        populate_history_snapshot(
            dh,
            item,
            season=dh.season,
            episode=dh.episode,
            only_missing=True,
        )
        dh.media_item_id = None
        dh.season_id = None
        dh.episode_id = None
        dh.media_item = None
        dh.season = None
        dh.episode = None
    if rows:
        await session.flush()


async def resolve_effective_search_config(
    session: AsyncSession,
    item: MediaItem,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve the active search & scoring configuration from payload, sticky item config, or preset."""
    payload = payload or {}

    # 1. Resolve preset
    preset_id: int | None = item.preset_id
    explicit_custom_preset = False
    if "preset_id" in payload:
        raw_pid = payload.get("preset_id")
        if raw_pid in (None, "", "custom", "none"):
            preset_id = None
            explicit_custom_preset = True
        else:
            try:
                preset_id = int(raw_pid)
            except (TypeError, ValueError):
                preset_id = None
                explicit_custom_preset = True

    preset = await get_preset(session, preset_id) if preset_id else None
    if (
        preset is None
        and not explicit_custom_preset
        and not item.custom_search_config_json
        and not payload
    ):
        preset = await get_default_preset(session)

    # 2. Base values from preset
    primary_language: str | None = preset.primary_language if preset else "en"
    fallback_language: str | None = preset.fallback_language if preset else None
    video_quality_mode: str = preset.video_quality_mode if preset else "best"
    audio_quality_mode: str = preset.audio_quality_mode if preset else "best"
    custom_config: dict[str, Any] = dict(preset.custom_config) if preset else {}

    # 3. Overlay item sticky custom_search_config_json if present
    sticky_cfg: dict[str, Any] = {}
    if item.custom_search_config_json:
        try:
            parsed_sticky = json.loads(item.custom_search_config_json)
            if isinstance(parsed_sticky, dict):
                sticky_cfg = parsed_sticky
                if sticky_cfg.get("primary_language") is not None:
                    primary_language = sticky_cfg.get("primary_language")
                if "fallback_language" in sticky_cfg:
                    fallback_language = sticky_cfg.get("fallback_language")
                if sticky_cfg.get("video_quality_mode"):
                    video_quality_mode = str(sticky_cfg["video_quality_mode"])
                if sticky_cfg.get("audio_quality_mode"):
                    audio_quality_mode = str(sticky_cfg["audio_quality_mode"])
                for k in CUSTOM_CONFIG_KEYS:
                    if k in sticky_cfg and sticky_cfg[k] is not None:
                        custom_config[k] = sticky_cfg[k]
        except Exception:
            sticky_cfg = {}

    # 4. Overlay explicit payload parameters
    if payload.get("primary_language") is not None:
        primary_language = str(payload["primary_language"]).strip() or "en"
    if "fallback_language" in payload:
        fb = payload.get("fallback_language")
        fallback_language = (
            None
            if fb is None or str(fb).strip().lower() in ("", "none", "null")
            else str(fb).strip()
        )
    if payload.get("video_quality_mode"):
        video_quality_mode = str(payload["video_quality_mode"]).strip().lower()
    if payload.get("audio_quality_mode"):
        audio_quality_mode = str(payload["audio_quality_mode"]).strip().lower()

    if (
        "custom_config" in payload
        or "custom_config_json" in payload
        or any(k in payload for k in CUSTOM_CONFIG_KEYS)
    ):
        extracted_str = extract_custom_config_json(payload)
        try:
            extracted_dict = json.loads(extracted_str)
            if isinstance(extracted_dict, dict):
                custom_config.update(extracted_dict)
        except Exception:
            pass

    if preset is not None:
        base_allow = bool(getattr(preset, "allow_season_packs", False))
        base_prefer = bool(getattr(preset, "prefer_season_packs", False))
    else:
        base_allow = bool(getattr(item, "allow_season_packs", False))
        base_prefer = bool(getattr(item, "prefer_season_packs", False))
        if "allow_season_packs" in sticky_cfg or "prefer_season_packs" in sticky_cfg:
            base_allow, base_prefer = normalize_season_pack_flags(
                sticky_cfg.get("allow_season_packs", base_allow),
                sticky_cfg.get("prefer_season_packs", base_prefer),
                default_allow=base_allow,
                default_prefer=base_prefer,
            )

    if "allow_season_packs" in payload or "prefer_season_packs" in payload:
        req_allow, req_prefer = normalize_season_pack_flags(
            payload.get("allow_season_packs", base_allow),
            payload.get("prefer_season_packs", base_prefer),
            default_allow=base_allow,
            default_prefer=base_prefer,
        )
        if (
            preset is None
            or payload.get("preset_id") in (None, "", "custom", "none")
            or (req_allow, req_prefer) != (base_allow, base_prefer)
        ):
            allow_season_packs, prefer_season_packs = req_allow, req_prefer
        else:
            allow_season_packs, prefer_season_packs = normalize_season_pack_flags(
                base_allow,
                base_prefer,
                default_allow=base_allow,
                default_prefer=base_prefer,
            )
    else:
        allow_season_packs, prefer_season_packs = normalize_season_pack_flags(
            base_allow,
            base_prefer,
            default_allow=base_allow,
            default_prefer=base_prefer,
        )

    auto_advance_seasons = bool(item.auto_advance_seasons)
    if (
        "auto_advance_seasons" in payload
        and payload["auto_advance_seasons"] is not None
    ):
        val = payload["auto_advance_seasons"]
        auto_advance_seasons = (
            val
            if isinstance(val, bool)
            else str(val).strip().lower() in ("1", "true", "yes", "on")
        )

    advance_allowed = await can_auto_advance(session, item)
    if not advance_allowed:
        auto_advance_seasons = False

    return {
        "preset_id": preset.id if preset else preset_id,
        "primary_language": primary_language,
        "fallback_language": fallback_language,
        "video_quality_mode": video_quality_mode,
        "audio_quality_mode": audio_quality_mode,
        "custom_config": custom_config,
        "allow_season_packs": allow_season_packs,
        "prefer_season_packs": prefer_season_packs,
        "auto_advance_seasons": auto_advance_seasons,
        "can_auto_advance": advance_allowed,
    }


async def persist_sticky_preferences(
    session: AsyncSession, item: MediaItem, payload: dict[str, Any]
) -> None:
    """Persist sticky search configuration from a Push Modal request onto MediaItem."""
    sticky_keys = (
        "preset_id",
        "allow_season_packs",
        "prefer_season_packs",
        "auto_advance_seasons",
        "custom_search_config_json",
        "custom_config",
        "custom_config_json",
        "primary_language",
        "fallback_language",
        "video_quality_mode",
        "audio_quality_mode",
        *CUSTOM_CONFIG_KEYS,
    )
    sticky_payload: dict[str, Any] = {
        k: payload[k] for k in sticky_keys if k in payload
    }
    if "auto_advance_seasons" in sticky_payload and not await can_auto_advance(
        session, item
    ):
        del sticky_payload["auto_advance_seasons"]
    if sticky_payload:
        await update_item_sticky_search_config(session, item.id, sticky_payload)


async def _score_and_partition_candidates(
    raw_results: list[dict[str, Any]],
    blacklisted_guids: set[str],
    blacklisted_titles: set[str],
    expected_title: str | None,
    expected_year: int | None,
    expected_alt_title: str | None,
    expected_season: int | None,
    expected_episode: int | None,
    expected_season_title: str | None,
    runtime_minutes: int | None,
    effective_cfg: dict[str, Any],
    media_type: str | None = None,
    season_episode_count: int | None = None,
    session: AsyncSession | None = None,
    item_id: int | None = None,
) -> dict[str, Any]:
    """Score and partition; re-query with the localized title if no Primary hit (m82)."""
    kwargs: dict[str, Any] = {
        "blacklisted_guids": blacklisted_guids,
        "blacklisted_titles": blacklisted_titles,
        "expected_title": expected_title,
        "expected_year": expected_year,
        "expected_season": expected_season,
        "expected_episode": expected_episode,
        "expected_season_title": expected_season_title,
        "runtime_minutes": runtime_minutes,
        "effective_cfg": effective_cfg,
        "media_type": media_type,
        "season_episode_count": season_episode_count,
        "session": session,
        "item_id": item_id,
    }
    partitioned = await _score_and_partition_once(
        raw_results, expected_alt_title=expected_alt_title, **kwargs
    )
    if partitioned["primary"] or session is None or item_id is None:
        return partitioned
    extra = await _localized_requery(
        session,
        item_id=item_id,
        expected_title=expected_title,
        media_type=media_type,
        season_number=expected_season,
        episode_number=expected_episode,
        language=effective_cfg.get("primary_language"),
    )
    if extra is None:
        return partitioned
    localized, new_raw = extra
    seen_guids = {str(r.get("guid") or r.get("link") or "") for r in raw_results}
    seen_titles = {str(r.get("title") or "").strip().lower() for r in raw_results}
    merged = list(raw_results)
    for r in new_raw:
        guid = str(r.get("guid") or r.get("link") or "")
        norm = str(r.get("title") or "").strip().lower()
        if guid in seen_guids or norm in seen_titles:
            continue
        seen_guids.add(guid)
        seen_titles.add(norm)
        merged.append(r)
    if len(merged) == len(raw_results):
        return partitioned
    return await _score_and_partition_once(
        merged, expected_alt_title=localized, **kwargs
    )


async def _localized_requery(
    session: AsyncSession,
    *,
    item_id: int,
    expected_title: str | None,
    media_type: str | None,
    season_number: int | None,
    episode_number: int | None,
    language: str | None,
) -> tuple[str, list[dict[str, Any]]] | None:
    """Query the indexers again with the TMDB title in ``language`` (None if not useful)."""
    from app.services.localized_titles import get_localized_title

    if not language or language.lower() == "en":
        return None
    try:
        item = await session.get(MediaItem, item_id)
        if item is None or item.tmdb_id is None or expected_title != item.title:
            return None
        is_movie = media_type == "movie"
        kind = "movie" if is_movie else "series"
        localized = await get_localized_title(
            kind, item.tmdb_id, language, item.title, session=session
        )
        if not localized:
            return None
        if is_movie:
            raw = await _query_movie_across_indexers(
                session, item, title_override=localized
            )
        else:
            raw = await _query_show_across_indexers(
                session, item, season_number, episode_number, title_override=localized
            )
    except Exception as exc:
        logger.debug("Localized-title fallback skipped for item %s: %s", item_id, exc)
        return None
    logger.debug(
        "[SEARCH] Localized fallback '%s' returned %d candidates.", localized, len(raw)
    )
    return localized, raw


async def _score_and_partition_once(
    raw_results: list[dict[str, Any]],
    blacklisted_guids: set[str],
    blacklisted_titles: set[str],
    expected_title: str | None,
    expected_year: int | None,
    expected_alt_title: str | None,
    expected_season: int | None,
    expected_episode: int | None,
    expected_season_title: str | None,
    runtime_minutes: int | None,
    effective_cfg: dict[str, Any],
    media_type: str | None = None,
    season_episode_count: int | None = None,
    session: AsyncSession | None = None,
    item_id: int | None = None,
) -> dict[str, Any]:
    """Score raw indexer results and partition them into primary, fallback, mismatched, and all_valid."""
    primary_list: list[dict[str, Any]] = []
    fallback_list: list[dict[str, Any]] = []
    mismatched_list: list[dict[str, Any]] = []

    seen_guids: set[str] = set()
    seen_titles: set[str] = set()

    primary_lang = effective_cfg.get("primary_language")
    fallback_lang = effective_cfg.get("fallback_language")
    v_mode = effective_cfg.get("video_quality_mode", "best")
    a_mode = effective_cfg.get("audio_quality_mode", "best")
    custom_cfg = effective_cfg.get("custom_config") or {}

    rejected_by_l1 = 0
    for raw in raw_results:
        title = str(raw.get("title") or "")
        guid = str(raw.get("guid") or raw.get("link") or "")
        size_bytes = int(raw.get("size") or raw.get("size_bytes") or 0)

        if not title or not guid:
            continue
        norm_title = title.strip().lower()
        if guid in seen_guids or norm_title in seen_titles:
            continue
        seen_guids.add(guid)
        seen_titles.add(norm_title)

        if (
            guid in blacklisted_guids
            or title in blacklisted_titles
            or norm_title in blacklisted_titles
        ):
            continue

        parsed = parse_release_name(title)

        # Layer 1: Indexer metadata filtering (drop and blacklist password-protected and under-sized fakes)
        is_sp = expected_season is not None and expected_episode is None
        is_fake, fake_reason = is_indexer_metadata_fake(
            raw_item=raw,
            parsed_resolution=parsed.resolution,
            media_type=media_type or "movie",
            is_season_pack=is_sp,
        )
        if is_fake:
            rejected_by_l1 += 1
            logger.warning("Rejected Layer 1 fake release '%s': %s", title, fake_reason)
            blacklisted_guids.add(guid)
            blacklisted_titles.add(title)
            blacklisted_titles.add(norm_title)
            if session is not None:
                bl = BlacklistedRelease(
                    media_item_id=item_id,
                    nzb_guid=guid,
                    nzb_title=title,
                    reason=f"Layer 1 Fake: {fake_reason}",
                )
                session.add(bl)
                await session.commit()
            continue

        score_res = score_release(
            parsed=parsed,
            size_bytes=size_bytes,
            runtime_minutes=runtime_minutes,
            expected_title=expected_title,
            expected_year=expected_year,
            expected_alt_title=expected_alt_title,
            expected_season=expected_season,
            expected_episode=expected_episode,
            expected_season_title=expected_season_title,
            primary_language=primary_lang,
            fallback_language=fallback_lang,
            api_language=raw.get("api_language"),
            video_quality_mode=v_mode,
            audio_quality_mode=a_mode,
            custom_config=custom_cfg,
            media_type=media_type,
            season_episode_count=season_episode_count,
        )

        entry = {
            "title": title,
            "guid": guid,
            "size_bytes": size_bytes,
            "size_gb": round(size_bytes / (1024**3), 2),
            "parsed": parsed,
            "resolution": parsed.resolution,
            "source": parsed.source,
            "video_codec": parsed.video_codec,
            "audio_codec": parsed.audio_codec,
            "release_group": parsed.release_group,
            "indexer_name": raw.get("_indexer_name"),
            "indexer_url": raw.get("_indexer_url"),
            "indexer_key": raw.get("_indexer_key"),
            "is_season_pack": is_sp,
        }

        if not score_res.is_rejected:
            rounded_score = round(score_res.score, 1)
            entry["score"] = rounded_score
            entry["score_res"] = score_res
            entry["bitrate_mbps"] = score_res.bitrate_mbps
            entry["normalized_bitrate_mbps"] = score_res.normalized_bitrate_mbps
            entry["is_primary"] = score_res.is_primary
            entry["is_fallback"] = score_res.is_fallback
            entry["matched_language"] = score_res.matched_language
            entry["feature_pills"] = build_release_feature_pills(
                parsed=parsed,
                score=rounded_score,
                size_bytes=size_bytes,
                is_fallback=bool(score_res.is_fallback),
                is_mismatch=False,
                matched_language=score_res.matched_language,
                api_language=raw.get("api_language"),
                bitrate_mbps=score_res.bitrate_mbps,
                normalized_bitrate_mbps=score_res.normalized_bitrate_mbps,
                runtime_minutes=runtime_minutes,
                media_type=media_type,
                season_episode_count=season_episode_count,
            )
            if score_res.is_primary:
                primary_list.append(entry)
            else:
                fallback_list.append(entry)
        elif score_res.reject_reason and score_res.reject_reason.startswith(
            "Language mismatch:"
        ):
            # Evaluate without language restriction to see if it qualifies for the Mismatched tier
            any_lang_res = score_release(
                parsed=parsed,
                size_bytes=size_bytes,
                runtime_minutes=runtime_minutes,
                expected_title=expected_title,
                expected_year=expected_year,
                expected_alt_title=expected_alt_title,
                expected_season=expected_season,
                expected_episode=expected_episode,
                expected_season_title=expected_season_title,
                primary_language="any",
                fallback_language=None,
                api_language=raw.get("api_language"),
                video_quality_mode=v_mode,
                audio_quality_mode=a_mode,
                custom_config=custom_cfg,
                media_type=media_type,
                season_episode_count=season_episode_count,
            )
            if not any_lang_res.is_rejected:
                mismatch_score = round(any_lang_res.score, 1)
                entry["score"] = mismatch_score
                entry["score_res"] = any_lang_res
                entry["bitrate_mbps"] = any_lang_res.bitrate_mbps
                entry["normalized_bitrate_mbps"] = any_lang_res.normalized_bitrate_mbps
                entry["is_primary"] = False
                entry["is_fallback"] = False
                entry["matched_language"] = any_lang_res.matched_language
                entry["feature_pills"] = build_release_feature_pills(
                    parsed=parsed,
                    score=mismatch_score,
                    size_bytes=size_bytes,
                    is_fallback=False,
                    is_mismatch=True,
                    matched_language=any_lang_res.matched_language,
                    api_language=raw.get("api_language"),
                    bitrate_mbps=any_lang_res.bitrate_mbps,
                    normalized_bitrate_mbps=any_lang_res.normalized_bitrate_mbps,
                    runtime_minutes=runtime_minutes,
                    media_type=media_type,
                    season_episode_count=season_episode_count,
                )
                mismatched_list.append(entry)

    primary_list.sort(key=lambda x: x["score"], reverse=True)
    fallback_list.sort(key=lambda x: x["score"], reverse=True)
    mismatched_list.sort(key=lambda x: x["score"], reverse=True)

    all_valid = primary_list + fallback_list

    logger.debug(
        "[SEARCH] Filtered %d valid candidates (%d rejected by Layer 1, %d language mismatches).",
        len(all_valid),
        rejected_by_l1,
        len(mismatched_list),
    )

    if all_valid:
        top_cand = all_valid[0]
        logger.debug(
            "[SEARCH] Top candidate selected: '%s' (Score: %.1f, Lang: %s, Size: %.2f GB).",
            top_cand["title"],
            top_cand.get("score", 0.0),
            top_cand.get("matched_language") or "unknown",
            top_cand.get("size_gb", 0.0),
        )

    target_lbl = expected_title or "Media"
    if expected_season is not None and expected_episode is not None:
        target_lbl = (
            f"{target_lbl} S{int(expected_season):02d}E{int(expected_episode):02d}"
        )
    elif expected_season is not None:
        target_lbl = f"{target_lbl} S{int(expected_season):02d} (Pack)"

    if all_valid:
        logger.info(
            "    🏆 Top %d releases for %s:",
            min(5, len(all_valid)),
            target_lbl,
        )
        for i, c in enumerate(all_valid[:5]):
            tier_lbl = "Fallback" if c.get("is_fallback") else "Primary"
            logger.info(
                "       %d. [%.1f] (%s) %s",
                i + 1,
                c["score"],
                tier_lbl,
                c["title"],
            )
    else:
        logger.info(
            "    ❌ No matching releases for %s (raw=%d, lang_mismatch=%d).",
            target_lbl,
            len(raw_results),
            len(mismatched_list),
        )

    return {
        "primary": primary_list,
        "fallback": fallback_list,
        "mismatched": mismatched_list,
        "all_valid": all_valid,
        "had_candidates": len(all_valid) > 0,
        "had_raw_results": len(raw_results) > 0,
    }


def _serialize_candidate(cand: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-serializable candidate dictionary (stripping internal dataclasses)."""
    return {k: v for k, v in cand.items() if k not in ("parsed", "score_res")}


_search_outages: ContextVar[dict[str, tuple[str, str]] | None] = ContextVar(
    "search_outages", default=None
)


def _note_search_outage(indexer_name: str, exc: BaseException) -> None:
    """Remember an indexer outage hit during a manual search (for the Retry Search warning)."""
    failures = _search_outages.get()
    if failures is not None and isinstance(exc, newznab_client.IndexerOutageError):
        failures[indexer_name] = (
            indexer_name,
            exc.verdict.label if exc.verdict else "an error",
        )


async def _query_movie_across_indexers(
    session: AsyncSession,
    item: MediaItem,
    title_override: str | None = None,
    use_external_ids: bool = True,
) -> list[dict[str, Any]]:
    """Query active indexers for a standalone movie or franchise movie entry."""
    search_title = title_override or item.title
    imdb_id = item.imdb_id if use_external_ids else None
    tmdb_id = item.tmdb_id if use_external_ids else None

    indexers = await get_active_indexers(session)
    all_results: list[dict[str, Any]] = []

    if indexers:
        for idx in indexers:
            cat_id = resolve_categories(idx, "movie")
            try:
                logger.debug(
                    "[SEARCH] Searching indexer '%s' for '%s' (Categories: %s)...",
                    idx.name,
                    search_title,
                    cat_id,
                )
                res = await newznab_client.search_movie(
                    imdb_id=imdb_id,
                    tmdb_id=tmdb_id,
                    title=search_title,
                    category=cat_id,
                    api_url=idx.api_url,
                    api_key=idx.api_key,
                    session=session,
                )
                logger.debug(
                    "[SEARCH] Indexer '%s' returned %d raw candidates.",
                    idx.name,
                    len(res),
                )
                for r in res:
                    r["_indexer_name"] = idx.name
                    r["_indexer_url"] = idx.api_url
                    r["_indexer_key"] = idx.api_key
                all_results.extend(res)
            except Exception as exc:
                _note_search_outage(idx.name, exc)
                logger.warning("Indexer '%s' movie search error: %s", idx.name, exc)
    else:
        cat_id = DEFAULT_MOVIE_CATEGORIES
        logger.debug(
            "[SEARCH] Searching indexer 'Default' for '%s' (Categories: %s)...",
            search_title,
            cat_id,
        )
        res = await newznab_client.search_movie(
            imdb_id=imdb_id,
            tmdb_id=tmdb_id,
            title=search_title,
            category=cat_id,
            session=session,
        )
        logger.debug(
            "[SEARCH] Indexer 'Default' returned %d raw candidates.",
            len(res),
        )
        all_results.extend(res)

    return all_results


async def _query_show_across_indexers(
    session: AsyncSession,
    item: MediaItem,
    season_number: int | None,
    episode_number: int | None = None,
    title_override: str | None = None,
) -> list[dict[str, Any]]:
    """Query active indexers for a TV/Anime season pack or episode."""
    search_title = title_override or item.title
    kind = "anime" if item.media_type == MediaType.ANIME else "series"

    ep_str = str(episode_number) if episode_number is not None else None
    search_label = (
        f"{search_title} S{int(season_number):02d}E{int(episode_number):02d}"
        if season_number is not None and episode_number is not None
        else (
            f"{search_title} S{int(season_number):02d} (Pack)"
            if season_number is not None
            else search_title
        )
    )
    indexers = await get_active_indexers(session)
    all_results: list[dict[str, Any]] = []

    if indexers:
        for idx in indexers:
            cat_id = resolve_categories(idx, kind)
            try:
                logger.debug(
                    "[SEARCH] Searching indexer '%s' for '%s' (Categories: %s)...",
                    idx.name,
                    search_label,
                    cat_id,
                )
                res = await newznab_client.search_show(
                    tvdb_id=item.tvdb_id,
                    tmdb_id=item.tmdb_id,
                    imdb_id=item.imdb_id,
                    title=search_title,
                    season=season_number,
                    ep=ep_str,
                    category=cat_id,
                    api_url=idx.api_url,
                    api_key=idx.api_key,
                    session=session,
                )
                logger.debug(
                    "[SEARCH] Indexer '%s' returned %d raw candidates.",
                    idx.name,
                    len(res),
                )
                for r in res:
                    r["_indexer_name"] = idx.name
                    r["_indexer_url"] = idx.api_url
                    r["_indexer_key"] = idx.api_key
                all_results.extend(res)
            except Exception as exc:
                _note_search_outage(idx.name, exc)
                logger.warning("Indexer '%s' show search error: %s", idx.name, exc)
    else:
        cat_id = ANIME_CATEGORY_FALLBACK if kind == "anime" else DEFAULT_TV_CATEGORIES
        logger.debug(
            "[SEARCH] Searching indexer 'Default' for '%s' (Categories: %s)...",
            search_label,
            cat_id,
        )
        res = await newznab_client.search_show(
            tvdb_id=item.tvdb_id,
            tmdb_id=item.tmdb_id,
            imdb_id=item.imdb_id,
            title=search_title,
            season=season_number,
            ep=ep_str,
            category=cat_id,
            session=session,
        )
        logger.debug(
            "[SEARCH] Indexer 'Default' returned %d raw candidates.",
            len(res),
        )
        all_results.extend(res)

    return all_results


def _log_dry_run_interception(
    item_title: str,
    target_label: str,
    release_title: str,
    score: Any,
) -> None:
    logger.info(
        "\n============================================================\n"
        "[DRY RUN] SIMULATED DISPATCH (dispatch skipped)\n"
        "Title:         %s\n"
        "Target:        %s\n"
        "Release:       %s\n"
        "Score:         %s\n"
        "============================================================",
        item_title,
        target_label,
        release_title,
        score,
    )


async def _simulate_push_candidate(
    session: AsyncSession,
    cand: dict[str, Any],
    item: MediaItem,
    season: Season | None = None,
    episode: Episode | None = None,
    push_mode: str = "auto",
    event_type: str = "push_initiated",
    deliver: bool | None = None,
) -> DownloadHistory:
    from app.core.transfer_poller import register_dry_run_push
    from app.services import discord

    target_label = discord.format_target_label(
        item=item, season=season, episode=episode
    )
    title = cand["title"]
    score = cand.get("score")
    _log_dry_run_interception(
        item_title=item.title,
        target_label=target_label,
        release_title=title,
        score=score,
    )

    parsed = cand.get("parsed") or parse_release_name(title)
    score_res = cand.get("score_res")
    item_media_type = (
        item.media_type.value
        if hasattr(item.media_type, "value")
        else str(item.media_type or "")
    )
    feature_pills = build_release_feature_pills(
        release_title=title,
        score=cand.get("score"),
        size_bytes=cand.get("size_bytes"),
        is_fallback=bool(cand.get("is_fallback", False)),
        matched_language=cand.get("matched_language"),
        bitrate_mbps=(
            score_res.bitrate_mbps if score_res else cand.get("bitrate_mbps")
        ),
        runtime_minutes=item.runtime_minutes,
        media_type=item_media_type,
        season_episode_count=season.episode_count if season else None,
    )

    register_dry_run_push(
        media_item_id=item.id,
        media_title=item.title,
        release_title=title,
        target_badge=target_label,
        feature_pills=feature_pills,
        push_mode=push_mode,
        score=cand.get("score"),
        size_bytes=cand.get("size_bytes"),
        resolution=parsed.resolution if parsed else cand.get("resolution"),
        source=parsed.source if parsed else cand.get("source"),
        language=cand.get("matched_language"),
        season_id=season.id if season else None,
        episode_id=episode.id if episode else None,
    )

    target_season_id = season.id if season else (episode.season_id if episode else None)
    target_episode_id = episode.id if episode else None
    now_utc = datetime.now(timezone.utc)

    history = DownloadHistory(
        media_item_id=item.id,
        season_id=target_season_id,
        episode_id=target_episode_id,
        nzb_title=title,
        nzb_guid=cand.get("guid"),
        score=cand.get("score"),
        size_bytes=cand.get("size_bytes"),
        resolution=parsed.resolution if parsed else cand.get("resolution"),
        video_codec=parsed.video_codec if parsed else cand.get("video_codec"),
        audio_codec=parsed.audio_codec if parsed else cand.get("audio_codec"),
        source=parsed.source if parsed else cand.get("source"),
        release_group=parsed.release_group if parsed else cand.get("release_group"),
        bitrate_mbps=(
            score_res.bitrate_mbps if score_res else cand.get("bitrate_mbps")
        ),
        downloader_hash=None,
        downloader_transfer_id=None,
        dispatched_at=now_utc,
        is_fallback=bool(cand.get("is_fallback", False)),
        grabbed_language=cand.get("matched_language"),
        push_mode=push_mode,
        status_detail="simulated",
        is_dismissed=True,
        is_dry_run=True,
        progress_pct=100.0,
        completed_at=now_utc,
        notification_sent=False,
        deliver_enabled=await resolve_deliver_enabled(session, deliver),
    )
    history.media_item = item
    if season is not None:
        history.season = season
    if episode is not None:
        history.episode = episode
    populate_history_snapshot(history, item, season=season, episode=episode)
    session.add(history)
    await session.commit()
    if history.deliver_enabled:
        await log_dry_run_delivery_plan(session, history)

    return history


async def dispatch_nzb_to_active_downloader(
    session: AsyncSession,
    nzb_bytes: bytes,
    filename: str,
    *,
    password: str | None,
    media_type: MediaType | str,
    is_anime: bool,
) -> tuple[BaseDownloader, DownloaderUploadResult]:
    """Upload an NZB through the active downloader (m57, ADR-131).

    Raises ``DownloaderError`` when no downloader is active; transport errors
    propagate as ``DownloaderNetworkError``.
    """
    from app.services.downloaders.factory import (
        category_for_media,
        get_active_downloader_client,
    )

    client = await get_active_downloader_client(session)
    if client is None:
        raise DownloaderError("No active downloader configured")
    category = category_for_media(client.provider, media_type, is_anime)
    result = await client.upload_nzb(
        nzb_bytes, filename, password=password, category=category, is_manual=True
    )
    return client, result


async def _dispatch_candidate_list_to_downloader(
    session: AsyncSession,
    candidates: list[dict[str, Any]],
    item: MediaItem,
    season: Season | None = None,
    episode: Episode | None = None,
    push_mode: str = "auto",
    event_type: str = "push_initiated",
    status_reason: str | None = None,
    reset_fail_count: bool = True,
    error_out: list[str] | None = None,
    is_upgrade: bool = False,
    replaces_history_id: int | None = None,
    deliver: bool | None = None,
) -> DownloadHistory | dict[str, Any] | None:
    """Iterate through scored candidates, verify Layer 2 NZB fake check, and dispatch to the downloader."""
    is_movie = episode is None and (
        season is None or getattr(season, "entry_type", "season") == "movie"
    )
    media_type_str = "movie" if is_movie else "episode"

    for cand in candidates:
        guid = cand["guid"]
        title = cand["title"]

        cooldown_rem = get_downloader_cooldown_remaining()
        if cooldown_rem > 0:
            err_str = downloader_rate_limit_detail()
            logger.warning(
                "Skipping NZB fetch & downloader dispatch for '%s': %s",
                title,
                err_str,
            )
            if error_out is not None:
                error_out.append(err_str)
            return None

        prefetched = cand.get("_prefetched_nzb")
        if prefetched is not None:
            nzb_bytes, filename = prefetched
        else:
            try:
                nzb_bytes, filename = await newznab_client.fetch_nzb_bytes(
                    guid,
                    api_url=cand.get("indexer_url"),
                    api_key=cand.get("indexer_key"),
                    session=session,
                )
            except newznab_client.IndexerRateLimitError as exc:
                logger.warning(
                    "Indexer '%s' grab limit reached (HTTP 429): %s",
                    cand.get("indexer_url") or "unknown",
                    exc,
                )
                if error_out is not None:
                    error_out.append(
                        f"Indexer rate limit exceeded ({exc.indexer_url}): {exc}"
                    )
                return None
            except newznab_client.IndexerOutageError as exc:
                logger.warning("Indexer outage fetching '%s': %s", title, exc)
                if error_out is not None:
                    error_out.append(
                        make_outage_marker(
                            "indexer",
                            newznab_client.indexer_display_name(
                                exc.indexer_url or cand.get("indexer_url") or ""
                            ),
                            exc.verdict.label if exc.verdict else "outage",
                        )
                    )
                return None
            except newznab_client.IndexerAuthError as exc:
                logger.warning("Indexer auth error fetching '%s': %s", title, exc)
                if error_out is not None:
                    error_out.append(
                        "failed: indexer authentication error "
                        f"({exc.verdict.label if exc.verdict else 'rejected'})"
                    )
                return None
            except Exception as exc:
                logger.warning("Failed fetching NZB bytes for '%s': %s", title, exc)
                continue

            cand_is_sp = bool(
                cand.get("is_season_pack")
                or (season is not None and episode is None and not is_movie)
            )
            is_fake, fake_reason = is_nzb_content_fake(
                nzb_bytes,
                media_type=media_type_str,
                is_season_pack=cand_is_sp,
            )
            if is_fake:
                err_msg = f"NZB flagged as fake/executable: {fake_reason}"
                logger.warning("Rejected fake NZB '%s': %s", title, err_msg)
                bl = BlacklistedRelease(
                    media_item_id=item.id,
                    nzb_guid=guid,
                    nzb_title=title,
                    reason=err_msg,
                )
                session.add(bl)
                await session.commit()
                continue

        cand_sp_flag = bool(
            cand.get("is_season_pack")
            or (season is not None and episode is None and not is_movie)
        )
        logger.debug(
            "[SEARCH] Dispatching '%s' to the downloader (Push Mode: %s, Season Pack: %s)...",
            title,
            push_mode,
            cand_sp_flag,
        )

        if settings.dry_run:
            return await _simulate_push_candidate(
                session=session,
                cand=cand,
                item=item,
                season=season,
                episode=episode,
                push_mode=push_mode,
                event_type=event_type,
                deliver=deliver,
            )

        nzb_password = cand.get("_nzb_password") or extract_nzb_password(nzb_bytes)

        try:
            client, upload = await dispatch_nzb_to_active_downloader(
                session,
                nzb_bytes,
                filename,
                password=nzb_password,
                media_type=item.media_type,
                is_anime=bool(
                    item.media_type == MediaType.ANIME
                    or getattr(item, "is_anime_movie", False)
                ),
            )
        except DownloaderError as exc:
            logger.warning(
                "Downloader dispatch halted for '%s' due to downloader error/cooldown: %s",
                title,
                exc,
            )
            if error_out is not None:
                error_out.append(str(exc))
            return None
        if upload.is_rate_limited:
            err_str = upload.detail or (
                f"{client.display_name} rate limit exceeded "
                f"(cooldown: {int(upload.cooldown_seconds or 0)}s)"
            )
            logger.warning("Downloader dispatch halted for '%s': %s", title, err_str)
            if error_out is not None:
                error_out.append(err_str)
            return None
        if upload.is_outage:
            logger.warning(
                "%s outage during dispatch of '%s': %s",
                client.display_name,
                title,
                upload.detail,
            )
            if error_out is not None:
                error_out.append(
                    make_outage_marker(
                        "downloader",
                        client.display_name,
                        upload.outage_label or "outage",
                    )
                )
            return None
        if upload.is_auth_error:
            logger.warning(
                "%s authentication error for '%s': %s",
                client.display_name,
                title,
                upload.detail,
            )
            if error_out is not None:
                error_out.append(
                    "failed: downloader authentication error "
                    f"({(upload.detail or 'credentials rejected').strip()[:80]})"
                )
            return None
        if not upload.success or not (upload.transfer_id or upload.raw.get("hash")):
            logger.warning(
                "%s dispatch failed for '%s': %s",
                client.display_name,
                title,
                upload.detail,
            )
            continue
        downloader_result = upload.raw

        parsed = cand.get("parsed")
        score_res = cand.get("score_res")

        target_season_id = (
            season.id if season else (episode.season_id if episode else None)
        )
        target_episode_id = episode.id if episode else None
        new_transfer_id = upload.transfer_id

        logger.debug(
            "[SEARCH] Dispatched successfully to the downloader -> Transfer ID: %s.",
            new_transfer_id or downloader_result.get("hash"),
        )

        superseded_transfer_ids: list[str] = []
        if reset_fail_count:
            from sqlalchemy import and_, or_

            if target_season_id is None and target_episode_id is None:
                prior_stmt = select(DownloadHistory).where(
                    DownloadHistory.media_item_id == item.id,
                    DownloadHistory.season_id.is_(None),
                    DownloadHistory.episode_id.is_(None),
                    DownloadHistory.is_dismissed.is_(False),
                )
            elif target_episode_id is None:
                prior_stmt = select(DownloadHistory).where(
                    DownloadHistory.media_item_id == item.id,
                    DownloadHistory.season_id == target_season_id,
                    DownloadHistory.is_dismissed.is_(False),
                )
            else:
                prior_stmt = select(DownloadHistory).where(
                    DownloadHistory.media_item_id == item.id,
                    DownloadHistory.is_dismissed.is_(False),
                    or_(
                        DownloadHistory.episode_id == target_episode_id,
                        and_(
                            DownloadHistory.season_id == target_season_id,
                            DownloadHistory.episode_id.is_(None),
                        ),
                    ),
                )
            prior_rows = (await session.execute(prior_stmt)).scalars().all()
            for pr in prior_rows:
                if pr.downloader_transfer_id and str(pr.downloader_transfer_id) != str(
                    new_transfer_id or ""
                ):
                    superseded_transfer_ids.append(str(pr.downloader_transfer_id))
                pr.status_detail = "replaced"
                pr.is_dismissed = True
                pr.notification_sent = True

        history = DownloadHistory(
            media_item_id=item.id,
            season_id=target_season_id,
            episode_id=target_episode_id,
            nzb_title=title,
            nzb_guid=guid,
            score=cand.get("score"),
            size_bytes=cand.get("size_bytes"),
            resolution=parsed.resolution if parsed else cand.get("resolution"),
            video_codec=parsed.video_codec if parsed else cand.get("video_codec"),
            audio_codec=parsed.audio_codec if parsed else cand.get("audio_codec"),
            source=parsed.source if parsed else cand.get("source"),
            release_group=parsed.release_group if parsed else cand.get("release_group"),
            bitrate_mbps=(
                score_res.bitrate_mbps if score_res else cand.get("bitrate_mbps")
            ),
            downloader_hash=str(downloader_result.get("hash"))
            if client.provider_type == "torbox" and downloader_result.get("hash")
            else None,
            downloader_type=client.provider_type,
            downloader_transfer_id=new_transfer_id,
            dispatched_at=datetime.now(timezone.utc),
            is_fallback=bool(cand.get("is_fallback", False)),
            grabbed_language=cand.get("matched_language"),
            push_mode=push_mode,
            status_detail="downloading",
            is_upgrade=is_upgrade,
            replaces_history_id=replaces_history_id,
            notification_sent=False,
            deliver_enabled=await resolve_deliver_enabled(session, deliver),
        )
        history.media_item = item
        if season is not None:
            history.season = season
        if episode is not None:
            history.episode = episode
        populate_history_snapshot(history, item, season=season, episode=episode)
        session.add(history)

        pushed_at = history.dispatched_at
        if episode is not None:
            episode.status = EpisodeStatus.DOWNLOADING
            if not is_upgrade and episode.first_pushed_at is None:
                episode.first_pushed_at = pushed_at
            if reset_fail_count:
                episode.fail_count = 0
            episode.last_error = None
            if season is not None and season.status in (
                SeasonStatus.COMPLETED,
                SeasonStatus.DOWNLOADED,
            ):
                season.status = SeasonStatus.DOWNLOADING
        elif season is not None:
            season.status = SeasonStatus.DOWNLOADING
            if reset_fail_count:
                season.fail_count = 0
            season.last_error = None
            for ep in season.episodes:
                if ep.status != EpisodeStatus.FUTURE:
                    ep.status = EpisodeStatus.DOWNLOADING
                    if not is_upgrade and ep.first_pushed_at is None:
                        ep.first_pushed_at = pushed_at
                    if reset_fail_count:
                        ep.fail_count = 0
                    ep.last_error = None
        else:
            item.status = MediaStatus.DOWNLOADING
            if reset_fail_count:
                item.fail_count = 0
            item.last_error = None

        await session.commit()

        if superseded_transfer_ids:
            from app.core.transfer_poller import _delete_and_purge_transfer

            for old_tid in superseded_transfer_ids:
                await _delete_and_purge_transfer(session, old_tid)
            await session.commit()

        from app.core.transfer_poller import wake_transfer_poller

        wake_transfer_poller()
        return history

    if settings.dry_run:
        return {"status": "simulated", "candidate": None}
    return None


async def _load_blacklisted_sets(
    session: AsyncSession, media_item_id: int | None = None
) -> tuple[set[str], set[str]]:
    if media_item_id is not None:
        stmt = select(BlacklistedRelease).where(
            or_(
                BlacklistedRelease.media_item_id == media_item_id,
                BlacklistedRelease.media_item_id.is_(None),
            )
        )
    else:
        stmt = select(BlacklistedRelease).where(
            BlacklistedRelease.media_item_id.is_(None)
        )
    rows = (await session.execute(stmt)).scalars().all()
    guids = {r.nzb_guid for r in rows if r.nzb_guid}
    titles: set[str] = set()
    for r in rows:
        if r.nzb_title:
            titles.add(r.nzb_title)
            titles.add(r.nzb_title.strip().lower())
    return guids, titles


def _candidate_beats_pack(ep_cand: dict[str, Any], pack_cand: dict[str, Any]) -> bool:
    """Return True strictly when ep_cand outranks pack_cand by language tier first, then score.
    Ties return False so Season Packs win on equal quality."""
    ep_primary = not bool(ep_cand.get("is_fallback", False))
    pack_primary = not bool(pack_cand.get("is_fallback", False))
    if ep_primary != pack_primary:
        return ep_primary
    return float(ep_cand.get("score") or 0.0) > float(pack_cand.get("score") or 0.0)


async def _verify_layer2_in_memory(
    session: AsyncSession,
    candidates: list[dict[str, Any]],
    item: MediaItem,
    media_type_str: str = "episode",
    is_season_pack: bool = False,
    blacklisted_guids: set[str] | None = None,
    blacklisted_titles: set[str] | None = None,
) -> dict[str, Any] | None:
    """Fetch NZB bytes and run Layer 2 fake detection in memory without uploading to the downloader."""
    for cand in candidates:
        guid = cand["guid"]
        title = cand["title"]
        prefetched = cand.get("_prefetched_nzb")
        if prefetched is not None:
            return cand
        try:
            nzb_bytes, filename = await newznab_client.fetch_nzb_bytes(
                guid,
                api_url=cand.get("indexer_url"),
                api_key=cand.get("indexer_key"),
                session=session,
            )
        except Exception as exc:
            logger.warning("Failed fetching NZB bytes for '%s': %s", title, exc)
            continue

        cand_is_sp = bool(
            cand.get("is_season_pack") or is_season_pack or (media_type_str == "show")
        )
        is_fake, fake_reason = is_nzb_content_fake(
            nzb_bytes,
            media_type=media_type_str,
            is_season_pack=cand_is_sp,
        )
        if is_fake:
            err_msg = f"NZB flagged as fake/executable: {fake_reason}"
            logger.warning("Rejected fake NZB '%s': %s", title, err_msg)
            bl = BlacklistedRelease(
                media_item_id=item.id,
                nzb_guid=guid,
                nzb_title=title,
                reason=err_msg,
            )
            session.add(bl)
            await session.commit()
            if blacklisted_guids is not None and guid:
                blacklisted_guids.add(guid)
            if blacklisted_titles is not None and title:
                blacklisted_titles.add(title)
            continue

        cand["_prefetched_nzb"] = (nzb_bytes, filename)
        cand["_nzb_password"] = extract_nzb_password(nzb_bytes)
        return cand
    return None


def _failure_reason_from_partitioned(partitioned: dict[str, Any] | None) -> str:
    """Return a descriptive failed status_detail string based on indexer & scorer results."""
    if not partitioned or not partitioned.get("had_raw_results"):
        return "failed: No matching releases found on indexers"
    if not partitioned.get("had_candidates"):
        return "failed: All indexer results rejected by quality/language filters"
    return "failed: downloader upload failed — all candidates exhausted"


async def _notify_terminal_outage(
    session: AsyncSession,
    item: MediaItem,
    season: Season | None,
    episode: Episode | None,
    reason: str,
) -> None:
    """Terminal outage failure alerts through the regular failure event (notify_on_failure)."""
    from app.services.notifications import (
        dispatch_notification_event,
        format_target_label,
    )

    try:
        await dispatch_notification_event(
            session,
            event_type="failure",
            media_title=item.title,
            media_year=item.year,
            target_label=format_target_label(item, season, episode),
            status_reason=reason.removeprefix("failed: "),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Terminal outage alert failed: %s", exc)


def _is_queueable_error(msg: str | None) -> bool:
    """429 rate limits and outage markers both queue the target (ADR-159)."""
    return is_rate_limit_error(msg) or _marker_of(msg) is not None


def _marker_of(msg: str | None) -> OutageMarker | None:
    """Parse an outage marker, tolerating ``failed: `` prefixes added by callers."""
    text = msg or ""
    while text.startswith("failed: "):
        text = text[len("failed: ") :]
    return parse_outage_marker(text)


async def _record_auto_push_failure(
    session: AsyncSession,
    *,
    item: MediaItem,
    season: Season | None = None,
    episode: Episode | None = None,
    reason: str,
    push_mode: str = "auto",
    candidate: dict[str, Any] | None = None,
    rate_limited: bool | None = None,
) -> DownloadHistory:
    """Persist a visible failed DownloadHistory row for an explicit push target that dispatched nothing.

    If ``reason`` describes a downloader rate limit, the target is instead queued as a
    ``RATE_LIMITED`` (paused) push that resumes automatically after the cooldown (ADR-094).
    """
    while reason.startswith("failed: failed: "):
        reason = reason[len("failed: ") :]
    marker = _marker_of(reason)
    attempts = 0
    terminal_outage = False
    if marker is not None:
        rate_limited = True
    elif rate_limited is None:
        rate_limited = is_rate_limit_error(reason)
    target_season_id = (
        season.id if season is not None else (episode.season_id if episode else None)
    )
    target_episode_id = episode.id if episode is not None else None

    prior_stmt = select(DownloadHistory).where(
        DownloadHistory.media_item_id == item.id,
        DownloadHistory.season_id.is_(None)
        if target_season_id is None
        else DownloadHistory.season_id == target_season_id,
        DownloadHistory.episode_id.is_(None)
        if target_episode_id is None
        else DownloadHistory.episode_id == target_episode_id,
        DownloadHistory.downloader_transfer_id.is_(None),
        DownloadHistory.is_dismissed.is_(False),
    )
    prior_rows = (await session.execute(prior_stmt)).scalars().all()
    prior_attempts = max((r.outage_attempts or 0 for r in prior_rows), default=0)
    for prior_row in prior_rows:
        prior_row.is_dismissed = True
        prior_row.notification_sent = True
    if marker is not None:
        attempts = prior_attempts + 1
        if attempts > MAX_OUTAGE_RETRIES:
            terminal_outage = True
            rate_limited = False
            reason = terminal_detail(marker.kind)

    if episode is not None and season is not None:
        eff_s = int(season.type_number or season.season_number)
        label = f"{item.title} S{eff_s:02d}E{int(episode.episode_number):02d}"
    elif season is not None:
        if getattr(season, "entry_type", "season") == "movie":
            label = (
                season.title or f"{item.title} — Movie {int(season.type_number or 1)}"
            )
        else:
            eff_s = int(season.type_number or season.season_number)
            label = f"{item.title} — Season {eff_s}"
    else:
        label = f"{item.title} ({item.year})" if item.year else item.title

    cand_parsed = candidate.get("parsed") if candidate else None
    cand_title = str(candidate.get("title") or "").strip() if candidate else ""
    cand_guid = (
        (str(candidate.get("guid") or "").strip() or None) if candidate else None
    )

    history = DownloadHistory(
        media_item_id=item.id,
        season_id=target_season_id,
        episode_id=target_episode_id,
        nzb_guid=cand_guid if rate_limited else None,
        nzb_title=(
            cand_title
            if (rate_limited and cand_title)
            else (
                f"{label} — Paused ({resume_time_label()})"
                if rate_limited
                else f"{label} — No matching release dispatched"
            )
        ),
        downloader_transfer_id=None,
        downloader_hash=None,
        score=float(candidate.get("score") or 0.0)
        if (rate_limited and candidate)
        else 0.0,
        size_bytes=int(candidate.get("size_bytes") or 0)
        if (rate_limited and candidate and candidate.get("size_bytes"))
        else None,
        resolution=(cand_parsed.resolution if cand_parsed else None)
        if rate_limited
        else None,
        video_codec=(cand_parsed.video_codec if cand_parsed else None)
        if rate_limited
        else None,
        audio_codec=(cand_parsed.audio_codec if cand_parsed else None)
        if rate_limited
        else None,
        source=(cand_parsed.source if cand_parsed else None) if rate_limited else None,
        release_group=(cand_parsed.release_group if cand_parsed else None)
        if rate_limited
        else None,
        is_fallback=bool(candidate.get("is_fallback", False))
        if (rate_limited and candidate)
        else False,
        grabbed_language=candidate.get("matched_language")
        if (rate_limited and candidate)
        else None,
        status_detail=(
            paused_detail(marker.kind, marker.label)
            if marker is not None and rate_limited
            else RATE_LIMITED_DETAIL
            if rate_limited
            else reason
        ),
        outage_attempts=attempts,
        push_mode=push_mode,
        is_dismissed=False,
        notification_sent=True,
        progress_pct=0.0,
        download_speed_bytes=0,
        eta_seconds=None,
        failed_at=None if rate_limited else datetime.now(timezone.utc),
    )
    history.media_item = item
    if season is not None:
        history.season = season
    if episode is not None:
        history.episode = episode
    populate_history_snapshot(history, item, season=season, episode=episode)
    session.add(history)

    if rate_limited:
        if episode is not None:
            episode.status = EpisodeStatus.RATE_LIMITED
            if season is not None:
                season.status = SeasonStatus.RATE_LIMITED
        elif season is not None:
            season.status = SeasonStatus.RATE_LIMITED
        item.status = MediaStatus.RATE_LIMITED
    elif episode is not None:
        episode.status = EpisodeStatus.FAILED
        episode.last_error = reason
        if season is not None:
            season.status = SeasonStatus.FAILED
        item.status = MediaStatus.FAILED
    elif season is not None:
        season.status = SeasonStatus.FAILED
        season.last_error = reason
        item.status = MediaStatus.FAILED
    else:
        item.status = MediaStatus.FAILED
        item.last_error = reason

    if "authentication error" in reason:
        log_failure(
            session,
            episode or season or item,
            "auth_error",
            reason.removeprefix("failed: "),
        )
    elif terminal_outage:
        log_failure(session, episode or season or item, "outage", reason)
        await _notify_terminal_outage(session, item, season, episode, reason)

    await session.flush()
    from app.core.transfer_poller import _recalculate_parent_status

    await _recalculate_parent_status(session, item)
    if rate_limited and marker is not None:
        record_outage_from_marker(marker, attempts)
    if rate_limited:
        from app.services.indexers import newznab as newznab_client

        dl_name, dl_remaining = longest_downloader_cooldown()
        tb_cooldown = int(dl_remaining)
        idx_cooldown = int(newznab_client.get_all_indexer_cooldowns_remaining())
        source_name = "Indexer" if "indexer" in reason.lower() else dl_name
        cooldown_sec = idx_cooldown if "indexer" in reason.lower() else tb_cooldown
        t_label = (
            f"S{int(season.type_number or season.season_number):02d}E{int(episode.episode_number):02d}"
            if episode is not None and season is not None
            else (
                f"Season {int(season.type_number or season.season_number)} Pack"
                if season is not None
                else "Movie"
            )
        )
        logger.debug(
            "[QUEUE] Enqueued push for '%s' target '%s' -> HTTP 429 from %s (Cooldown: %ds).",
            item.title,
            t_label,
            source_name,
            cooldown_sec,
        )
        logger.warning(
            "⏸️ Queued rate-limited push for '%s' (%s).", item.title, resume_time_label()
        )
        rate_limit_queue.wake()
        (outage_episode if marker is not None else rate_limit_episode).on_enqueue()
    return history


async def _push_single_season_or_movie_entry(
    session: AsyncSession,
    item: MediaItem,
    season: Season,
    effective_cfg: dict[str, Any],
    blacklisted_guids: set[str],
    blacklisted_titles: set[str],
    is_auto_advance: bool = False,
    reset_fail_count: bool = True,
    _candidates_found_acc: list[bool] | None = None,
) -> list[DownloadHistory] | dict[str, Any]:
    """Search, score, and push a single chronological Season or Franchise Movie entry.

    Args:
        _candidates_found_acc: Optional mutable list; if provided, ``True`` is appended
            whenever at least one scored valid candidate is found, allowing the caller to
            distinguish "nothing on indexers" from "releases found but the downloader skipped all".
    """
    from sqlalchemy import inspect as sa_inspect

    ev_type = "auto_advance" if is_auto_advance else "push_initiated"
    entry_type = getattr(season, "entry_type", "season") or "season"
    item_media_type = (
        item.media_type.value
        if hasattr(item.media_type, "value")
        else str(item.media_type or "")
    )
    allow_season_packs, prefer_season_packs = normalize_season_pack_flags(
        effective_cfg.get("allow_season_packs", False),
        effective_cfg.get("prefer_season_packs", False),
    )
    dispatched: list[DownloadHistory] = []

    cooldown_rem = get_downloader_cooldown_remaining()
    if cooldown_rem > 0 and not settings.dry_run:
        err_reason = f"failed: {downloader_rate_limit_detail()}"
        logger.warning(
            "Skipping season/movie push for '%s' (%s) — %s",
            item.title,
            season.title or f"Season {season.season_number}",
            err_reason,
        )
        if reset_fail_count:
            await _record_auto_push_failure(
                session,
                item=item,
                season=season,
                episode=None,
                reason=err_reason,
            )
            await session.commit()
        return dispatched

    if entry_type == "movie":
        raw_results = await _query_movie_across_indexers(
            session,
            item,
            title_override=season.title or item.title,
            use_external_ids=False,
        )
        partitioned = await _score_and_partition_candidates(
            raw_results=raw_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=season.title or item.title,
            expected_year=season.air_date.year if season.air_date else None,
            expected_alt_title=item.title,
            expected_season=None,
            expected_episode=None,
            expected_season_title=season.title,
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type="movie",
            session=session,
            item_id=item.id,
        )
        if partitioned["had_candidates"] and _candidates_found_acc is not None:
            _candidates_found_acc.append(True)
        tb_errors: list[str] = []
        hist = await _dispatch_candidate_list_to_downloader(
            session,
            partitioned["all_valid"],
            item=item,
            season=season,
            episode=None,
            push_mode="auto",
            event_type=ev_type,
            reset_fail_count=reset_fail_count,
            error_out=tb_errors,
        )
        if isinstance(hist, DownloadHistory):
            dispatched.append(hist)
        elif (
            not settings.dry_run
            and reset_fail_count
            and (not is_auto_advance or tb_errors)
        ):
            err_msg = (
                tb_errors[-1]
                if tb_errors
                else _failure_reason_from_partitioned(partitioned)
            )
            is_rl = _is_queueable_error(err_msg)
            await _record_auto_push_failure(
                session,
                item=item,
                season=season,
                episode=None,
                reason=f"failed: {err_msg}" if tb_errors else err_msg,
                rate_limited=is_rl,
            )
            await session.commit()
        return dispatched

    # TV / Anime Season entry (`entry_type == "season"`)
    has_movie_entries = any(
        getattr(s, "entry_type", "season") == "movie" for s in (item.seasons or [])
    )
    if has_movie_entries or (season.type_number and season.type_number > 1):
        effective_s_num = int(season.type_number or season.season_number)
    else:
        effective_s_num = int(season.season_number)

    if "episodes" in sa_inspect(season).unloaded:
        await session.refresh(season, ["episodes"])

    expected_season_title = (
        season.title if season.title and season.title != item.title else None
    )
    sorted_eps = sorted(season.episodes, key=lambda e: e.episode_number)
    ep_count = season.episode_count or len(sorted_eps)
    has_acquired_episodes = any(
        ep.status
        in (
            EpisodeStatus.DOWNLOADED,
            EpisodeStatus.COMPLETED,
            EpisodeStatus.DOWNLOADING,
        )
        for ep in sorted_eps
    )
    missing_released_eps = [
        ep
        for ep in sorted_eps
        if ep.status
        not in (
            EpisodeStatus.DOWNLOADED,
            EpisodeStatus.COMPLETED,
            EpisodeStatus.DOWNLOADING,
            EpisodeStatus.FUTURE,
        )
    ]

    # When a user explicitly selects an already-COMPLETED/DOWNLOADED season in the Push Modal,
    # re-search and re-push all released episodes (or a Season Pack) of that season.
    if not is_auto_advance and reset_fail_count and not missing_released_eps:
        missing_released_eps = [
            ep for ep in sorted_eps if ep.status != EpisodeStatus.FUTURE
        ]
        has_acquired_episodes = False

    pack_partitioned: dict[str, Any] | None = None

    # State 3: Prefer Season Pack (allow_season_packs=True, prefer_season_packs=True)
    if allow_season_packs and prefer_season_packs:
        raw_pack_results = await _query_show_across_indexers(
            session, item, season_number=effective_s_num, episode_number=None
        )
        pack_partitioned = await _score_and_partition_candidates(
            raw_results=raw_pack_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=item.title,
            expected_year=None,
            expected_alt_title=item.alt_title,
            expected_season=effective_s_num,
            expected_episode=None,
            expected_season_title=expected_season_title,
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type=item_media_type,
            season_episode_count=ep_count,
            session=session,
            item_id=item.id,
        )
        if pack_partitioned["had_candidates"] and _candidates_found_acc is not None:
            _candidates_found_acc.append(True)
        pack_tb_errors: list[str] = []
        hist = await _dispatch_candidate_list_to_downloader(
            session,
            pack_partitioned["all_valid"],
            item=item,
            season=season,
            episode=None,
            push_mode="auto",
            event_type=ev_type,
            reset_fail_count=reset_fail_count,
            error_out=pack_tb_errors,
        )
        if isinstance(hist, DownloadHistory):
            dispatched.append(hist)
            return dispatched
        if pack_tb_errors and not settings.dry_run:
            if reset_fail_count:
                err_msg = pack_tb_errors[-1]
                is_rl = _is_queueable_error(err_msg)
                await _record_auto_push_failure(
                    session,
                    item=item,
                    season=season,
                    episode=None,
                    reason=f"failed: {err_msg}",
                    rate_limited=is_rl,
                )
                if is_indexer_rate_limit_error(err_msg):
                    for rem_ep in missing_released_eps:
                        if rem_ep.status not in (
                            EpisodeStatus.DOWNLOADING,
                            EpisodeStatus.DOWNLOADED,
                            EpisodeStatus.COMPLETED,
                        ):
                            rem_ep.status = EpisodeStatus.RATE_LIMITED
                await session.commit()
            return dispatched

    # State 2: Score Decides (allow_season_packs=True, prefer_season_packs=False)
    elif allow_season_packs and not prefer_season_packs and not has_acquired_episodes:
        raw_pack_results = await _query_show_across_indexers(
            session, item, season_number=effective_s_num, episode_number=None
        )
        pack_partitioned = await _score_and_partition_candidates(
            raw_results=raw_pack_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=item.title,
            expected_year=None,
            expected_alt_title=item.alt_title,
            expected_season=effective_s_num,
            expected_episode=None,
            expected_season_title=expected_season_title,
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type=item_media_type,
            season_episode_count=ep_count,
            session=session,
            item_id=item.id,
        )
        pack_candidates = pack_partitioned["all_valid"]
        if pack_partitioned["had_candidates"] and _candidates_found_acc is not None:
            _candidates_found_acc.append(True)

        verified_pack = (
            await _verify_layer2_in_memory(
                session,
                pack_candidates,
                item=item,
                media_type_str="episode",
                is_season_pack=True,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
            )
            if pack_candidates
            else None
        )

        if verified_pack is not None and not missing_released_eps:
            pack_tb_errors = []
            hist = await _dispatch_candidate_list_to_downloader(
                session,
                [verified_pack],
                item=item,
                season=season,
                episode=None,
                push_mode="auto",
                event_type=ev_type,
                reset_fail_count=reset_fail_count,
                error_out=pack_tb_errors,
            )
            if isinstance(hist, DownloadHistory):
                dispatched.append(hist)
                return dispatched
            if pack_tb_errors and not settings.dry_run:
                if reset_fail_count:
                    err_msg = pack_tb_errors[-1]
                    is_rl = _is_queueable_error(err_msg)
                    await _record_auto_push_failure(
                        session,
                        item=item,
                        season=season,
                        episode=None,
                        reason=f"failed: {err_msg}",
                        rate_limited=is_rl,
                    )
                    if is_indexer_rate_limit_error(err_msg):
                        for rem_ep in missing_released_eps:
                            if rem_ep.status not in (
                                EpisodeStatus.DOWNLOADING,
                                EpisodeStatus.DOWNLOADED,
                                EpisodeStatus.COMPLETED,
                            ):
                                rem_ep.status = EpisodeStatus.RATE_LIMITED
                    await session.commit()
                return dispatched

        if verified_pack is not None and missing_released_eps:
            first_ep = missing_released_eps[0]
            raw_e01_results = await _query_show_across_indexers(
                session,
                item,
                season_number=effective_s_num,
                episode_number=first_ep.episode_number,
            )
            e01_partitioned = await _score_and_partition_candidates(
                raw_results=raw_e01_results,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
                expected_title=item.title,
                expected_year=None,
                expected_alt_title=item.alt_title,
                expected_season=effective_s_num,
                expected_episode=first_ep.episode_number,
                expected_season_title=expected_season_title,
                runtime_minutes=item.runtime_minutes,
                effective_cfg=effective_cfg,
                media_type=item_media_type,
                season_episode_count=ep_count,
                session=session,
                item_id=item.id,
            )
            e01_candidates = e01_partitioned["all_valid"]
            beating_e01_cands = [
                c for c in e01_candidates if _candidate_beats_pack(c, verified_pack)
            ]
            verified_e01 = (
                await _verify_layer2_in_memory(
                    session,
                    beating_e01_cands,
                    item=item,
                    media_type_str="episode",
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                )
                if beating_e01_cands
                else None
            )

            # Case 2a: Pack wins or ties on E01 probe -> dispatch verified Pack immediately without querying E02..En
            if verified_e01 is None:
                pack_tb_errors = []
                hist = await _dispatch_candidate_list_to_downloader(
                    session,
                    [verified_pack],
                    item=item,
                    season=season,
                    episode=None,
                    push_mode="auto",
                    event_type=ev_type,
                    reset_fail_count=reset_fail_count,
                    error_out=pack_tb_errors,
                )
                if isinstance(hist, DownloadHistory):
                    dispatched.append(hist)
                    return dispatched
                if pack_tb_errors and not settings.dry_run:
                    if reset_fail_count:
                        err_msg = pack_tb_errors[-1]
                        is_rl = _is_queueable_error(err_msg)
                        await _record_auto_push_failure(
                            session,
                            item=item,
                            season=season,
                            episode=None,
                            reason=f"failed: {err_msg}",
                            rate_limited=is_rl,
                        )
                        if is_indexer_rate_limit_error(err_msg):
                            for rem_ep in missing_released_eps:
                                if rem_ep.status not in (
                                    EpisodeStatus.DOWNLOADING,
                                    EpisodeStatus.DOWNLOADED,
                                    EpisodeStatus.COMPLETED,
                                ):
                                    rem_ep.status = EpisodeStatus.RATE_LIMITED
                        await session.commit()
                    return dispatched

            # Case 2b: E01 beats Pack -> Pre-flight resolve and Layer-2 verify all remaining episodes in memory before any downloader dispatch
            verified_ep_candidates: list[tuple[Episode, dict[str, Any]]] = [
                (first_ep, verified_e01)  # type: ignore[list-item]
            ]
            abort_to_pack = False

            for ep in missing_released_eps[1:]:
                raw_ep_results = await _query_show_across_indexers(
                    session,
                    item,
                    season_number=effective_s_num,
                    episode_number=ep.episode_number,
                )
                ep_part = await _score_and_partition_candidates(
                    raw_results=raw_ep_results,
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                    expected_title=item.title,
                    expected_year=None,
                    expected_alt_title=item.alt_title,
                    expected_season=effective_s_num,
                    expected_episode=ep.episode_number,
                    expected_season_title=expected_season_title,
                    runtime_minutes=item.runtime_minutes,
                    effective_cfg=effective_cfg,
                    media_type=item_media_type,
                    season_episode_count=ep_count,
                    session=session,
                    item_id=item.id,
                )
                cands = ep_part["all_valid"]
                if not cands:
                    abort_to_pack = True
                    break
                verified_cand = await _verify_layer2_in_memory(
                    session,
                    cands,
                    item=item,
                    media_type_str="episode",
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                )
                if verified_cand is None:
                    abort_to_pack = True
                    break
                verified_ep_candidates.append((ep, verified_cand))

            if not abort_to_pack:
                pack_is_primary = not bool(verified_pack.get("is_fallback", False))
                any_ep_fallback = any(
                    bool(c.get("is_fallback", False)) for _, c in verified_ep_candidates
                )
                avg_ep_score = sum(
                    float(c.get("score") or 0.0) for _, c in verified_ep_candidates
                ) / len(verified_ep_candidates)
                pack_score = float(verified_pack.get("score") or 0.0)
                if (pack_is_primary and any_ep_fallback) or (
                    avg_ep_score <= pack_score
                ):
                    abort_to_pack = True

            if abort_to_pack:
                pack_tb_errors = []
                hist = await _dispatch_candidate_list_to_downloader(
                    session,
                    [verified_pack],
                    item=item,
                    season=season,
                    episode=None,
                    push_mode="auto",
                    event_type=ev_type,
                    reset_fail_count=reset_fail_count,
                    error_out=pack_tb_errors,
                )
                if isinstance(hist, DownloadHistory):
                    dispatched.append(hist)
                    return dispatched
                if pack_tb_errors and not settings.dry_run:
                    if reset_fail_count:
                        err_msg = pack_tb_errors[-1]
                        is_rl = _is_queueable_error(err_msg)
                        await _record_auto_push_failure(
                            session,
                            item=item,
                            season=season,
                            episode=None,
                            reason=f"failed: {err_msg}",
                            rate_limited=is_rl,
                        )
                        if is_indexer_rate_limit_error(err_msg):
                            for rem_ep in missing_released_eps:
                                if rem_ep.status not in (
                                    EpisodeStatus.DOWNLOADING,
                                    EpisodeStatus.DOWNLOADED,
                                    EpisodeStatus.COMPLETED,
                                ):
                                    rem_ep.status = EpisodeStatus.RATE_LIMITED
                        await session.commit()
                    return dispatched
            else:
                for ep, verified_cand in verified_ep_candidates:
                    ep_tb_errors: list[str] = []
                    ep_hist = await _dispatch_candidate_list_to_downloader(
                        session,
                        [verified_cand],
                        item=item,
                        season=season,
                        episode=ep,
                        push_mode="auto",
                        event_type=ev_type,
                        reset_fail_count=reset_fail_count,
                        error_out=ep_tb_errors,
                    )
                    if isinstance(ep_hist, DownloadHistory):
                        dispatched.append(ep_hist)
                        await session.commit()
                    elif ep_tb_errors and not settings.dry_run:
                        if reset_fail_count:
                            err_msg = ep_tb_errors[-1]
                            is_rl = _is_queueable_error(err_msg)
                            await _record_auto_push_failure(
                                session,
                                item=item,
                                season=season,
                                episode=None if is_rl else ep,
                                reason=f"failed: {err_msg}",
                                rate_limited=is_rl,
                            )
                            if is_indexer_rate_limit_error(err_msg):
                                for rem_ep in missing_released_eps:
                                    if rem_ep.status not in (
                                        EpisodeStatus.DOWNLOADING,
                                        EpisodeStatus.DOWNLOADED,
                                        EpisodeStatus.COMPLETED,
                                    ):
                                        rem_ep.status = EpisodeStatus.RATE_LIMITED
                            await session.commit()
                        break
                    elif (
                        not settings.dry_run
                        and not is_auto_advance
                        and reset_fail_count
                    ):
                        await _record_auto_push_failure(
                            session,
                            item=item,
                            season=season,
                            episode=ep,
                            reason="failed: downloader upload failed — all candidates exhausted",
                        )
                        await session.commit()
                return dispatched

    # State 1 (allow_season_packs=False) or Pack-to-Episode Fallback (when no valid Season Pack exists)
    for ep in missing_released_eps:
        cooldown_rem = get_downloader_cooldown_remaining()
        if cooldown_rem > 0 and not settings.dry_run:
            if reset_fail_count:
                # Queue the whole season: on resume only the still-missing episodes are pushed.
                await _record_auto_push_failure(
                    session,
                    item=item,
                    season=season,
                    episode=None,
                    reason=f"failed: {downloader_rate_limit_detail()}",
                    rate_limited=True,
                )
                await session.commit()
            break
        raw_ep_results = await _query_show_across_indexers(
            session,
            item,
            season_number=effective_s_num,
            episode_number=ep.episode_number,
        )
        ep_partitioned = await _score_and_partition_candidates(
            raw_results=raw_ep_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=item.title,
            expected_year=None,
            expected_alt_title=item.alt_title,
            expected_season=effective_s_num,
            expected_episode=ep.episode_number,
            expected_season_title=expected_season_title,
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type=item_media_type,
            season_episode_count=ep_count,
            session=session,
            item_id=item.id,
        )
        if ep_partitioned["had_candidates"] and _candidates_found_acc is not None:
            _candidates_found_acc.append(True)
        ep_tb_errors = []
        ep_hist = await _dispatch_candidate_list_to_downloader(
            session,
            ep_partitioned["all_valid"],
            item=item,
            season=season,
            episode=ep,
            push_mode="auto",
            event_type=ev_type,
            reset_fail_count=reset_fail_count,
            error_out=ep_tb_errors,
        )
        if isinstance(ep_hist, DownloadHistory):
            dispatched.append(ep_hist)
            await session.commit()
        elif ep_tb_errors and not settings.dry_run:
            if reset_fail_count:
                err_msg = ep_tb_errors[-1]
                is_rl = _is_queueable_error(err_msg)
                await _record_auto_push_failure(
                    session,
                    item=item,
                    season=season,
                    episode=None if is_rl else ep,
                    reason=f"failed: {err_msg}",
                    rate_limited=is_rl,
                )
                if is_indexer_rate_limit_error(err_msg):
                    for rem_ep in missing_released_eps:
                        if rem_ep.status not in (
                            EpisodeStatus.DOWNLOADING,
                            EpisodeStatus.DOWNLOADED,
                            EpisodeStatus.COMPLETED,
                        ):
                            rem_ep.status = EpisodeStatus.RATE_LIMITED
                await session.commit()
            break
        elif not settings.dry_run and not is_auto_advance and reset_fail_count:
            await _record_auto_push_failure(
                session,
                item=item,
                season=season,
                episode=ep,
                reason=_failure_reason_from_partitioned(ep_partitioned),
            )
            await session.commit()

    if (
        not settings.dry_run
        and not missing_released_eps
        and not dispatched
        and not is_auto_advance
        and reset_fail_count
    ):
        await _record_auto_push_failure(
            session,
            item=item,
            season=season,
            episode=None,
            reason=_failure_reason_from_partitioned(pack_partitioned),
        )
        await session.commit()

    return dispatched


async def advance_season_buffer(
    session: AsyncSession,
    item: MediaItem,
    base_watch_order: int,
    effective_cfg: dict[str, Any] | None = None,
    excluded_season_ids: set[int] | None = None,
) -> list[DownloadHistory]:
    """Advance the 1-season-ahead buffer from chronological entry `base_watch_order`.

    - Pushes the immediate next chronological entry (`N+1`) if released and not already
      `DOWNLOADING`, `DOWNLOADED`, or `COMPLETED`.
    - Movie Bridge Lookahead: Whenever the target auto-advance entry has `entry_type == 'movie'`,
      pushes both that Movie entry AND the following chronological entry (`N+2`, continuing
      across any consecutive franchise movies) so the next season is ready on the downloader.
    """
    from sqlalchemy import inspect as sa_inspect

    if "seasons" in sa_inspect(item).unloaded or not item.seasons:
        await session.refresh(item, ["seasons"])
    if not item.seasons:
        return []

    if effective_cfg is None:
        effective_cfg = await resolve_effective_search_config(session, item)

    blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
        session, item.id
    )
    excluded = set(excluded_season_ids or set())

    sorted_seasons = sorted(
        item.seasons, key=lambda s: (s.watch_order, s.season_number)
    )
    remaining = [s for s in sorted_seasons if s.watch_order > base_watch_order]
    if not remaining:
        return []

    dispatched: list[DownloadHistory] = []
    idx = 0
    while idx < len(remaining):
        candidate_entry = remaining[idx]
        is_eligible = (
            candidate_entry.id not in excluded
            and candidate_entry.status
            not in (
                SeasonStatus.FUTURE,
                SeasonStatus.DOWNLOADED,
                SeasonStatus.COMPLETED,
                SeasonStatus.DOWNLOADING,
            )
            and not candidate_entry.is_tba
        )
        cooldown_rem = get_downloader_cooldown_remaining()
        if cooldown_rem > 0:
            if is_eligible:
                await _record_auto_push_failure(
                    session,
                    item=item,
                    season=candidate_entry,
                    episode=None,
                    reason=f"failed: {downloader_rate_limit_detail()}",
                )
                await session.commit()
                excluded.add(candidate_entry.id)
            else:
                break
        elif is_eligible:
            if candidate_entry.status == SeasonStatus.IGNORED:
                from app.services.watchlist_ingest import set_season_searchable

                await set_season_searchable(session, candidate_entry)
                await session.flush()
            hists = await _push_single_season_or_movie_entry(
                session=session,
                item=item,
                season=candidate_entry,
                effective_cfg=effective_cfg,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
                is_auto_advance=True,
            )
            if isinstance(hists, list):
                dispatched.extend(hists)
            excluded.add(candidate_entry.id)

        entry_type = getattr(candidate_entry, "entry_type", "season") or "season"
        if (
            entry_type == "movie"
            and candidate_entry.status != SeasonStatus.FUTURE
            and not candidate_entry.is_tba
        ):
            idx += 1
            continue
        break

    if dispatched:
        from app.services.discord import dispatch_batch_notification_event

        await session.commit()
        await dispatch_batch_notification_event(
            session,
            event_type="auto_advance",
            histories=dispatched,
            item=item,
        )

    return dispatched


def extract_watched_order(
    item: MediaItem,
    item_data: dict[str, Any],
    matched_season: Season | None = None,
) -> int:
    """Extract the highest chronological `watch_order` K where the watchlist provider reports user watch progress."""
    seasons_list = sorted(
        item.seasons or [], key=lambda s: (s.watch_order, s.season_number)
    )

    def _order_for_season_num(s_num: int, has_episodes_hint: bool = False) -> int:
        if not seasons_list:
            return s_num
        if has_episodes_hint:
            for s in seasons_list:
                if (
                    getattr(s, "entry_type", "season") == "season"
                    and s.type_number == s_num
                ):
                    return int(s.watch_order)
        for s in seasons_list:
            if s.season_number == s_num:
                return int(s.watch_order)
        for s in seasons_list:
            if s.type_number == s_num:
                return int(s.watch_order)
        for s in seasons_list:
            if s.watch_order == s_num:
                return int(s.watch_order)
        return s_num

    detected_orders: list[int] = []

    # 1. Explicit watch_order / last_watched_order
    for explicit_key in ("watch_order", "last_watched_order"):
        val = item_data.get(explicit_key)
        if isinstance(val, int) and val > 0:
            detected_orders.append(val)

    # 2. Explicit last_watched_season / watched_season
    for season_key in ("last_watched_season", "watched_season"):
        val = item_data.get(season_key)
        if isinstance(val, int) and val > 0:
            detected_orders.append(_order_for_season_num(val, has_episodes_hint=True))

    # 3. `seasons` array in watchlist payload (e.g. [{"number": 2, "episodes": [{"number": 1}]}])
    raw_seasons = item_data.get("seasons")
    if isinstance(raw_seasons, list):
        for s_dict in raw_seasons:
            if not isinstance(s_dict, dict):
                continue
            if "watch_order" in s_dict and isinstance(s_dict["watch_order"], int):
                ep_val = s_dict.get("episodes")
                w_cnt = s_dict.get("watched_episodes") or s_dict.get(
                    "watched_episodes_count"
                )
                is_w = (
                    bool(ep_val)
                    if "episodes" in s_dict
                    else (
                        int(w_cnt or 0) > 0
                        if w_cnt is not None
                        else bool(s_dict.get("watched", True))
                    )
                )
                if is_w and s_dict["watch_order"] > 0:
                    detected_orders.append(int(s_dict["watch_order"]))
                continue

            s_num_raw = (
                s_dict.get("number")
                if s_dict.get("number") is not None
                else (
                    s_dict.get("season")
                    if s_dict.get("season") is not None
                    else s_dict.get("season_number")
                )
            )
            if s_num_raw is None:
                continue
            try:
                s_num = int(s_num_raw)
            except (TypeError, ValueError):
                continue
            if s_num <= 0:
                continue

            if "episodes" in s_dict:
                is_watched = bool(s_dict["episodes"])
            elif "watched_episodes" in s_dict or "watched_episodes_count" in s_dict:
                is_watched = (
                    int(
                        s_dict.get("watched_episodes")
                        or s_dict.get("watched_episodes_count")
                        or 0
                    )
                    > 0
                )
            elif "watched" in s_dict:
                is_watched = bool(s_dict["watched"])
            else:
                is_watched = True

            if is_watched:
                detected_orders.append(
                    _order_for_season_num(
                        s_num,
                        has_episodes_hint=(
                            "episodes" in s_dict or "watched_episodes" in s_dict
                        ),
                    )
                )

    # 4. `last_watched` string (e.g. "S02E01")
    last_watched_str = item_data.get("last_watched")
    if isinstance(last_watched_str, str) and last_watched_str.strip():
        m_se = re.search(r"(?i)S(\d+)\s*E(\d+)", last_watched_str)
        if m_se:
            detected_orders.append(
                _order_for_season_num(int(m_se.group(1)), has_episodes_hint=True)
            )
        elif re.search(r"(?i)EP?\s*(\d+)", last_watched_str):
            if matched_season is not None:
                detected_orders.append(int(matched_season.watch_order))
            elif seasons_list:
                detected_orders.append(int(seasons_list[0].watch_order))

    # 5. `watched_episodes` (list or int) / `watched_episodes_count` (int)
    watched_eps = item_data.get("watched_episodes")
    if isinstance(watched_eps, list) and len(watched_eps) > 0:
        for ep_entry in watched_eps:
            if isinstance(ep_entry, dict):
                ep_s = ep_entry.get("season") or ep_entry.get("season_number")
                if ep_s is not None:
                    try:
                        detected_orders.append(
                            _order_for_season_num(int(ep_s), has_episodes_hint=True)
                        )
                    except (TypeError, ValueError):
                        pass
                elif matched_season is not None:
                    detected_orders.append(int(matched_season.watch_order))
                elif seasons_list:
                    detected_orders.append(int(seasons_list[0].watch_order))
            elif isinstance(ep_entry, str):
                m_ep = re.search(r"(?i)S(\d+)\s*E(\d+)", ep_entry)
                if m_ep:
                    detected_orders.append(
                        _order_for_season_num(
                            int(m_ep.group(1)), has_episodes_hint=True
                        )
                    )
            elif isinstance(ep_entry, int):
                if matched_season is not None:
                    detected_orders.append(int(matched_season.watch_order))
                elif seasons_list:
                    detected_orders.append(int(seasons_list[0].watch_order))

    ep_count_val = (
        watched_eps
        if isinstance(watched_eps, int)
        else item_data.get("watched_episodes_count")
    )
    if isinstance(ep_count_val, int) and ep_count_val > 0:
        if matched_season is not None:
            detected_orders.append(int(matched_season.watch_order))
        elif not detected_orders:
            if seasons_list:
                detected_orders.append(int(seasons_list[0].watch_order))
            else:
                detected_orders.append(1)

    # 6. Completed status on a consolidated season/movie entry
    status_str = str(item_data.get("status") or "").lower()
    if matched_season is not None and (
        status_str == "completed" or item_data.get("watched") is True
    ):
        detected_orders.append(int(matched_season.watch_order))

    return max(detected_orders) if detected_orders else 0


def effective_watch_order(item: MediaItem) -> int:
    """Furthest watch order across the item and all its watchlist sources (furthest wins)."""
    from sqlalchemy import inspect as sa_inspect

    sources = (
        []
        if "watchlist_sources" in sa_inspect(item).unloaded
        else item.watchlist_sources or []
    )
    return max(
        [int(item.last_watched_order or 0)]
        + [int(s.last_watched_order or 0) for s in sources]
    )


async def evaluate_watch_progress_and_advance(
    session: AsyncSession,
    item: MediaItem,
    item_data: dict[str, Any],
    matched_season: Season | None = None,
) -> list[DownloadHistory]:
    """Inspect watchlist item payload for user watch progress, update `last_watched_order`,
    and trigger Auto-Advance season expansion (with Movie Bridge Lookahead) if enabled."""
    from sqlalchemy import inspect as sa_inspect

    status_str = str(item_data.get("status") or "").lower()
    if status_str == "completed" or item_data.get("watched_completed") is True:
        if matched_season is None:
            item.watched_completed = True

    if hasattr(session, "refresh"):
        # Pending source rows must be flushed first, or the loaded collection is stale.
        await session.flush()
        stale = ["watchlist_sources"]
        if "seasons" in sa_inspect(item).unloaded:
            stale.append("seasons")
        await session.refresh(item, stale)

    detected_order = max(
        extract_watched_order(
            item=item, item_data=item_data, matched_season=matched_season
        ),
        effective_watch_order(item),
    )
    current_order = int(item.last_watched_order or 0)
    if detected_order <= current_order:
        return []

    if has_active_outage():
        # Leave last_watched_order untouched so the next sync re-triggers the advance.
        logger.info("[AUTO-ADVANCE] Skipped for '%s': outage active", item.title)
        return []

    item.last_watched_order = detected_order
    await session.flush()

    if not getattr(item, "auto_advance_seasons", False):
        return []

    from app.core.automation import classify_v3_status_tier

    if classify_v3_status_tier(item) == "on_hold":
        return []

    return await advance_season_buffer(
        session=session,
        item=item,
        base_watch_order=detected_order,
    )


async def execute_auto_push(
    session: AsyncSession,
    item_id: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute Auto-Push Best (`push_mode = 'auto'`) for a MediaItem and its selected seasons/episodes."""
    from app.services.discord import dispatch_batch_notification_event

    payload = payload or {}
    stmt = (
        select(MediaItem)
        .where(MediaItem.id == item_id)
        .options(
            selectinload(MediaItem.provider),
            selectinload(MediaItem.seasons).selectinload(Season.episodes),
        )
    )
    item = (await session.execute(stmt)).scalars().first()
    if item is None:
        return {"pushed": False, "error": "MediaItem not found", "status_code": 404}

    # 1. Persist sticky search preferences on the MediaItem and commit immediately before network I/O
    await persist_sticky_preferences(session, item, payload)
    await session.commit()

    # 2. Resolve effective scoring config
    effective_cfg = await resolve_effective_search_config(session, item, payload)
    auto_advance_seasons = bool(effective_cfg["auto_advance_seasons"])

    blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
        session, item.id
    )

    season_ids = _parse_int_list(
        payload.get("season_ids")
        if "season_ids" in payload
        else payload.get("season_ids[]")
    )
    episode_ids = _parse_int_list(
        payload.get("episode_ids")
        if "episode_ids" in payload
        else payload.get("episode_ids[]")
    )

    dispatched_histories: list[DownloadHistory] = []
    pushed_seasons: list[Season] = []
    any_candidates_found: bool = (
        False  # True if any search returned ≥1 scored valid release
    )

    item_media_type = (
        item.media_type.value
        if hasattr(item.media_type, "value")
        else str(item.media_type or "")
    )

    # Case 1: Standalone Movie (or movie item with no seasons)
    if item.media_type == MediaType.MOVIE and not season_ids and not episode_ids:
        if not settings.dry_run:
            item.fail_count = 0
            await session.commit()
        cooldown_rem = get_downloader_cooldown_remaining()
        if cooldown_rem > 0 and not settings.dry_run:
            await _record_auto_push_failure(
                session,
                item=item,
                season=None,
                episode=None,
                reason=f"failed: {downloader_rate_limit_detail()}",
            )
            await session.commit()
        else:
            raw_movie_results = await _query_movie_across_indexers(session, item)
            movie_partitioned = await _score_and_partition_candidates(
                raw_results=raw_movie_results,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
                expected_title=item.title,
                expected_year=item.year,
                expected_alt_title=item.alt_title,
                expected_season=None,
                expected_episode=None,
                expected_season_title=None,
                runtime_minutes=item.runtime_minutes,
                effective_cfg=effective_cfg,
                media_type="movie",
                session=session,
                item_id=item.id,
            )
            if movie_partitioned["had_candidates"]:
                any_candidates_found = True
            movie_tb_errors: list[str] = []
            hist = await _dispatch_candidate_list_to_downloader(
                session,
                movie_partitioned["all_valid"],
                item=item,
                season=None,
                episode=None,
                push_mode="auto",
                event_type="push_initiated",
                reset_fail_count=True,
                error_out=movie_tb_errors,
            )
            if isinstance(hist, DownloadHistory):
                dispatched_histories.append(hist)
            elif not settings.dry_run:
                reason = (
                    f"failed: {movie_tb_errors[-1]}"
                    if movie_tb_errors
                    else _failure_reason_from_partitioned(movie_partitioned)
                )
                await _record_auto_push_failure(
                    session,
                    item=item,
                    season=None,
                    episode=None,
                    reason=reason,
                )
            await session.commit()
            if dispatched_histories:
                if settings.dry_run:
                    m_ev_type = "dry_run"
                    m_reason = f"🧪 [DRY RUN] Simulated Push Summary: {len(dispatched_histories)} release(s) simulated for {item.title}"
                else:
                    m_ev_type = "push_initiated"
                    m_reason = None
                await dispatch_batch_notification_event(
                    session,
                    event_type=m_ev_type,
                    histories=dispatched_histories,
                    item=item,
                    reason=m_reason,
                    is_dry_run=True if settings.dry_run else None,
                )

    # Case 2: Series / Anime selected seasons (or default to first monitored/searching season if none specified)
    else:
        seasons_by_id = {s.id: s for s in item.seasons}
        if not season_ids and not episode_ids and item.seasons:
            default_seasons = [
                s
                for s in sorted(item.seasons, key=lambda x: x.watch_order)
                if s.monitored and s.status != SeasonStatus.FUTURE
            ]
            if not default_seasons:
                default_seasons = [
                    s
                    for s in sorted(item.seasons, key=lambda x: x.watch_order)
                    if s.status != SeasonStatus.FUTURE
                ][:1]
            season_ids = [s.id for s in default_seasons]

        selected_seasons = sorted(
            [seasons_by_id[sid] for sid in season_ids if sid in seasons_by_id],
            key=lambda s: s.watch_order,
        )
        covered_season_ids: set[int] = set()
        candidates_found_acc: list[bool] = []
        for season in selected_seasons:
            if not settings.dry_run:
                season.fail_count = 0
                for ep in season.episodes:
                    ep.fail_count = 0
                await session.commit()
            s_hists = await _push_single_season_or_movie_entry(
                session=session,
                item=item,
                season=season,
                effective_cfg=effective_cfg,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
                is_auto_advance=False,
                _candidates_found_acc=candidates_found_acc,
            )
            if isinstance(s_hists, list) and s_hists:
                dispatched_histories.extend(s_hists)
                pushed_seasons.append(season)
            covered_season_ids.add(season.id)
        if candidates_found_acc:
            any_candidates_found = True

        # Case 3: Explicitly selected individual episodes not already covered by a season push
        if episode_ids:
            eps_by_id = {ep.id: (s, ep) for s in item.seasons for ep in s.episodes}
            for eid in episode_ids:
                if eid not in eps_by_id:
                    continue
                parent_s, ep_obj = eps_by_id[eid]
                if parent_s.id in covered_season_ids:
                    continue
                cooldown_rem = get_downloader_cooldown_remaining()
                if cooldown_rem > 0 and not settings.dry_run:
                    await _record_auto_push_failure(
                        session,
                        item=item,
                        season=parent_s,
                        episode=ep_obj,
                        reason=f"failed: {downloader_rate_limit_detail()}",
                    )
                    await session.commit()
                    continue
                if not settings.dry_run:
                    ep_obj.fail_count = 0
                    await session.commit()
                raw_ep_results = await _query_show_across_indexers(
                    session,
                    item,
                    season_number=parent_s.season_number,
                    episode_number=ep_obj.episode_number,
                )
                ep_partitioned = await _score_and_partition_candidates(
                    raw_results=raw_ep_results,
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                    expected_title=item.title,
                    expected_year=None,
                    expected_alt_title=item.alt_title,
                    expected_season=parent_s.season_number,
                    expected_episode=ep_obj.episode_number,
                    expected_season_title=(
                        parent_s.title
                        if parent_s.title and parent_s.title != item.title
                        else None
                    ),
                    runtime_minutes=item.runtime_minutes,
                    effective_cfg=effective_cfg,
                    media_type=item_media_type,
                    season_episode_count=parent_s.episode_count
                    or len(parent_s.episodes),
                    session=session,
                    item_id=item.id,
                )
                if ep_partitioned["had_candidates"]:
                    any_candidates_found = True
                ep_tb_errors: list[str] = []
                ep_hist = await _dispatch_candidate_list_to_downloader(
                    session,
                    ep_partitioned["all_valid"],
                    item=item,
                    season=parent_s,
                    episode=ep_obj,
                    push_mode="auto",
                    event_type="push_initiated",
                    error_out=ep_tb_errors,
                )
                if isinstance(ep_hist, DownloadHistory):
                    dispatched_histories.append(ep_hist)
                elif not settings.dry_run:
                    if ep_tb_errors:
                        await _record_auto_push_failure(
                            session,
                            item=item,
                            season=parent_s,
                            episode=ep_obj,
                            reason=f"failed: {ep_tb_errors[-1]}",
                        )
                    else:
                        await _record_auto_push_failure(
                            session,
                            item=item,
                            season=parent_s,
                            episode=ep_obj,
                            reason=_failure_reason_from_partitioned(ep_partitioned),
                        )
                    await session.commit()

        await session.commit()
        if dispatched_histories:
            if settings.dry_run:
                ev_type = "dry_run"
                batch_reason = f"🧪 [DRY RUN] Simulated Push Summary: {len(dispatched_histories)} release(s) simulated for {item.title}"
            else:
                ev_type = "push_initiated"
                batch_reason = None
            await dispatch_batch_notification_event(
                session,
                event_type=ev_type,
                histories=list(dispatched_histories),
                item=item,
                reason=batch_reason,
                is_dry_run=True if settings.dry_run else None,
            )

        # Season Expansion & Movie Bridge Lookahead when auto_advance_seasons is ON
        if auto_advance_seasons and pushed_seasons:
            max_order = max(s.watch_order for s in pushed_seasons)
            adv_hists = await advance_season_buffer(
                session=session,
                item=item,
                base_watch_order=max_order,
                effective_cfg=effective_cfg,
                excluded_season_ids=covered_season_ids,
            )
            dispatched_histories.extend(adv_hists)

    await session.commit()
    pushed = len(dispatched_histories) > 0
    no_release_reason: str | None = None
    if not pushed:
        if any_candidates_found:
            # Indexers returned scored releases but the downloader rejected / NZB fetch failed for all
            no_release_reason = "candidates_not_dispatched"
        else:
            # Indexers returned zero scored releases (nothing found or all filtered by scorer)
            no_release_reason = "no_indexer_results"

    resp: dict[str, Any] = {
        "pushed": pushed,
        "push_mode": "auto",
        "dispatched_count": len(dispatched_histories),
        "history_ids": [h.id for h in dispatched_histories],
        "guids": [h.nzb_guid for h in dispatched_histories],
        "no_release_reason": no_release_reason,
        "status_code": 200,
    }
    if settings.dry_run:
        resp["status"] = "simulated"
        resp["simulated_count"] = len(dispatched_histories)
    return resp


async def execute_manual_search(
    session: AsyncSession,
    item_id: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute Search & Pick Manually (`push_mode = 'manual'`) for the Unified Push Modal."""
    payload = payload or {}
    outage_failures: dict[str, tuple[str, str]] = {}
    _search_outages.set(outage_failures)
    stmt = (
        select(MediaItem)
        .where(MediaItem.id == item_id)
        .options(
            selectinload(MediaItem.provider),
            selectinload(MediaItem.seasons).selectinload(Season.episodes),
        )
    )
    item = (await session.execute(stmt)).scalars().first()
    if item is None:
        return {"error": "MediaItem not found", "status_code": 404}

    await persist_sticky_preferences(session, item, payload)
    await session.commit()
    effective_cfg = await resolve_effective_search_config(session, item, payload)
    allow_season_packs, _ = normalize_season_pack_flags(
        effective_cfg.get("allow_season_packs", False),
        effective_cfg.get("prefer_season_packs", False),
    )
    blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
        session, item.id
    )

    season_ids = _parse_int_list(
        payload.get("season_ids")
        if "season_ids" in payload
        else payload.get("season_ids[]")
    )
    episode_ids = _parse_int_list(
        payload.get("episode_ids")
        if "episode_ids" in payload
        else payload.get("episode_ids[]")
    )

    pack_primary: list[dict[str, Any]] = []
    pack_fallback: list[dict[str, Any]] = []
    pack_mismatched: list[dict[str, Any]] = []
    episode_accordions: list[dict[str, Any]] = []
    item_media_type = (
        item.media_type.value
        if hasattr(item.media_type, "value")
        else str(item.media_type or "")
    )

    if item.media_type == MediaType.MOVIE and not season_ids and not episode_ids:
        raw_movie_results = await _query_movie_across_indexers(session, item)
        partitioned = await _score_and_partition_candidates(
            raw_results=raw_movie_results,
            blacklisted_guids=blacklisted_guids,
            blacklisted_titles=blacklisted_titles,
            expected_title=item.title,
            expected_year=item.year,
            expected_alt_title=item.alt_title,
            expected_season=None,
            expected_episode=None,
            expected_season_title=None,
            runtime_minutes=item.runtime_minutes,
            effective_cfg=effective_cfg,
            media_type="movie",
            session=session,
            item_id=item.id,
        )
        pack_primary.extend(_serialize_candidate(c) for c in partitioned["primary"])
        pack_fallback.extend(_serialize_candidate(c) for c in partitioned["fallback"])
        pack_mismatched.extend(
            _serialize_candidate(c) for c in partitioned["mismatched"]
        )
    else:
        seasons_by_id = {s.id: s for s in item.seasons}
        if not season_ids and not episode_ids and item.seasons:
            first_s = sorted(item.seasons, key=lambda x: x.watch_order)[0]
            season_ids = [first_s.id]

        for sid in season_ids:
            season = seasons_by_id.get(sid)
            if season is None:
                continue
            entry_type = getattr(season, "entry_type", "season") or "season"
            s_ep_count = season.episode_count or len(season.episodes)
            if entry_type == "movie":
                raw_res = await _query_movie_across_indexers(
                    session,
                    item,
                    title_override=season.title or item.title,
                    use_external_ids=False,
                )
                partitioned = await _score_and_partition_candidates(
                    raw_results=raw_res,
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                    expected_title=season.title or item.title,
                    expected_year=season.air_date.year if season.air_date else None,
                    expected_alt_title=item.title,
                    expected_season=None,
                    expected_episode=None,
                    expected_season_title=season.title,
                    runtime_minutes=item.runtime_minutes,
                    effective_cfg=effective_cfg,
                    media_type="movie",
                    session=session,
                    item_id=item.id,
                )
            elif allow_season_packs:
                raw_res = await _query_show_across_indexers(
                    session,
                    item,
                    season_number=season.season_number,
                    episode_number=None,
                )
                partitioned = await _score_and_partition_candidates(
                    raw_results=raw_res,
                    blacklisted_guids=blacklisted_guids,
                    blacklisted_titles=blacklisted_titles,
                    expected_title=item.title,
                    expected_year=None,
                    expected_alt_title=item.alt_title,
                    expected_season=season.season_number,
                    expected_episode=None,
                    expected_season_title=(
                        season.title
                        if season.title and season.title != item.title
                        else None
                    ),
                    runtime_minutes=item.runtime_minutes,
                    effective_cfg=effective_cfg,
                    media_type=item_media_type,
                    season_episode_count=s_ep_count,
                    session=session,
                    item_id=item.id,
                )
            else:
                partitioned = {
                    "primary": [],
                    "fallback": [],
                    "mismatched": [],
                    "all_valid": [],
                }

            for tier_key, target_list in (
                ("primary", pack_primary),
                ("fallback", pack_fallback),
                ("mismatched", pack_mismatched),
            ):
                for c in partitioned[tier_key]:
                    sc = _serialize_candidate(c)
                    sc["season_id"] = season.id
                    sc["season_number"] = season.season_number
                    sc["entry_type"] = entry_type
                    target_list.append(sc)

        # Build episode accordions for requested episode_ids (or episodes of requested TV seasons if none explicitly passed)
        target_episodes: list[tuple[Season, Episode]] = []
        if episode_ids:
            eps_map = {ep.id: (s, ep) for s in item.seasons for ep in s.episodes}
            for eid in episode_ids:
                if eid in eps_map:
                    target_episodes.append(eps_map[eid])
        else:
            for sid in season_ids:
                season = seasons_by_id.get(sid)
                if season and getattr(season, "entry_type", "season") == "season":
                    for ep in sorted(season.episodes, key=lambda e: e.episode_number):
                        target_episodes.append((season, ep))

        for parent_s, ep_obj in target_episodes:
            raw_ep_res = await _query_show_across_indexers(
                session,
                item,
                season_number=parent_s.season_number,
                episode_number=ep_obj.episode_number,
            )
            ep_part = await _score_and_partition_candidates(
                raw_results=raw_ep_res,
                blacklisted_guids=blacklisted_guids,
                blacklisted_titles=blacklisted_titles,
                expected_title=item.title,
                expected_year=None,
                expected_alt_title=item.alt_title,
                expected_season=parent_s.season_number,
                expected_episode=ep_obj.episode_number,
                expected_season_title=(
                    parent_s.title
                    if parent_s.title and parent_s.title != item.title
                    else None
                ),
                runtime_minutes=item.runtime_minutes,
                effective_cfg=effective_cfg,
                media_type=item_media_type,
                season_episode_count=parent_s.episode_count or len(parent_s.episodes),
                session=session,
                item_id=item.id,
            )
            ep_primary = [_serialize_candidate(c) for c in ep_part["primary"]]
            ep_fallback = [_serialize_candidate(c) for c in ep_part["fallback"]]
            ep_mismatched = [_serialize_candidate(c) for c in ep_part["mismatched"]]
            episode_accordions.append(
                {
                    "episode_id": ep_obj.id,
                    "season_id": parent_s.id,
                    "season_number": parent_s.season_number,
                    "episode_number": ep_obj.episode_number,
                    "primary": ep_primary,
                    "fallback": ep_fallback,
                    "mismatched": ep_mismatched,
                    "releases": ep_primary + ep_fallback + ep_mismatched,
                }
            )

    await session.commit()
    search_outage: dict[str, str] | None = None
    if outage_failures and not (
        pack_primary
        or pack_fallback
        or pack_mismatched
        or any(a["releases"] for a in episode_accordions)
    ):
        active_names = {i.name for i in await get_active_indexers(session)}
        if active_names and active_names <= set(outage_failures):
            name, label = outage_failures[sorted(active_names)[0]]
            search_outage = {"provider_name": name, "label": label}
    return {
        "search_outage": search_outage,
        "item_id": item.id,
        "pack_or_movie_releases": {
            "primary": pack_primary,
            "fallback": pack_fallback,
            "mismatched": pack_mismatched,
            "releases": pack_primary + pack_fallback + pack_mismatched,
        },
        "episode_accordions": episode_accordions,
        "status_code": 200,
    }


async def execute_manual_grab(
    session: AsyncSession,
    item_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch a manually selected release from the Push Modal to the downloader with `push_mode = 'manual'`."""
    from app.services.discord import dispatch_batch_notification_event

    stmt = (
        select(MediaItem)
        .where(MediaItem.id == item_id)
        .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
    )
    item = (await session.execute(stmt)).scalars().first()
    if item is None:
        return {"pushed": False, "error": "MediaItem not found", "status_code": 404}

    guid = str(payload.get("guid") or "").strip()
    title = str(payload.get("title") or "").strip()
    if not guid or not title:
        return {
            "pushed": False,
            "error": "Both guid and title are required",
            "status_code": 400,
        }

    season_id = payload.get("season_id")
    episode_id = payload.get("episode_id")

    season_obj: Season | None = None
    episode_obj: Episode | None = None

    if episode_id is not None and str(episode_id).strip() != "":
        try:
            eid = int(episode_id)
            for s in item.seasons:
                for ep in s.episodes:
                    if ep.id == eid:
                        season_obj = s
                        episode_obj = ep
                        break
        except (TypeError, ValueError):
            pass

    if season_obj is None and season_id is not None and str(season_id).strip() != "":
        try:
            sid = int(season_id)
            season_obj = next((s for s in item.seasons if s.id == sid), None)
        except (TypeError, ValueError):
            pass

    if not settings.dry_run:
        if episode_obj is not None:
            episode_obj.fail_count = 0
        elif season_obj is not None:
            season_obj.fail_count = 0
            for ep in season_obj.episodes:
                ep.fail_count = 0
        else:
            item.fail_count = 0
        await session.commit()

    if (
        "auto_advance_seasons" in payload
        and payload["auto_advance_seasons"] is not None
    ):
        val = payload["auto_advance_seasons"]
        item.auto_advance_seasons = (
            val
            if isinstance(val, bool)
            else str(val).strip().lower() in ("1", "true", "yes", "on")
        )
        await session.commit()

    parsed = parse_release_name(title)
    candidate = {
        "guid": guid,
        "title": title,
        "size_bytes": int(payload.get("size_bytes") or 0),
        "score": float(payload.get("score") or 0.0),
        "parsed": parsed,
        "score_res": None,
        "indexer_url": payload.get("indexer_url"),
        "indexer_key": payload.get("indexer_key"),
        "is_fallback": bool(payload.get("is_fallback", False)),
        "matched_language": payload.get("matched_language"),
    }

    manual_tb_errors: list[str] = []
    hist = await _dispatch_candidate_list_to_downloader(
        session,
        [candidate],
        item=item,
        season=season_obj,
        episode=episode_obj,
        push_mode="manual",
        reset_fail_count=True,
        error_out=manual_tb_errors,
    )
    if isinstance(hist, dict) and hist.get("status") == "simulated":
        return {
            "pushed": True,
            "push_mode": "manual",
            "status": "simulated",
            "candidate": hist.get("candidate") or candidate,
            "history_id": None,
            "guid": guid,
            "transfer_id": None,
            "auto_advanced_count": 0,
            "status_code": 200,
        }
    if hist is None:
        err_msg = (
            manual_tb_errors[-1]
            if manual_tb_errors
            else "Release failed fake verification or downloader dispatch"
        )
        if _is_queueable_error(err_msg):
            queued_hist = await _record_auto_push_failure(
                session,
                item=item,
                season=season_obj,
                episode=episode_obj,
                reason=f"failed: {err_msg}",
                push_mode="manual",
                candidate=candidate,
            )
            if (
                season_obj is not None
                and episode_obj is None
                and getattr(item, "auto_advance_seasons", False)
            ):
                await advance_season_buffer(
                    session=session,
                    item=item,
                    base_watch_order=season_obj.watch_order,
                    excluded_season_ids={season_obj.id},
                )
            await session.commit()
            return {
                "pushed": True,
                "queued": True,
                "rate_limit_source": rate_limit_source_label(
                    active_rate_limit_sources()
                ),
                "push_mode": "manual",
                "history_id": queued_hist.id,
                "guid": queued_hist.nzb_guid,
                "transfer_id": None,
                "status_code": 200,
            }
        await session.commit()
        return {
            "pushed": False,
            "push_mode": "manual",
            "error": err_msg,
            "status_code": 400,
        }

    assert isinstance(hist, DownloadHistory)
    await dispatch_batch_notification_event(
        session,
        event_type="push_initiated",
        histories=[hist],
        item=item,
    )

    adv_hists: list[DownloadHistory] = []
    if (
        season_obj is not None
        and episode_obj is None
        and getattr(item, "auto_advance_seasons", False)
    ):
        adv_hists = await advance_season_buffer(
            session=session,
            item=item,
            base_watch_order=season_obj.watch_order,
            excluded_season_ids={season_obj.id},
        )

    await session.commit()
    resp = {
        "pushed": True,
        "push_mode": "manual",
        "history_id": hist.id,
        "guid": hist.nzb_guid,
        "transfer_id": hist.downloader_transfer_id,
        "auto_advanced_count": len(adv_hists),
        "status_code": 200,
    }
    if getattr(hist, "is_dry_run", False) or settings.dry_run:
        resp["status"] = "simulated"
    return resp


class TitledEntry:
    """Read-only view of a ``Season`` carrying a resolved ``display_title`` string.

    Needed because ``Season.display_title`` is a method; templates and JSON want a string.
    """

    def __init__(self, season: Season, display_title: str) -> None:
        self._season = season
        self.display_title = display_title

    def __getattr__(self, name: str) -> Any:
        return getattr(self._season, name)


async def get_anime_title_language(session: AsyncSession) -> str:
    """Configured anime title language (romaji when unset)."""
    from app.db.models import SystemSettings

    raw = (
        await session.execute(
            select(SystemSettings.anime_title_language).where(SystemSettings.id == 1)
        )
    ).scalar_one_or_none()
    return normalize_anime_title_language(raw)


async def get_push_modal_context(
    session: AsyncSession, item_id: int
) -> dict[str, Any] | None:
    """Build context dictionary for `GET /api/items/{item_id}/push-modal`."""
    stmt = (
        select(MediaItem)
        .where(MediaItem.id == item_id)
        .options(selectinload(MediaItem.seasons).selectinload(Season.episodes))
    )
    item = (await session.execute(stmt)).scalars().first()
    if item is None:
        return None

    presets = await list_presets(session)
    effective_cfg = await resolve_effective_search_config(session, item)
    language = await get_anime_title_language(session)
    entries = [
        TitledEntry(s, anime_season_title_for(item, s, language))
        for s in sorted(item.seasons, key=lambda s: (s.watch_order, s.season_number))
    ]

    is_movie_type = item.media_type == MediaType.MOVIE
    default_min_gb = 1.5 if is_movie_type else 0.2
    default_max_gb = 45.0 if is_movie_type else 15.0

    presets_data = [
        {
            "id": p.id,
            "name": p.name,
            "is_default": p.is_default,
            "primary_language": p.primary_language,
            "fallback_language": p.fallback_language or "none",
            "video_quality_mode": p.video_quality_mode,
            "audio_quality_mode": p.audio_quality_mode,
            "allow_season_packs": bool(p.allow_season_packs),
            "prefer_season_packs": bool(p.prefer_season_packs)
            if bool(p.allow_season_packs)
            else False,
            "custom_config": p.custom_config,
        }
        for p in presets
    ]

    unpushed_season_ids = [
        s.id
        for s in entries
        if s.status
        not in (
            SeasonStatus.COMPLETED,
            SeasonStatus.DOWNLOADED,
            SeasonStatus.DOWNLOADING,
            SeasonStatus.FUTURE,
        )
    ]
    default_selected_season_ids = (
        [unpushed_season_ids[0]] if unpushed_season_ids else []
    )

    return {
        "item": item,
        "anime_title_language": language,
        "display_title": anime_title_for(item, language),
        "entries": entries,
        "presets": presets,
        "presets_data": presets_data,
        "presets_json": json.dumps(presets_data),
        "effective_config": effective_cfg,
        "effective_config_json": json.dumps(effective_cfg),
        "default_min_size_gb": default_min_gb,
        "default_max_size_gb": default_max_gb,
        "unpushed_season_ids": unpushed_season_ids,
        "default_selected_season_ids": default_selected_season_ids,
        "auto_push_episodes": bool(item.auto_push_episodes),
        "can_auto_advance": bool(effective_cfg.get("can_auto_advance", True)),
        "auto_advance_unavailable_message": AUTO_ADVANCE_UNAVAILABLE_MESSAGE,
        **await delivery_toggle_context(session),
    }
