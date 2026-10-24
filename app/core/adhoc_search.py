"""Scoped ad-hoc release search and scoring pipeline (m52).

Extracted from the manual search endpoint: Newznab dispatch per row scope,
dedup, blacklist, Layer 1 fake detection, filtering, scoring and bucketing.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.fake_detector import extract_nzb_password
from app.core.outage import (
    active_outages,
    has_active_outage,
    paused_detail,
    record_outage,
)
from app.core.parser import parse_release_name
from app.core.rate_limit_queue import (
    active_rate_limit_sources,
    downloader_rate_limit_detail,
    get_downloader_cooldown_remaining,
    rate_limit_source_label,
)
from app.db.models import DownloadHistory, Provider
from app.services import provider_service
from app.services.delivery.hooks import (
    log_dry_run_delivery_plan,
    resolve_deliver_enabled,
)
from app.services.downloaders.base import DOWNLOADER_DISPLAY_NAMES, DownloaderError
from app.services.downloaders.factory import (
    downloader_class,
    get_active_downloader_client,
)
from app.services.indexers import newznab as newznab_client
from app.services.indexers.catalog import default_indexer, resolve_categories

logger = logging.getLogger(__name__)

_CATEGORY_KINDS = frozenset({"movie", "series", "anime", "anime-movie"})


@dataclass
class AdHocFilters:
    primary_language: str = "any"
    fallback_language: str = "none"
    language: str = "any"
    resolution: str = "any"
    source: str = "any"
    hdr: str = "any"
    video_codec: str = "any"
    audio_tier: str = "any"
    audio_channels: str = "any"


@dataclass
class AdHocSearchResult:
    primary_results: list[dict[str, Any]]
    fallback_results: list[dict[str, Any]]
    mismatched_results: list[dict[str, Any]]
    error_message: str | None = None
    outage: dict[str, str] | None = (
        None  # {"provider_name", "label"} when all indexers are down
    )


def is_match(filter_val: str, parsed_val: str | None, orig_title: str = "") -> bool:
    fv = (filter_val or "any").lower()
    if fv == "any":
        return True
    combined = f"{parsed_val or ''} {orig_title}".lower()
    if fv == "sdr":
        return not any(k in combined for k in ["hdr", "dv", "dolby vision", "dovi"])
    if fv in ("hdr10plus", "hdr10+"):
        return any(k in combined for k in ["hdr10plus", "hdr10+"])
    if fv in ("dv", "dolby vision"):
        return any(k in combined for k in ["dv", "dolby vision", "dovi"])
    if fv in ("hevc", "h265", "x265"):
        return any(k in combined for k in ["hevc", "h265", "x265", "h.265"])
    if fv in ("x264", "h264", "avc"):
        return any(k in combined for k in ["x264", "h264", "avc", "h.264"])
    if fv in ("bluray", "blu-ray"):
        return any(k in combined for k in ["bluray", "blu-ray", "bdrip", "brrip"])
    if fv in ("web", "web-dl", "webrip"):
        return any(k in combined for k in ["web", "web-dl", "webdl", "webrip"])
    return fv in combined


def check_audio_tier(
    filter_val: str, parsed_codec: str | None, orig_title: str = ""
) -> bool:
    fv = (filter_val or "any").lower()
    if fv == "any":
        return True
    pc = f"{parsed_codec or ''} {orig_title}".lower()
    if fv == "lossless" and any(
        x in pc
        for x in [
            "truehd",
            "dts:x",
            "dts-x",
            "dtsx",
            "dts-hd",
            "dtshd",
            "lpcm",
            "pcm",
            "flac",
            "atmos",
        ]
    ):
        return True
    if fv == "dts" and "dts" in pc:
        return True
    if fv in ("ddp", "dd+", "eac3", "ac3") and any(
        x in pc for x in ["eac3", "e-ac-3", "dd+", "ddp", "ac3", "dolby"]
    ):
        return True
    if fv == "aac" and any(x in pc for x in ["aac", "opus", "mp3"]):
        return True
    if fv == "tier1" and any(x in pc for x in ["truehd", "dts:x", "auro"]):
        return True
    if fv == "tier2" and any(x in pc for x in ["dts-hd", "lpcm", "flac"]):
        return True
    if fv == "tier3" and "atmos" in pc and ("eac3" in pc or "dd+" in pc):
        return True
    if fv == "tier4" and any(x in pc for x in ["eac3", "dts", "ac3", "dolby digital"]):
        return True
    if fv == "tier5" and any(x in pc for x in ["aac", "opus", "mp3"]):
        return True
    if fv == "tier1" and "truehd atmos" in pc:
        return True
    return fv in pc


def _media_type_for(kind: str, season: int | None, episode: int | None) -> str | None:
    kind_norm = (kind or "any").strip().lower()
    if kind_norm in ("movie", "anime-movie"):
        return "movie"
    if kind_norm == "anime":
        return "anime"
    if kind_norm == "series" or season is not None or episode is not None:
        return "show"
    return None


async def _fetch_raw_releases(
    *,
    kind: str,
    titles: list[str],
    imdb_id: str | None,
    tmdb_id: int | None,
    tvdb_id: int | None,
    season: int | None,
    episode: int | None,
    title_only: frozenset[str] = frozenset(),
    languages: tuple[str | None, ...] = (),
) -> tuple[list[dict[str, Any]], str | None, dict[str, str] | None]:
    """Run one Newznab search per distinct title and merge by guid/title."""
    from app.db.database import async_session_factory

    async with async_session_factory() as idx_session:
        indexers: list[Provider | None] = list(
            await provider_service.get_active_indexers(idx_session)
        )
    if not indexers:
        indexers = [None]  # default indexer; falls back to built-in categories
    distinct: list[str | None] = []
    seen_norm: set[str] = set()
    for t in titles:
        clean = (t or "").strip()
        if clean and clean.lower() not in seen_norm:
            seen_norm.add(clean.lower())
            distinct.append(clean)
    if not distinct:
        distinct = [None]

    def call(title: str | None, idx: Provider | None) -> Any:
        # Indexers prefer ids over ``q``; a translated title must be searched alone.
        ids_off = title is not None and title.lower() in title_only
        c_imdb = None if ids_off else imdb_id
        c_tmdb = None if ids_off else tmdb_id
        c_tvdb = None if ids_off else tvdb_id
        if idx is not None:
            cat_id: int | str | None = (
                resolve_categories(idx, kind, languages)
                if kind in _CATEGORY_KINDS
                else None
            )
            idx_kwargs: dict[str, Any] = {
                "api_url": idx.api_url,
                "api_key": idx.api_key,
            }
        else:
            cat_id = (
                resolve_categories(default_indexer(), kind, languages)
                if kind in _CATEGORY_KINDS
                else None
            )
            idx_kwargs = {}
        if kind in ("movie", "anime-movie"):
            return newznab_client.search_movie(
                title=title,
                category=cat_id,
                tmdb_id=c_tmdb,
                imdb_id=c_imdb,
                **idx_kwargs,
            )
        if kind == "series":
            return newznab_client.search_show(
                title=title,
                category=cat_id,
                season=season,
                ep=episode,
                tvdb_id=c_tvdb,
                tmdb_id=c_tmdb,
                imdb_id=c_imdb,
                **idx_kwargs,
            )
        return newznab_client.search_raw(
            query=title,
            category=cat_id,
            season=season,
            ep=episode,
            imdb_id=c_imdb,
            tmdb_id=c_tmdb,
            tvdb_id=c_tvdb,
            **idx_kwargs,
        )

    outcomes = await asyncio.gather(
        *(call(t, idx) for idx in indexers for t in distinct),
        return_exceptions=True,
    )
    merged: list[dict[str, Any]] = []
    errors: list[newznab_client.IndexerError] = []
    for outcome in outcomes:
        if isinstance(outcome, newznab_client.IndexerError):
            errors.append(outcome)
        elif isinstance(outcome, BaseException):
            raise outcome
        else:
            merged.extend(outcome)
    if errors and len(errors) == len(outcomes):
        first = errors[0]
        outage = None
        if all(isinstance(e, newznab_client.IndexerOutageError) for e in errors):
            assert isinstance(first, newznab_client.IndexerOutageError)
            outage = {
                "provider_name": newznab_client.indexer_display_name(first.indexer_url)
                if first.indexer_url
                else "Indexer",
                "label": first.verdict.label if first.verdict else str(first),
            }
        return [], str(first), outage
    for err in errors:
        logger.warning("Partial ad-hoc search failure ignored: %s", err)
    return merged, None, None


async def _localized_query_title(
    *,
    kind: str,
    tmdb_id: int | None,
    titles: list[str],
    primary_language: str,
) -> str | None:
    """TMDB title in the Primary language (filter, else default preset), or None."""
    from app.db.database import async_session_factory
    from app.services.localized_titles import get_localized_title
    from app.services.preset_service import get_default_preset

    display = next((t for t in titles if t and t.strip()), "")
    if tmdb_id is None or not display:
        return None
    async with async_session_factory() as session:
        language: str | None = primary_language
        if language in ("any", ""):
            preset = await get_default_preset(session)
            language = preset.primary_language if preset else None
        return await get_localized_title(
            kind, tmdb_id, language, display, session=session
        )


async def search_and_score_releases(
    *,
    kind: str,
    scope: str,
    titles: list[str],
    year: int | None,
    imdb_id: str | None,
    tmdb_id: int | None,
    tvdb_id: int | None,
    season: int | None,
    episode: int | None,
    filters: AdHocFilters,
    limit: int,
) -> AdHocSearchResult:
    """Search the indexers for ``scope`` and return scored, bucketed releases.

    ``scope`` is ``item``, ``season`` or ``episode``. A ``season`` scope keeps
    both season packs and single episodes of that season. ``year`` is accepted
    for caller symmetry; Newznab dispatch does not use it.
    """
    from app.core.fake_detector import is_indexer_metadata_fake
    from app.core.parser import build_release_feature_pills, parse_release_name
    from app.core.push_engine import _load_blacklisted_sets
    from app.core.scorer import score_release
    from app.db.database import async_session_factory
    from app.db.models import BlacklistedRelease

    kind = (kind or "any").strip().lower()
    if scope == "item":
        season = episode = None
    elif scope == "season":
        episode = None

    localized = await _localized_query_title(
        kind=kind,
        tmdb_id=tmdb_id,
        titles=titles,
        primary_language=filters.primary_language,
    )
    if localized:
        titles = [*titles, localized]
        title_only = frozenset({localized.lower()})
    else:
        title_only = frozenset()

    prim_lang = (
        filters.primary_language
        if filters.primary_language not in ("any", "")
        else (filters.language if filters.language not in ("any", "") else None)
    )
    fall_lang = (
        filters.fallback_language
        if filters.fallback_language not in ("none", "any", "")
        else None
    )

    raw_results, error_message, outage = await _fetch_raw_releases(
        kind=kind,
        titles=titles,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        season=season,
        episode=episode,
        title_only=title_only,
        languages=(prim_lang, fall_lang),
    )
    if error_message:
        return AdHocSearchResult([], [], [], error_message, outage)

    primary_results: list[dict[str, Any]] = []
    fallback_results: list[dict[str, Any]] = []
    mismatched_results: list[dict[str, Any]] = []

    async with async_session_factory() as session:
        blacklisted_guids, blacklisted_titles = await _load_blacklisted_sets(
            session, None
        )

    seen_guids: set[str] = set()
    seen_titles: set[str] = set()
    ad_hoc_media_type = _media_type_for(kind, season, episode)
    season_scope = scope == "season" and season is not None

    for item in raw_results:
        title = str(item.get("title") or "")
        if not title:
            continue
        guid_or_link = str(item.get("guid") or item.get("link") or "")
        norm_title = title.strip().lower()
        if (guid_or_link and guid_or_link in seen_guids) or norm_title in seen_titles:
            continue
        if guid_or_link:
            seen_guids.add(guid_or_link)
        seen_titles.add(norm_title)

        if (
            (guid_or_link and guid_or_link in blacklisted_guids)
            or title in blacklisted_titles
            or norm_title in blacklisted_titles
        ):
            continue

        parsed = parse_release_name(title)

        # Layer 1: Indexer metadata filtering (drop and globally blacklist fake releases)
        is_sp = season is not None and episode is None and parsed.episode is None
        is_fake, fake_reason = is_indexer_metadata_fake(
            raw_item=item,
            parsed_resolution=parsed.resolution,
            media_type=ad_hoc_media_type or "movie",
            is_season_pack=is_sp,
        )
        if is_fake:
            logger.warning(
                "Rejected Layer 1 fake release in manual search '%s': %s",
                title,
                fake_reason,
            )
            if guid_or_link:
                blacklisted_guids.add(guid_or_link)
            blacklisted_titles.add(title)
            blacklisted_titles.add(norm_title)
            async with async_session_factory() as bl_session:
                new_bl = BlacklistedRelease(
                    media_item_id=None,
                    nzb_guid=guid_or_link or None,
                    nzb_title=title,
                    reason=f"Layer 1 Fake: {fake_reason}",
                )
                bl_session.add(new_bl)
                await bl_session.commit()
            continue

        if not is_match(filters.resolution, parsed.resolution, title):
            continue
        if not is_match(filters.source, parsed.source, title):
            continue
        if not is_match(filters.hdr, parsed.hdr, title):
            continue
        if not is_match(filters.video_codec, parsed.video_codec, title):
            continue
        if not is_match(filters.audio_channels, parsed.audio_channels, title):
            continue
        if not check_audio_tier(filters.audio_tier, parsed.audio_codec, title):
            continue

        size_bytes = int(item.get("size") or item.get("size_bytes") or 0)
        # Season scope keeps single episodes: score them against their own episode
        # number so season isolation still applies but they are not rejected as
        # "not a season pack".
        ep_int = (
            parsed.episode if season_scope and parsed.episode is not None else episode
        )
        sr = score_release(
            parsed,
            size_bytes=size_bytes,
            age_days=0,
            expected_season=season,
            expected_episode=ep_int,
            primary_language=prim_lang,
            fallback_language=fall_lang,
            api_language=item.get("api_language"),
            media_type=ad_hoc_media_type,
        )

        if sr.is_rejected:
            if sr.reject_reason and "Language mismatch" in sr.reject_reason:
                sr_mismatch = score_release(
                    parsed,
                    size_bytes=size_bytes,
                    age_days=0,
                    expected_season=season,
                    expected_episode=ep_int,
                    primary_language=None,
                    fallback_language=None,
                    api_language=item.get("api_language"),
                    media_type=ad_hoc_media_type,
                )
                item["score"] = sr_mismatch.score
                item["parsed"] = parsed
                item["is_mismatch"] = True
                item["reject_reason"] = sr.reject_reason
                item["feature_pills"] = build_release_feature_pills(
                    parsed=parsed,
                    score=sr_mismatch.score,
                    size_bytes=size_bytes,
                    is_fallback=False,
                    is_mismatch=True,
                    matched_language=sr_mismatch.matched_language,
                    api_language=item.get("api_language"),
                    bitrate_mbps=sr_mismatch.bitrate_mbps,
                    normalized_bitrate_mbps=sr_mismatch.normalized_bitrate_mbps,
                    media_type=ad_hoc_media_type,
                )
                mismatched_results.append(item)
            continue

        item["score"] = sr.score
        item["parsed"] = parsed
        item["is_primary"] = sr.is_primary
        item["is_fallback"] = sr.is_fallback
        item["feature_pills"] = build_release_feature_pills(
            parsed=parsed,
            score=sr.score,
            size_bytes=size_bytes,
            is_fallback=bool(sr.is_fallback),
            is_mismatch=False,
            matched_language=sr.matched_language,
            api_language=item.get("api_language"),
            bitrate_mbps=sr.bitrate_mbps,
            normalized_bitrate_mbps=sr.normalized_bitrate_mbps,
            media_type=ad_hoc_media_type,
        )
        if sr.is_fallback:
            fallback_results.append(item)
        else:
            primary_results.append(item)

    for bucket in (primary_results, fallback_results, mismatched_results):
        bucket.sort(key=lambda x: x["score"], reverse=True)

    return AdHocSearchResult(
        primary_results[:limit],
        fallback_results[:limit],
        mismatched_results[:limit],
        None,
    )


@dataclass
class AdHocPushOutcome:
    ok: bool
    message: str
    status_code: int = 200
    history: DownloadHistory | None = None
    hard_failure: bool = False  # downloader-level failure; a batch must stop


def _target_label(season: int | None, episode: int | None) -> str | None:
    """Match push_engine.build_history_target_label for attached rows."""
    if season is not None and episode is not None:
        return f"S{season:02d}E{episode:02d}"
    if episode is not None:
        return f"E{episode:02d}"
    if season is not None:
        return f"Season {season} Pack"
    return None


async def _queue_adhoc_outage(
    session: AsyncSession,
    *,
    release_link: str,
    nzb_title: str,
    nzb_guid: str | None,
    score: float | None,
    size_bytes: int | None,
    is_fallback: bool,
    matched_language: str | None,
    media_title: str | None,
    media_year: int | None,
    media_kind: str | None,
    poster_url: str | None,
    tmdb_id: int | None,
    imdb_id: str | None,
    anilist_id: int | None,
    season: int | None,
    episode: int | None,
) -> AdHocPushOutcome:
    """Record a ``paused:`` ad-hoc row while a provider outage is active (ADR-159)."""
    outage = active_outages()[0]
    parsed = parse_release_name(nzb_title)
    session.add(
        DownloadHistory(
            media_item_id=None,
            is_ad_hoc=True,
            push_mode="manual",
            media_title=media_title,
            media_year=media_year,
            media_type_label=media_kind,
            target_label=_target_label(season, episode),
            poster_url=poster_url,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            anilist_id=anilist_id,
            nzb_title=nzb_title,
            nzb_guid=nzb_guid,
            score=score,
            size_bytes=size_bytes,
            resolution=parsed.resolution if parsed else None,
            video_codec=parsed.video_codec if parsed else None,
            audio_codec=parsed.audio_codec if parsed else None,
            source=parsed.source if parsed else None,
            release_group=parsed.release_group if parsed else None,
            is_fallback=is_fallback,
            grabbed_language=matched_language or None,
            status_detail=paused_detail(outage.kind, outage.label),
            outage_attempts=outage.attempt,
            is_dismissed=False,
            notification_sent=True,
            progress_pct=0.0,
        )
    )
    await session.commit()
    label = rate_limit_source_label(active_rate_limit_sources())
    return AdHocPushOutcome(
        True,
        f"Push queued — will start automatically once {label} recovers.",
        200,
    )


async def push_adhoc_release(
    session: AsyncSession,
    *,
    release_link: str,
    nzb_title: str,
    nzb_guid: str | None = None,
    score: float | None = None,
    size_bytes: int | None = None,
    is_fallback: bool = False,
    matched_language: str | None = None,
    media_title: str | None = None,
    media_year: int | None = None,
    media_kind: str | None = None,
    poster_url: str | None = None,
    tmdb_id: int | None = None,
    imdb_id: str | None = None,
    anilist_id: int | None = None,
    season: int | None = None,
    episode: int | None = None,
    deliver: bool | None = None,
) -> AdHocPushOutcome:
    """Send one release to the active downloader and record a detached ad-hoc DownloadHistory row.

    No row is written on failure. Links are restricted to ``magnet:`` URIs and
    http(s) URLs hosted on a configured indexer (SSRF guard).
    """
    is_magnet = release_link.startswith("magnet:")
    indexer = None
    if not is_magnet:
        parsed_url = urlparse(release_link)
        host = (parsed_url.hostname or "").lower()
        if parsed_url.scheme not in ("http", "https") or not host:
            return AdHocPushOutcome(False, "Invalid release link.", 400)
        for candidate in await provider_service.get_active_indexers(session):
            indexer_host = (urlparse(candidate.api_url or "").hostname or "").lower()
            if indexer_host and (
                host == indexer_host or host.endswith("." + indexer_host)
            ):
                indexer = candidate
                break
        if indexer is None:
            return AdHocPushOutcome(
                False, "Release link host is not a configured indexer.", 400
            )

    queue_kwargs = dict(
        release_link=release_link,
        nzb_title=nzb_title,
        nzb_guid=nzb_guid,
        score=score,
        size_bytes=size_bytes,
        is_fallback=is_fallback,
        matched_language=matched_language,
        media_title=media_title,
        media_year=media_year,
        media_kind=media_kind,
        poster_url=poster_url,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        anilist_id=anilist_id,
        season=season,
        episode=episode,
    )
    if has_active_outage():
        return await _queue_adhoc_outage(session, **queue_kwargs)  # type: ignore[arg-type]
    if get_downloader_cooldown_remaining() > 0:
        return AdHocPushOutcome(
            False, downloader_rate_limit_detail(), 400, hard_failure=True
        )

    result: dict[str, Any] | None = None
    downloader_type = "torbox"
    downloader_name = DOWNLOADER_DISPLAY_NAMES[downloader_type]
    if settings.dry_run:
        result = {}
        active = await get_active_downloader_client(session)
        if active is not None:
            downloader_type = active.provider_type
            downloader_name = active.display_name
    else:
        try:
            if is_magnet:
                active = await get_active_downloader_client(session)
                magnet_cls = (
                    type(active) if active is not None else downloader_class("torbox")
                )
                if not magnet_cls.supports_magnet:
                    return AdHocPushOutcome(
                        False,
                        f"Magnet links are not supported by {active.display_name}."
                        if active is not None
                        else "Magnet links are not supported.",
                        400,
                    )
                result = await magnet_cls.send_magnet(release_link, is_manual=True)
            else:
                nzb_bytes, filename = await newznab_client.fetch_nzb_bytes(
                    release_link,
                    api_url=indexer.api_url if indexer else None,
                    api_key=indexer.api_key if indexer else None,
                    session=session,
                )
                from app.core.push_engine import dispatch_nzb_to_active_downloader

                client, upload = await dispatch_nzb_to_active_downloader(
                    session,
                    nzb_bytes,
                    filename,
                    password=extract_nzb_password(nzb_bytes),
                    media_type=media_kind or "",
                    is_anime=(media_kind or "").lower() == "anime",
                )
                downloader_type = client.provider_type
                downloader_name = client.display_name
                if upload.is_outage:
                    record_outage(
                        "downloader",
                        client.provider_type,
                        client.display_name,
                        upload.outage_label or "outage",
                        1,
                    )
                    return await _queue_adhoc_outage(session, **queue_kwargs)  # type: ignore[arg-type]
                result = (
                    {**upload.raw, "id": upload.transfer_id}
                    if upload.success
                    else {"error": upload.detail}
                )
        except newznab_client.IndexerOutageError as exc:
            record_outage(
                "indexer",
                exc.indexer_url or (indexer.api_url if indexer else "") or "",
                newznab_client.indexer_display_name(
                    exc.indexer_url or (indexer.api_url if indexer else "") or ""
                ),
                exc.verdict.label if exc.verdict else "outage",
                1,
            )
            return await _queue_adhoc_outage(session, **queue_kwargs)  # type: ignore[arg-type]
        except newznab_client.IndexerError as exc:
            return AdHocPushOutcome(False, f"NZB Download Error: {exc}", 400)
        except DownloaderError as exc:
            return AdHocPushOutcome(False, str(exc), 400, hard_failure=True)
        if not result or not (result.get("hash") or result.get("id")):
            err = (result or {}).get("error") or "Failed to send"
            return AdHocPushOutcome(False, str(err), 400, hard_failure=True)

    parsed = parse_release_name(nzb_title)
    now_utc = datetime.now(timezone.utc)
    result = result or {}
    history = DownloadHistory(
        media_item_id=None,
        season_id=None,
        episode_id=None,
        is_ad_hoc=True,
        push_mode="manual",
        media_title=media_title,
        media_year=media_year,
        media_type_label=media_kind,
        target_label=_target_label(season, episode),
        poster_url=poster_url,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        anilist_id=anilist_id,
        nzb_title=nzb_title,
        nzb_guid=nzb_guid,
        score=score,
        size_bytes=size_bytes,
        resolution=parsed.resolution if parsed else None,
        video_codec=parsed.video_codec if parsed else None,
        audio_codec=parsed.audio_codec if parsed else None,
        source=parsed.source if parsed else None,
        release_group=parsed.release_group if parsed else None,
        downloader_hash=str(result["hash"])
        if downloader_type == "torbox" and result.get("hash")
        else None,
        downloader_type=downloader_type,
        downloader_transfer_id=str(result["id"]) if result.get("id") else None,
        dispatched_at=now_utc,
        is_fallback=is_fallback,
        grabbed_language=matched_language or None,
        notification_sent=False,
        deliver_enabled=await resolve_deliver_enabled(session, deliver),
    )
    if settings.dry_run:
        from app.core.transfer_poller import register_dry_run_push

        register_dry_run_push(
            media_item_id=None,
            media_title=media_title or nzb_title,
            release_title=nzb_title,
            target_badge=history.target_label or "Movie",
            push_mode="manual",
            score=score,
            size_bytes=size_bytes,
            resolution=history.resolution,
            source=history.source,
            language=history.grabbed_language,
        )
        history.status_detail = "simulated"
        history.is_dismissed = True
        history.is_dry_run = True
        history.progress_pct = 100.0
        history.completed_at = now_utc
    else:
        history.status_detail = "downloading"
        history.is_dry_run = False
    session.add(history)
    await session.commit()
    if settings.dry_run and history.deliver_enabled:
        await log_dry_run_delivery_plan(session, history)
    logger.info(
        "%s ad-hoc push: '%s' (%s, score=%s, size=%s B, hash=%s)",
        "[DRY RUN] Simulated" if settings.dry_run else "Sent",
        nzb_title,
        history.target_label or media_kind or "release",
        score,
        size_bytes,
        history.downloader_hash or "-",
    )

    if not settings.dry_run:
        from app.core.transfer_poller import wake_transfer_poller

        wake_transfer_poller()
    return AdHocPushOutcome(True, f"Sent to {downloader_name}!", 200, history)
