"""AniList GraphQL client.

Serves two roles — metadata provider (anime hierarchy, posters) and watchlist
provider (user media list). Kept flat on purpose; do not move into
`watchlists/` or `metadata/`.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import DEFAULT_USER_AGENT
from app.core.outage import (
    active_metadata_outages,
    clear_metadata_outage,
    record_metadata_outage,
)
from app.core.rate_limiter import RateLimiter
from app.services.outage_classifier import classify_exception, classify_http_status

logger = logging.getLogger(__name__)


class _OutageLogFilter(logging.Filter):
    """Downgrade ERROR to WARNING while AniList is flagged down.

    The outage already alerts once (down/recovered), so per-chunk failures must
    not also be forwarded as ERROR alerts.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno == logging.ERROR and any(
            name == ANILIST_NAME for name, _ in active_metadata_outages()
        ):
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True


logger.addFilter(_OutageLogFilter())
ANILIST_URL = "https://graphql.anilist.co"
ANILIST_NAME = "AniList"
_limiter = RateLimiter(0.5)


def _sanitize_error_message(exc: Exception) -> str:
    raw = str(exc) or type(exc).__name__
    first_line = raw.splitlines()[0]
    clean = re.sub(r"<.*?>", " ", first_line)
    return re.sub(r"\s+", " ", clean).strip()[:200]


async def _post_with_retry(
    client: httpx.AsyncClient,
    json_data: dict[str, Any],
    max_retries: int = 3,
    extra_headers: dict[str, str] | None = None,
) -> httpx.Response:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    resp: httpx.Response | None = None
    for attempt in range(max_retries):
        await _limiter.wait()
        try:
            resp = await client.post(ANILIST_URL, json=json_data, headers=headers)
        except httpx.RequestError as exc:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.warning(
                    "AniList request error (%s). Retrying in %d seconds (attempt %d/%d)...",
                    _sanitize_error_message(exc),
                    wait_time,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(wait_time)
                continue
            verdict = classify_exception(exc)
            if verdict:
                record_metadata_outage(ANILIST_NAME, verdict.label)
            raise

        if resp.status_code == 429:
            try:
                retry_after = max(1, int(float(resp.headers.get("Retry-After", 10))))
            except (ValueError, TypeError):
                retry_after = 10
            if attempt < max_retries - 1:
                logger.warning(
                    "AniList rate limited (429). Retrying after %d seconds...",
                    retry_after,
                )
                await asyncio.sleep(retry_after)
                continue

        if 500 <= resp.status_code < 600 or resp.status_code in (
            520,
            521,
            522,
            523,
            524,
            525,
            526,
            530,
        ):
            try:
                body = resp.json()
                if (
                    isinstance(body, dict)
                    and isinstance(body.get("data"), dict)
                    and any(v is not None for v in body["data"].values())
                ):
                    return resp
            except Exception:
                pass
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.warning(
                    "AniList server error (%d). Retrying in %d seconds (attempt %d/%d)...",
                    resp.status_code,
                    wait_time,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(wait_time)
                continue

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            status_verdict = classify_http_status(resp.status_code)
            # 401 is a bad user token, not an outage; AniList signals a disabled
            # API with 403.
            if status_verdict and (
                status_verdict.kind == "outage" or resp.status_code == 403
            ):
                record_metadata_outage(ANILIST_NAME, status_verdict.label)
            raise
        clear_metadata_outage(ANILIST_NAME)

        # Pre-emptive sleep if remaining requests are very low
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining:
            try:
                if int(remaining) < 3:
                    logger.info(
                        "AniList rate limit running low, backing off pre-emptively for 3 seconds..."
                    )
                    await asyncio.sleep(3)
            except ValueError:
                pass

        return resp
    assert resp is not None
    return resp  # Should never reach here due to raise_for_status inside loop


async def get_anime_id_by_mal_id(mal_id: int) -> int | None:
    """Resolve an AniList Anime ID directly from a MyAnimeList (idMal) integer ID."""
    if not mal_id or int(mal_id) <= 0:
        return None
    query_str = """
    query ($idMal: Int) {
      Media(idMal: $idMal, type: ANIME) {
        id
        idMal
      }
    }
    """
    variables = {"idMal": int(mal_id)}
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            media = (resp.json().get("data") or {}).get("Media") or {}
            aid = media.get("id")
            return int(aid) if aid is not None else None
        except Exception as e:
            logger.error(
                "AniList get_anime_id_by_mal_id failed for MAL ID %d: %s",
                mal_id,
                _sanitize_error_message(e),
            )
            return None


async def get_anime_ids_by_mal_ids(mal_ids: list[int]) -> dict[int, int]:
    """Batch-resolve multiple MyAnimeList IDs (idMal_in) to AniList IDs in chunks of 50."""
    unique_ids = sorted({int(m) for m in mal_ids if m and int(m) > 0})
    if not unique_ids:
        return {}

    query_str = """
    query ($malIds: [Int]) {
      Page(page: 1, perPage: 50) {
        media(idMal_in: $malIds, type: ANIME) {
          id
          idMal
        }
      }
    }
    """
    mapping: dict[int, int] = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        for idx in range(0, len(unique_ids), 50):
            chunk = unique_ids[idx : idx + 50]
            try:
                resp = await _post_with_retry(
                    client, {"query": query_str, "variables": {"malIds": chunk}}
                )
                media_list = ((resp.json().get("data") or {}).get("Page") or {}).get(
                    "media"
                ) or []
                for node in media_list:
                    aid = node.get("id")
                    mid = node.get("idMal")
                    if aid is not None and mid is not None:
                        mapping[int(mid)] = int(aid)
            except Exception as e:
                logger.error(
                    "AniList get_anime_ids_by_mal_ids failed for chunk %s: %s",
                    chunk,
                    _sanitize_error_message(e),
                )
    return mapping


async def get_anime_details_batch_by_ids(
    anilist_ids: list[int],
) -> dict[int, dict[str, Any]]:
    """Batch-fetch airing status, episode count, and startDate for up to 50 AniList IDs per request."""
    unique_ids = sorted({int(a) for a in anilist_ids if a and int(a) > 0})
    if not unique_ids:
        return {}

    query_str = """
    query ($ids: [Int]) {
      Page(page: 1, perPage: 50) {
        media(id_in: $ids, type: ANIME) {
          id
          idMal
          format
          episodes
          status
          startDate { year month day }
          title { romaji english native }
        }
      }
    }
    """
    results: dict[int, dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        for idx in range(0, len(unique_ids), 50):
            chunk = unique_ids[idx : idx + 50]
            try:
                resp = await _post_with_retry(
                    client, {"query": query_str, "variables": {"ids": chunk}}
                )
                media_list = ((resp.json().get("data") or {}).get("Page") or {}).get(
                    "media"
                ) or []
                for node in media_list:
                    aid = node.get("id")
                    if aid is not None:
                        results[int(aid)] = node
            except Exception as e:
                logger.error(
                    "AniList get_anime_details_batch_by_ids failed for chunk %s: %s",
                    chunk,
                    _sanitize_error_message(e),
                )
    return results


async def search_anime_id_by_title(title: str, year: int | None = None) -> int | None:
    """
    Searches AniList for an Anime by title (and optionally matches year).
    Returns the matching AniList ID or None.
    """
    query_str = """
    query ($search: String) {
      Page(page: 1, perPage: 10) {
        media(search: $search, type: ANIME) {
          id
          title { romaji english native }
          startDate { year }
          format
        }
      }
    }
    """
    variables = {"search": title}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            data = resp.json()
            media_list = data.get("data", {}).get("Page", {}).get("media", [])
            if not media_list:
                return None
            if year:
                for item in media_list:
                    item_year = item.get("startDate", {}).get("year")
                    if item_year and item_year == year:
                        return item.get("id")
            return media_list[0].get("id")
        except Exception as e:
            logger.error(
                "AniList search_anime_id_by_title failed for '%s': %s",
                title,
                _sanitize_error_message(e),
            )
            return None


_MEDIA_FIELDS = """
  id
  title { romaji english native }
  synonyms
  startDate { year }
  format
  episodes
  status
  description(asHtml: false)
  coverImage { large }
  nextAiringEpisode { episode }
  idMal
"""


async def search_anime_media(search: str, per_page: int = 10) -> list[dict[str, Any]]:
    """Search AniList anime media by title. Returns an empty list on error."""
    query_str = f"""
    query ($search: String, $perPage: Int) {{
      Page(page: 1, perPage: $perPage) {{
        media(search: $search, type: ANIME) {{
          {_MEDIA_FIELDS}
        }}
      }}
    }}
    """
    variables = {"search": search, "perPage": per_page}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            media: list[dict[str, Any]] = (
                (resp.json().get("data") or {}).get("Page") or {}
            ).get("media") or []
            return media
        except Exception as e:
            logger.error(
                "AniList search_anime_media failed for '%s': %s",
                search,
                _sanitize_error_message(e),
            )
            return []


async def get_anime_media_by_id(anilist_id: int) -> dict[str, Any] | None:
    """Fetch one AniList anime media entry by ID. None on error or not found."""
    query_str = f"""
    query ($id: Int) {{
      Media(id: $id, type: ANIME) {{
        {_MEDIA_FIELDS}
      }}
    }}
    """
    variables = {"id": anilist_id}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            media: dict[str, Any] | None = (resp.json().get("data") or {}).get("Media")
            return media
        except Exception as e:
            logger.error(
                "AniList get_anime_media_by_id failed for ID %d: %s",
                anilist_id,
                _sanitize_error_message(e),
            )
            return None


async def get_anime_airing_schedule(anilist_id: int) -> list[tuple[int, datetime]]:
    """Return (episode_number, airing_at_utc) for every not-yet-aired episode, sorted by episode."""
    query_str = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        airingSchedule(notYetAired: true, perPage: 50) {
          nodes { episode airingAt }
        }
      }
    }
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": {"id": anilist_id}}
            )
            media = (resp.json().get("data") or {}).get("Media") or {}
            nodes = (media.get("airingSchedule") or {}).get("nodes") or []
        except Exception as e:
            logger.error(
                "AniList get_anime_airing_schedule failed for ID %d: %s",
                anilist_id,
                _sanitize_error_message(e),
            )
            return []

    schedule: list[tuple[int, datetime]] = []
    for node in nodes:
        try:
            episode, airing_at = node["episode"], node["airingAt"]
            if isinstance(episode, bool) or isinstance(airing_at, bool):
                continue
            schedule.append(
                (int(episode), datetime.fromtimestamp(int(airing_at), tz=timezone.utc))
            )
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
    return sorted(schedule, key=lambda entry: entry[0])


async def get_anime_aliases(anilist_id: int) -> list[str]:
    """Fetches title aliases for a specific AniList Anime ID."""
    query_str = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        title { romaji english native }
        synonyms
      }
    }
    """
    variables = {"id": anilist_id}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            data = resp.json().get("data", {}).get("Media") or {}

            titles = data.get("title") or {}
            aliases = set()
            for k in ("romaji", "english", "native"):
                if titles.get(k):
                    aliases.add(titles[k])

            for syn in data.get("synonyms") or []:
                if syn:
                    aliases.add(syn)

            return list(aliases)
        except Exception as e:
            logger.error(
                "AniList get_anime_aliases failed for ID %d: %s",
                anilist_id,
                _sanitize_error_message(e),
            )
            return []


async def get_anime_season_details(anilist_id: int) -> dict[str, Any] | None:
    """Fetches episode counts and airing info for a specific AniList Anime ID."""
    query_str = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        id
        title { romaji english native }
        episodes
        status
        startDate { year month day }
        endDate { year month day }
        synonyms
        idMal
      }
    }
    """
    variables = {"id": anilist_id}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await _post_with_retry(
                client, {"query": query_str, "variables": variables}
            )
            data = resp.json()
            return data.get("data", {}).get("Media")
        except Exception as e:
            logger.error(
                "AniList get_anime_season_details failed for ID %d: %s",
                anilist_id,
                _sanitize_error_message(e),
            )
            return None


async def get_anime_sequels(anilist_id: int) -> list[dict[str, Any]]:
    """Traverses SEQUEL relations to fetch all direct sequels recursively or just a flat list?
    Usually, we fetch the immediate SEQUEL, then fetch its SEQUEL, etc.
    To avoid many API calls, we can fetch recursively in GraphQL or just fetch immediate.
    We will just fetch the immediate sequels. We can recursively fetch in the caller or here.
    Let's fetch recursively here."""
    sequels = []
    current_id: int | None = anilist_id

    query_str = """

    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        relations {
          edges {
            relationType
            node {
              id
              type
              format
              episodes
              title { romaji english native }
              status
              startDate { year month day }
              idMal
            }
          }
        }
      }
    }
    """

    async with httpx.AsyncClient(timeout=10.0) as client:
        visited = {current_id}
        while current_id:
            try:
                variables = {"id": current_id}
                resp = await _post_with_retry(
                    client, {"query": query_str, "variables": variables}
                )
                data = resp.json()
                media = data.get("data", {}).get("Media")
                if not media:
                    break

                edges = media.get("relations", {}).get("edges", [])
                next_id = None
                for edge in edges:
                    if edge.get("relationType") == "SEQUEL":
                        node = edge.get("node")
                        # We only want TV/OVA/ONA formats, maybe MOVIE? Usually TV sequels are next seasons.
                        if (
                            node
                            and node.get("type") == "ANIME"
                            and node.get("format") in ("TV", "TV_SHORT", "OVA", "ONA")
                        ):
                            node_id = node.get("id")
                            if node_id not in visited:
                                sequels.append(node)
                                visited.add(node_id)
                                next_id = node_id
                                break  # just follow the first direct sequel

                current_id = next_id

            except Exception as e:
                logger.error(
                    "AniList get_anime_sequels failed for ID %s: %s",
                    current_id,
                    _sanitize_error_message(e),
                )
                break

    return sequels


async def get_anime_root_and_hierarchy(anilist_id: int) -> dict[str, Any] | None:
    """
    Traverses PREQUEL relations up to the root franchise anime, then traverses
    SEQUEL relations down to build the complete chronological hierarchy of
    both TV/ONA seasons and canonical MOVIE entries, bridging across OVAs/Specials.
    """
    query_str = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        id
        type
        format
        episodes
        title { romaji english native }
        status
        startDate { year month day }
        idMal
        relations {
          edges {
            relationType
            node {
              id
              type
              format
              episodes
              title { romaji english native }
              status
              startDate { year month day }
              idMal
            }
          }
        }
      }
    }
    """

    CANONICAL_FORMATS = {"TV", "TV_SHORT", "ONA", "MOVIE"}
    BRIDGE_FORMATS = {"TV", "TV_SHORT", "ONA", "OVA", "MOVIE", "SPECIAL", None}

    async with httpx.AsyncClient(timeout=10.0) as client:
        media_cache: dict[int, dict[str, Any]] = {}

        async def fetch_media(
            mid: int, *, has_fallback: bool = False
        ) -> dict[str, Any] | None:
            if mid in media_cache:
                return media_cache[mid]
            try:
                variables = {"id": mid}
                resp = await _post_with_retry(
                    client, {"query": query_str, "variables": variables}
                )
                data = resp.json()
                media_item = data.get("data", {}).get("Media")
                if media_item:
                    media_cache[mid] = media_item
                return media_item
            except Exception as e:
                err_msg = _sanitize_error_message(e)
                if has_fallback:
                    logger.warning(
                        "AniList fetch_media failed for ID %d (%s); using relation edge metadata fallback.",
                        mid,
                        err_msg,
                    )
                else:
                    logger.error(
                        "AniList fetch_media failed for ID %d: %s", mid, err_msg
                    )
                return None

        # Step 1: Traverse up to find root
        current_id: int | None = anilist_id
        root_node: dict[str, Any] | None = None
        visited_up: set[int] = set()
        pending_edge_node: dict[str, Any] | None = None

        while current_id and current_id not in visited_up:
            visited_up.add(current_id)
            media = await fetch_media(
                current_id, has_fallback=pending_edge_node is not None
            )
            if not media:
                if pending_edge_node is not None and (
                    pending_edge_node.get("format") in CANONICAL_FORMATS
                    or root_node is None
                ):
                    root_node = pending_edge_node
                    media_cache[current_id] = pending_edge_node
                break

            if media.get("format") in CANONICAL_FORMATS or root_node is None:
                root_node = media

            edges = media.get("relations", {}).get("edges", [])
            next_id = None
            next_edge_node: dict[str, Any] | None = None
            for edge in edges:
                if edge.get("relationType") in ("PREQUEL", "PARENT"):
                    node = edge.get("node")
                    if (
                        node
                        and node.get("type") == "ANIME"
                        and node.get("format") in BRIDGE_FORMATS
                    ):
                        node_id = node.get("id")
                        if node_id and node_id not in visited_up:
                            next_id = node_id
                            next_edge_node = node
                            break

            current_id = next_id
            pending_edge_node = next_edge_node

        if not root_node:
            return None

        # Step 2: Traverse down following SEQUELs across intermediate bridges
        hierarchy: list[dict[str, Any]] = []
        hierarchy_ids: set[int] = set()
        root_id = int(root_node["id"]) if root_node.get("id") else 0
        visited_down: set[int] = {root_id} if root_id else set()
        queue: list[dict[str, Any]] = [root_node]

        while queue:
            curr = queue.pop(0)
            curr_id = curr.get("id")
            if (
                (curr.get("format") in CANONICAL_FORMATS or curr.get("format") is None)
                and isinstance(curr_id, int)
                and curr_id not in hierarchy_ids
            ):
                hierarchy.append(curr)
                hierarchy_ids.add(curr_id)

            edges = curr.get("relations", {}).get("edges", [])
            # Collect outgoing sequels and bridge relations
            for edge in edges:
                rel = edge.get("relationType")
                node = edge.get("node")
                if not node or node.get("type") != "ANIME":
                    continue
                node_id = node.get("id")
                if not node_id or node_id in visited_down:
                    continue
                node_format = node.get("format")
                if node_format not in BRIDGE_FORMATS:
                    continue

                if rel == "SEQUEL":
                    visited_down.add(node_id)
                    child_media = await fetch_media(node_id, has_fallback=True) or node
                    queue.append(child_media)
                elif (
                    rel in ("SIDE_STORY", "ALTERNATIVE")
                    and curr.get("format") not in CANONICAL_FORMATS
                ):
                    visited_down.add(node_id)
                    child_media = await fetch_media(node_id, has_fallback=True) or node
                    queue.append(child_media)

        # Fallback for standalone OVA/SPECIAL anime with no TV/ONA/MOVIE relations
        if not hierarchy and root_node:
            hierarchy.append(root_node)

        # Sort hierarchy by start date to guarantee chronological order
        hierarchy.sort(
            key=lambda m: (
                (m.get("startDate") or {}).get("year") or 9999,
                (m.get("startDate") or {}).get("month") or 99,
                (m.get("startDate") or {}).get("day") or 99,
            )
        )

        season_counter = 0
        movie_counter = 0
        for idx, node in enumerate(hierarchy, start=1):
            fmt = str(node.get("format") or "TV").upper()
            if fmt == "MOVIE":
                entry_type = "movie"
                movie_counter += 1
                type_number = movie_counter
            else:
                entry_type = "season"
                season_counter += 1
                type_number = season_counter
            node["entry_type"] = entry_type
            node["watch_order"] = idx
            node["type_number"] = type_number

        canonical_root = hierarchy[0] if hierarchy else root_node
        return {"root": canonical_root, "hierarchy": hierarchy}


_USER_MEDIA_LIST_QUERY = """
query ($u: String) {
  MediaListCollection(userName: $u, type: ANIME) {
    lists {
      entries {
        status
        progress
        media {
          id
          idMal
          format
          title { romaji english }
          startDate { year }
          coverImage { large }
        }
      }
    }
  }
}
"""

_PRIVATE_PROFILE_MSG = "AniList profile not found or private — add an access token"


async def get_user_media_list(
    username: str, access_token: str | None = None
) -> list[dict[str, Any]]:
    """Fetch every anime list entry (all statuses) of an AniList user.

    Raises:
        WatchlistFetchError: profile not found/private, rate limited or HTTP failure.
    """
    from app.services.watchlist_providers import WatchlistFetchError

    headers = {"Authorization": f"Bearer {access_token}"} if access_token else None
    payload = {"query": _USER_MEDIA_LIST_QUERY, "variables": {"u": username}}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_with_retry(client, payload, extra_headers=headers)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in (403, 404):
            raise WatchlistFetchError(_PRIVATE_PROFILE_MSG) from exc
        if status == 429:
            retry = exc.response.headers.get("Retry-After", "unknown")
            raise WatchlistFetchError(
                f"AniList rate limited (Retry-After: {retry})"
            ) from exc
        raise WatchlistFetchError(f"AniList request failed (HTTP {status})") from exc
    except httpx.RequestError as exc:
        raise WatchlistFetchError(
            f"AniList request failed: {_sanitize_error_message(exc)}"
        ) from exc

    try:
        body = resp.json()
    except ValueError as exc:
        raise WatchlistFetchError("AniList returned an invalid response") from exc
    errors = body.get("errors") if isinstance(body, dict) else None
    if errors:
        raise WatchlistFetchError(_PRIVATE_PROFILE_MSG)
    collection = (body.get("data") or {}).get("MediaListCollection") or {}
    entries: list[dict[str, Any]] = []
    for lst in collection.get("lists") or []:
        entries.extend(e for e in (lst.get("entries") or []) if e.get("media"))
    return entries


_CHECK_USER_QUERY = "query ($u: String) { User(name: $u) { id name } }"


async def check_user(
    username: str, access_token: str | None = None
) -> tuple[bool, str]:
    """Diagnostics probe: does the AniList user exist (and is it readable)?"""
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else None
    payload = {"query": _CHECK_USER_QUERY, "variables": {"u": username}}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await _post_with_retry(client, payload, extra_headers=headers)
        body = resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (403, 404):
            return False, "AniList user not found or private"
        return False, f"HTTP {exc.response.status_code}"
    except (httpx.RequestError, ValueError) as exc:
        return False, f"AniList request failed: {_sanitize_error_message(exc)}"
    user = ((body.get("data") or {}).get("User")) if isinstance(body, dict) else None
    if not user or body.get("errors"):
        return False, "AniList user not found or private"
    return True, f"User: {user.get('name') or username}"
