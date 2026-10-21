from unittest.mock import AsyncMock, patch

import pytest

from app.services.anilist import get_anime_root_and_hierarchy


@pytest.mark.asyncio
async def test_franchise_traversal_bridges_intermediate_ovas():
    # Simulates To LOVE-Ru franchise graph:
    # S1 (TV, ID 3457, 26 eps, 2008)
    #   -> SEQUEL -> OVA1 (OVA, ID 5667, 6 eps, 2009)
    #       -> SEQUEL -> S2 (TV, ID 9181 Motto To LOVE-Ru, 12 eps, 2010)
    #           -> SEQUEL -> S3 (TV, ID 13663 To LOVE-Ru Darkness, 12 eps, 2012)
    #               -> SEQUEL -> OVA2 (OVA, ID 13959, 6 eps, 2012)
    #                   -> SEQUEL -> S4 (TV, ID 20999 To LOVE-Ru Darkness 2nd, 14 eps, 2015)

    nodes = {
        3457: {
            "id": 3457,
            "type": "ANIME",
            "format": "TV",
            "episodes": 26,
            "title": {"english": "To LOVE-Ru", "romaji": "To LOVE-Ru"},
            "startDate": {"year": 2008, "month": 4, "day": 4},
            "idMal": 3457,
            "relations": {
                "edges": [
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 5667, "type": "ANIME", "format": "OVA"},
                    }
                ]
            },
        },
        5667: {
            "id": 5667,
            "type": "ANIME",
            "format": "OVA",
            "episodes": 6,
            "title": {"english": "To LOVE-Ru OVA", "romaji": "To LOVE-Ru OVA"},
            "startDate": {"year": 2009, "month": 4, "day": 3},
            "idMal": 5667,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 3457, "type": "ANIME", "format": "TV"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 9181, "type": "ANIME", "format": "TV"},
                    },
                ]
            },
        },
        9181: {
            "id": 9181,
            "type": "ANIME",
            "format": "TV",
            "episodes": 12,
            "title": {"english": "Motto To LOVE-Ru", "romaji": "Motto To LOVE-Ru"},
            "startDate": {"year": 2010, "month": 10, "day": 6},
            "idMal": 9181,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 5667, "type": "ANIME", "format": "OVA"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 13663, "type": "ANIME", "format": "TV"},
                    },
                ]
            },
        },
        13663: {
            "id": 13663,
            "type": "ANIME",
            "format": "TV",
            "episodes": 12,
            "title": {
                "english": "To LOVE-Ru Darkness",
                "romaji": "To LOVE-Ru Darkness",
            },
            "startDate": {"year": 2012, "month": 10, "day": 6},
            "idMal": 13663,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 9181, "type": "ANIME", "format": "TV"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 13959, "type": "ANIME", "format": "OVA"},
                    },
                ]
            },
        },
        13959: {
            "id": 13959,
            "type": "ANIME",
            "format": "OVA",
            "episodes": 6,
            "title": {
                "english": "To LOVE-Ru Darkness OVA",
                "romaji": "To LOVE-Ru Darkness OVA",
            },
            "startDate": {"year": 2012, "month": 8, "day": 17},
            "idMal": 13959,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 13663, "type": "ANIME", "format": "TV"},
                    },
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 20999, "type": "ANIME", "format": "TV"},
                    },
                ]
            },
        },
        20999: {
            "id": 20999,
            "type": "ANIME",
            "format": "TV",
            "episodes": 14,
            "title": {
                "english": "To LOVE-Ru Darkness 2nd",
                "romaji": "To LOVE-Ru Darkness 2nd",
            },
            "startDate": {"year": 2015, "month": 7, "day": 7},
            "idMal": 20999,
            "relations": {
                "edges": [
                    {
                        "relationType": "PREQUEL",
                        "node": {"id": 13959, "type": "ANIME", "format": "OVA"},
                    }
                ]
            },
        },
    }

    async def mock_post(client, payload):
        from unittest.mock import MagicMock

        variables = payload.get("variables", {})
        node_id = variables.get("id")
        data = {"data": {"Media": nodes.get(node_id)}}
        resp = MagicMock()
        resp.json.return_value = data
        return resp

    with patch("app.services.anilist._post_with_retry", side_effect=mock_post):
        # 1. Starting from Season 1 (ID 3457)
        res = await get_anime_root_and_hierarchy(3457)
        assert res is not None
        assert res["root"]["id"] == 3457
        assert len(res["hierarchy"]) == 4

        # Exactly 4 TV seasons in order
        h_ids = [m["id"] for m in res["hierarchy"]]
        assert h_ids == [3457, 9181, 13663, 20999]

        # 2. Starting out-of-order from Season 3 (ID 13663)
        res_from_s3 = await get_anime_root_and_hierarchy(13663)
        assert res_from_s3 is not None
        assert res_from_s3["root"]["id"] == 3457
        assert [m["id"] for m in res_from_s3["hierarchy"]] == [
            3457,
            9181,
            13663,
            20999,
        ]


@pytest.mark.asyncio
async def test_franchise_traversal_circular_detection():
    # Circular graph where Node 1 and Node 2 point to each other
    nodes = {
        100: {
            "id": 100,
            "type": "ANIME",
            "format": "TV",
            "title": {"english": "Loop S1", "romaji": "Loop S1"},
            "startDate": {"year": 2020},
            "relations": {
                "edges": [
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 200, "type": "ANIME", "format": "TV"},
                    }
                ]
            },
        },
        200: {
            "id": 200,
            "type": "ANIME",
            "format": "TV",
            "title": {"english": "Loop S2", "romaji": "Loop S2"},
            "startDate": {"year": 2021},
            "relations": {
                "edges": [
                    {
                        "relationType": "SEQUEL",
                        "node": {"id": 100, "type": "ANIME", "format": "TV"},
                    }
                ]
            },
        },
    }

    async def mock_post(client, payload):
        from unittest.mock import MagicMock

        variables = payload.get("variables", {})
        node_id = variables.get("id")
        data = {"data": {"Media": nodes.get(node_id)}}
        resp = MagicMock()
        resp.json.return_value = data
        return resp

    with patch("app.services.anilist._post_with_retry", side_effect=mock_post):
        res = await get_anime_root_and_hierarchy(100)
        assert res is not None
        assert len(res["hierarchy"]) == 2
        assert [m["id"] for m in res["hierarchy"]] == [100, 200]


@pytest.mark.asyncio
async def test_franchise_traversal_standalone():
    nodes = {
        500: {
            "id": 500,
            "type": "ANIME",
            "format": "TV",
            "title": {"english": "Solo Series", "romaji": "Solo Series"},
            "startDate": {"year": 2023},
            "relations": {"edges": []},
        }
    }

    async def mock_post(client, payload):
        from unittest.mock import MagicMock

        variables = payload.get("variables", {})
        node_id = variables.get("id")
        data = {"data": {"Media": nodes.get(node_id)}}
        resp = MagicMock()
        resp.json.return_value = data
        return resp

    with patch("app.services.anilist._post_with_retry", side_effect=mock_post):
        res = await get_anime_root_and_hierarchy(500)
        assert res is not None
        assert len(res["hierarchy"]) == 1
        assert res["hierarchy"][0]["id"] == 500


@pytest.mark.asyncio
async def test_post_with_retry_retries_on_500_and_request_error():
    import httpx

    from app.services.anilist import ANILIST_URL, _post_with_retry

    request = httpx.Request("POST", ANILIST_URL)
    resp_500 = httpx.Response(
        500,
        request=request,
        text="<html><body>500 Internal Server Error</body></html>",
    )
    resp_200 = httpx.Response(
        200,
        request=request,
        json={
            "data": {
                "Media": {"id": 11741, "title": {"romaji": "Fate/Zero 2nd Season"}}
            }
        },
    )

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post = AsyncMock(
        side_effect=[
            httpx.ReadTimeout("Read timed out", request=request),
            resp_500,
            resp_200,
        ]
    )

    with (
        patch("app.services.anilist.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        patch("app.services.anilist._limiter.wait", new=AsyncMock()),
    ):
        result = await _post_with_retry(
            mock_client,
            {"query": "query {}", "variables": {"id": 11741}},
            max_retries=3,
        )

    assert result.status_code == 200
    assert mock_client.post.await_count == 3
    assert mock_sleep.await_count == 2


@pytest.mark.asyncio
async def test_fetch_media_500_exhaustion_sanitizes_mozilla_url_and_uses_edge_fallback():
    import logging

    import httpx

    from app.services.anilist import ANILIST_URL

    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _ListHandler(level=logging.WARNING)
    al_logger = logging.getLogger("app.services.anilist")
    al_logger.addHandler(handler)

    request = httpx.Request("POST", ANILIST_URL)
    node_10087 = {
        "id": 10087,
        "type": "ANIME",
        "format": "TV",
        "episodes": 13,
        "title": {"romaji": "Fate/Zero", "english": "Fate/Zero"},
        "status": "FINISHED",
        "startDate": {"year": 2011, "month": 10, "day": 2},
        "idMal": 10087,
        "relations": {
            "edges": [
                {
                    "relationType": "SEQUEL",
                    "node": {
                        "id": 11741,
                        "type": "ANIME",
                        "format": "TV",
                        "episodes": 12,
                        "title": {
                            "romaji": "Fate/Zero 2nd Season",
                            "english": "Fate/Zero Season 2",
                        },
                        "status": "FINISHED",
                        "startDate": {"year": 2012, "month": 4, "day": 8},
                        "idMal": 11741,
                    },
                }
            ]
        },
    }

    async def fake_post(url, json=None, headers=None):
        mid = (json or {}).get("variables", {}).get("id")
        if mid == 10087:
            return httpx.Response(
                200, request=request, json={"data": {"Media": node_10087}}
            )
        return httpx.Response(500, request=request, text="Internal Server Error")

    try:
        with (
            patch("httpx.AsyncClient.post", side_effect=fake_post),
            patch("app.services.anilist.asyncio.sleep", new=AsyncMock()),
            patch("app.services.anilist._limiter.wait", new=AsyncMock()),
        ):
            # 1. Traversal from 10087 where sequel 11741 fails with 500 on all retries:
            # Should fall back to edge node metadata and only log a warning (no ERROR alert)
            res = await get_anime_root_and_hierarchy(10087)
            assert res is not None
            assert [m["id"] for m in res["hierarchy"]] == [10087, 11741]
            assert res["hierarchy"][1]["episodes"] == 12
            assert res["hierarchy"][1]["title"]["english"] == "Fate/Zero Season 2"
            error_records = [r for r in records if r.levelno >= logging.ERROR]
            assert len(error_records) == 0

            # 2. Direct call on 11741 where root node itself fails on all retries:
            # Logs a single-line sanitized message without developer.mozilla.org
            # boilerplate; the 500s flag an AniList outage, so it is downgraded
            # to WARNING (the outage alert replaces the per-request ERROR alert).
            records.clear()
            res_direct = await get_anime_root_and_hierarchy(11741)
            assert res_direct is None
            failures = [r for r in records if "11741" in r.getMessage()]
            assert len(failures) == 1
            assert failures[0].levelno == logging.WARNING
            assert "developer.mozilla.org" not in failures[0].getMessage()
    finally:
        al_logger.removeHandler(handler)
