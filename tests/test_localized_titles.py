from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from app.services import localized_titles
from app.services.localized_titles import get_localized_title

DETAILS: dict[str, Any] = {
    "title": "The Upside",
    "translations": {
        "translations": [
            {
                "iso_639_1": "de",
                "iso_3166_1": "DE",
                "data": {"title": "Mein bester & ich"},
            }
        ]
    },
}


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    localized_titles.clear_localized_title_cache()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "tmdb_id", "language", "display", "expected"),
    [
        ("movie", 1, "de", "The Upside", "Mein bester & ich"),
        ("movie", 1, "DE", "The Upside", "Mein bester & ich"),
        ("movie", 1, "en", "The Upside", None),
        ("movie", 1, None, "The Upside", None),
        ("movie", 1, "fr", "The Upside", None),
        ("movie", 1, "de", "Mein bester & ich", None),
        ("movie", None, "de", "The Upside", None),
    ],
)
async def test_get_localized_title(
    kind: str, tmdb_id: int | None, language: str | None, display: str, expected: Any
) -> None:
    with patch("app.services.tmdb.get_movie_details", AsyncMock(return_value=DETAILS)):
        assert await get_localized_title(kind, tmdb_id, language, display) == expected


@pytest.mark.anyio
async def test_tv_uses_name_field() -> None:
    payload = {
        "translations": {
            "translations": [{"iso_639_1": "de", "data": {"name": "Der Test"}}]
        }
    }
    with patch("app.services.tmdb.get_show_details", AsyncMock(return_value=payload)):
        assert await get_localized_title("anime", 5, "de", "The Test") == "Der Test"


@pytest.mark.anyio
async def test_cache_and_failures_not_cached() -> None:
    mock = AsyncMock(side_effect=[None, DETAILS])
    with patch("app.services.tmdb.get_movie_details", mock):
        assert await get_localized_title("movie", 1, "de", "The Upside") is None
        assert await get_localized_title("movie", 1, "de", "The Upside") is not None
        assert await get_localized_title("movie", 1, "de", "The Upside") is not None
    assert mock.await_count == 2
