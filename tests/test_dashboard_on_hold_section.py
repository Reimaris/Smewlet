"""Dashboard Section 4: Movies tab shows only a paused-list 'On Hold' section."""

import html as html_lib

import pytest

from tests.conftest import (
    make_movie,
    make_source,
    make_watchlist_provider,
    seed_ready_setup,
)

pytestmark = pytest.mark.anyio


async def test_on_hold_section_movie_tab_expressions(db_session, async_client):
    await seed_ready_setup(db_session)
    provider = await make_watchlist_provider(db_session, "simkl")
    movie = await make_movie(db_session, provider_id=provider.id)
    await make_source(db_session, movie, provider, "on_hold", "h")
    await db_session.commit()

    html = html_lib.unescape((await async_client.get("/")).text)
    assert "hasTierItems(tier)" in html
    assert "x-show=\"tab !== 'movie' || hasTierItems('on_hold')\"" in html
    assert (
        "x-text=\"tab === 'movie' ? 'On Hold' : 'On Hold & Awaiting Seasons'\"" in html
    )
    assert "'(Paused lists)'" in html
    assert "video-table-body-on-hold" in html
