"""Active Pushes badge reflects the downloader's transfer phase, not always 'Downloading'."""

import pytest

from app.core.transfer_poller import get_active_pushes
from app.db.models import MediaStatus
from tests.conftest import make_history, make_movie


@pytest.mark.parametrize(
    ("status_detail", "badge"),
    [
        ("downloading", "Downloading"),
        ("processing", "Processing"),
        ("unpacking", "Unpacking"),
        ("verifying", "Verifying"),
        ("queued", "Queued"),
    ],
)
async def test_badge_matches_transfer_phase(db_session, status_detail, badge):
    movie = await make_movie(db_session, status=MediaStatus.DOWNLOADING)
    hist = await make_history(
        db_session,
        item=movie,
        status_detail=status_detail,
        is_dismissed=False,
        downloader_transfer_id="tb-1",
    )
    await db_session.commit()
    push = next(
        p for p in await get_active_pushes(db_session) if p["history_id"] == hist.id
    )
    assert push["status"] == badge
