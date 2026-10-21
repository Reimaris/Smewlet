import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.outage import (
    active_metadata_outages,
    clear_outages,
    has_active_outage,
    outage_banner_context,
)
from app.main import app
from app.services import anilist

client = TestClient(app)


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch):
    async def _noop() -> None:
        return None

    monkeypatch.setattr(anilist._limiter, "wait", _noop)
    clear_outages()
    yield
    clear_outages()


async def _post(status: int) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status, json={"data": {"Page": {"media": []}}})
    )
    async with httpx.AsyncClient(transport=transport) as c:
        await anilist._post_with_retry(c, {"query": "{}"}, max_retries=1)


@pytest.mark.asyncio
async def test_403_flags_banner_without_pausing_pushes():
    with pytest.raises(httpx.HTTPStatusError):
        await _post(403)
    assert active_metadata_outages() == [("AniList", "HTTP 403")]
    assert not has_active_outage()
    ctx = outage_banner_context()
    assert ctx is not None and ctx["kind"] == "metadata"
    html = client.get("/api/outages/banner").text
    assert 'data-outage-kind="metadata"' in html
    assert "AniList returned HTTP 403" in html
    assert "Resume Queue" not in html


@pytest.mark.asyncio
async def test_success_clears_banner():
    with pytest.raises(httpx.HTTPStatusError):
        await _post(403)
    await _post(200)
    assert active_metadata_outages() == []
    assert outage_banner_context() is None


@pytest.mark.asyncio
async def test_404_is_not_an_outage():
    with pytest.raises(httpx.HTTPStatusError):
        await _post(404)
    assert active_metadata_outages() == []
