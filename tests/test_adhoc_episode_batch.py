"""Ad-hoc episode batch push (m85, ADR-186)."""

from datetime import date
from typing import Any

import pytest

from app.core import adhoc_batch
from app.core.adhoc_batch import (
    has_in_flight_adhoc_push,
    run_episode_batch,
    select_batch_episodes,
)
from app.core.adhoc_search import AdHocFilters, AdHocPushOutcome, AdHocSearchResult
from tests.conftest import make_delivery_provider, make_history

TODAY = date(2026, 10, 24)
CANDIDATE = {
    "title": "Test.Show.S01E01.1080p.WEB-DL",
    "link": "https://indexer.example.com/getnzb/1",
    "guid": "guid-1",
    "score": 8500.0,
    "size": 1500000000,
}


def _eps(*air_dates: str | None) -> list[Any]:
    return [
        {"episode_number": i, "name": f"Ep {i}", "air_date": d}
        for i, d in enumerate(air_dates, start=1)
    ]


def test_select_batch_episodes_skips_unaired_keeps_undated() -> None:
    aired, unaired = select_batch_episodes(
        _eps("2020-01-01", "2999-01-01", None), TODAY
    )
    assert [e["episode_number"] for e in aired] == [1, 3]
    assert unaired == 1


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"status_detail": "downloading"}, True),
        ({"status_detail": "completed", "delivery_status": "none"}, False),
        (
            {
                "status_detail": "completed",
                "deliver_enabled": True,
                "delivery_status": "delivering",
            },
            True,
        ),
        ({"status_detail": "downloading", "is_dismissed": True}, False),
        ({"status_detail": "downloading", "target_label": "S01E02"}, False),
    ],
)
async def test_has_in_flight(db_session: Any, overrides: dict, expected: bool) -> None:
    base = dict(
        is_ad_hoc=True,
        media_title="Test Show",
        target_label="S01E01",
        is_dismissed=False,
    )
    await make_history(db_session, **{**base, **overrides})
    await db_session.commit()
    assert (
        await has_in_flight_adhoc_push(
            db_session,
            tmdb_id=None,
            anilist_id=None,
            media_title="Test Show",
            target_label="S01E01",
        )
        is expected
    )


@pytest.fixture
def batch_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    env: dict[str, Any] = {
        "searches": [],
        "pushes": [],
        "episodes": {1: _eps("2020-01-01", "2020-01-08", "2999-01-01"), 2: _eps(None)},
        "no_release": set(),
        "push_outcome": AdHocPushOutcome(True, "ok"),
    }

    async def fake_seasons(*a: Any, **k: Any) -> list[Any]:
        return [{"season_number": 2}, {"season_number": 1}]

    async def fake_episodes(
        provider: str, ext_id: int, kind: str, season: int, s: Any = None
    ) -> list[Any]:
        return env["episodes"][season]

    async def fake_search(**kw: Any) -> AdHocSearchResult:
        env["searches"].append((kw["scope"], kw["season"], kw["episode"]))
        if (kw["season"], kw["episode"]) in env["no_release"]:
            return AdHocSearchResult([], [], [])
        return AdHocSearchResult([CANDIDATE], [], [])

    async def fake_push(session: Any, **kw: Any) -> AdHocPushOutcome:
        env["pushes"].append(kw)
        return env["push_outcome"]

    monkeypatch.setattr(adhoc_batch, "list_seasons", fake_seasons)
    monkeypatch.setattr(adhoc_batch, "list_episodes", fake_episodes)
    monkeypatch.setattr(adhoc_batch, "search_and_score_releases", fake_search)
    monkeypatch.setattr(adhoc_batch, "push_adhoc_release", fake_push)
    return env


def _run(
    batch_scope: str = "all", season: int | None = None, deliver: bool | None = None
):
    return run_episode_batch(
        provider="tmdb",
        ext_id=1396,
        kind="series",
        batch_scope=batch_scope,
        season=season,
        titles=["Test Show"],
        year=2020,
        imdb_id=None,
        tmdb_id=1396,
        tvdb_id=None,
        anilist_id=None,
        poster_url=None,
        media_title="Test Show",
        filters=AdHocFilters(),
        limit=10,
        deliver=deliver,
    )


async def test_batch_all_runs_seasons_ascending_episode_scope(batch_env: dict) -> None:
    summary = await _run(deliver=True)
    assert batch_env["searches"] == [
        ("episode", 1, 1),
        ("episode", 1, 2),
        ("episode", 2, 1),
    ]
    assert summary.pushed == ["S01E01", "S01E02", "S02E01"]
    assert summary.skipped_unaired == 1
    assert all(p["deliver"] is True for p in batch_env["pushes"])


async def test_batch_single_season(batch_env: dict) -> None:
    summary = await _run("season", 2)
    assert summary.pushed == ["S02E01"]


async def test_batch_continues_on_no_release_and_skips_in_flight(
    batch_env: dict, db_session: Any
) -> None:
    batch_env["no_release"].add((1, 1))
    await make_history(
        db_session,
        is_ad_hoc=True,
        media_title="Test Show",
        tmdb_id=1396,
        target_label="S01E02",
        status_detail="downloading",
        is_dismissed=False,
    )
    await db_session.commit()
    summary = await _run()
    assert summary.no_release == ["S01E01"]
    assert summary.skipped_in_flight == ["S01E02"]
    assert summary.pushed == ["S02E01"]


async def test_batch_aborts_on_hard_failure(batch_env: dict) -> None:
    batch_env["push_outcome"] = AdHocPushOutcome(
        False, "Invalid API key", 401, hard_failure=True
    )
    summary = await _run()
    assert summary.aborted_reason == "Invalid API key"
    assert len(batch_env["searches"]) == 1


BATCH_FORM = {
    "batch_scope": "season",
    "season": "1",
    "provider": "tmdb",
    "ext_id": "1396",
    "kind": "series",
    "title": "Test Show",
}


async def test_endpoint_starts_batch(
    async_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    async def fake_run(**kw: Any) -> None:
        calls.append(kw)

    monkeypatch.setattr(adhoc_batch, "run_episode_batch", fake_run)
    resp = await async_client.post("/api/search/manual/batch-push", data=BATCH_FORM)
    assert resp.status_code == 200
    assert resp.headers["X-Status-Span"] == "1"
    assert "smewlet:batch-dispatched" in resp.headers["HX-Trigger"]
    import asyncio

    await asyncio.sleep(0)
    assert calls and calls[0]["season"] == 1 and calls[0]["tmdb_id"] == 1396


@pytest.mark.parametrize(
    "override",
    [{"batch_scope": "bogus"}, {"season": ""}, {"kind": "movie"}, {"title": " "}],
)
async def test_endpoint_validation(async_client: Any, override: dict) -> None:
    resp = await async_client.post(
        "/api/search/manual/batch-push", data={**BATCH_FORM, **override}
    )
    assert resp.status_code == 400


async def test_season_rows_have_batch_button(
    async_client: Any, db_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_search

    async def fake_seasons(*a: Any, **k: Any) -> list[Any]:
        return [
            media_search.SeasonSummary(
                season_number=1, name="Season 1", episode_count=3, air_year=2020
            )
        ]

    monkeypatch.setattr(media_search, "list_seasons", fake_seasons)
    await make_delivery_provider(db_session)
    await db_session.commit()
    resp = await async_client.get(
        "/api/search/manual/media/tmdb/1396/seasons?kind=series&title=Test+Show"
    )
    assert "Push Season (episodes)" in resp.text
    assert "batch_scope" in resp.text


async def test_batch_continues_after_per_release_failure(batch_env: dict) -> None:
    batch_env["push_outcome"] = AdHocPushOutcome(False, "NZB Download Error: dead", 400)
    summary = await _run()
    assert summary.aborted_reason is None
    assert summary.no_release == ["S01E01", "S01E02", "S02E01"]


async def test_batch_counts_queued_pushes_separately(batch_env: dict) -> None:
    batch_env["push_outcome"] = AdHocPushOutcome(True, "queued", 200, queued=True)
    summary = await _run(batch_scope="season", season=2)
    assert summary.pushed == []
    assert summary.queued == ["S02E01"]
