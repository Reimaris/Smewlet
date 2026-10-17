"""File classification and subtitle naming for local directory delivery (m78)."""

from __future__ import annotations

import pytest

from app.services.delivery.filtering import classify_files, subtitle_language
from app.services.downloaders.base import DownloaderFile

MB = 1024 * 1024
MOVIE_MIN = 150 * MB
EPISODE_MIN = 25 * MB


def _kinds(files: list[DownloaderFile], **kw: object) -> dict[str, str]:
    return {
        c.file.name: c.kind
        for c in classify_files(files, **kw)  # type: ignore[arg-type]
    }


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("a.nfo", "ignore"),
        ("a.txt", "ignore"),
        ("a.url", "ignore"),
        ("a.exe", "ignore"),
        ("a.rar", "ignore"),
        ("a.m4v", "video"),
        ("a.TS", "video"),
        ("a.mkv", "video"),
        ("a.srt", "subtitle"),
        ("a.idx", "subtitle"),
        ("a.sub", "subtitle"),
    ],
)
def test_extension_classification(name: str, kind: str) -> None:
    result = classify_files(
        [DownloaderFile(name, 500 * MB)], is_movie=False, min_video_bytes=EPISODE_MIN
    )
    assert result[0].kind == kind


def test_sample_by_name_and_by_size() -> None:
    files = [
        DownloaderFile("Example.Sample.mkv", 900 * MB),
        DownloaderFile("tiny.mkv", 10 * MB),
        DownloaderFile("Sample/real.mkv", 900 * MB),
        DownloaderFile("real.mkv", 900 * MB),
    ]
    got = classify_files(files, is_movie=False, min_video_bytes=EPISODE_MIN)
    assert [(c.is_sample, c.kind) for c in got] == [
        (True, "ignore"),
        (True, "ignore"),
        (True, "ignore"),
        (False, "video"),
    ]


def test_movie_keeps_only_largest_video() -> None:
    files = [
        DownloaderFile("small.mkv", 200 * MB),
        DownloaderFile("big.mkv", 4000 * MB),
        DownloaderFile("big.de.srt", 1),
    ]
    assert _kinds(files, is_movie=True, min_video_bytes=MOVIE_MIN) == {
        "small.mkv": "ignore",
        "big.mkv": "video",
        "big.de.srt": "subtitle",
    }
    assert (
        _kinds(files, is_movie=False, min_video_bytes=MOVIE_MIN)["small.mkv"] == "video"
    )


def test_idx_sub_pair_are_subtitles() -> None:
    files = [DownloaderFile("m.idx", 10), DownloaderFile("m.sub", 5 * MB)]
    assert set(_kinds(files, is_movie=True, min_video_bytes=MOVIE_MIN).values()) == {
        "subtitle"
    }


@pytest.mark.parametrize(
    ("name", "lang"),
    [
        ("Movie.de.srt", "de"),
        ("Movie.EN.srt", "en"),
        ("Movie.srt", None),
        ("Show.S01E02.srt", None),
        ("Subs/Movie.eng.ass", "eng"),
        ("Movie.2024.srt", None),
    ],
)
def test_subtitle_language(name: str, lang: str | None) -> None:
    assert subtitle_language(name) == lang
