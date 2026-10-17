"""Classify the files of a finished release into videos, subtitles and ignorable leftovers."""

from __future__ import annotations

import posixpath
import re
from collections.abc import Sequence
from dataclasses import dataclass

from app.services.downloaders.base import DownloaderFile

VIDEO_EXTENSIONS: frozenset[str] = frozenset({".mkv", ".mp4", ".avi", ".m4v", ".ts"})
SUBTITLE_EXTENSIONS: frozenset[str] = frozenset(
    {".srt", ".ass", ".ssa", ".sub", ".idx"}
)

_LANGUAGE_RE = re.compile(r"[a-z]{2,3}")


@dataclass(frozen=True)
class ClassifiedFile:
    file: DownloaderFile
    kind: str  # "video" | "subtitle" | "ignore"
    is_sample: bool
    language: str | None  # subtitle language suffix ("de", "en"), None when unknown


def _basename(name: str) -> str:
    return posixpath.basename(name.replace("\\", "/"))


def _extension(name: str) -> str:
    return posixpath.splitext(_basename(name))[1].lower()


def subtitle_language(name: str) -> str | None:
    """Return the language suffix before the extension (``Movie.de.srt`` -> ``de``)."""
    stem = posixpath.splitext(_basename(name))[0]
    _, dot, suffix = stem.rpartition(".")
    suffix = suffix.lower()
    if dot and _LANGUAGE_RE.fullmatch(suffix):
        return suffix
    return None


def classify_files(
    files: Sequence[DownloaderFile], *, is_movie: bool, min_video_bytes: int
) -> list[ClassifiedFile]:
    """Classify ``files``; for movies only the largest non-sample video stays a video."""
    result: list[ClassifiedFile] = []
    for f in files:
        ext = _extension(f.name)
        if ext in VIDEO_EXTENSIONS:
            is_sample = "sample" in f.name.lower() or f.size_bytes < min_video_bytes
            result.append(
                ClassifiedFile(f, "ignore" if is_sample else "video", is_sample, None)
            )
        elif ext in SUBTITLE_EXTENSIONS:
            result.append(
                ClassifiedFile(f, "subtitle", False, subtitle_language(f.name))
            )
        else:
            result.append(ClassifiedFile(f, "ignore", False, None))

    if is_movie:
        videos = [c for c in result if c.kind == "video"]
        if len(videos) > 1:
            keep = max(videos, key=lambda c: c.file.size_bytes)
            result = [
                ClassifiedFile(c.file, "ignore", c.is_sample, None)
                if c.kind == "video" and c is not keep
                else c
                for c in result
            ]
    return result
