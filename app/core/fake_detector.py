import html
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Layer 1 Conservative Size Lower Bounds (Zero-False-Positive Principle)
MIN_EPISODE_BYTES = 50 * 1024 * 1024  # 50MB (protects anime shorts and mini-episodes)
MIN_MOVIE_BYTES = 300 * 1024 * 1024  # 300MB
MIN_4K_MOVIE_BYTES = int(1.5 * 1024 * 1024 * 1024)  # 1.5GB


# Fake/Executable extensions
BAD_EXTENSIONS = {
    ".exe",
    ".scr",
    ".bat",
    ".vbs",
    ".iso",
    ".msi",
    ".cmd",
    ".com",
    ".sh",
    ".bin",
    ".jar",
    ".apk",
    ".lnk",
    ".dll",
    ".ps1",
}
PHISHING_FILES = {"password.txt", "pass.txt", "instructions.txt", "readme_first.txt"}
VIDEO_ARCHIVE_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".avi",
    ".ts",
    ".m4v",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".mpg",
    ".mpeg",
    ".vob",
    ".iso",
    ".rar",
    ".r00",
    ".r01",
    ".part01.rar",
    ".part1.rar",
    ".7z",
    ".zip",
    ".par2",
}

PLAYABLE_VIDEO_EXTENSIONS = (
    ".mkv",
    ".mp4",
    ".avi",
    ".ts",
    ".m2ts",
    ".webm",
    ".mov",
    ".wmv",
    ".flv",
    ".mpg",
    ".mpeg",
    ".m4v",
    ".vob",
)

_SUBJECT_VIDEO_RE = re.compile(
    r"(?P<stem>[^\"<>|?*]+?)(?P<ext>\.(?:mkv|mp4|avi|ts|m2ts|webm|mov|wmv|flv|mpg|mpeg|m4v|vob))(?=$|[\"\s)\]])",
    re.IGNORECASE,
)
_SUBJECT_ARCHIVE_RE = re.compile(
    r"(?P<stem>[^\"<>|?*]+?)(?P<ext>\.(?:part\d+\.rar|r\d{2}|rar|7z|zip|iso|0\d{2}))(?=$|[\"\s)\]])",
    re.IGNORECASE,
)


def _bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / (1024 * 1024)


def is_indexer_metadata_fake(
    raw_item: dict[str, Any],
    parsed_resolution: str | None = None,
    media_type: str = "movie",
    is_season_pack: bool = False,
) -> tuple[bool, str | None]:
    """Layer 1: Evaluates raw indexer metadata prior to release scoring.

    Flags releases if password-protected, pointing to a password URL,
    or below conservative size lower bounds (protecting against junk/stub releases).
    """
    title = str(raw_item.get("title") or "unknown")

    def _ret(is_fake: bool, reason: str | None) -> tuple[bool, str | None]:
        if is_fake:
            logger.debug("[FAKEDETECT] [L1] Release '%s' rejected: %s", title, reason)
        else:
            logger.debug(
                "[FAKEDETECT] [L1] Release '%s' passed Layer 1 inspection.", title
            )
        return is_fake, reason

    # 1. Password checks
    raw_pwd = raw_item.get("password")
    try:
        pwd = int(raw_pwd or 0)
    except (ValueError, TypeError):
        pwd = 0
    if pwd > 0:
        return _ret(True, "Indexer reports release is password protected")

    raw_pwdurl = str(raw_item.get("passwordurl") or "").strip()
    if raw_pwdurl:
        return _ret(True, "Indexer reports password URL")

    # 2. Conservative size lower bounds (skip if size missing or <= 0)
    try:
        size_bytes = int(raw_item.get("size") or raw_item.get("size_bytes") or 0)
    except (ValueError, TypeError):
        size_bytes = 0

    if size_bytes <= 0:
        return _ret(False, None)

    mtype = (
        media_type.value
        if hasattr(media_type, "value")
        else str(media_type or "").lower()
    )

    if mtype in ("movie", "anime-movie") and not is_season_pack:
        if parsed_resolution == "2160p":
            if size_bytes < MIN_4K_MOVIE_BYTES:
                return _ret(
                    True,
                    f"Release size ({_bytes_to_mb(size_bytes):.1f}MB) is below 4K movie threshold (1.5GB)",
                )
        else:
            if size_bytes < MIN_MOVIE_BYTES:
                return _ret(
                    True,
                    f"Release size ({_bytes_to_mb(size_bytes):.1f}MB) is below movie threshold (300MB)",
                )
    else:
        # Episodes, anime, and season packs
        if size_bytes < MIN_EPISODE_BYTES:
            return _ret(
                True,
                f"Release size ({_bytes_to_mb(size_bytes):.1f}MB) is below episode threshold (50MB)",
            )

    return _ret(False, None)


def _is_sample_stem(raw_stem: str) -> bool:
    """Return True if a file path/name stem (without its video/archive extension) denotes a sample file."""
    normalized = raw_stem.lower().strip().replace("\\", "/")
    normalized = re.sub(
        r"^(?:\s*(?:\[\d+/\d+\]|\(\d+/\d+\))\s*[-:]?\s*)+", "", normalized
    )
    parts = [p.strip() for p in normalized.split("/") if p.strip()]
    if not parts:
        return False

    if any(part in ("sample", "samples") for part in parts[:-1]):
        return True

    stem = parts[-1]
    tokens = stem.split()
    last_token = tokens[-1] if tokens else stem

    if stem == "sample" or last_token == "sample":
        return True
    if stem.endswith(
        (".sample", "-sample", "_sample", " sample")
    ) or last_token.endswith((".sample", "-sample", "_sample")):
        return True
    if stem.startswith(("sample-", "sample_")) or last_token.startswith(
        ("sample-", "sample_")
    ):
        return True
    if re.search(r"[._\-]sample[._\-]", stem):
        return True
    return False


def _is_sample_video_filename(raw_name: str) -> bool:
    """Return True if *raw_name* is a playable video filename that represents a sample clip."""
    fname = raw_name.lower().strip()
    for vid_ext in PLAYABLE_VIDEO_EXTENSIONS:
        if fname.endswith(vid_ext):
            return _is_sample_stem(raw_name[: -len(vid_ext)])
    return False


# Layer 2 Conservative Segment Payload Lower Bounds
MIN_SEGMENT_MOVIE_BYTES = 30 * 1024 * 1024  # 30MB
MIN_SEGMENT_EPISODE_BYTES = 10 * 1024 * 1024  # 10MB

_FALSEY_PASSWORDS = {"", "0", "no", "none", "false", "n/a"}


def extract_nzb_password(nzb_xml_bytes: bytes) -> str | None:
    """Extract embedded archive password from <meta type="password">...</meta> in NZB header.

    Returns the stripped password string if present and not falsey ('0', 'no', 'none', 'false', 'n/a', '').
    Returns None if no password tag is present or if the value is falsey.
    """
    try:
        content = nzb_xml_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return None

    pwd_meta_match = re.search(
        r'<meta\b[^>]*?\btype=["\']password["\'][^>]*>(.*?)</meta>',
        content,
        re.IGNORECASE | re.DOTALL,
    )
    if not pwd_meta_match:
        return None

    val = pwd_meta_match.group(1).strip()
    if val.lower() in _FALSEY_PASSWORDS:
        return None
    return val


def is_nzb_content_fake(
    nzb_xml_bytes: bytes,
    media_type: str = "movie",
    is_season_pack: bool = False,
) -> tuple[bool, str | None]:
    """Layer 2: Parses raw in-memory .nzb XML to inspect header metadata, <file subject="..."> strings, and segment sizes."""

    def _ret(is_fake: bool, reason: str | None) -> tuple[bool, str | None]:
        pwd_info = extract_nzb_password(nzb_xml_bytes)
        summary = (
            f"rejected ({reason})"
            if is_fake
            else f"passed (password_protected={bool(pwd_info)})"
        )
        logger.debug("[FAKEDETECT] [L2] Inspecting NZB payload -> %s", summary)
        return is_fake, reason

    try:
        content = nzb_xml_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return _ret(True, "Failed to decode NZB bytes")

    # 1. Extract all file subjects (supporting both double- and single-quoted XML attributes and HTML entities)
    raw_subjects = re.findall(
        r"<file\b[^>]*?\bsubject=(?:\"([^\"]*)\"|'([^']*)')",
        content,
        re.IGNORECASE | re.DOTALL,
    )
    subjects = [html.unescape(a or b) for a, b in raw_subjects if (a or b)]
    if not subjects:
        return _ret(True, "No <file> subjects found in NZB XML")

    has_valid_payload = False
    has_main_video = False
    has_sample_video = False
    has_main_archive = False
    has_sample_archive = False

    for subject in subjects:
        subject_lower = subject.lower()

        # Check bad extensions
        for bad_ext in BAD_EXTENSIONS:
            if bad_ext in subject_lower:
                return _ret(True, f"Contains executable/malicious extension: {bad_ext}")

        # Check phishing files
        for phish in PHISHING_FILES:
            if phish in subject_lower:
                return _ret(True, f"Contains phishing instructions: {phish}")

        for vid_match in _SUBJECT_VIDEO_RE.finditer(subject):
            if _is_sample_stem(vid_match.group("stem")):
                has_sample_video = True
            else:
                has_main_video = True

        for arch_match in _SUBJECT_ARCHIVE_RE.finditer(subject):
            if _is_sample_stem(arch_match.group("stem")):
                has_sample_archive = True
            else:
                has_main_archive = True

        # Check payload types
        for vid_ext in VIDEO_ARCHIVE_EXTENSIONS:
            if vid_ext in subject_lower:
                has_valid_payload = True
                break
        if not has_valid_payload:
            # Check for multipart rar / par2 / numeric extensions like .001, .r02, .vol01, or yEnc
            if re.search(r"\.(par2|r\d{2}|\d{3}|part\d+|vol\d+)|yenc", subject_lower):
                has_valid_payload = True

    if (
        (has_sample_video or has_sample_archive)
        and not has_main_video
        and not has_main_archive
    ):
        return _ret(
            True,
            "Contains only sample video file without full video/archive payload",
        )

    if not has_valid_payload:
        # If segments exist and no bad extension or phishing file was detected, allow obfuscated usenet files
        has_segments = bool(re.search(r"<segment", content, re.IGNORECASE))
        if has_segments:
            has_valid_payload = True

    if not has_valid_payload:
        return _ret(True, "Does not contain expected video/archive segments")

    # 3. Segment Payload Sizing (aggregate bytes across all <segment> tags)
    total_segment_bytes = sum(
        int(b)
        for b in re.findall(r'<segment\b[^>]*?\bbytes="(\d+)"', content, re.IGNORECASE)
    )
    if total_segment_bytes > 0:
        mtype = (
            media_type.value
            if hasattr(media_type, "value")
            else str(media_type or "").lower()
        )
        if mtype in ("movie", "anime-movie") and not is_season_pack:
            if total_segment_bytes < MIN_SEGMENT_MOVIE_BYTES:
                return _ret(
                    True,
                    f"Aggregate segment payload ({_bytes_to_mb(total_segment_bytes):.1f}MB) is below movie threshold (30MB)",
                )
        else:
            if total_segment_bytes < MIN_SEGMENT_EPISODE_BYTES:
                return _ret(
                    True,
                    f"Aggregate segment payload ({_bytes_to_mb(total_segment_bytes):.1f}MB) is below episode threshold (10MB)",
                )

    return _ret(False, None)


# Layer 3 Conservative Video File Lower Bounds
MIN_VIDEO_MOVIE_BYTES = 150 * 1024 * 1024  # 150MB
MIN_VIDEO_EPISODE_BYTES = (
    25 * 1024 * 1024
)  # 25MB (protects anime shorts and mini-episodes)


def _extract_file_entries(files: list[Any]) -> list[tuple[str, int]]:
    """Recursively extract (file_name, file_size) tuples from flat or nested downloader file entries."""
    entries: list[tuple[str, int]] = []
    for f in files:
        if isinstance(f, str):
            if f.strip():
                entries.append((f, 0))
        elif hasattr(f, "size_bytes") and hasattr(f, "name"):  # DownloaderFile
            fname = str(f.path or f.name or "")
            if fname.strip():
                entries.append((fname, int(f.size_bytes or 0)))
        elif isinstance(f, dict):
            name = f.get("name") or f.get("short_name") or f.get("path")
            raw_size = f.get("size") if f.get("size") is not None else f.get("bytes")
            size_int = 0
            if raw_size is not None:
                try:
                    size_int = int(raw_size)
                except (ValueError, TypeError):
                    size_int = 0
            if name and isinstance(name, str) and name.strip():
                entries.append((name, size_int))
            nested = f.get("files")
            if isinstance(nested, list):
                entries.extend(_extract_file_entries(nested))
    return entries


def has_usable_file_entries(files: list[Any] | None) -> bool:
    """True when the downloader file list contains at least one named file."""
    return bool(files) and bool(_extract_file_entries(files or []))


def _describe_files_for_log(files: list[Any] | None, limit: int = 20) -> str:
    """Compact raw file list description (type, name, size, keys) for debug logs."""
    parts: list[str] = []
    for f in (files or [])[:limit]:
        if isinstance(f, dict):
            parts.append(
                f"dict(name={f.get('name')!r}, size={f.get('size')!r}, "
                f"keys={sorted(f)})"
            )
        else:
            parts.append(
                f"{type(f).__name__}(name={getattr(f, 'name', f)!r}, "
                f"size={getattr(f, 'size_bytes', None)!r})"
            )
    extra = len(files or []) - limit
    suffix = f", ... +{extra} more" if extra > 0 else ""
    return f"[{', '.join(parts)}{suffix}]"


def _extract_file_names(files: list[Any]) -> list[str]:
    """Recursively extract file names from flat or nested downloader file entries."""
    return [name for name, _ in _extract_file_entries(files)]


_ARCHIVE_RE = re.compile(
    r"\.(?:part\d+\.rar|r\d{2}|rar|7z|zip|par2)$",
    re.IGNORECASE,
)


def is_transfer_filelist_fake(
    tb_files: list[Any] | None,
    media_type: str = "movie",
    is_season_pack: bool = False,
    release_name: str | None = None,
) -> tuple[bool, str | None, bool]:
    """Layer 3: Evaluates a completed transfer's file list.

    Returns:
        tuple[is_fake: bool, reason: str | None, is_unextracted_archive: bool]
    """
    rel_label = f"'{release_name}'" if release_name else "payload"

    def _ret(
        is_fake: bool, reason: str | None, is_archive: bool
    ) -> tuple[bool, str | None, bool]:
        if is_fake:
            summary = (
                f"unextracted archive ({reason})"
                if is_archive
                else f"rejected ({reason})"
            )
        else:
            summary = "verified clean (valid playable video found)"
        logger.debug(
            "[FAKEDETECT] [L3] Inspecting filelist for %s -> %s", rel_label, summary
        )
        return is_fake, reason, is_archive

    logger.debug(
        "[FAKEDETECT] [L3] Raw filelist for %s: %d entries %s",
        rel_label,
        len(tb_files or []),
        _describe_files_for_log(tb_files),
    )

    if not tb_files:
        return _ret(True, "Downloader returned empty file list", False)

    file_entries = _extract_file_entries(tb_files)
    if not file_entries:
        return _ret(True, "Downloader returned empty file list", False)

    has_valid_payload = False
    has_sample_video = False
    has_archive = False
    playable_videos: list[tuple[str, int]] = []

    for raw_name, size in file_entries:
        fname = raw_name.lower().strip()

        for bad_ext in BAD_EXTENSIONS:
            if fname.endswith(bad_ext):
                return _ret(True, f"Downloaded executable file: {raw_name}", False)

        matched_vid = False
        for vid_ext in PLAYABLE_VIDEO_EXTENSIONS:
            if fname.endswith(vid_ext):
                matched_vid = True
                if _is_sample_video_filename(raw_name):
                    has_sample_video = True
                else:
                    has_valid_payload = True
                    playable_videos.append((raw_name, size))
                break

        if not matched_vid and _ARCHIVE_RE.search(fname):
            has_archive = True

    if not has_valid_payload:
        if has_archive:
            return _ret(True, "Unextracted archive payload awaiting extraction", True)
        if has_sample_video:
            return _ret(
                True, "Downloaded payload contains only sample video file(s)", False
            )
        return _ret(True, "Downloaded payload lacks a valid playable video file", False)

    # Conservative Video Size Thresholds
    max_video_size = max([sz for _, sz in playable_videos], default=0)
    if max_video_size > 0:
        mtype = (
            media_type.value
            if hasattr(media_type, "value")
            else str(media_type or "").lower()
        )
        if mtype in ("movie", "anime-movie") and not is_season_pack:
            if max_video_size < MIN_VIDEO_MOVIE_BYTES:
                return _ret(
                    True,
                    f"Downloaded video payload ({_bytes_to_mb(max_video_size):.1f}MB) is unrealistically small (< 150MB)",
                    False,
                )
        else:
            if max_video_size < MIN_VIDEO_EPISODE_BYTES:
                return _ret(
                    True,
                    f"Downloaded video payload ({_bytes_to_mb(max_video_size):.1f}MB) is unrealistically small (< 25MB)",
                    False,
                )

    return _ret(False, None, False)
