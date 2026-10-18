"""Route classification for the setup guard (m64).

Every URL is either ``open`` (reachable while setup is incomplete),
``watchlist`` (needs an active watchlist provider) or ``core`` (needs the
core prerequisites: TMDB key, indexer, downloader). New routes must be added
to one of the lists below; ``is_reviewed`` backs a test that enforces this.
"""

import re
from typing import Literal

RouteClass = Literal["open", "watchlist", "core"]

ALWAYS_ALLOWED_PREFIXES: tuple[str, ...] = (
    "/static",
    "/settings",
    "/onboarding",
    "/api/onboarding",
    "/api/providers/test",
    "/api/notifications",
    "/api/settings",
    # Configuration actions reachable from Settings stay usable on an incomplete install.
    "/api/presets",
    "/api/blacklist",
    "/setup",
    "/health",
    # Provider auth flows and diagnostics are needed to fix an incomplete setup.
    # provider-specific code (allowlisted)
    "/simkl/auth",
    "/feedback",
    "/system",
    "/docs",
    "/openapi.json",
    "/redoc",
)

WATCHLIST_ROUTE_PREFIXES: tuple[str, ...] = (
    "/sync",
    "/api/items",
    "/items",
)

WATCHLIST_ROUTE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/settings/provider/[^/]+/sync$"),
)

# Explicitly reviewed routes that need the core prerequisites (Level B), including
# the ad-hoc search routes that stay usable without a watchlist provider.
CORE_PREFIXES: tuple[str, ...] = (
    "/api/search/manual",
    # provider-specific code (allowlisted)
    "/api/torbox/add",
    "/api/pushes",
    "/api/outages",
    "/api/delivery/offline-banner",
    "/api/history",
)
CORE_EXACT_PATHS: tuple[str, ...] = ("/",)


def _matches_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def _any_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(_matches_prefix(path, p) for p in prefixes)


def _is_watchlist(path: str) -> bool:
    return _any_prefix(path, WATCHLIST_ROUTE_PREFIXES) or any(
        p.match(path) for p in WATCHLIST_ROUTE_PATTERNS
    )


def is_reviewed(path: str) -> bool:
    """True if the path is covered by an explicit list (not the core default)."""
    return (
        _is_watchlist(path)
        or _any_prefix(path, ALWAYS_ALLOWED_PREFIXES)
        or _any_prefix(path, CORE_PREFIXES)
        or path in CORE_EXACT_PATHS
    )


def classify_path(path: str, method: str) -> RouteClass:
    """Classify a request path. Unreviewed paths default to the strict ``core``."""
    if _is_watchlist(path):
        return "watchlist"
    if _any_prefix(path, ALWAYS_ALLOWED_PREFIXES):
        return "open"
    return "core"
