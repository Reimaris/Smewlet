"""Privacy-safe environment diagnostics for the in-app feedback helper (ADR-127, ADR-135)."""

from __future__ import annotations

import platform
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Provider
from app.services.downloaders.base import DOWNLOADER_DISPLAY_NAMES
from app.services.indexers.catalog import INDEXER_PRESETS
from app.services.watchlists import WATCHLIST_DISPLAY_NAMES
from app.version import APP_VERSION

GITHUB_REPO_URL: str = "https://github.com/Reimaris/Smewlet"
GITHUB_BUG_REPORT_URL: str = f"{GITHUB_REPO_URL}/issues/new?template=bug_report.yml"
GITHUB_FEATURE_REQUEST_URL: str = (
    f"{GITHUB_REPO_URL}/issues/new?template=feature_request.yml"
)
GITHUB_QUESTION_URL: str = f"{GITHUB_REPO_URL}/discussions/new?category=q-a"
GITHUB_SECURITY_ADVISORY_URL: str = f"{GITHUB_REPO_URL}/security/advisories/new"

PROVIDER_TYPE_LABELS: dict[str, str] = {
    **DOWNLOADER_DISPLAY_NAMES,
    **WATCHLIST_DISPLAY_NAMES,
    "tmdb": "TMDB",
    **{p.type: p.name for p in INDEXER_PRESETS},
    "newznab": "Newznab",
}


@dataclass(frozen=True)
class FeedbackDiagnostics:
    """The only environment facts exposed in a feedback report."""

    app_version: str
    python_version: str
    os_arch: str
    deployment: str
    active_provider_types: list[str]


def detect_deployment() -> str:
    """Return "Docker" if running inside a container, else "Native"."""
    return "Docker" if Path("/.dockerenv").exists() else "Native"


async def collect_feedback_diagnostics(session: AsyncSession) -> FeedbackDiagnostics:
    """Collect diagnostics; only `Provider.type` of active providers is read."""
    types = (
        (
            await session.execute(
                select(Provider.type).where(Provider.is_active.is_(True))
            )
        )
        .scalars()
        .all()
    )
    labels = sorted({PROVIDER_TYPE_LABELS.get(t, t.capitalize()) for t in types})
    return FeedbackDiagnostics(
        app_version=APP_VERSION,
        python_version=platform.python_version(),
        os_arch=f"{platform.system()} {platform.machine()}",
        deployment=detect_deployment(),
        active_provider_types=labels,
    )


def build_diagnostics_table_markdown(diag: FeedbackDiagnostics) -> str:
    """Return the diagnostics as a Markdown table for one-click clipboard copy."""
    providers = ", ".join(diag.active_provider_types) or "None"
    return (
        "| Field | Value |\n"
        "| --- | --- |\n"
        f"| Smewlet Version | {diag.app_version} |\n"
        f"| Python | {diag.python_version} |\n"
        f"| OS / Architecture | {diag.os_arch} |\n"
        f"| Deployment | {diag.deployment} |\n"
        f"| Active Providers | {providers} |\n"
    )
