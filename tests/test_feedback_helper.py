"""Tests for the in-app feedback helper (m53, ADR-127)."""

from pathlib import Path

import pytest
import yaml

from app.core.diagnostics import (
    GITHUB_BUG_REPORT_URL,
    GITHUB_FEATURE_REQUEST_URL,
    GITHUB_QUESTION_URL,
    GITHUB_SECURITY_ADVISORY_URL,
    FeedbackDiagnostics,
    build_diagnostics_table_markdown,
    collect_feedback_diagnostics,
    detect_deployment,
)
from app.db.models import Provider
from app.version import APP_VERSION

SECRETS = [
    "SECRET-KEY-123",
    "INDEXER-KEY-456",
    "private.example",
    "My Secret Box",
    "Private Indexer",
]


@pytest.fixture
async def seeded_providers(db_session):
    db_session.add_all(
        [
            Provider(
                category="downloader",
                type="torbox",
                name="My Secret Box",
                api_key="SECRET-KEY-123",
                api_url="https://private.example/api",
                is_active=True,
            ),
            Provider(
                category="indexer",
                type="newznab",
                name="Private Indexer",
                api_key="INDEXER-KEY-456",
                api_url="https://idx.example",
                is_active=True,
            ),
            Provider(
                category="metadata",
                type="tmdb",
                name="tmdb",
                api_key="TMDB-KEY-789",
                is_active=False,
            ),
        ]
    )
    await db_session.commit()


async def test_collect_diagnostics_excludes_inactive(db_session, seeded_providers):
    diag = await collect_feedback_diagnostics(db_session)
    assert diag.active_provider_types == ["Newznab", "TorBox"]


async def test_collect_diagnostics_empty_db(db_session):
    diag = await collect_feedback_diagnostics(db_session)
    assert diag.active_provider_types == []


async def test_modal_bug_default_is_privacy_safe(async_client, seeded_providers):
    resp = await async_client.get("/feedback/modal")
    assert resp.status_code == 200
    for needle in ("| Field | Value |", APP_VERSION, GITHUB_BUG_REPORT_URL):
        assert needle in resp.text
    assert "### Steps to Reproduce" not in resp.text
    for secret in SECRETS:
        assert secret not in resp.text


@pytest.mark.parametrize("tab", ["feature", "question"])
async def test_modal_other_tabs(async_client, tab):
    resp = await async_client.get(f"/feedback/modal?type={tab}")
    assert resp.status_code == 200
    assert (
        f"activeTab: &#34;{tab}&#34;" in resp.text or f'activeTab: "{tab}"' in resp.text
    )
    for url in (GITHUB_BUG_REPORT_URL, GITHUB_FEATURE_REQUEST_URL, GITHUB_QUESTION_URL):
        assert url in resp.text


async def test_modal_invalid_type(async_client):
    assert (await async_client.get("/feedback/modal?type=bogus")).status_code == 422


async def test_modal_security_link(async_client):
    resp = await async_client.get("/feedback/modal")
    assert GITHUB_SECURITY_ADVISORY_URL in resp.text
    assert 'target="_blank" rel="noopener noreferrer"' in resp.text


@pytest.mark.parametrize(("exists", "expected"), [(True, "Docker"), (False, "Native")])
def test_detect_deployment(monkeypatch, exists, expected):
    monkeypatch.setattr(Path, "exists", lambda self: exists)
    assert detect_deployment() == expected


DIAG = FeedbackDiagnostics(
    "1.0.0", "3.12.3", "Linux x86_64", "Docker", ["Newznab", "TorBox"]
)


def test_diagnostics_table_markdown_exact():
    assert build_diagnostics_table_markdown(DIAG) == (
        "| Field | Value |\n| --- | --- |\n| Smewlet Version | 1.0.0 |\n"
        "| Python | 3.12.3 |\n| OS / Architecture | Linux x86_64 |\n"
        "| Deployment | Docker |\n| Active Providers | Newznab, TorBox |\n"
    )


def test_diagnostics_table_markdown_no_providers():
    md = build_diagnostics_table_markdown(
        FeedbackDiagnostics("1.0.0", "3.12.3", "Linux x86_64", "Docker", [])
    )
    assert "| Active Providers | None |" in md


def test_url_constants():
    base = "https://github.com/Reimaris/Smewlet"
    assert GITHUB_BUG_REPORT_URL == f"{base}/issues/new?template=bug_report.yml"
    assert (
        GITHUB_FEATURE_REQUEST_URL == f"{base}/issues/new?template=feature_request.yml"
    )
    assert GITHUB_QUESTION_URL == f"{base}/discussions/new?category=q-a"
    assert GITHUB_SECURITY_ADVISORY_URL == f"{base}/security/advisories/new"


async def test_settings_has_feedback_buttons(async_client):
    resp = await async_client.get("/settings")
    for t in ("bug", "feature", "question"):
        assert f'hx-get="/feedback/modal?type={t}"' in resp.text


TEMPLATE_DIR = Path(__file__).resolve().parents[1] / ".github" / "ISSUE_TEMPLATE"
FORMS = {
    "bug_report.yml": (
        "bug",
        {
            "what_happened",
            "steps_to_reproduce",
            "expected_behavior",
            "actual_behavior",
            "diagnostics",
            "checklist",
        },
    ),
    "feature_request.yml": (
        "enhancement",
        {"problem_statement", "proposed_solution", "checklist"},
    ),
}


def _load(name):
    return yaml.safe_load((TEMPLATE_DIR / name).read_text(encoding="utf-8"))


def test_issue_config():
    cfg = _load("config.yml")
    assert cfg["blank_issues_enabled"] is False
    # Questions live in Discussions; SECURITY.md is already listed by GitHub itself.
    assert [c["url"] for c in cfg["contact_links"]] == [
        "https://github.com/Reimaris/Smewlet/discussions"
    ]
    assert not (TEMPLATE_DIR / "question.yml").exists()
    security = (TEMPLATE_DIR.parents[1] / "SECURITY.md").read_text(encoding="utf-8")
    assert GITHUB_SECURITY_ADVISORY_URL in security


@pytest.mark.parametrize("name", list(FORMS))
def test_issue_forms_valid(name):
    label, required_ids = FORMS[name]
    form = _load(name)
    assert form["labels"] == [label]
    fields = [f for f in form["body"] if f["type"] != "markdown"]
    ids = [f["id"] for f in fields]
    assert len(ids) == len(set(ids))
    required = {
        f["id"]
        for f in fields
        if f["type"] == "checkboxes" or f.get("validations", {}).get("required")
    }
    assert required == required_ids
    for f in fields:
        if f["type"] == "checkboxes":
            assert all(o["required"] is True for o in f["attributes"]["options"])


def test_issue_form_field_details():
    bug = {f["id"]: f for f in _load("bug_report.yml")["body"] if "id" in f}
    assert bug["logs"]["attributes"]["render"] == "shell"
    assert not bug["logs"]["validations"]["required"]
    assert not bug["sanitized_database"]["validations"]["required"]
    assert (
        "Export Sanitized DB" in bug["sanitized_database"]["attributes"]["description"]
    )
