"""Tests for the global log secret Redaction Filter (ADR-097)."""

import logging

import pytest

from app.core.log_redaction import (
    RedactionFilter,
    clear_secrets,
    install_model_listeners,
    mask_secret,
    redact_text,
    refresh_secrets,
    register_secret,
)
from app.db.models import Provider

KEY = "abcd1234efgh5678ijkl"


@pytest.fixture(autouse=True)
def _clean_secrets():
    clear_secrets()
    yield
    clear_secrets()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (KEY, "abcd…kl"),
        ("short", "***"),
        ("elevenchars", "***"),
        ("twelvechars!", "twel…s!"),
    ],
)
def test_mask_secret_format(value, expected):
    assert mask_secret(value) == expected


@pytest.mark.parametrize(
    "text",
    [
        f"GET https://idx.example/api?t=search&apikey={KEY}&q=foo",
        f"failed api_key={KEY}",
        f'payload {{"apikey": "{KEY}"}}',
        f"Authorization: Bearer {KEY}",
    ],
)
def test_regex_patterns_mask_key(text):
    out = redact_text(text)
    assert KEY not in out
    assert "abcd…kl" in out


def test_non_secret_text_untouched():
    assert (
        redact_text("Pushed Movie (2024) to TorBox") == "Pushed Movie (2024) to TorBox"
    )


def test_literal_secret_masked_anywhere():
    register_secret("plainsecretvalue99")
    assert "plainsecretvalue99" not in redact_text("boom: plainsecretvalue99 leaked")


def test_short_literals_not_registered():
    register_secret("abc")
    assert redact_text("abc def") == "abc def"


def _record(msg, *args, exc_info=None):
    return logging.LogRecord("t", logging.ERROR, __file__, 1, msg, args, exc_info)


def test_filter_masks_message_and_args():
    rec = _record("calling %s", f"http://x/api?apikey={KEY}")
    RedactionFilter().filter(rec)
    assert KEY not in rec.getMessage()


def test_filter_masks_traceback():
    register_secret(KEY)
    try:
        raise RuntimeError(f"bad url http://x/?apikey={KEY}")
    except RuntimeError:
        import sys

        rec = _record("oops", exc_info=sys.exc_info())
    RedactionFilter().filter(rec)
    assert rec.exc_text and KEY not in rec.exc_text
    formatted = logging.Formatter("%(message)s").format(rec)
    assert KEY not in formatted


async def test_refresh_and_model_listeners(db_session):
    install_model_listeners()
    db_session.add(Provider(category="indexer", type="newznab", name="I1", api_key=KEY))
    await db_session.flush()
    assert KEY not in redact_text(f"url token {KEY}")

    clear_secrets()
    await refresh_secrets(db_session)
    assert KEY not in redact_text(f"again {KEY}")


@pytest.mark.parametrize(
    "text",
    [
        "GET /auth?code_verifier=" + KEY,
        "params code_challenge=" + KEY + " done",
        'body {"authToken": "' + KEY + '"}',
        "authToken=" + KEY,
        "X-Plex-Token: " + KEY,
        "url?X-Plex-Token=" + KEY,
        "refresh_token=" + KEY,
    ],
)
def test_new_provider_keys_and_headers_masked(text):
    assert KEY not in redact_text(text)


async def test_mal_refresh_token_and_plex_token_registered(db_session):
    install_model_listeners()
    db_session.add_all(
        [
            Provider(
                name="MAL",
                type="mal",
                category="watchlist",
                access_token="mal-access-0123456789",
                config_json='{"refresh_token": "mal-refresh-0123456789"}',
            ),
            Provider(
                name="Plex",
                type="plex",
                category="watchlist",
                access_token="plex-token-0123456789",
            ),
        ]
    )
    await db_session.commit()
    out = redact_text(
        "mal-access-0123456789 mal-refresh-0123456789 plex-token-0123456789"
    )
    assert "0123456789" not in out


@pytest.mark.parametrize(
    "text",
    [
        "GET http://nas:8080/sabnzbd/api?apikey=abc123&mode=queue",
        "POST https://www.premiumize.me/api/transfer/create?apikey=abc123",
        "form data: apikey=abc123&mode=addfile",
    ],
)
def test_downloader_apikey_in_urls_and_forms_is_redacted(text: str) -> None:
    from app.core.log_redaction import redact_text

    assert "abc123" not in redact_text(text)
    assert "apikey=***" in redact_text(text)


async def test_scan_secrets_registered_and_masked(db_session):
    install_model_listeners()
    db_session.add(
        Provider(
            name="Delivery",
            type="local_directory",
            category="delivery",
            config_json=(
                '{"scan_plex_token": "test-plex-token-value",'
                ' "scan_jellyfin_api_key": "test-jellyfin-key-value"}'
            ),
        )
    )
    await db_session.commit()
    out = redact_text("a test-plex-token-value b test-jellyfin-key-value")
    assert "-value" not in out


@pytest.mark.parametrize(
    "text",
    [
        'config {"scan_plex_token": "zzzz-plex-secret"}',
        "scan_jellyfin_api_key=zzzz-plex-secret",
    ],
)
def test_scan_keys_masked_by_pattern(text):
    assert "zzzz-plex-secret" not in redact_text(text)
