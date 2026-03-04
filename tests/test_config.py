"""Tests for configuration helpers."""

from meme_wrangler.config import (
    DEFAULT_BACKUP_INTERVAL_HOURS,
    DEFAULT_BACKUP_RETAIN_COUNT,
    Config,
    _normalize_database_url,
    is_neon_url,
)


def test_neon_url_detected():
    url = "postgresql://user:pass@ep-cool-123.us-east-2.aws.neon.tech/neondb"
    assert is_neon_url(url) is True


def test_local_url_not_neon():
    assert is_neon_url("postgresql://meme:meme@localhost:5432/meme_wrangler") is False


def test_normalize_preserves_query_params():
    """sslmode=require in Neon URLs must survive host normalisation."""
    import os

    orig = os.environ.get("POSTGRES_HOST")
    try:
        os.environ["POSTGRES_HOST"] = "neon-host"
        url = "postgresql://u:p@localhost:5432/db?sslmode=require"
        result = _normalize_database_url(url)
        assert "neon-host" in result
        assert "sslmode=require" in result
    finally:
        if orig is None:
            os.environ.pop("POSTGRES_HOST", None)
        else:
            os.environ["POSTGRES_HOST"] = orig


def test_normalize_no_override():
    """Without POSTGRES_HOST override, URL should pass through unchanged."""
    import os

    orig = os.environ.get("POSTGRES_HOST")
    try:
        os.environ.pop("POSTGRES_HOST", None)
        url = "postgresql://u:p@localhost:5432/db"
        assert _normalize_database_url(url) == url
    finally:
        if orig is not None:
            os.environ["POSTGRES_HOST"] = orig


def test_invalid_backup_interval_parsed_safely_until_validation(monkeypatch):
    monkeypatch.setenv("MEMEBOT_BACKUP_INTERVAL_HOURS", "not-a-float")
    monkeypatch.delenv("MEMEBOT_BACKUP_RETAIN_COUNT", raising=False)

    cfg = Config()

    assert cfg.backup_interval_hours == DEFAULT_BACKUP_INTERVAL_HOURS
    with __import__("pytest").raises(SystemExit) as exc:
        cfg.validate()

    message = str(exc.value)
    assert "MEMEBOT_BACKUP_INTERVAL_HOURS" in message
    assert "not-a-float" in message


def test_invalid_backup_retain_count_parsed_safely_until_validation(monkeypatch):
    monkeypatch.delenv("MEMEBOT_BACKUP_INTERVAL_HOURS", raising=False)
    monkeypatch.setenv("MEMEBOT_BACKUP_RETAIN_COUNT", "not-an-int")

    cfg = Config()

    assert cfg.backup_retain_count == DEFAULT_BACKUP_RETAIN_COUNT
    with __import__("pytest").raises(SystemExit) as exc:
        cfg.validate()

    message = str(exc.value)
    assert "MEMEBOT_BACKUP_RETAIN_COUNT" in message
    assert "not-an-int" in message


def test_invalid_numeric_parse_errors_are_deterministic(monkeypatch):
    monkeypatch.setenv("MEMEBOT_BACKUP_INTERVAL_HOURS", "bad-float")
    monkeypatch.setenv("MEMEBOT_BACKUP_RETAIN_COUNT", "bad-int")

    cfg = Config()

    with __import__("pytest").raises(SystemExit) as exc:
        cfg.validate()

    assert (
        str(exc.value)
        == "Invalid numeric environment variable value(s): "
        "MEMEBOT_BACKUP_INTERVAL_HOURS='bad-float', "
        "MEMEBOT_BACKUP_RETAIN_COUNT='bad-int'"
    )
