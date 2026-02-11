"""Tests for configuration helpers."""

from meme_wrangler.config import is_neon_url, _normalize_database_url


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
