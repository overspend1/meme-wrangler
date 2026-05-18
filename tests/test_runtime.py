from datetime import datetime, timedelta, timezone

from meme_wrangler.runtime import (
    StorageSnapshot,
    format_public_meme_id,
    parse_byte_size,
    parse_public_meme_id,
    predict_fill_date,
)


def test_public_meme_id_round_trip():
    assert format_public_meme_id(7) == "MEME-0007"
    assert parse_public_meme_id("MEME-0007") == 7
    assert parse_public_meme_id("7") == 7


def test_parse_byte_size_prefers_long_suffixes():
    assert parse_byte_size("1mb") == 1024**2
    assert parse_byte_size("2gb") == 2 * 1024**3


def test_predict_fill_date_returns_future_date():
    now = datetime.now(timezone.utc)
    samples = [
        StorageSnapshot(
            timestamp=(now - timedelta(days=2)).isoformat(),
            active_profile="default",
            db_bytes=100,
            backup_bytes=100,
            cache_bytes=0,
            log_bytes=0,
            runtime_bytes=0,
            total_bytes=200,
            local_free_bytes=1000,
            db_limit_bytes=None,
        ),
        StorageSnapshot(
            timestamp=now.isoformat(),
            active_profile="default",
            db_bytes=300,
            backup_bytes=300,
            cache_bytes=0,
            log_bytes=0,
            runtime_bytes=0,
            total_bytes=600,
            local_free_bytes=600,
            db_limit_bytes=None,
        ),
    ]

    fill_date = predict_fill_date(samples, current_total=600, capacity_bytes=None, free_bytes=600)
    assert fill_date is not None
    assert datetime.fromisoformat(fill_date).date() >= now.date()
