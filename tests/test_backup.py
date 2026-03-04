"""Tests for backup utilities (no DB required)."""

import asyncio
import gzip
import json
import tempfile
from pathlib import Path

from unittest.mock import AsyncMock, patch

import pytest

from meme_wrangler.backup import (
    _checksum,
    create_backup,
    load_backup_data,
    rotate_backups,
    verify_latest_backup,
)
from meme_wrangler.models import Meme


# ------------------------------------------------------------------
# Checksum
# ------------------------------------------------------------------

def test_checksum_deterministic():
    data = b"hello world"
    assert _checksum(data) == _checksum(data)


def test_checksum_changes_with_input():
    assert _checksum(b"a") != _checksum(b"b")


# ------------------------------------------------------------------
# load_backup_data: plain JSON
# ------------------------------------------------------------------

def test_load_plain_json():
    payload = {"version": 1, "memes": []}
    raw = json.dumps(payload).encode("utf-8")
    result = load_backup_data(raw)
    assert result == payload


# ------------------------------------------------------------------
# load_backup_data: gzip
# ------------------------------------------------------------------

def test_load_gzip_json():
    payload = {"version": 2, "memes": [{"id": 1}]}
    raw = json.dumps(payload).encode("utf-8")
    compressed = gzip.compress(raw)
    result = load_backup_data(compressed)
    assert result == payload


# ------------------------------------------------------------------
# Meme round-trip
# ------------------------------------------------------------------

def test_meme_from_dict_round_trip():
    d = {
        "id": 42,
        "owner_file_id": "abc123",
        "mime_type": "image",
        "scheduled_ts": 1700000000,
        "posted": 0,
        "created_ts": 1699999000,
        "preview_file_id": "abc123",
        "caption": "funny",
    }
    meme = Meme.from_dict(d)
    assert meme.to_dict() == d


# ------------------------------------------------------------------
# Rotation
# ------------------------------------------------------------------

def test_rotate_removes_old_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # Create 5 backup files
        for i in range(5):
            (tmp / f"memes-backup-2025010{i}-120000.json.gz").write_bytes(
                gzip.compress(b"{}")
            )

        with patch("meme_wrangler.backup.cfg") as mock_cfg:
            mock_cfg.backup_dir = tmp
            mock_cfg.backup_retain_count = 2

            with patch("meme_wrangler.backup._backup_dir", return_value=tmp):
                removed = rotate_backups()

        remaining = list(tmp.glob("memes-backup-*.json.gz"))
        assert len(remaining) == 2
        assert removed >= 3


# ------------------------------------------------------------------
# Verify
# ------------------------------------------------------------------

def test_verify_no_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("meme_wrangler.backup._backup_dir", return_value=Path(tmpdir)):
            ok, msg = verify_latest_backup()
        assert not ok
        assert "No backup" in msg


def test_verify_no_sidecar():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        f = tmp / "memes-backup-20250101-120000.json.gz"
        f.write_bytes(gzip.compress(b"{}"))
        with patch("meme_wrangler.backup._backup_dir", return_value=tmp):
            ok, msg = verify_latest_backup()
        assert not ok
        assert "sidecar" in msg.lower()


def test_verify_valid():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        data = gzip.compress(b'{"version":2}')
        f = tmp / "memes-backup-20250101-120000.json.gz"
        f.write_bytes(data)
        sidecar = f.with_suffix(f.suffix + ".sha256")
        sidecar.write_text(_checksum(data))
        with patch("meme_wrangler.backup._backup_dir", return_value=tmp):
            ok, msg = verify_latest_backup()
        assert ok
        assert "OK" in msg


def test_verify_tampered():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        data = gzip.compress(b'{"version":2}')
        f = tmp / "memes-backup-20250101-120000.json.gz"
        f.write_bytes(data)
        sidecar = f.with_suffix(f.suffix + ".sha256")
        sidecar.write_text("bad_checksum")
        with patch("meme_wrangler.backup._backup_dir", return_value=tmp):
            ok, msg = verify_latest_backup()
        assert not ok
        assert "FAILED" in msg


def test_create_backup_generates_distinct_filenames_for_quick_runs():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        class _AcquireCtx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def fetch(self, _query):
                return []

        class _FakePool:
            def acquire(self):
                return _AcquireCtx()

        with patch("meme_wrangler.backup.get_pool", new=AsyncMock(return_value=_FakePool())):
            with patch("meme_wrangler.backup._backup_dir", return_value=tmp):
                with patch("meme_wrangler.backup.rotate_backups", return_value=0):
                    with patch("meme_wrangler.backup.cfg") as mock_cfg:
                        mock_cfg.backup_store_in_db = False
                        first_path, *_ = asyncio.run(create_backup())
                        second_path, *_ = asyncio.run(create_backup())

        assert first_path.exists()
        assert second_path.exists()
        assert first_path != second_path
        assert first_path.name.startswith("memes-backup-")
        assert second_path.name.startswith("memes-backup-")
        assert first_path.suffixes == [".json", ".gz"]
        assert second_path.suffixes == [".json", ".gz"]
        assert first_path.with_suffix(first_path.suffix + ".sha256").exists()
        assert second_path.with_suffix(second_path.suffix + ".sha256").exists()
