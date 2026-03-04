"""Backup manager - create, restore, rotate, verify, and store in DB."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from meme_wrangler.config import cfg, IST
from meme_wrangler.db import get_pool
from meme_wrangler.models import BackupPayload, BackupStatus, Meme

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _checksum(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def _backup_dir() -> Path:
    cfg.backup_dir.mkdir(parents=True, exist_ok=True)
    return cfg.backup_dir


# ---------------------------------------------------------------------------
# Core backup operations
# ---------------------------------------------------------------------------


async def create_backup(
    *,
    send_document_to: Optional[int] = None,
    bot: Optional[Any] = None,
) -> tuple[Path, int, int]:
    """Export all memes as a gzip-compressed JSON file.

    Returns ``(backup_path, total_memes, scheduled_count)``.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, owner_file_id, mime_type, scheduled_ts, "
            "       posted, created_ts, preview_file_id, caption "
            "FROM memes ORDER BY id"
        )

    memes = [Meme.from_record(r) for r in rows]
    scheduled = [m for m in memes if m.posted == 0]

    payload = BackupPayload(
        version=2,
        generated_at=datetime.now(IST).isoformat(),
        memes=memes,
        scheduled_memes=scheduled,
    )

    raw_json = json.dumps(payload.to_dict(), indent=2).encode("utf-8")
    compressed = gzip.compress(raw_json)
    digest = _checksum(compressed)

    backup_root = _backup_dir()
    timestamp = datetime.now(IST).strftime("%Y%m%d-%H%M%S-%f")
    filename = f"memes-backup-{timestamp}-{uuid4().hex[:8]}.json.gz"
    backup_path = backup_root / filename

    backup_path.write_bytes(compressed)

    # Write checksum sidecar
    sidecar = backup_path.with_suffix(backup_path.suffix + ".sha256")
    sidecar.write_text(digest, encoding="utf-8")

    logger.info(
        "Backup written to %s (%d bytes, checksum %s)",
        backup_path,
        len(compressed),
        digest[:12],
    )

    # Store metadata (and optionally payload) in the DB
    if cfg.backup_store_in_db:
        try:
            await _store_backup_in_db(
                filename=filename,
                total=len(memes),
                scheduled=len(scheduled),
                checksum=digest,
                payload_json=raw_json if len(raw_json) < 5_000_000 else None,
                size_bytes=len(compressed),
            )
        except Exception as exc:
            logger.warning("Failed to store backup metadata in DB: %s", exc)

    # Rotate old files
    try:
        rotate_backups()
    except Exception as exc:
        logger.warning("Backup rotation failed: %s", exc)

    # Optionally send via Telegram
    if send_document_to and bot:
        try:
            from telegram import InputFile  # type: ignore[import-untyped]

            with backup_path.open("rb") as fh:
                await bot.send_document(
                    send_document_to,
                    InputFile(fh, filename=filename),
                    caption=(
                        f"Backup created: {len(memes)} total memes "
                        f"({len(scheduled)} scheduled)."
                    ),
                )
        except Exception as exc:
            logger.warning("Failed to send backup document via Telegram: %s", exc)

    return backup_path, len(memes), len(scheduled)


async def _store_backup_in_db(
    *,
    filename: str,
    total: int,
    scheduled: int,
    checksum: str,
    payload_json: Optional[bytes],
    size_bytes: int,
) -> None:
    """Insert a row into the ``backups`` table."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backups
                (filename, total_memes, scheduled_memes,
                 checksum, payload, size_bytes)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            """,
            filename,
            total,
            scheduled,
            checksum,
            payload_json.decode("utf-8") if payload_json else None,
            size_bytes,
        )


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------


def rotate_backups() -> int:
    """Delete old backup files beyond the retention count.

    Returns the number of files removed.
    """
    backup_root = _backup_dir()
    files = sorted(
        backup_root.glob("memes-backup-*.json*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    # Keep sidecars paired with their main file
    main_files = [f for f in files if not f.name.endswith(".sha256")]
    to_remove = main_files[cfg.backup_retain_count :]
    removed = 0
    for path in to_remove:
        try:
            path.unlink()
            removed += 1
            # Remove sidecar if present
            sidecar = path.with_suffix(path.suffix + ".sha256")
            if sidecar.exists():
                sidecar.unlink()
                removed += 1
            logger.info("Rotated old backup: %s", path.name)
        except OSError as exc:
            logger.warning("Could not remove %s: %s", path, exc)
    return removed


# ---------------------------------------------------------------------------
# Integrity verification
# ---------------------------------------------------------------------------


def verify_latest_backup() -> tuple[bool, str]:
    """Check the most recent backup against its sidecar checksum.

    Returns ``(ok, message)``.
    """
    backup_root = _backup_dir()
    files = sorted(
        backup_root.glob("memes-backup-*.json.gz"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        return False, "No backup files found."

    latest = files[0]
    sidecar = latest.with_suffix(latest.suffix + ".sha256")
    if not sidecar.exists():
        return False, f"No checksum sidecar for {latest.name}."

    expected = sidecar.read_text(encoding="utf-8").strip()
    actual = _checksum(latest.read_bytes())

    if expected == actual:
        return True, f"{latest.name} integrity OK (SHA-256 matches)."
    return False, (
        f"{latest.name} FAILED integrity check! "
        f"Expected {expected[:16]}..., got {actual[:16]}..."
    )


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


def load_backup_data(raw: bytes) -> dict:
    """Parse backup bytes - handles both plain JSON and gzip."""
    try:
        decompressed = gzip.decompress(raw)
    except gzip.BadGzipFile:
        decompressed = raw  # plain JSON
    return json.loads(decompressed.decode("utf-8"))


async def restore_memes(memes: list[Meme]) -> int:
    """Replace all memes in the DB with *memes* from a backup.

    Returns the number of records imported.
    """
    pool = await get_pool()
    records = [m.to_insert_tuple() for m in memes]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE TABLE memes RESTART IDENTITY")
            if records:
                await conn.executemany(
                    """
                    INSERT INTO memes
                        (id, owner_file_id, mime_type, scheduled_ts,
                         posted, created_ts, preview_file_id, caption)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    records,
                )
                max_id = max(r[0] for r in records)
            else:
                max_id = 0

            seq_name = await conn.fetchval(
                "SELECT pg_get_serial_sequence('memes', 'id')"
            )
            if seq_name:
                if max_id > 0:
                    await conn.execute(
                        "SELECT setval($1, $2, true)", seq_name, max_id
                    )
                else:
                    await conn.execute(
                        "SELECT setval($1, 1, false)", seq_name
                    )

    return len(records)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


async def get_backup_status() -> BackupStatus:
    """Gather backup statistics from disk and DB."""
    backup_root = _backup_dir()
    disk_files = list(backup_root.glob("memes-backup-*.json*"))
    main_files = [f for f in disk_files if not f.name.endswith(".sha256")]
    disk_bytes = sum(f.stat().st_size for f in disk_files)

    # Latest file info
    last_time: Optional[str] = None
    total_memes = 0
    sched_memes = 0
    if main_files:
        latest = max(main_files, key=lambda p: p.stat().st_mtime)
        last_time = datetime.fromtimestamp(
            latest.stat().st_mtime, tz=IST
        ).isoformat()
        try:
            data = load_backup_data(latest.read_bytes())
            total_memes = len(data.get("memes", []))
            sched_memes = len(data.get("scheduled_memes", []))
        except Exception:
            pass

    # DB count
    db_count = 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) AS cnt FROM backups")
            db_count = int(row["cnt"]) if row else 0
    except Exception:
        pass

    return BackupStatus(
        last_backup_time=last_time,
        backups_on_disk=len(main_files),
        backups_in_db=db_count,
        total_memes=total_memes,
        scheduled_memes=sched_memes,
        disk_usage_bytes=disk_bytes,
    )
