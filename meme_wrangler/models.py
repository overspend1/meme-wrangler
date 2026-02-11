"""Data models used across the Meme Wrangler package."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class Meme:
    """In-memory representation of a row in the ``memes`` table."""

    id: int
    owner_file_id: str
    mime_type: Optional[str]
    scheduled_ts: int
    posted: int
    created_ts: int
    preview_file_id: Optional[str] = None
    caption: Optional[str] = None

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_record(cls, record) -> "Meme":
        """Build from an *asyncpg.Record*."""
        return cls(
            id=int(record["id"]),
            owner_file_id=record["owner_file_id"],
            mime_type=record["mime_type"],
            scheduled_ts=int(record["scheduled_ts"]),
            posted=int(record["posted"]),
            created_ts=int(record["created_ts"]),
            preview_file_id=record.get("preview_file_id"),
            caption=record.get("caption"),
        )

    @classmethod
    def from_dict(cls, d: dict) -> "Meme":
        """Build from a backup-JSON dictionary."""
        return cls(
            id=int(d["id"]),
            owner_file_id=d["owner_file_id"],
            mime_type=d.get("mime_type"),
            scheduled_ts=int(d["scheduled_ts"]),
            posted=int(d.get("posted", 0)),
            created_ts=int(d["created_ts"]),
            preview_file_id=d.get("preview_file_id"),
            caption=d.get("caption"),
        )

    def to_dict(self) -> dict:
        """Serialise to a plain dict suitable for JSON export."""
        return asdict(self)

    def to_insert_tuple(self) -> tuple:
        """Return a tuple matching the INSERT column order used by restore."""
        return (
            self.id,
            self.owner_file_id,
            self.mime_type,
            self.scheduled_ts,
            self.posted,
            self.created_ts,
            self.preview_file_id,
            self.caption,
        )


@dataclass
class BackupPayload:
    """Structure of a full backup JSON file."""

    version: int
    generated_at: str
    memes: list[Meme]
    scheduled_memes: list[Meme]

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "memes": [m.to_dict() for m in self.memes],
            "scheduled_memes": [m.to_dict() for m in self.scheduled_memes],
        }


@dataclass
class BackupStatus:
    """Summary returned by ``BackupManager.get_status()``."""

    last_backup_time: Optional[str]
    backups_on_disk: int
    backups_in_db: int
    total_memes: int
    scheduled_memes: int
    disk_usage_bytes: int
