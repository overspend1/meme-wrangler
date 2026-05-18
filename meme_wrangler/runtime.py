from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlparse, urlunparse

try:
    import asyncpg  # type: ignore
except ModuleNotFoundError:
    asyncpg = None  # type: ignore


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_utcnow() -> str:
    return utcnow().isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_database_url(url: str, host_override: Optional[str] = None) -> str:
    """Replace localhost hosts with a configured host for container runs."""

    if not host_override:
        return url

    host_override = host_override.strip()
    if not host_override or host_override in {"localhost", "127.0.0.1", "::1"}:
        return url

    parsed = urlparse(url)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        return url

    if "@" in parsed.netloc:
        auth_prefix, _, _ = parsed.netloc.rpartition("@")
        auth_segment = f"{auth_prefix}@"
    else:
        auth_segment = ""

    if parsed.port is not None:
        port_fragment = f":{parsed.port}"
    else:
        port_fragment = ""

    target_host = host_override
    if ":" in target_host and not target_host.startswith("["):
        target_host = f"[{target_host}]"

    rebuilt = parsed._replace(netloc=f"{auth_segment}{target_host}{port_fragment}")
    return urlunparse(rebuilt)


def build_postgres_url() -> Optional[str]:
    raw_url = os.environ.get("DATABASE_URL") or os.environ.get("MEMEBOT_DB")
    if not raw_url:
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        db_name = os.environ.get("POSTGRES_DB")
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        if user and password and db_name:
            raw_url = (
                f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
                f"@{host}:{port}/{db_name}"
            )
    if not raw_url:
        return None
    return normalize_database_url(raw_url, os.environ.get("POSTGRES_HOST"))


def format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(max(value, 0))
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def parse_byte_size(raw_value: Optional[str]) -> Optional[int]:
    if raw_value is None:
        return None
    value = raw_value.strip().lower()
    if not value:
        return None
    suffixes = [
        ("tb", 1024**4),
        ("gb", 1024**3),
        ("mb", 1024**2),
        ("kb", 1024),
        ("b", 1),
    ]
    for suffix, factor in suffixes:
        if value.endswith(suffix):
            number = float(value[: -len(suffix)].strip())
            return int(number * factor)
    return int(value)


def format_public_meme_id(db_id: int) -> str:
    return f"MEME-{db_id:04d}"


def parse_public_meme_id(raw_value: str) -> Optional[int]:
    candidate = raw_value.strip().upper()
    if candidate.isdigit():
        return int(candidate)
    if not candidate.startswith("MEME-"):
        return None
    suffix = candidate[5:]
    if not suffix.isdigit():
        return None
    return int(suffix)


@dataclass
class MemeRecord:
    id: int
    owner_file_id: str
    mime_type: Optional[str]
    scheduled_ts: int
    posted: int
    created_ts: int
    preview_file_id: Optional[str]
    caption: Optional[str]


@dataclass
class BackendProfile:
    key: str
    kind: str
    label: str
    url: Optional[str] = None
    path: Optional[str] = None
    storage_limit_bytes: Optional[int] = None
    fallback_hours: int = 48

    def summary(self) -> str:
        return f"{self.label} ({self.kind})"


class BaseBackend:
    def __init__(self, profile: BackendProfile):
        self.profile = profile

    async def init_schema(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def list_pending_memes(self) -> list[MemeRecord]:
        raise NotImplementedError

    async def list_due_memes(self, now_ts: int) -> list[MemeRecord]:
        raise NotImplementedError

    async def get_last_scheduled_ts(self) -> Optional[int]:
        raise NotImplementedError

    async def insert_meme(
        self,
        owner_file_id: str,
        mime_type: Optional[str],
        scheduled_ts: int,
        created_ts: int,
        preview_file_id: Optional[str],
        caption: Optional[str],
    ) -> int:
        raise NotImplementedError

    async def mark_posted(self, meme_id: int) -> None:
        raise NotImplementedError

    async def delete_pending(self, meme_ids: list[int]) -> list[int]:
        raise NotImplementedError

    async def get_meme(self, meme_id: int) -> Optional[MemeRecord]:
        raise NotImplementedError

    async def get_pending_meme(self, meme_id: int) -> Optional[MemeRecord]:
        raise NotImplementedError

    async def get_next_pending_meme(self) -> Optional[MemeRecord]:
        raise NotImplementedError

    async def update_schedule(self, meme_id: int, scheduled_ts: int) -> bool:
        raise NotImplementedError

    async def update_many_schedules(self, updates: list[tuple[int, int]]) -> int:
        raise NotImplementedError

    async def export_memes(self) -> list[MemeRecord]:
        raise NotImplementedError

    async def import_memes(self, memes: list[MemeRecord]) -> None:
        raise NotImplementedError

    async def count_memes(self) -> dict[str, int]:
        raise NotImplementedError

    async def size_bytes(self) -> int:
        raise NotImplementedError


class PostgresBackend(BaseBackend):
    def __init__(self, profile: BackendProfile):
        if asyncpg is None:
            raise RuntimeError("asyncpg must be installed for PostgreSQL support.")
        if not profile.url:
            raise RuntimeError(f"Profile {profile.key} is missing a PostgreSQL URL.")
        super().__init__(profile)
        self._pool: Optional[asyncpg.pool.Pool] = None

    async def _pool_ready(self) -> asyncpg.pool.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.profile.url, min_size=1, max_size=5)
        return self._pool

    async def init_schema(self) -> None:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memes (
                    id SERIAL PRIMARY KEY,
                    owner_file_id TEXT NOT NULL,
                    mime_type TEXT,
                    scheduled_ts BIGINT NOT NULL,
                    posted INTEGER DEFAULT 0,
                    created_ts BIGINT NOT NULL,
                    preview_file_id TEXT,
                    caption TEXT
                )
                """
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @staticmethod
    def _row_to_record(row: Any) -> MemeRecord:
        return MemeRecord(
            id=int(row["id"]),
            owner_file_id=row["owner_file_id"],
            mime_type=row["mime_type"],
            scheduled_ts=int(row["scheduled_ts"]),
            posted=int(row["posted"]),
            created_ts=int(row["created_ts"]),
            preview_file_id=row["preview_file_id"],
            caption=row["caption"],
        )

    async def list_pending_memes(self) -> list[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0
                ORDER BY scheduled_ts ASC
                """
            )
        return [self._row_to_record(row) for row in rows]

    async def list_due_memes(self, now_ts: int) -> list[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0 AND scheduled_ts <= $1
                ORDER BY scheduled_ts ASC
                """,
                now_ts,
            )
        return [self._row_to_record(row) for row in rows]

    async def get_last_scheduled_ts(self) -> Optional[int]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT scheduled_ts FROM memes WHERE posted=0 ORDER BY scheduled_ts DESC LIMIT 1"
            )
        return int(row["scheduled_ts"]) if row else None

    async def insert_meme(
        self,
        owner_file_id: str,
        mime_type: Optional[str],
        scheduled_ts: int,
        created_ts: int,
        preview_file_id: Optional[str],
        caption: Optional[str],
    ) -> int:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    """
                    INSERT INTO memes (
                        owner_file_id, mime_type, scheduled_ts, created_ts, preview_file_id, caption
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id
                    """,
                    owner_file_id,
                    mime_type,
                    scheduled_ts,
                    created_ts,
                    preview_file_id,
                    caption,
                )
            )

    async def mark_posted(self, meme_id: int) -> None:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE memes SET posted=1 WHERE id=$1", meme_id)

    async def delete_pending(self, meme_ids: list[int]) -> list[int]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "DELETE FROM memes WHERE posted=0 AND id = ANY($1::int[]) RETURNING id",
                meme_ids,
            )
        return [int(row["id"]) for row in rows]

    async def get_meme(self, meme_id: int) -> Optional[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE id=$1
                """,
                meme_id,
            )
        return self._row_to_record(row) if row else None

    async def get_pending_meme(self, meme_id: int) -> Optional[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE id=$1 AND posted=0
                """,
                meme_id,
            )
        return self._row_to_record(row) if row else None

    async def get_next_pending_meme(self) -> Optional[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0
                ORDER BY scheduled_ts ASC
                LIMIT 1
                """
            )
        return self._row_to_record(row) if row else None

    async def update_schedule(self, meme_id: int, scheduled_ts: int) -> bool:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE memes SET scheduled_ts=$1 WHERE id=$2 AND posted=0",
                scheduled_ts,
                meme_id,
            )
        return result.endswith("1")

    async def update_many_schedules(self, updates: list[tuple[int, int]]) -> int:
        updated = 0
        for scheduled_ts, meme_id in updates:
            if await self.update_schedule(meme_id, scheduled_ts):
                updated += 1
        return updated

    async def export_memes(self) -> list[MemeRecord]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                ORDER BY id ASC
                """
            )
        return [self._row_to_record(row) for row in rows]

    async def import_memes(self, memes: list[MemeRecord]) -> None:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE TABLE memes RESTART IDENTITY")
                if memes:
                    await conn.executemany(
                        """
                        INSERT INTO memes (
                            id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                            preview_file_id, caption
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        """,
                        [
                            (
                                record.id,
                                record.owner_file_id,
                                record.mime_type,
                                record.scheduled_ts,
                                record.posted,
                                record.created_ts,
                                record.preview_file_id,
                                record.caption,
                            )
                            for record in memes
                        ],
                    )
                    max_id = max(record.id for record in memes)
                else:
                    max_id = 0
                seq_name = await conn.fetchval("SELECT pg_get_serial_sequence('memes', 'id')")
                if seq_name:
                    if max_id > 0:
                        await conn.execute("SELECT setval($1, $2, true)", seq_name, max_id)
                    else:
                        await conn.execute("SELECT setval($1, 1, false)", seq_name)

    async def count_memes(self) -> dict[str, int]:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            total = int(await conn.fetchval("SELECT COUNT(*) FROM memes"))
            scheduled = int(await conn.fetchval("SELECT COUNT(*) FROM memes WHERE posted=0"))
        return {"total": total, "scheduled": scheduled, "posted": total - scheduled}

    async def size_bytes(self) -> int:
        pool = await self._pool_ready()
        async with pool.acquire() as conn:
            return int(await conn.fetchval("SELECT pg_total_relation_size('memes')"))


class SQLiteBackend(BaseBackend):
    def __init__(self, profile: BackendProfile):
        if not profile.path:
            raise RuntimeError(f"Profile {profile.key} is missing a SQLite path.")
        super().__init__(profile)
        self.path = Path(profile.path)
        self._conn: Optional[sqlite3.Connection] = None

    def _conn_ready(self) -> sqlite3.Connection:
        if self._conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._conn = conn
        return self._conn

    async def _run(self, fn, *args):
        return await asyncio.to_thread(fn, *args)

    async def init_schema(self) -> None:
        def _work() -> None:
            conn = self._conn_ready()
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_file_id TEXT NOT NULL,
                    mime_type TEXT,
                    scheduled_ts INTEGER NOT NULL,
                    posted INTEGER DEFAULT 0,
                    created_ts INTEGER NOT NULL,
                    preview_file_id TEXT,
                    caption TEXT
                )
                """
            )
            conn.commit()

        await self._run(_work)

    async def close(self) -> None:
        def _work() -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        await self._run(_work)

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> MemeRecord:
        return MemeRecord(
            id=int(row["id"]),
            owner_file_id=row["owner_file_id"],
            mime_type=row["mime_type"],
            scheduled_ts=int(row["scheduled_ts"]),
            posted=int(row["posted"]),
            created_ts=int(row["created_ts"]),
            preview_file_id=row["preview_file_id"],
            caption=row["caption"],
        )

    async def list_pending_memes(self) -> list[MemeRecord]:
        def _work() -> list[MemeRecord]:
            rows = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0
                ORDER BY scheduled_ts ASC
                """
            ).fetchall()
            return [self._row_to_record(row) for row in rows]

        return await self._run(_work)

    async def list_due_memes(self, now_ts: int) -> list[MemeRecord]:
        def _work() -> list[MemeRecord]:
            rows = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0 AND scheduled_ts <= ?
                ORDER BY scheduled_ts ASC
                """,
                (now_ts,),
            ).fetchall()
            return [self._row_to_record(row) for row in rows]

        return await self._run(_work)

    async def get_last_scheduled_ts(self) -> Optional[int]:
        def _work() -> Optional[int]:
            row = self._conn_ready().execute(
                "SELECT scheduled_ts FROM memes WHERE posted=0 ORDER BY scheduled_ts DESC LIMIT 1"
            ).fetchone()
            return int(row["scheduled_ts"]) if row else None

        return await self._run(_work)

    async def insert_meme(
        self,
        owner_file_id: str,
        mime_type: Optional[str],
        scheduled_ts: int,
        created_ts: int,
        preview_file_id: Optional[str],
        caption: Optional[str],
    ) -> int:
        def _work() -> int:
            conn = self._conn_ready()
            cursor = conn.execute(
                """
                INSERT INTO memes (
                    owner_file_id, mime_type, scheduled_ts, created_ts, preview_file_id, caption
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (owner_file_id, mime_type, scheduled_ts, created_ts, preview_file_id, caption),
            )
            conn.commit()
            return int(cursor.lastrowid)

        return await self._run(_work)

    async def mark_posted(self, meme_id: int) -> None:
        def _work() -> None:
            conn = self._conn_ready()
            conn.execute("UPDATE memes SET posted=1 WHERE id=?", (meme_id,))
            conn.commit()

        await self._run(_work)

    async def delete_pending(self, meme_ids: list[int]) -> list[int]:
        def _work() -> list[int]:
            if not meme_ids:
                return []
            conn = self._conn_ready()
            placeholders = ",".join("?" for _ in meme_ids)
            rows = conn.execute(
                f"SELECT id FROM memes WHERE posted=0 AND id IN ({placeholders})",
                meme_ids,
            ).fetchall()
            deleted = [int(row["id"]) for row in rows]
            conn.execute(f"DELETE FROM memes WHERE posted=0 AND id IN ({placeholders})", meme_ids)
            conn.commit()
            return deleted

        return await self._run(_work)

    async def get_meme(self, meme_id: int) -> Optional[MemeRecord]:
        def _work() -> Optional[MemeRecord]:
            row = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE id=?
                """,
                (meme_id,),
            ).fetchone()
            return self._row_to_record(row) if row else None

        return await self._run(_work)

    async def get_pending_meme(self, meme_id: int) -> Optional[MemeRecord]:
        def _work() -> Optional[MemeRecord]:
            row = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE id=? AND posted=0
                """,
                (meme_id,),
            ).fetchone()
            return self._row_to_record(row) if row else None

        return await self._run(_work)

    async def get_next_pending_meme(self) -> Optional[MemeRecord]:
        def _work() -> Optional[MemeRecord]:
            row = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                WHERE posted=0
                ORDER BY scheduled_ts ASC
                LIMIT 1
                """
            ).fetchone()
            return self._row_to_record(row) if row else None

        return await self._run(_work)

    async def update_schedule(self, meme_id: int, scheduled_ts: int) -> bool:
        def _work() -> bool:
            conn = self._conn_ready()
            cursor = conn.execute(
                "UPDATE memes SET scheduled_ts=? WHERE id=? AND posted=0",
                (scheduled_ts, meme_id),
            )
            conn.commit()
            return cursor.rowcount > 0

        return await self._run(_work)

    async def update_many_schedules(self, updates: list[tuple[int, int]]) -> int:
        updated = 0
        for scheduled_ts, meme_id in updates:
            if await self.update_schedule(meme_id, scheduled_ts):
                updated += 1
        return updated

    async def export_memes(self) -> list[MemeRecord]:
        def _work() -> list[MemeRecord]:
            rows = self._conn_ready().execute(
                """
                SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                       preview_file_id, caption
                FROM memes
                ORDER BY id ASC
                """
            ).fetchall()
            return [self._row_to_record(row) for row in rows]

        return await self._run(_work)

    async def import_memes(self, memes: list[MemeRecord]) -> None:
        def _work() -> None:
            conn = self._conn_ready()
            conn.execute("DELETE FROM memes")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='memes'")
            if memes:
                conn.executemany(
                    """
                    INSERT INTO memes (
                        id, owner_file_id, mime_type, scheduled_ts, posted, created_ts,
                        preview_file_id, caption
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record.id,
                            record.owner_file_id,
                            record.mime_type,
                            record.scheduled_ts,
                            record.posted,
                            record.created_ts,
                            record.preview_file_id,
                            record.caption,
                        )
                        for record in memes
                    ],
                )
            conn.commit()

        await self._run(_work)

    async def count_memes(self) -> dict[str, int]:
        def _work() -> dict[str, int]:
            conn = self._conn_ready()
            total = int(conn.execute("SELECT COUNT(*) FROM memes").fetchone()[0])
            scheduled = int(conn.execute("SELECT COUNT(*) FROM memes WHERE posted=0").fetchone()[0])
            return {"total": total, "scheduled": scheduled, "posted": total - scheduled}

        return await self._run(_work)

    async def size_bytes(self) -> int:
        def _work() -> int:
            return self.path.stat().st_size if self.path.exists() else 0

        return await self._run(_work)


def load_db_profiles() -> tuple[dict[str, BackendProfile], str]:
    raw_profiles = os.environ.get("MEMEBOT_DB_PROFILES")
    active_key = os.environ.get("MEMEBOT_ACTIVE_DB", "default")
    profiles: dict[str, BackendProfile] = {}
    if raw_profiles:
        decoded = json.loads(raw_profiles)
        if not isinstance(decoded, dict):
            raise RuntimeError("MEMEBOT_DB_PROFILES must be a JSON object.")
        for key, value in decoded.items():
            if not isinstance(value, dict):
                raise RuntimeError(f"Database profile {key} must be an object.")
            kind = str(value.get("kind", "")).strip().lower()
            label = str(value.get("label") or key)
            profile = BackendProfile(
                key=key,
                kind=kind,
                label=label,
                url=value.get("url"),
                path=value.get("path"),
                storage_limit_bytes=parse_byte_size(
                    str(value["storage_limit_bytes"]) if "storage_limit_bytes" in value else None
                ),
                fallback_hours=int(value.get("fallback_hours", 48)),
            )
            profiles[key] = profile
    else:
        database_url = build_postgres_url()
        if database_url:
            profiles["default"] = BackendProfile(
                key="default",
                kind="postgresql",
                label=os.environ.get("MEMEBOT_DB_LABEL", "Primary PostgreSQL"),
                url=database_url,
                storage_limit_bytes=parse_byte_size(os.environ.get("MEMEBOT_DB_LIMIT")),
            )
        sqlite_path = os.environ.get("MEMEBOT_SQLITE_PATH")
        if sqlite_path:
            profiles["sqlite"] = BackendProfile(
                key="sqlite",
                kind="sqlite",
                label=os.environ.get("MEMEBOT_SQLITE_LABEL", "Local SQLite"),
                path=sqlite_path,
                storage_limit_bytes=parse_byte_size(os.environ.get("MEMEBOT_SQLITE_LIMIT")),
            )
        if not profiles:
            raise RuntimeError(
                "Configure DATABASE_URL/MEMEBOT_DB or MEMEBOT_DB_PROFILES before starting the bot."
            )
    if active_key not in profiles:
        active_key = next(iter(profiles))
    return profiles, active_key


@dataclass
class StorageSnapshot:
    timestamp: str
    active_profile: str
    db_bytes: int
    backup_bytes: int
    cache_bytes: int
    log_bytes: int
    runtime_bytes: int
    total_bytes: int
    local_free_bytes: int
    db_limit_bytes: Optional[int]


def directory_size(path: Optional[Path]) -> int:
    if path is None or not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def predict_fill_date(
    samples: list[StorageSnapshot],
    current_total: int,
    capacity_bytes: Optional[int],
    free_bytes: Optional[int] = None,
) -> Optional[str]:
    if len(samples) < 2:
        return None
    first = samples[0]
    last = samples[-1]
    span_seconds = (
        datetime.fromisoformat(last.timestamp) - datetime.fromisoformat(first.timestamp)
    ).total_seconds()
    if span_seconds <= 0:
        return None
    delta = last.total_bytes - first.total_bytes
    if delta <= 0:
        return None
    growth_per_second = delta / span_seconds
    if free_bytes is not None and free_bytes > 0:
        remaining = free_bytes
    elif capacity_bytes is not None and capacity_bytes > current_total:
        remaining = capacity_bytes - current_total
    else:
        return None
    seconds_left = remaining / growth_per_second
    return (utcnow() + timedelta(seconds=seconds_left)).date().isoformat()


class DatabaseRuntime:
    def __init__(
        self,
        profiles: dict[str, BackendProfile],
        active_key: str,
        backup_dir: Path,
        runtime_dir: Path,
        cache_dir: Optional[Path] = None,
        log_dir: Optional[Path] = None,
    ):
        self.profiles = profiles
        self.active_key = active_key
        self.backup_dir = backup_dir
        self.runtime_dir = runtime_dir
        self.cache_dir = cache_dir
        self.log_dir = log_dir
        self.state_path = runtime_dir / "db_state.json"
        self.storage_history_path = runtime_dir / "storage-history.jsonl"
        self.operation_lock = asyncio.Lock()
        self._backends: dict[str, BaseBackend] = {}
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self._load_state()

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        active_key = payload.get("active_key")
        if isinstance(active_key, str) and active_key in self.profiles:
            self.active_key = active_key

    def _save_state(self, extra: Optional[dict[str, Any]] = None) -> None:
        payload = {
            "active_key": self.active_key,
            "updated_at": iso_utcnow(),
        }
        if extra:
            payload.update(extra)
        self.state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    async def close(self) -> None:
        for backend in self._backends.values():
            await backend.close()

    async def get_backend(self, key: Optional[str] = None) -> BaseBackend:
        profile_key = key or self.active_key
        if profile_key not in self._backends:
            profile = self.profiles[profile_key]
            if profile.kind in {"postgres", "postgresql", "neon", "supabase", "cockroachdb"}:
                backend = PostgresBackend(profile)
            elif profile.kind in {"sqlite", "turso"}:
                backend = SQLiteBackend(profile)
            else:
                raise RuntimeError(
                    f"Profile {profile_key} uses unsupported backend kind '{profile.kind}'."
                )
            await backend.init_schema()
            self._backends[profile_key] = backend
        return self._backends[profile_key]

    async def init_active_backend(self) -> None:
        await self.get_backend(self.active_key)
        self._save_state()

    async def snapshot_counts(self, key: Optional[str] = None) -> dict[str, int]:
        backend = await self.get_backend(key)
        return await backend.count_memes()

    async def export_payload(self, key: Optional[str] = None) -> dict[str, Any]:
        profile_key = key or self.active_key
        backend = await self.get_backend(profile_key)
        memes = await backend.export_memes()
        counts = await backend.count_memes()
        meme_dicts = [asdict(record) for record in memes]
        integrity_input = {
            "profile": profile_key,
            "counts": counts,
            "memes": meme_dicts,
        }
        checksum = _sha256_text(_canonical_json(integrity_input))
        return {
            "version": 2,
            "generated_at": iso_utcnow(),
            "profile": profile_key,
            "kind": self.profiles[profile_key].kind,
            "counts": counts,
            "memes": meme_dicts,
            "integrity": {
                "algorithm": "sha256",
                "checksum": checksum,
                "record_count": len(meme_dicts),
            },
        }

    @staticmethod
    def verify_payload(payload: dict[str, Any]) -> None:
        memes = payload.get("memes")
        counts = payload.get("counts")
        integrity = payload.get("integrity")
        if not isinstance(memes, list) or not isinstance(counts, dict) or not isinstance(integrity, dict):
            raise RuntimeError("Backup payload is incomplete.")
        canonical = {
            "profile": payload.get("profile"),
            "counts": counts,
            "memes": memes,
        }
        checksum = _sha256_text(_canonical_json(canonical))
        if integrity.get("checksum") != checksum:
            raise RuntimeError("Backup checksum verification failed.")
        if int(integrity.get("record_count", -1)) != len(memes):
            raise RuntimeError("Backup record count verification failed.")
        total = int(counts.get("total", -1))
        scheduled = int(counts.get("scheduled", -1))
        posted = int(counts.get("posted", -1))
        if total != len(memes) or total != scheduled + posted:
            raise RuntimeError("Backup counts are inconsistent.")

    async def write_backup_file(self, key: Optional[str] = None) -> tuple[Path, dict[str, Any]]:
        payload = await self.export_payload(key)
        self.verify_payload(payload)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        path = self.backup_dir / f"memes-backup-{timestamp}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        written = json.loads(path.read_text(encoding="utf-8"))
        self.verify_payload(written)
        return path, written

    async def import_payload(self, payload: dict[str, Any], target_key: str) -> dict[str, int]:
        self.verify_payload(payload)
        backend = await self.get_backend(target_key)
        records = [
            MemeRecord(
                id=int(item["id"]),
                owner_file_id=item["owner_file_id"],
                mime_type=item.get("mime_type"),
                scheduled_ts=int(item["scheduled_ts"]),
                posted=int(item.get("posted", 0)),
                created_ts=int(item["created_ts"]),
                preview_file_id=item.get("preview_file_id"),
                caption=item.get("caption"),
            )
            for item in payload["memes"]
        ]
        await backend.import_memes(records)
        counts = await backend.count_memes()
        expected = payload["counts"]
        if counts != expected:
            raise RuntimeError(
                f"Target verification failed. Expected {expected}, received {counts}."
            )
        return counts

    def latest_backup_path(self) -> Optional[Path]:
        backups = sorted(self.backup_dir.glob("memes-backup-*.json"))
        if not backups:
            return None
        return backups[-1]

    def load_backup_file(self, path: Path) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.verify_payload(payload)
        return payload

    async def migrate_to(
        self,
        target_key: str,
        *,
        allow_backup_fallback: bool = False,
    ) -> dict[str, Any]:
        if target_key == self.active_key:
            raise RuntimeError("Target database is already active.")
        if target_key not in self.profiles:
            raise RuntimeError(f"Unknown database profile: {target_key}")

        async with self.operation_lock:
            source_key = self.active_key
            started_at = utcnow()
            warnings: list[str] = []
            try:
                path, payload = await self.write_backup_file(source_key)
                source_mode = "live"
            except Exception:
                if not allow_backup_fallback:
                    raise
                latest = self.latest_backup_path()
                if latest is None:
                    raise RuntimeError("Live export failed and no verified backup is available.")
                payload = self.load_backup_file(latest)
                path = latest
                source_mode = "backup"
                warnings.append("Switched using latest verified backup because live export failed.")
            source_counts = payload["counts"]
            target_counts = await self.import_payload(payload, target_key)
            self.active_key = target_key
            fallback_until = utcnow() + timedelta(hours=self.profiles[source_key].fallback_hours)
            summary = {
                "source_key": source_key,
                "target_key": target_key,
                "source_counts": source_counts,
                "target_counts": target_counts,
                "backup_path": str(path),
                "source_mode": source_mode,
                "started_at": started_at.isoformat(),
                "completed_at": iso_utcnow(),
                "fallback_until": fallback_until.isoformat(),
                "warnings": warnings,
            }
            self._save_state({"last_transition": summary})
            return summary

    async def capture_storage_snapshot(self) -> StorageSnapshot:
        backend = await self.get_backend(self.active_key)
        db_bytes = await backend.size_bytes()
        backup_bytes = directory_size(self.backup_dir)
        cache_bytes = directory_size(self.cache_dir)
        log_bytes = directory_size(self.log_dir)
        runtime_bytes = directory_size(self.runtime_dir)
        total_bytes = db_bytes + backup_bytes + cache_bytes + log_bytes + runtime_bytes
        usage = shutil.disk_usage(self.backup_dir)
        snapshot = StorageSnapshot(
            timestamp=iso_utcnow(),
            active_profile=self.active_key,
            db_bytes=db_bytes,
            backup_bytes=backup_bytes,
            cache_bytes=cache_bytes,
            log_bytes=log_bytes,
            runtime_bytes=runtime_bytes,
            total_bytes=total_bytes,
            local_free_bytes=usage.free,
            db_limit_bytes=self.profiles[self.active_key].storage_limit_bytes,
        )
        with self.storage_history_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(asdict(snapshot)) + "\n")
        return snapshot

    def storage_history(self, days: int = 30) -> list[StorageSnapshot]:
        if not self.storage_history_path.exists():
            return []
        cutoff = utcnow() - timedelta(days=days)
        snapshots: list[StorageSnapshot] = []
        for line in self.storage_history_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
                timestamp = datetime.fromisoformat(payload["timestamp"])
            except (KeyError, ValueError, json.JSONDecodeError):
                continue
            if timestamp < cutoff:
                continue
            snapshots.append(StorageSnapshot(**payload))
        return snapshots
