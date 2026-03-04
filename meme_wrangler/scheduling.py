"""Meme scheduling logic - slot computation and DB operations."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from meme_wrangler.config import cfg, IST, ist_localize, ensure_ist, SLOTS
from meme_wrangler.db import get_pool
from meme_wrangler.models import Meme

logger = logging.getLogger(__name__)


SCHEDULE_MEME_LOCK_KEY = 984331


async def compute_next_slot(
    after_dt: Optional[datetime] = None,
) -> datetime:
    """Return the next slot datetime strictly after *after_dt* (IST).

    When *after_dt* is ``None`` the current IST time is used.
    """
    if after_dt is None:
        after_dt = datetime.now(IST)
    else:
        after_dt = ensure_ist(after_dt)

    today = after_dt.date()
    for slot in SLOTS:
        candidate = ist_localize(datetime.combine(today, slot))
        if candidate > after_dt:
            return candidate

    next_day = today + timedelta(days=1)
    return ist_localize(datetime.combine(next_day, SLOTS[0]))


async def get_last_scheduled_ts(conn) -> Optional[int]:
    """Fetch the highest ``scheduled_ts`` among unposted memes."""
    row = await conn.fetchrow(
        "SELECT scheduled_ts FROM memes "
        "WHERE posted = 0 "
        "ORDER BY scheduled_ts DESC LIMIT 1"
    )
    return row["scheduled_ts"] if row else None


async def schedule_meme(
    owner_file_id: str,
    mime_type: str,
    caption: Optional[str] = None,
) -> datetime:
    """Insert a new meme and return its scheduled datetime (IST)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            lock_start = time.monotonic()
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1)",
                SCHEDULE_MEME_LOCK_KEY,
            )
            lock_wait_ms = (time.monotonic() - lock_start) * 1000
            logger.info(
                "Acquired schedule lock key=%s wait_ms=%.2f",
                SCHEDULE_MEME_LOCK_KEY,
                lock_wait_ms,
            )

            last_ts = await get_last_scheduled_ts(conn)
            if last_ts is None:
                ref_dt = datetime.now(IST)
            else:
                ref_dt = datetime.fromtimestamp(last_ts, tz=IST)

            next_dt = await compute_next_slot(ref_dt)
            created_ts = int(datetime.now(IST).timestamp())

            await conn.execute(
                """
                INSERT INTO memes
                    (owner_file_id, mime_type, scheduled_ts,
                     created_ts, preview_file_id, caption)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                owner_file_id,
                mime_type,
                int(next_dt.timestamp()),
                created_ts,
                owner_file_id,  # preview_file_id = owner_file_id
                caption,
            )
    return next_dt


async def fetch_pending_memes() -> list[Meme]:
    """Return all unposted memes ordered by scheduled time."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, owner_file_id, mime_type, scheduled_ts, "
            "       posted, created_ts, preview_file_id, caption "
            "FROM memes "
            "WHERE posted = 0 "
            "ORDER BY scheduled_ts ASC"
        )
    return [Meme.from_record(r) for r in rows]


async def fetch_due_memes() -> list[Meme]:
    """Return memes whose scheduled time has passed."""
    now_ts = int(datetime.now(IST).timestamp())
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, owner_file_id, mime_type, scheduled_ts, "
            "       posted, created_ts, preview_file_id, caption "
            "FROM memes "
            "WHERE posted = 0 AND scheduled_ts <= $1 "
            "ORDER BY scheduled_ts ASC",
            now_ts,
        )
    return [Meme.from_record(r) for r in rows]


async def mark_posted(meme_id: int) -> None:
    """Flag a meme as posted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE memes SET posted = 1 WHERE id = $1", meme_id
        )


async def delete_memes(meme_ids: list[int]) -> None:
    """Remove unposted memes by their IDs."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM memes WHERE posted = 0 AND id = ANY($1::int[])",
            meme_ids,
        )


async def fetch_meme_by_id(meme_id: int) -> Optional[Meme]:
    """Fetch a single meme (posted or not)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, owner_file_id, mime_type, scheduled_ts, "
            "       posted, created_ts, preview_file_id, caption "
            "FROM memes WHERE id = $1",
            meme_id,
        )
    return Meme.from_record(row) if row else None


async def fetch_next_unposted(meme_id: Optional[int] = None) -> Optional[Meme]:
    """Fetch a specific unposted meme or the earliest one."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if meme_id is not None:
            row = await conn.fetchrow(
                "SELECT id, owner_file_id, mime_type, scheduled_ts, "
                "       posted, created_ts, preview_file_id, caption "
                "FROM memes WHERE posted = 0 AND id = $1",
                meme_id,
            )
        else:
            row = await conn.fetchrow(
                "SELECT id, owner_file_id, mime_type, scheduled_ts, "
                "       posted, created_ts, preview_file_id, caption "
                "FROM memes WHERE posted = 0 "
                "ORDER BY scheduled_ts ASC LIMIT 1"
            )
    return Meme.from_record(row) if row else None


async def reschedule_single(meme_id: int, new_ts: int) -> None:
    """Change the scheduled timestamp of an unposted meme."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE memes SET scheduled_ts = $1 "
            "WHERE id = $2 AND posted = 0",
            new_ts,
            meme_id,
        )


async def reschedule_batch(updates: list[tuple[int, int]]) -> None:
    """Batch-update scheduled timestamps.  Each tuple is ``(new_ts, meme_id)``."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            "UPDATE memes SET scheduled_ts = $1 "
            "WHERE id = $2 AND posted = 0",
            updates,
        )
