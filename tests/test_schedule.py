"""Tests for slot-computation logic in ``meme_wrangler.scheduling``."""

import asyncio
from datetime import datetime

import pytest

from meme_wrangler.config import SLOTS
from meme_wrangler.scheduling import compute_next_slot, schedule_meme


@pytest.mark.asyncio
async def test_before_first_slot():
    """A time before 11:00 should yield the 11:00 slot on the same day."""
    dt = datetime(2025, 10, 18, 9, 0)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[0]
    assert result.date().day == 18


@pytest.mark.asyncio
async def test_between_first_and_second_slot():
    """12:00 is between 11:00 and 16:00 -> next slot is 16:00."""
    dt = datetime(2025, 10, 18, 12, 0)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[1]


@pytest.mark.asyncio
async def test_after_last_slot():
    """22:00 is past the last slot -> wraps to next day's first slot."""
    dt = datetime(2025, 10, 18, 22, 0)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[0]
    assert result.date().day == 19


@pytest.mark.asyncio
async def test_exactly_on_slot_time():
    """Exactly at 11:00 should give 16:00 (strictly after)."""
    dt = datetime(2025, 10, 18, 11, 0)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[1]


@pytest.mark.asyncio
async def test_just_before_midnight():
    """23:59 should wrap to next day 11:00."""
    dt = datetime(2025, 10, 18, 23, 59)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[0]
    assert result.date().day == 19


@pytest.mark.asyncio
async def test_midnight():
    """00:00 should give 11:00 same day."""
    dt = datetime(2025, 10, 18, 0, 0)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[0]
    assert result.date().day == 18


@pytest.mark.asyncio
async def test_between_second_and_third_slot():
    """17:30 is between 16:00 and 21:00 -> next slot is 21:00."""
    dt = datetime(2025, 10, 18, 17, 30)
    result = await compute_next_slot(dt)
    assert result.time() == SLOTS[2]


@pytest.mark.asyncio
async def test_none_uses_current_time():
    """Passing None should not raise and should return a future datetime."""
    result = await compute_next_slot(None)
    assert result is not None
    assert result.tzinfo is not None


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self):
        self._advisory_lock = asyncio.Lock()
        self.scheduled = []

    def transaction(self):
        return _FakeTransaction()

    async def fetchrow(self, query, *args):
        if "ORDER BY scheduled_ts DESC" in query:
            if not self.scheduled:
                return None
            return {"scheduled_ts": max(self.scheduled)}
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    async def execute(self, query, *args):
        if "pg_advisory_xact_lock" in query:
            await self._advisory_lock.acquire()
            return "SELECT 1"
        if "INSERT INTO memes" in query:
            self.scheduled.append(args[2])
            self._advisory_lock.release()
            return "INSERT 0 1"
        raise AssertionError(f"Unexpected execute query: {query}")


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _FakeAcquire(self.conn)


@pytest.mark.asyncio
async def test_schedule_meme_concurrent_calls_are_serialized(monkeypatch):
    """Concurrent schedules should allocate unique, ordered slots."""
    conn = _FakeConn()

    async def fake_get_pool():
        return _FakePool(conn)

    monkeypatch.setattr("meme_wrangler.scheduling.get_pool", fake_get_pool)

    results = await asyncio.gather(
        *[schedule_meme(f"file-{i}", "image/jpeg") for i in range(5)]
    )

    scheduled_ts = [int(dt.timestamp()) for dt in results]
    assert len(set(scheduled_ts)) == 5
    assert scheduled_ts == sorted(scheduled_ts)
    assert all(later > earlier for earlier, later in zip(scheduled_ts, scheduled_ts[1:]))
