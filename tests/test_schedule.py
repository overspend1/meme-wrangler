"""Tests for slot-computation logic in ``meme_wrangler.scheduling``."""

import asyncio
from datetime import datetime, time

import pytest

from meme_wrangler.config import SLOTS
from meme_wrangler.scheduling import compute_next_slot


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
