"""Tests for handler decorators."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from meme_wrangler.decorators import owner_only


@pytest.mark.asyncio
async def test_owner_only_allows_owner():
    """Handler executes when user is in owner_ids."""
    handler = AsyncMock(return_value="ok")
    wrapped = owner_only(handler)

    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=12345),
        message=AsyncMock(),
    )
    context = SimpleNamespace()

    with patch("meme_wrangler.decorators.cfg") as mock_cfg:
        mock_cfg.owner_ids = {12345}
        result = await wrapped(update, context)

    handler.assert_awaited_once()
    assert result == "ok"


@pytest.mark.asyncio
async def test_owner_only_rejects_stranger():
    """Handler does NOT execute for non-owners; reply is sent."""
    handler = AsyncMock()
    wrapped = owner_only(handler)

    reply_mock = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99999),
        message=SimpleNamespace(reply_text=reply_mock),
    )
    context = SimpleNamespace()

    with patch("meme_wrangler.decorators.cfg") as mock_cfg:
        mock_cfg.owner_ids = {12345}
        result = await wrapped(update, context)

    handler.assert_not_awaited()
    reply_mock.assert_awaited_once()
    assert result is None
