"""Tests for the unified media sender."""

import pytest
from unittest.mock import AsyncMock, patch

from meme_wrangler.media import send_media_with_fallback


@pytest.mark.asyncio
async def test_video_sent_first_for_video_mime():
    bot = AsyncMock()
    result = await send_media_with_fallback(
        bot, 123, "fileid", mime="video/mp4", caption="c", meme_id=1
    )
    bot.send_video.assert_awaited_once()
    assert result is True


@pytest.mark.asyncio
async def test_photo_fallback_when_video_fails():
    bot = AsyncMock()
    bot.send_video.side_effect = Exception("nope")
    result = await send_media_with_fallback(
        bot, 123, "fileid", mime="video", caption="c", meme_id=2
    )
    # After video fails, should try photo
    bot.send_photo.assert_awaited_once()
    assert result is True


@pytest.mark.asyncio
async def test_document_fallback():
    bot = AsyncMock()
    bot.send_photo.side_effect = Exception("nope")
    result = await send_media_with_fallback(
        bot, 123, "fileid", mime="image", caption="c", meme_id=3
    )
    bot.send_document.assert_awaited_once()
    assert result is True


@pytest.mark.asyncio
async def test_all_methods_fail_returns_false():
    bot = AsyncMock()
    bot.send_video.side_effect = Exception("nope")
    bot.send_photo.side_effect = Exception("nope")
    bot.send_document.side_effect = Exception("nope")
    bot.get_file.side_effect = Exception("nope")

    result = await send_media_with_fallback(
        bot, 123, "fileid", mime="image", caption="c", meme_id=4
    )
    assert result is False
