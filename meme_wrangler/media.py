"""Unified media-sending helper with fallback chain.

This replaces the duplicated try-video / try-photo / try-document /
download-reupload logic that was scattered across multiple handlers.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def send_media_with_fallback(
    bot: Any,
    chat_id: int | str,
    file_id: str,
    mime: Optional[str] = None,
    caption: Optional[str] = None,
    *,
    meme_id: Optional[int] = None,
) -> bool:
    """Try to send *file_id* to *chat_id* using the best Telegram method.

    Fallback order:
      1. ``send_video`` (if *mime* starts with ``"video"``)
      2. ``send_photo``
      3. ``send_document``
      4. Download from Telegram and re-upload as document

    Returns ``True`` on success.
    """
    tag = f"meme_id={meme_id}" if meme_id else f"file={file_id[:20]}"

    # 1. send_video for video content
    if mime and mime.startswith("video"):
        try:
            await bot.send_video(chat_id, file_id, caption=caption)
            return True
        except Exception as exc:
            logger.debug("send_video failed (%s): %s", tag, exc)

    # 2. send_photo
    try:
        await bot.send_photo(chat_id, file_id, caption=caption)
        return True
    except Exception as exc:
        logger.debug("send_photo failed (%s): %s", tag, exc)

    # 3. send_document (still using original file_id)
    try:
        await bot.send_document(chat_id, file_id, caption=caption)
        return True
    except Exception as exc:
        logger.debug("send_document failed (%s): %s", tag, exc)

    # 4. Download + re-upload
    try:
        tg_file = await bot.get_file(file_id)
        bio = io.BytesIO()
        await tg_file.download(out=bio)
        bio.seek(0)

        # Lazy import to keep top-level clean when telegram isn't installed
        from telegram import InputFile  # type: ignore[import-untyped]

        if mime and mime.startswith("video"):
            fname = f"meme_{meme_id}.mp4" if meme_id else "meme.mp4"
            await bot.send_video(
                chat_id, InputFile(bio, filename=fname), caption=caption
            )
        else:
            fname = f"meme_{meme_id}.jpg" if meme_id else "meme.jpg"
            try:
                await bot.send_photo(
                    chat_id, InputFile(bio, filename=fname), caption=caption
                )
            except Exception:
                bio.seek(0)
                fname = f"meme_{meme_id}" if meme_id else "meme"
                await bot.send_document(
                    chat_id, InputFile(bio, filename=fname), caption=caption
                )
        return True
    except Exception as exc:
        logger.warning("download+reupload failed (%s): %s", tag, exc)

    return False
