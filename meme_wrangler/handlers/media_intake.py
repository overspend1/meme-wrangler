"""Media intake handler - processes memes sent via DM."""

from __future__ import annotations

import logging
from typing import Any

from meme_wrangler.config import cfg
from meme_wrangler.decorators import owner_only
from meme_wrangler.scheduling import schedule_meme
from meme_wrangler import backup as backup_mod

logger = logging.getLogger(__name__)


@owner_only
async def handle_media(update: Any, context: Any) -> None:
    """Accept a photo/video/animation and schedule it."""
    msg = update.message
    file_id = None
    mime = None
    caption = msg.caption

    if msg.photo:
        file_id = msg.photo[-1].file_id
        mime = "image"
    elif msg.video:
        file_id = msg.video.file_id
        mime = "video"
    elif msg.animation:
        file_id = msg.animation.file_id
        mime = "image"  # GIFs treated as image
    else:
        await msg.reply_text("Please send a photo, animation (GIF) or video.")
        return

    scheduled_dt = await schedule_meme(file_id, mime, caption)
    await msg.reply_text(
        f"Scheduled for: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S IST')}"
    )

    # Automatic backup after each intake
    try:
        path, total, sched = await backup_mod.create_backup()
        logger.info(
            "Auto-backup at %s after scheduling meme (total=%d, sched=%d)",
            path,
            total,
            sched,
        )
    except Exception as exc:
        logger.exception("Auto-backup failed after scheduling: %s", exc)
