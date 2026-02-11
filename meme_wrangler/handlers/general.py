"""General commands: /start, /help."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

HELP_TEXT = """
<b>Meme Wrangler Bot Command Reference</b>

<b>General:</b>
  <b>/start</b> -- Welcome message.
  <b>/help</b> -- This help text.

<b>Scheduling Memes:</b>
  <b>Send a photo/video/animation</b> (DM the bot):
    Schedules it for the next slot (11:00, 16:00, 21:00 IST).
    Add a caption to include it with the post.

  <b>/scheduled</b> -- List all pending memes with previews.
  <b>/unschedule &lt;id1&gt; [id2 ...]</b> -- Remove memes from the queue.
  <b>/postnow [id]</b> -- Post the next meme (or a specific one) immediately.
  <b>/preview &lt;id&gt;</b> -- Preview a scheduled meme.
  <b>/log</b> -- Show the last 10 posting events.

<b>Maintenance:</b>
  <b>/backup &lt;password&gt;</b> -- Export all memes as a compressed JSON backup.
  <b>/restore &lt;password&gt;</b> -- Reply to a backup file to restore.
  <b>/backupstatus</b> -- Show backup statistics.
  <b>/verifybackup</b> -- Verify the latest backup's integrity.

<b>Advanced Scheduling:</b>
  <b>/scheduleat id: &lt;id&gt; &lt;HH:MM&gt;</b> -- Reschedule one meme.
  <b>/scheduleat ids: &lt;start&gt;-&lt;end&gt; &lt;YYYY-MM-DD&gt;</b> -- Reschedule a range.

<b>Notes:</b>
  Only owners (OWNER_ID) can use admin commands.
  All times are in <b>IST (Asia/Kolkata)</b>.
""".strip()


async def start(update: Any, context: Any) -> None:
    """Handle /start."""
    await update.message.reply_text(
        "Hi! I schedule memes to the configured channel."
    )


async def helpcmd(update: Any, context: Any) -> None:
    """Handle /help."""
    await update.message.reply_text(
        HELP_TEXT, parse_mode="HTML", disable_web_page_preview=True
    )
