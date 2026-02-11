"""Owner-only administration commands."""

from __future__ import annotations

import logging
import re
from datetime import datetime, time as dtime
from typing import Any

from meme_wrangler.config import cfg, IST, ist_localize, SLOTS
from meme_wrangler.decorators import owner_only
from meme_wrangler.media import send_media_with_fallback
from meme_wrangler.poster import posting_log
from meme_wrangler.scheduling import (
    delete_memes,
    fetch_meme_by_id,
    fetch_next_unposted,
    fetch_pending_memes,
    mark_posted,
    reschedule_batch,
    reschedule_single,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# /scheduled
# ------------------------------------------------------------------

@owner_only
async def scheduled(update: Any, context: Any) -> None:
    """Show all pending memes with inline previews."""
    memes = await fetch_pending_memes()
    if not memes:
        await update.message.reply_text("No scheduled memes.")
        return

    for meme in memes:
        file_id = meme.preview_file_id or meme.owner_file_id
        parts = [
            f"ID: {meme.id}",
            f"Time: {datetime.fromtimestamp(meme.scheduled_ts, tz=IST).strftime('%Y-%m-%d %H:%M:%S IST')}",
            f"Type: {meme.mime_type}",
        ]
        if meme.caption:
            parts.append(f"Caption: {meme.caption}")
        caption = ", ".join(parts)

        sent = await send_media_with_fallback(
            context.bot,
            update.effective_chat.id,
            file_id,
            mime=meme.mime_type,
            caption=caption,
            meme_id=meme.id,
        )
        if not sent:
            await update.message.reply_text(caption)


# ------------------------------------------------------------------
# /unschedule
# ------------------------------------------------------------------

@owner_only
async def unschedule(update: Any, context: Any) -> None:
    """Remove one or more memes from the queue."""
    if not context.args or not all(a.isdigit() for a in context.args):
        await update.message.reply_text(
            "Usage: /unschedule <id1> <id2> ..."
        )
        return

    ids = [int(a) for a in context.args]
    await delete_memes(ids)
    joined = ", ".join(str(i) for i in ids)
    await update.message.reply_text(
        f"Unscheduled memes with IDs: {joined} "
        "(if they existed and were not yet posted)."
    )


# ------------------------------------------------------------------
# /preview
# ------------------------------------------------------------------

@owner_only
async def preview(update: Any, context: Any) -> None:
    """Preview a single meme by ID."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /preview <id>")
        return

    meme_id = int(context.args[0])
    try:
        await update.message.reply_text(f"Previewing meme {meme_id}...")
    except Exception:
        pass

    meme = await fetch_meme_by_id(meme_id)
    if not meme:
        await update.message.reply_text(
            f"No meme found with ID {meme_id}."
        )
        return

    sent = await send_media_with_fallback(
        context.bot,
        update.effective_chat.id,
        meme.owner_file_id,
        mime=meme.mime_type,
        caption=f"Preview ID {meme_id}",
        meme_id=meme_id,
    )
    if not sent:
        await update.message.reply_text(
            f"Could not preview meme {meme_id} - all send methods failed."
        )


# ------------------------------------------------------------------
# /postnow
# ------------------------------------------------------------------

@owner_only
async def postnow(update: Any, context: Any) -> None:
    """Immediately post the next scheduled meme (or a specific one)."""
    meme_id = None
    if context.args and context.args[0].isdigit():
        meme_id = int(context.args[0])

    meme = await fetch_next_unposted(meme_id)
    if not meme:
        label = f"with ID {meme_id}" if meme_id else ""
        await update.message.reply_text(
            f"No scheduled meme {label} to post."
        )
        return

    sent = await send_media_with_fallback(
        context.bot,
        cfg.channel_id,
        meme.owner_file_id,
        mime=meme.mime_type,
        caption=meme.caption,
        meme_id=meme.id,
    )
    if sent:
        await mark_posted(meme.id)
        await update.message.reply_text(
            f"Posted meme with ID {meme.id} to channel."
        )
    else:
        await update.message.reply_text(
            f"Failed to post meme {meme.id} - all send methods failed."
        )


# ------------------------------------------------------------------
# /log
# ------------------------------------------------------------------

@owner_only
async def logcmd(update: Any, context: Any) -> None:
    """Show the last posting events."""
    if not posting_log:
        await update.message.reply_text("No posting events yet.")
        return
    await update.message.reply_text(
        "Last posting events:\n" + "\n".join(posting_log[-10:])
    )


# ------------------------------------------------------------------
# /scheduleat
# ------------------------------------------------------------------

@owner_only
async def scheduleat(update: Any, context: Any) -> None:
    """Reschedule memes: single ID to a time, or a range to a date."""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /scheduleat id: <id> <HH:MM> "
            "or /scheduleat ids: <start>-<end> <YYYY-MM-DD>"
        )
        return

    argstr = " ".join(context.args)

    # Single: /scheduleat id: 6 16:20
    m_single = re.match(r"id:\s*(\d+)\s+(\d{2}):(\d{2})$", argstr)
    # Range: /scheduleat ids: 5-10 2025-10-19
    m_range = re.match(r"ids:\s*(\d+)-(\d+)\s+(\d{4}-\d{2}-\d{2})$", argstr)

    if m_single:
        meme_id = int(m_single.group(1))
        hour, minute = int(m_single.group(2)), int(m_single.group(3))
        if not (0 <= hour < 24 and 0 <= minute < 60):
            await update.message.reply_text(
                "Invalid time format. Use 24h HH:MM."
            )
            return
        now_ist = datetime.now(IST)
        sched_dt = ist_localize(
            datetime(now_ist.year, now_ist.month, now_ist.day, hour, minute)
        )
        await reschedule_single(meme_id, int(sched_dt.timestamp()))
        await update.message.reply_text(
            f"Rescheduled meme ID {meme_id} for "
            f"{sched_dt.strftime('%Y-%m-%d %H:%M')} IST."
        )

    elif m_range:
        start_id = int(m_range.group(1))
        end_id = int(m_range.group(2))
        date_str = m_range.group(3)
        base_date = ist_localize(datetime.strptime(date_str, "%Y-%m-%d"))
        slot_times = [dtime(11, 0), dtime(16, 0), dtime(21, 0)]
        ids = list(range(start_id, end_id + 1))
        if not ids:
            await update.message.reply_text("Invalid ID range.")
            return
        updates: list[tuple[int, int]] = []
        for idx, mid in enumerate(ids):
            slot = slot_times[idx % len(slot_times)]
            sched_dt = base_date.replace(
                hour=slot.hour, minute=slot.minute, second=0, microsecond=0
            )
            updates.append((int(sched_dt.timestamp()), mid))
        await reschedule_batch(updates)
        await update.message.reply_text(
            f"Rescheduled memes IDs {start_id}-{end_id} for {date_str} "
            "in slots 11:00, 16:00, 21:00 IST (cycled)."
        )

    else:
        await update.message.reply_text(
            "Invalid format. Use /scheduleat id: <id> <HH:MM> "
            "or /scheduleat ids: <start>-<end> <YYYY-MM-DD>"
        )
