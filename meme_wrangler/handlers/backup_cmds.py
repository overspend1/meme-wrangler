"""Backup-related commands: /backup, /restore, /backupstatus, /verifybackup."""

from __future__ import annotations

import io
import json
import logging
from typing import Any

from meme_wrangler.config import cfg
from meme_wrangler.decorators import owner_only
from meme_wrangler.models import Meme
from meme_wrangler import backup as backup_mod

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# /backup
# ------------------------------------------------------------------

@owner_only
async def backup(update: Any, context: Any) -> None:
    """Export all memes as a compressed JSON backup."""
    if not cfg.verify_backup_password(
        context.args[0] if context.args else None
    ):
        await update.message.reply_text(
            "Backup password missing or incorrect. "
            "Usage: /backup <password>"
        )
        return

    try:
        path, total, sched = await backup_mod.create_backup(
            send_document_to=update.effective_chat.id,
            bot=context.bot,
        )
        logger.info("Backup exported to %s", path)
    except Exception as exc:
        logger.exception("Backup failed: %s", exc)
        await update.message.reply_text(
            f"Backup failed: {type(exc).__name__}: {exc}"
        )


# ------------------------------------------------------------------
# /restore
# ------------------------------------------------------------------

@owner_only
async def restore(update: Any, context: Any) -> None:
    """Restore memes from a backup file (reply to the file)."""
    if not cfg.verify_backup_password(
        context.args[0] if context.args else None
    ):
        await update.message.reply_text(
            "Backup password missing or incorrect. "
            "Usage: /restore <password> (reply to backup file)"
        )
        return

    replied = update.message.reply_to_message if update.message else None
    if not replied or not replied.document:
        await update.message.reply_text(
            "Reply to a backup JSON document with /restore."
        )
        return

    tg_file = await context.bot.get_file(replied.document.file_id)
    buf = io.BytesIO()
    await tg_file.download(out=buf)
    buf.seek(0)
    raw = buf.read()

    try:
        data = backup_mod.load_backup_data(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        await update.message.reply_text(f"Could not parse backup: {exc}")
        return

    raw_memes = data.get("memes")
    if not isinstance(raw_memes, list):
        await update.message.reply_text("Backup file missing 'memes' list.")
        return

    try:
        memes = [Meme.from_dict(item) for item in raw_memes]
    except (KeyError, TypeError, ValueError) as exc:
        await update.message.reply_text(f"Backup format error: {exc}")
        return

    count = await backup_mod.restore_memes(memes)
    scheduled_count = sum(1 for m in memes if m.posted == 0)
    await update.message.reply_text(
        f"Restore complete: {count} memes imported "
        f"({scheduled_count} scheduled)."
    )
    logger.info("Restored %d memes from backup", count)


# ------------------------------------------------------------------
# /backupstatus
# ------------------------------------------------------------------

@owner_only
async def backupstatus(update: Any, context: Any) -> None:
    """Show backup statistics."""
    status = await backup_mod.get_backup_status()
    kb = status.disk_usage_bytes / 1024
    lines = [
        f"Last backup: {status.last_backup_time or 'never'}",
        f"Backups on disk: {status.backups_on_disk}",
        f"Backups in DB: {status.backups_in_db}",
        f"Latest total memes: {status.total_memes}",
        f"Latest scheduled: {status.scheduled_memes}",
        f"Disk usage: {kb:.1f} KB",
    ]
    await update.message.reply_text("\n".join(lines))


# ------------------------------------------------------------------
# /verifybackup
# ------------------------------------------------------------------

@owner_only
async def verifybackup(update: Any, context: Any) -> None:
    """Verify the most recent backup file's integrity."""
    ok, message = backup_mod.verify_latest_backup()
    prefix = "OK" if ok else "WARN"
    await update.message.reply_text(f"[{prefix}] {message}")
