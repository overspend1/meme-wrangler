"""Meme Wrangler Bot - thin entry point.

All logic lives in the ``meme_wrangler`` package.  This file wires up
handlers, starts background tasks, and runs the Telegram polling loop.
"""

from __future__ import annotations

import asyncio
import logging

from telegram.ext import (  # type: ignore[import-untyped]
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from meme_wrangler.config import cfg
from meme_wrangler.db import init_db, close_pool
from meme_wrangler.poster import (
    periodic_poster,
    periodic_backup,
    periodic_health_check,
)

# Handlers
from meme_wrangler.handlers.general import start, helpcmd
from meme_wrangler.handlers.admin import (
    scheduled,
    unschedule,
    preview,
    postnow,
    logcmd,
    scheduleat,
)
from meme_wrangler.handlers.backup_cmds import (
    backup,
    restore,
    backupstatus,
    verifybackup,
)
from meme_wrangler.handlers.media_intake import handle_media

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    """Validate config, initialise the DB, register handlers, and run."""
    cfg.validate()

    # Bootstrap the database pool + schema migrations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(cfg.bot_token).build()

    # --- Command handlers ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", helpcmd))
    app.add_handler(CommandHandler("postnow", postnow))
    app.add_handler(CommandHandler("scheduled", scheduled))
    app.add_handler(CommandHandler("unschedule", unschedule))
    app.add_handler(CommandHandler("preview", preview))
    app.add_handler(CommandHandler("log", logcmd))
    app.add_handler(CommandHandler("backup", backup))
    app.add_handler(CommandHandler("restore", restore))
    app.add_handler(CommandHandler("backupstatus", backupstatus))
    app.add_handler(CommandHandler("verifybackup", verifybackup))
    app.add_handler(CommandHandler("scheduleat", scheduleat))

    # --- Media intake (private DMs only) ---
    media_filter = filters.ChatType.PRIVATE & (
        filters.PHOTO | filters.VIDEO | filters.ANIMATION
    )
    app.add_handler(MessageHandler(media_filter, handle_media))

    # --- Background tasks via post_init hook ---
    async def post_init(application) -> None:
        me = await application.bot.get_me()
        logger.info("Bot connected as @%s (id=%s)", me.username, me.id)
        asyncio.create_task(periodic_poster(application.bot))
        asyncio.create_task(periodic_backup())
        asyncio.create_task(periodic_health_check())

    app.post_init = post_init

    logger.info("Starting bot...")
    app.run_polling()


if __name__ == "__main__":
    main()
