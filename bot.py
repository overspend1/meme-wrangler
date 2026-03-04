"""Meme Wrangler Bot - thin entry point.

All logic lives in the ``meme_wrangler`` package.  This file wires up
handlers, starts background tasks, and runs the Telegram polling loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from telegram.ext import (  # type: ignore[import-untyped]
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from meme_wrangler.config import cfg
from meme_wrangler.db import close_pool, init_db
from meme_wrangler.poster import (
    periodic_backup,
    periodic_health_check,
    periodic_poster,
)

# Handlers
from meme_wrangler.handlers.admin import (
    logcmd,
    postnow,
    preview,
    scheduleat,
    scheduled,
    unschedule,
)
from meme_wrangler.handlers.backup_cmds import (
    backup,
    backupstatus,
    restore,
    verifybackup,
)
from meme_wrangler.handlers.general import helpcmd, start
from meme_wrangler.handlers.media_intake import handle_media

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TaskFactory = Callable[[Application], Awaitable[None]]


@dataclass(frozen=True)
class TaskSpec:
    """Describes a managed background task."""

    name: str
    factory: TaskFactory
    restart_on_failure: bool = True


BACKGROUND_TASKS_KEY = "background_tasks"
_BACKOFF_INITIAL_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 60.0


def _task_specs() -> list[TaskSpec]:
    return [
        TaskSpec("periodic_poster", lambda app: periodic_poster(app.bot)),
        TaskSpec("periodic_backup", lambda _app: periodic_backup()),
        TaskSpec("periodic_health_check", lambda _app: periodic_health_check()),
    ]


async def _supervise_task(
    application: Application,
    spec: TaskSpec,
) -> None:
    """Run a task and optionally restart it with backoff on unhandled failures."""
    backoff = _BACKOFF_INITIAL_SECONDS

    while True:
        try:
            await spec.factory(application)
            logger.info("Background task %s exited normally", spec.name)
            return
        except asyncio.CancelledError:
            logger.info("Background task %s cancelled", spec.name)
            raise
        except Exception:
            logger.exception("Background task %s crashed", spec.name)
            if not spec.restart_on_failure:
                return
            logger.warning(
                "Restarting background task %s in %.1f seconds",
                spec.name,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX_SECONDS)


def _register_handlers(app: Application) -> None:
    """Register all command and message handlers."""
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

    media_filter = filters.ChatType.PRIVATE & (
        filters.PHOTO | filters.VIDEO | filters.ANIMATION
    )
    app.add_handler(MessageHandler(media_filter, handle_media))


async def post_init(application: Application) -> None:
    """PTB lifecycle hook: log identity and start supervised background tasks."""
    me = await application.bot.get_me()
    logger.info("Bot connected as @%s (id=%s)", me.username, me.id)

    task_registry: dict[str, asyncio.Task[None]] = {}
    for spec in _task_specs():
        task_registry[spec.name] = asyncio.create_task(
            _supervise_task(application, spec),
            name=f"meme_wrangler:{spec.name}",
        )

    application.bot_data[BACKGROUND_TASKS_KEY] = task_registry


async def post_shutdown(application: Application) -> None:
    """PTB lifecycle hook: stop background tasks and close DB pool."""
    tasks: dict[str, asyncio.Task[None]] = application.bot_data.get(BACKGROUND_TASKS_KEY, {})
    running_tasks = [task for task in tasks.values() if not task.done()]

    for task in running_tasks:
        task.cancel()

    if running_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*running_tasks, return_exceptions=True),
                timeout=10,
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out while waiting for background tasks to cancel")

    await close_pool()


def build_application() -> Application:
    """Create and configure the PTB application instance."""
    app = ApplicationBuilder().token(cfg.bot_token).build()
    _register_handlers(app)
    app.post_init = post_init
    app.post_shutdown = post_shutdown
    return app


def main() -> None:
    """Validate config, initialise the DB, register handlers, and run."""
    cfg.validate()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = build_application()

    logger.info("Starting bot...")
    app.run_polling()


if __name__ == "__main__":
    main()
