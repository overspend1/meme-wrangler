"""Reusable handler decorators."""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable, Coroutine

from meme_wrangler.config import cfg

logger = logging.getLogger(__name__)


def owner_only(handler: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
    """Decorator that restricts a Telegram command handler to owner IDs."""

    @functools.wraps(handler)
    async def wrapper(update: Any, context: Any) -> Any:
        user = update.effective_user
        if user is None or user.id not in cfg.owner_ids:
            if update.message:
                await update.message.reply_text(
                    "Only the owner can use this command."
                )
            return None
        return await handler(update, context)

    return wrapper
