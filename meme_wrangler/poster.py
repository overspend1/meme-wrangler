"""Background tasks - periodic meme posting, backup, and health checks."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from meme_wrangler.config import cfg, IST
from meme_wrangler.db import check_pool_health
from meme_wrangler.media import send_media_with_fallback
from meme_wrangler.scheduling import fetch_due_memes, mark_posted
from meme_wrangler import backup as backup_mod

logger = logging.getLogger(__name__)

# In-memory log of recent posting events (capped at 100 entries).
posting_log: list[str] = []

_MAX_LOG = 100


def _append_log(entry: str) -> None:
    posting_log.append(entry)
    if len(posting_log) > _MAX_LOG:
        posting_log.pop(0)


# ---------------------------------------------------------------------------
# Periodic poster
# ---------------------------------------------------------------------------


async def pop_due_memes_and_post(bot) -> None:
    """Post all memes whose scheduled time has arrived."""
    memes = await fetch_due_memes()
    for meme in memes:
        try:
            sent = await send_media_with_fallback(
                bot,
                cfg.channel_id,
                meme.owner_file_id,
                mime=meme.mime_type,
                caption=meme.caption,
                meme_id=meme.id,
            )
            if sent:
                await mark_posted(meme.id)
                logger.info("Posted meme id=%s", meme.id)
                _append_log(
                    f"[SUCCESS] Posted meme id={meme.id} at "
                    f"{datetime.now(IST).isoformat(sep=' ')}"
                )
            else:
                _append_log(
                    f"[FAIL] Meme id={meme.id} at "
                    f"{datetime.now(IST).isoformat(sep=' ')}: "
                    "all send methods failed"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Failed to post meme id=%s: %s", meme.id, exc)
            _append_log(
                f"[FAIL] Meme id={meme.id} at "
                f"{datetime.now(IST).isoformat(sep=' ')}: "
                f"{type(exc).__name__}: {exc}"
            )


async def periodic_poster(bot) -> None:
    """Background loop that posts due memes every 30 seconds."""
    while True:
        try:
            await pop_due_memes_and_post(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in poster loop")
        await asyncio.sleep(30)


# ---------------------------------------------------------------------------
# Periodic backup
# ---------------------------------------------------------------------------


async def periodic_backup() -> None:
    """Background loop that creates a backup on a configurable interval."""
    interval = cfg.backup_interval_hours * 3600
    if interval <= 0:
        logger.info("Periodic backup disabled (interval=0)")
        return

    logger.info(
        "Periodic backup task started (every %.1f hours)",
        cfg.backup_interval_hours,
    )
    while True:
        await asyncio.sleep(interval)
        try:
            path, total, sched = await backup_mod.create_backup()
            logger.info(
                "Periodic backup created: %s (%d total, %d scheduled)",
                path,
                total,
                sched,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic backup failed")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


async def periodic_health_check() -> None:
    """Background loop that verifies pool liveness every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        try:
            ok = await check_pool_health()
            if not ok:
                logger.warning("DB health check triggered pool recreation")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Health check error")
