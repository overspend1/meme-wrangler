from __future__ import annotations

posting_log = []  # in-memory log


import asyncio
import hashlib
import hmac
import io
import json
import logging
import os
from urllib.parse import urlparse, urlunparse
from datetime import datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional, TYPE_CHECKING
from zoneinfo import ZoneInfo

try:
    import asyncpg  # type: ignore
except ModuleNotFoundError:
    asyncpg = None  # type: ignore

try:
    import pytz
except ModuleNotFoundError:
    pytz = None  # type: ignore

if TYPE_CHECKING:
    from telegram import Update, Message, InputFile
    from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters
else:
    try:
        from telegram import Update, Message, InputFile  # type: ignore
        from telegram.ext import (  # type: ignore
            ApplicationBuilder,
            ContextTypes,
            CommandHandler,
            MessageHandler,
            filters,
        )
    except ModuleNotFoundError:
        Update = Message = InputFile = Any  # type: ignore
        ContextTypes = SimpleNamespace(DEFAULT_TYPE=Any)  # type: ignore

        class _MissingTelegramModule:
            """Lazily raise when Telegram features are used without dependency."""

            def __getattr__(self, item):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot (missing telegram module)."
                )

            def __call__(self, *args, **kwargs):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot (missing telegram module)."
                )

        ApplicationBuilder = CommandHandler = MessageHandler = _MissingTelegramModule()  # type: ignore

        class _MissingFilters(SimpleNamespace):
            def __getattr__(self, item):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot (missing telegram filters)."
                )

        filters = _MissingFilters()  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IST timezone helpers
if pytz is not None:
    IST = pytz.timezone('Asia/Kolkata')

    def _ist_localize(dt: datetime) -> datetime:
        return IST.localize(dt)
else:
    IST = ZoneInfo('Asia/Kolkata')

    def _ist_localize(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST)


def _ensure_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return _ist_localize(dt)
    return dt.astimezone(IST)

def _build_database_url() -> Optional[str]:
    """Derive the database URL from explicit env vars or component pieces."""

    raw_url = os.environ.get("DATABASE_URL") or os.environ.get("MEMEBOT_DB")
    if not raw_url:
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        db_name = os.environ.get("POSTGRES_DB")
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        if user and password and db_name:
            raw_url = (
                f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
                f"@{host}:{port}/{db_name}"
            )
    if raw_url:
        return _normalize_database_url(raw_url)
    return None


def _normalize_database_url(url: str) -> str:
    """Replace localhost hosts with the configured Postgres host for container runs."""

    host_override = os.environ.get("POSTGRES_HOST")
    if not host_override:
        return url

    host_override = host_override.strip()
    if not host_override or host_override in {"localhost", "127.0.0.1", "::1"}:
        return url

    parsed = urlparse(url)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        return url

    if "@" in parsed.netloc:
        auth_prefix, _, _ = parsed.netloc.rpartition("@")
        auth_segment = f"{auth_prefix}@"
    else:
        auth_segment = ""

    if parsed.port is not None:
        port_fragment = f":{parsed.port}"
    else:
        port_env = os.environ.get("POSTGRES_PORT")
        port_fragment = f":{port_env}" if port_env else ""

    target_host = host_override
    if ":" in target_host and not target_host.startswith("["):
        target_host = f"[{target_host}]"

    new_netloc = f"{auth_segment}{target_host}{port_fragment}"
    rebuilt = parsed._replace(netloc=new_netloc)
    return urlunparse(rebuilt)


DATABASE_URL = _build_database_url()
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
CHANNEL_ID = os.environ.get("CHANNEL_ID")  # @channelusername or -100<id>
BACKUP_DIR = Path(os.environ.get("MEMEBOT_BACKUP_DIR", "backups"))
_HARDCODED_BACKUP_PASSWORD_HASH = "16c5b5ddf1b27f16ad5f801bb83595d00e666cc53085e53a4b1e67b715016251"
_HASH_OVERRIDE = os.environ.get("MEMEBOT_BACKUP_PASSWORD_HASH")
BACKUP_PASSWORD_HASH = _HASH_OVERRIDE if _HASH_OVERRIDE else _HARDCODED_BACKUP_PASSWORD_HASH

DB_POOL: Optional[asyncpg.pool.Pool] = None
_DB_INITIALIZED = False

SLOTS = [time(11, 0), time(16, 0), time(21, 0)]

async def init_db() -> asyncpg.pool.Pool:
    """Ensure the PostgreSQL pool and schema are ready."""
    if asyncpg is None:
        raise RuntimeError("asyncpg must be installed to use the database features.")
    global DB_POOL, _DB_INITIALIZED
    if DB_POOL is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL (or MEMEBOT_DB) must point to a PostgreSQL database")
        DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    if not _DB_INITIALIZED:
        async with DB_POOL.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memes (
                    id SERIAL PRIMARY KEY,
                    owner_file_id TEXT NOT NULL,
                    mime_type TEXT,
                    scheduled_ts BIGINT NOT NULL,
                    posted INTEGER DEFAULT 0,
                    created_ts BIGINT NOT NULL,
                    preview_file_id TEXT,
                    caption TEXT
                )
                """
            )
        _DB_INITIALIZED = True
    return DB_POOL


def _verify_backup_password(args: Optional[list[str]]) -> bool:
    if not args:
        return False
    candidate = args[0]
    candidate_hash = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return hmac.compare_digest(candidate_hash, BACKUP_PASSWORD_HASH)

async def compute_next_slot(after_dt: Optional[datetime] = None) -> datetime:
    """Return the next slot datetime from after_dt (exclusive). If after_dt is None, use now() in IST.
    All calculations and returns are in IST timezone."""
    if after_dt is None:
        # Get current time in IST
        after_dt = datetime.now(IST)
    else:
        after_dt = _ensure_ist(after_dt)
    
    # check same-day slots in IST
    today = after_dt.date()
    for slot in SLOTS:
        candidate = _ist_localize(datetime.combine(today, slot))
        if candidate > after_dt:
            return candidate
    # otherwise next day's first slot
    next_day = today + timedelta(days=1)
    return _ist_localize(datetime.combine(next_day, SLOTS[0]))

async def get_last_scheduled_ts(conn: asyncpg.Connection) -> Optional[int]:
    row = await conn.fetchrow(
        "SELECT scheduled_ts FROM memes WHERE posted=0 ORDER BY scheduled_ts DESC LIMIT 1"
    )
    return row["scheduled_ts"] if row else None

async def schedule_meme(owner_file_id: str, mime_type: str, caption: Optional[str] = None) -> datetime:
    pool = await init_db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Always schedule after the latest scheduled meme, even if it's far in the future
            last_ts = await get_last_scheduled_ts(conn)
            if last_ts is None:
                # no pending memes, schedule relative to now in IST
                ref_dt = datetime.now(IST)
            else:
                # Convert timestamp to IST-aware datetime
                ref_dt = datetime.fromtimestamp(last_ts, tz=IST)
            next_dt = await compute_next_slot(ref_dt)

            # context is not available here, so preview is best-effort: use owner_file_id for now
            preview_file_id = owner_file_id
            created_ts = int(datetime.now(IST).timestamp())

            await conn.execute(
                """
                INSERT INTO memes (owner_file_id, mime_type, scheduled_ts, created_ts, preview_file_id, caption)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                owner_file_id,
                mime_type,
                int(next_dt.timestamp()),
                created_ts,
                preview_file_id,
                caption,
            )
    return next_dt

async def pop_due_memes_and_post(context: ContextTypes.DEFAULT_TYPE):
    now_ts = int(datetime.now(IST).timestamp())
    pool = await init_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, owner_file_id, mime_type, caption
            FROM memes
            WHERE posted=0 AND scheduled_ts<= $1
            ORDER BY scheduled_ts ASC
            """,
            now_ts,
        )

    for row in rows:
        mid = row["id"]
        file_id = row["owner_file_id"]
        mime = row["mime_type"]
        caption = row["caption"]
        try:
            sent = False
            # Try video first when appropriate
            if mime and mime.startswith("video"):
                try:
                    await context.bot.send_video(CHANNEL_ID, file_id, caption=caption)
                    sent = True
                except Exception as e_video:
                    logger.warning("send_video failed for id=%s: %s", mid, e_video)
            if not sent:
                # try as photo/animation
                try:
                    await context.bot.send_photo(CHANNEL_ID, file_id, caption=caption)
                    sent = True
                except Exception as e_photo:
                    logger.warning("send_photo failed for id=%s: %s", mid, e_photo)
                    # fallback to sending as document
                    try:
                        await context.bot.send_document(CHANNEL_ID, file_id, caption=caption)
                        sent = True
                    except Exception as e_doc:
                        logger.warning("send_document failed for id=%s: %s", mid, e_doc)
                        # raise the last exception to be caught below
                        raise e_doc

            if sent:
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE memes SET posted=1 WHERE id=$1", mid)
                logger.info("Posted meme id=%s", mid)
                posting_log.append(f"[SUCCESS] Posted meme id={mid} at {datetime.now(IST).isoformat(sep=' ')}")
                if len(posting_log) > 100:
                    posting_log.pop(0)
        except Exception as e:
            logger.exception("Failed to post meme id=%s: %s", mid, e)
            posting_log.append(f"[FAIL] Meme id={mid} at {datetime.now(IST).isoformat(sep=' ')}: {type(e).__name__}: {e}")
            if len(posting_log) > 100:
                posting_log.pop(0)
async def scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return

    pool = await init_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, scheduled_ts, mime_type, preview_file_id, owner_file_id, caption
            FROM memes
            WHERE posted=0
            ORDER BY scheduled_ts ASC
            """
        )

    if not rows:
        await update.message.reply_text("No scheduled memes.")
        return

    # For each scheduled item, try to send a preview robustly (direct send, then download+reupload)
    for row in rows:
        mid = row["id"]
        ts = row["scheduled_ts"]
        mtype = row["mime_type"]
        preview_id = row["preview_file_id"]
        owner_file_id = row["owner_file_id"]
        user_caption = row["caption"]

        file_id = preview_id if preview_id else owner_file_id

        # Build caption with ID, time, type and user's caption if present
        caption_parts = [f"ID: {mid}", f"Time: {datetime.fromtimestamp(ts, tz=IST).strftime('%Y-%m-%d %H:%M:%S IST')}", f"Type: {mtype}"]
        if user_caption:
            caption_parts.append(f"Caption: {user_caption}")
        caption = ", ".join(caption_parts)

        sent = False
        # Try direct sends with fallbacks
        try:
            if mtype and mtype.startswith('video'):
                try:
                    if file_id:
                        await context.bot.send_video(update.effective_chat.id, file_id, caption=caption)
                        sent = True
                except Exception as e:  # direct video failed
                    logger.debug("scheduled: direct send_video failed for id=%s: %s", mid, e)

            if not sent and file_id:
                try:
                    await context.bot.send_photo(update.effective_chat.id, file_id, caption=caption)
                    sent = True
                except Exception as e:
                    logger.debug("scheduled: direct send_photo failed for id=%s: %s", mid, e)
                    try:
                        await context.bot.send_document(update.effective_chat.id, file_id, caption=caption)
                        sent = True
                    except Exception as e2:
                        logger.debug("scheduled: direct send_document failed for id=%s: %s", mid, e2)

            if not sent and file_id:
                # Attempt download + reupload
                try:
                    file = await context.bot.get_file(file_id)
                    bio = io.BytesIO()
                    await file.download(out=bio)
                    bio.seek(0)
                    if mtype and mtype.startswith('video'):
                        await context.bot.send_video(update.effective_chat.id, InputFile(bio, filename=f"meme_{mid}.mp4"), caption=caption)
                    else:
                        try:
                            await context.bot.send_photo(update.effective_chat.id, InputFile(bio, filename=f"meme_{mid}.jpg"), caption=caption)
                        except Exception:
                            bio.seek(0)
                            await context.bot.send_document(update.effective_chat.id, InputFile(bio, filename=f"meme_{mid}"), caption=caption)
                    sent = True
                except Exception as e:
                    logger.debug("scheduled: download+reupload failed for id=%s: %s", mid, e)

        except Exception as e:
            logger.exception("Unexpected error while previewing scheduled id=%s: %s", mid, e)

        if not sent:
            # If all attempts fail, send a text placeholder
            await update.message.reply_text(caption)
async def unschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not context.args or not all(arg.isdigit() for arg in context.args):
        await update.message.reply_text("Usage: /unschedule <id1> <id2> ...")
        return
    meme_ids = [int(arg) for arg in context.args]
    pool = await init_db()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM memes WHERE posted=0 AND id = ANY($1::int[])",
            meme_ids,
        )
    await update.message.reply_text(f"Unscheduled memes with IDs: {', '.join(str(mid) for mid in meme_ids)} (if they existed and were not posted yet).")


async def preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Preview a scheduled meme by id. Tries direct send, then downloads and reuploads as a document if needed."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /preview <id>")
        return
    meme_id = int(context.args[0])
    # immediate ack so owner knows the command was received
    try:
        await update.message.reply_text(f"Previewing meme {meme_id}...")
    except Exception:
        logger.debug("Could not send ack reply for preview %s", meme_id)
    pool = await init_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT owner_file_id, mime_type FROM memes WHERE id=$1",
            meme_id,
        )
    if not row:
        await update.message.reply_text(f"No meme found with ID {meme_id}.")
        return
    file_id = row["owner_file_id"]
    mime = row["mime_type"]
    chat_id = update.effective_chat.id
    # Try direct sends with fallbacks
    try:
        if mime and mime.startswith("video"):
            await context.bot.send_video(chat_id, file_id, caption=f"Preview ID {meme_id}")
            return
        try:
            await context.bot.send_photo(chat_id, file_id, caption=f"Preview ID {meme_id}")
            return
        except Exception as e_photo:
            logger.debug("Direct send_photo failed for preview id=%s: %s", meme_id, e_photo)
            # try send_document quick fallback
            try:
                await context.bot.send_document(chat_id, file_id, caption=f"Preview ID {meme_id}")
                return
            except Exception as e_doc:
                logger.debug("Direct send_document failed for preview id=%s: %s", meme_id, e_doc)
        # If direct fails, download and reupload
        file = await context.bot.get_file(file_id)
        bio = io.BytesIO()
        await file.download(out=bio)
        bio.seek(0)
        # pick send method based on mime
        if mime and mime.startswith("video"):
            await context.bot.send_video(chat_id, InputFile(bio, filename=f"meme_{meme_id}.mp4"), caption=f"Preview ID {meme_id}")
        else:
            # try as photo first, then document
            try:
                await context.bot.send_photo(chat_id, InputFile(bio, filename=f"meme_{meme_id}.jpg"), caption=f"Preview ID {meme_id}")
            except Exception:
                bio.seek(0)
                await context.bot.send_document(chat_id, InputFile(bio, filename=f"meme_{meme_id}"), caption=f"Preview ID {meme_id}")
    except Exception as e:
        logger.exception("Preview failed for id=%s: %s", meme_id, e)
        await update.message.reply_text(f"Failed to preview meme {meme_id}: {type(e).__name__}: {e}")

async def logcmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not posting_log:
        await update.message.reply_text("No posting events yet.")
        return
    await update.message.reply_text("Last posting events:\n" + "\n".join(posting_log[-10:]))


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not _verify_backup_password(context.args):
        await update.message.reply_text("Backup password missing or incorrect. Usage: /backup <password>")
        return

    try:
        backup_path, total_memes, scheduled_memes = await create_backup(
            send_document_to=update.effective_chat.id,
            bot=context.bot,
        )
        logger.info("Backup exported to %s", backup_path)
    except Exception as exc:
        logger.exception("Backup failed: %s", exc)
        await update.message.reply_text(f"Backup failed: {type(exc).__name__}: {exc}")

async def create_backup(send_document_to: Optional[int] = None, bot: Optional[Any] = None):
    pool = await init_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, owner_file_id, mime_type, scheduled_ts, posted, created_ts, preview_file_id, caption
            FROM memes
            ORDER BY id
            """
        )

    memes = []
    for row in rows:
        meme = {
            "id": int(row["id"]),
            "owner_file_id": row["owner_file_id"],
            "mime_type": row["mime_type"],
            "scheduled_ts": int(row["scheduled_ts"]),
            "posted": int(row["posted"]),
            "created_ts": int(row["created_ts"]),
            "preview_file_id": row["preview_file_id"],
            "caption": row["caption"],
        }
        memes.append(meme)

    scheduled_memes = [m for m in memes if m["posted"] == 0]
    payload = {
        "version": 1,
        "generated_at": datetime.now(IST).isoformat(),
        "memes": memes,
        "scheduled_memes": scheduled_memes,
    }

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    filename = f"memes-backup-{timestamp}.json"
    backup_path = BACKUP_DIR / filename
    with backup_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)

    if send_document_to and bot:
        with backup_path.open("rb") as fh:
            await bot.send_document(
                send_document_to,
                InputFile(fh, filename=filename),
                caption=f"Backup created: {len(memes)} total memes ({len(scheduled_memes)} scheduled).",
            )
    return backup_path, len(memes), len(scheduled_memes)


async def restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not _verify_backup_password(context.args):
        await update.message.reply_text("Backup password missing or incorrect. Usage: /restore <password> (reply to backup file)")
        return

    replied = update.message.reply_to_message if update.message else None
    if not replied or not replied.document:
        await update.message.reply_text("Reply to a backup JSON document with /restore.")
        return

    file = await context.bot.get_file(replied.document.file_id)
    buffer = io.BytesIO()
    await file.download(out=buffer)
    buffer.seek(0)
    try:
        data = json.loads(buffer.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        await update.message.reply_text(f"Could not parse backup: {exc}")
        return

    memes = data.get("memes")
    if not isinstance(memes, list):
        await update.message.reply_text("Backup file missing 'memes' list.")
        return

    records = []
    try:
        for item in memes:
            records.append(
                (
                    int(item["id"]),
                    item["owner_file_id"],
                    item.get("mime_type"),
                    int(item["scheduled_ts"]),
                    int(item.get("posted", 0)),
                    int(item["created_ts"]),
                    item.get("preview_file_id"),
                    item.get("caption"),
                )
            )
    except (KeyError, TypeError, ValueError) as exc:
        await update.message.reply_text(f"Backup format error: {exc}")
        return

    pool = await init_db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE TABLE memes RESTART IDENTITY")
            if records:
                await conn.executemany(
                    """
                    INSERT INTO memes (id, owner_file_id, mime_type, scheduled_ts, posted, created_ts, preview_file_id, caption)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    """,
                    records,
                )
                max_id = max(record[0] for record in records)
            else:
                max_id = 0

            seq_name = await conn.fetchval("SELECT pg_get_serial_sequence('memes', 'id')")
            if seq_name:
                if max_id > 0:
                    await conn.execute("SELECT setval($1, $2, true)", seq_name, max_id)
                else:
                    await conn.execute("SELECT setval($1, 1, false)", seq_name)

    scheduled_count = sum(1 for record in records if record[4] == 0)
    await update.message.reply_text(
        f"Restore complete: {len(records)} memes imported ({scheduled_count} scheduled)."
    )
    logger.info("Restored %s memes from backup", len(records))

async def periodic_poster(application):
    while True:
        try:
            await pop_due_memes_and_post(application)
        except Exception:
            logger.exception("Error in poster loop")
        await asyncio.sleep(30)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hi! I schedule memes to the configured channel.")

async def helpcmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a detailed help message with all commands."""
    help_text = (
        """
<b>🤖 <u>Meme Wrangler Bot Command Reference</u> 🤖</b>

<b>General:</b>
  <b>/start</b> — Show a welcome message.
  <b>/help</b> — Show this help message.

<b>Scheduling Memes:</b>
  <b>Send a photo/video/animation</b> (as a DM to the bot):
    Schedules it for the next available slot (11:00, 16:00, 21:00 IST).
    Add a caption to include it with the post.
    <i>Example:</i> Send a meme to the bot in DM with or without caption.

  <b>/scheduled</b> — List all scheduled memes with previews and their IDs, times, and types.

  <b>/unschedule &lt;id1&gt; [&lt;id2&gt; ...]</b> — Remove one or more memes from the schedule (by ID).
    <i>Example:</i> <code>/unschedule 3 5 7</code>

  <b>/postnow [id]</b> — Immediately post the next scheduled meme, or a specific meme by ID.
    <i>Example:</i> <code>/postnow</code> or <code>/postnow 6</code>

  <b>/preview &lt;id&gt;</b> — Preview a scheduled meme by its ID.
    <i>Example:</i> <code>/preview 4</code>

  <b>/log</b> — Show the last 10 posting events (success/failure log).

<b>Maintenance:</b>
  <b>/backup &lt;password&gt;</b> — Export all memes (including scheduled ones) as a JSON backup.
  <b>/restore &lt;password&gt;</b> — Reply to a backup JSON with this command to restore memes.

<b>Advanced Scheduling:</b>
  <b>/scheduleat id: &lt;id&gt; &lt;HH:MM&gt;</b> — Reschedule a single meme to a specific time (24h, IST).
    <i>Example:</i> <code>/scheduleat id: 6 16:20</code>

  <b>/scheduleat ids: &lt;start&gt;-&lt;end&gt; &lt;YYYY-MM-DD&gt;</b> — Reschedule a range of memes to a date, assigning slots (11:00, 16:00, 21:00 IST) in order.
    <i>Example:</i> <code>/scheduleat ids: 5-10 2025-10-19</code>

<b>Notes:</b>
• <b>Only the owner</b> (set by OWNER_ID) can use admin commands.
• All times are in <b>IST (Asia/Kolkata)</b>.
• Meme IDs are shown in <b>/scheduled</b> previews.
• Use <b>/preview</b> to check a meme before posting.

<b>✨ Enjoy effortless meme scheduling! ✨</b>
        """
    )
    await update.message.reply_text(help_text, parse_mode="HTML", disable_web_page_preview=True)

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.message
    user_id = msg.from_user.id
    if user_id != OWNER_ID:
        await msg.reply_text("Sorry, only the owner can send memes to schedule.")
        return

    # Determine the best file id and mime
    file_id = None
    mime = None
    caption = msg.caption  # Get caption if present
    
    if msg.photo:
        # highest resolution
        file = msg.photo[-1]
        file_id = file.file_id
        mime = 'image'
    elif msg.video:
        file_id = msg.video.file_id
        mime = 'video'
    elif msg.animation:
        file_id = msg.animation.file_id
        mime = 'image'  # gifs treated as image
    else:
        await msg.reply_text("Please send a photo, animation (GIF) or video.")
        return

    scheduled_dt = await schedule_meme(file_id, mime, caption)
    # scheduled_dt is already in IST timezone
    await msg.reply_text(f"Scheduled for: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
    # Automatic backup after each meme intake
    try:
        backup_path, total, scheduled_count = await create_backup()
        logger.info(
            "Automatic backup created at %s after scheduling meme. totals: %s (scheduled %s)",
            backup_path,
            total,
            scheduled_count,
        )
    except Exception as exc:
        logger.exception("Automatic backup failed after scheduling meme: %s", exc)

async def postnow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return

    # If an ID is provided, post that meme; else, post the next scheduled meme
    meme_id = None
    if context.args and context.args[0].isdigit():
        meme_id = int(context.args[0])

    pool = await init_db()
    async with pool.acquire() as conn:
        if meme_id is not None:
            row = await conn.fetchrow(
                "SELECT id, owner_file_id, mime_type FROM memes WHERE posted=0 AND id=$1",
                meme_id,
            )
            if not row:
                await update.message.reply_text(f"No scheduled meme with ID {meme_id} to post.")
                return
        else:
            row = await conn.fetchrow(
                "SELECT id, owner_file_id, mime_type FROM memes WHERE posted=0 ORDER BY scheduled_ts ASC LIMIT 1"
            )
            if not row:
                await update.message.reply_text("No scheduled memes to post.")
                return
    mid = row["id"]
    file_id = row["owner_file_id"]
    mime = row["mime_type"]
    try:
        if mime and mime.startswith("video"):
            await context.bot.send_video(CHANNEL_ID, file_id)
        else:
            await context.bot.send_photo(CHANNEL_ID, file_id)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE memes SET posted=1 WHERE id=$1", mid)
        await update.message.reply_text(f"Posted meme with ID {mid} to channel.")
    except Exception as e:
        await update.message.reply_text(f"Failed to post meme: {e}")

import re

async def scheduleat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("Only the owner can use this command.")
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /scheduleat id: <id> <HH:MM> or /scheduleat ids: <start>-<end> <YYYY-MM-DD>")
        return

    argstr = ' '.join(context.args)
    # Single ID mode: /scheduleat id: 6 16:20
    m_single = re.match(r'id:\s*(\d+)\s+(\d{2}):(\d{2})$', argstr)
    # Range mode: /scheduleat ids: 5-10 2025-10-19
    m_range = re.match(r'ids:\s*(\d+)-(\d+)\s+(\d{4}-\d{2}-\d{2})$', argstr)

    if m_single:
        meme_id = int(m_single.group(1))
        hour = int(m_single.group(2))
        minute = int(m_single.group(3))
        # Validate time
        if not (0 <= hour < 24 and 0 <= minute < 60):
            await update.message.reply_text("Invalid time format. Use 24h HH:MM.")
            return
        # Schedule meme at specified time today (IST)
        now_ist = datetime.now(IST)
        sched_dt = _ist_localize(datetime(now_ist.year, now_ist.month, now_ist.day, hour, minute))
        sched_ts = int(sched_dt.timestamp())
        pool = await init_db()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE memes SET scheduled_ts=$1 WHERE id=$2 AND posted=0",
                sched_ts,
                meme_id,
            )
        await update.message.reply_text(f"Rescheduled meme ID {meme_id} for {sched_dt.strftime('%Y-%m-%d %H:%M')} IST.")
        return

    elif m_range:
        start_id = int(m_range.group(1))
        end_id = int(m_range.group(2))
        date_str = m_range.group(3)
        from datetime import time as dtime
        base_date = _ist_localize(datetime.strptime(date_str, '%Y-%m-%d'))
        # Assign slots in order: 11:00, 16:00, 21:00, repeat
        slot_times = [dtime(11,0), dtime(16,0), dtime(21,0)]
        ids = list(range(start_id, end_id+1))
        if not ids:
            await update.message.reply_text("Invalid ID range.")
            return
        updates = []
        for idx, meme_id in enumerate(ids):
            slot = slot_times[idx % len(slot_times)]
            sched_dt = base_date.replace(hour=slot.hour, minute=slot.minute, second=0, microsecond=0)
            sched_ts = int(sched_dt.timestamp())
            updates.append((sched_ts, meme_id))
        pool = await init_db()
        async with pool.acquire() as conn:
            await conn.executemany(
                "UPDATE memes SET scheduled_ts=$1 WHERE id=$2 AND posted=0",
                updates,
            )
        await update.message.reply_text(f"Rescheduled memes IDs {start_id}-{end_id} for {date_str} in slots 11:00, 16:00, 21:00 IST (cycled).")
        return

    else:
        await update.message.reply_text("Invalid format. Use /scheduleat id: <id> <HH:MM> or /scheduleat ids: <start>-<end> <YYYY-MM-DD>")

def main():
    if not BOT_TOKEN:
        raise SystemExit("Please set TELEGRAM_BOT_TOKEN environment variable")
    if not OWNER_ID or OWNER_ID == 0:
        raise SystemExit("Please set OWNER_ID environment variable to your Telegram user id")
    if not CHANNEL_ID:
        raise SystemExit("Please set CHANNEL_ID to target channel (username or id)")
    if not DATABASE_URL:
        raise SystemExit("Please set DATABASE_URL (or MEMEBOT_DB) to a PostgreSQL connection string")

    # Initialize DB first
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', helpcmd))
    app.add_handler(CommandHandler('postnow', postnow))
    app.add_handler(CommandHandler('scheduled', scheduled))
    app.add_handler(CommandHandler('unschedule', unschedule))
    app.add_handler(CommandHandler('preview', preview))
    app.add_handler(CommandHandler('log', logcmd))
    app.add_handler(CommandHandler('backup', backup))
    app.add_handler(CommandHandler('restore', restore))
    app.add_handler(CommandHandler('scheduleat', scheduleat))
    media_filter = filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.ANIMATION)
    app.add_handler(MessageHandler(media_filter, handle_media))

    # run background poster using post_init hook
    async def post_init(application):
        me = await application.bot.get_me()
        logger.info("Bot connected as @%s (id=%s)", me.username, me.id)
        asyncio.create_task(periodic_poster(application))
    
    app.post_init = post_init

    logger.info("Starting bot...")
    app.run_polling()

if __name__ == '__main__':
    main()
