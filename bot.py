from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
from datetime import datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Optional
from zoneinfo import ZoneInfo

try:
    import pytz
except ModuleNotFoundError:
    pytz = None  # type: ignore

from meme_wrangler.runtime import (
    DatabaseRuntime,
    format_bytes,
    format_public_meme_id,
    load_db_profiles,
    parse_public_meme_id,
    predict_fill_date,
)

posting_log: list[str] = []

if TYPE_CHECKING:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message, Update
    from telegram.ext import (
        ApplicationBuilder,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
else:
    try:
        from telegram import (  # type: ignore
            InlineKeyboardButton,
            InlineKeyboardMarkup,
            InputFile,
            Message,
            Update,
        )
        from telegram.ext import (  # type: ignore
            ApplicationBuilder,
            CallbackQueryHandler,
            CommandHandler,
            ContextTypes,
            MessageHandler,
            filters,
        )
    except ModuleNotFoundError:
        Update = Message = InputFile = InlineKeyboardButton = InlineKeyboardMarkup = Any  # type: ignore
        ContextTypes = SimpleNamespace(DEFAULT_TYPE=Any)  # type: ignore

        class _MissingTelegramModule:
            def __getattr__(self, item):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot."
                )

            def __call__(self, *args, **kwargs):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot."
                )

        ApplicationBuilder = CallbackQueryHandler = CommandHandler = MessageHandler = (  # type: ignore
            _MissingTelegramModule()
        )

        class _MissingFilters(SimpleNamespace):
            def __getattr__(self, item):
                raise RuntimeError(
                    "python-telegram-bot must be installed to use the Meme Wrangler bot."
                )

        filters = _MissingFilters()  # type: ignore


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if pytz is not None:
    IST = pytz.timezone("Asia/Kolkata")

    def _ist_localize(dt: datetime) -> datetime:
        return IST.localize(dt)

else:
    IST = ZoneInfo("Asia/Kolkata")

    def _ist_localize(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST)


def _ensure_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return _ist_localize(dt)
    return dt.astimezone(IST)


BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
ADMIN_CHANNEL_ID = os.environ.get("MEMEBOT_ADMIN_CHANNEL_ID")
BACKUP_DIR = Path(os.environ.get("MEMEBOT_BACKUP_DIR", "backups"))
RUNTIME_DIR = Path(os.environ.get("MEMEBOT_RUNTIME_DIR", "runtime"))
CACHE_DIR = Path(os.environ.get("MEMEBOT_CACHE_DIR", "cache"))
LOG_DIR = Path(os.environ.get("MEMEBOT_LOG_DIR", "logs"))
ADMIN_EVENT_LOG = RUNTIME_DIR / "admin-events.jsonl"
ALERT_STATE_PATH = RUNTIME_DIR / "storage-alert-state.json"
PAGE_SIZE = 5
PENDING_ACTION_TTL = timedelta(minutes=10)
STORAGE_ALERT_CHECK_INTERVAL = timedelta(hours=6)

_HARDCODED_BACKUP_PASSWORD_HASH = (
    "16c5b5ddf1b27f16ad5f801bb83595d00e666cc53085e53a4b1e67b715016251"
)
_HASH_OVERRIDE = os.environ.get("MEMEBOT_BACKUP_PASSWORD_HASH")
BACKUP_PASSWORD_HASH = _HASH_OVERRIDE if _HASH_OVERRIDE else _HARDCODED_BACKUP_PASSWORD_HASH

_owner_ids_raw = os.environ.get("OWNER_ID", "0")
try:
    OWNER_IDS = {int(oid.strip()) for oid in _owner_ids_raw.split(",") if oid.strip()}
except ValueError as exc:
    logger.error("Failed to parse OWNER_ID '%s': %s", _owner_ids_raw, exc)
    OWNER_IDS = {0}

SLOTS = [time(11, 0), time(16, 0), time(21, 0)]
PENDING_ACTIONS: dict[str, dict[str, Any]] = {}
LAST_STORAGE_ALERT_CHECK: Optional[datetime] = None
_DB_RUNTIME: Optional[DatabaseRuntime] = None


def get_db_runtime() -> DatabaseRuntime:
    global _DB_RUNTIME
    if _DB_RUNTIME is None:
        profiles, active_key = load_db_profiles()
        _DB_RUNTIME = DatabaseRuntime(
            profiles=profiles,
            active_key=active_key,
            backup_dir=BACKUP_DIR,
            runtime_dir=RUNTIME_DIR,
            cache_dir=CACHE_DIR,
            log_dir=LOG_DIR,
        )
    return _DB_RUNTIME


async def init_db() -> DatabaseRuntime:
    runtime = get_db_runtime()
    await runtime.init_active_backend()
    return runtime


def _verify_backup_password(args: Optional[list[str]]) -> bool:
    if not args:
        return False
    candidate_hash = hashlib.sha256(args[0].encode("utf-8")).hexdigest()
    return hmac.compare_digest(candidate_hash, BACKUP_PASSWORD_HASH)


def is_owner(user_id: Optional[int]) -> bool:
    return user_id is not None and user_id in OWNER_IDS


def now_ist() -> datetime:
    return datetime.now(IST)


def public_meme_id(value: int) -> str:
    return format_public_meme_id(value)


def parse_meme_ref(raw_value: str) -> Optional[int]:
    return parse_public_meme_id(raw_value)


def channel_label() -> str:
    return CHANNEL_ID or "configured-channel"


def status_badge(posted: int) -> str:
    return "Posted" if posted else "Scheduled"


def format_ts_ist(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")


def cleanup_expired_actions() -> None:
    now = now_ist()
    expired = [
        token
        for token, payload in PENDING_ACTIONS.items()
        if datetime.fromisoformat(payload["expires_at"]) <= now
    ]
    for token in expired:
        payload = PENDING_ACTIONS.pop(token)
        log_admin_event(payload["kind"], payload["user_id"], "cancelled", payload["summary"])


def log_admin_event(action: str, user_id: int, outcome: str, detail: str) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    with ADMIN_EVENT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "timestamp": now_ist().isoformat(),
                    "action": action,
                    "user_id": user_id,
                    "outcome": outcome,
                    "detail": detail,
                }
            )
            + "\n"
        )


def load_admin_events() -> list[dict[str, Any]]:
    if not ADMIN_EVENT_LOG.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in ADMIN_EVENT_LOG.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def require_owner(update: Update) -> bool:
    return is_owner(getattr(update.effective_user, "id", None))


async def reply_admin_only(update: Update) -> None:
    if update.message:
        await update.message.reply_text("❌ Admin only · Permission denied")


def make_action_token() -> str:
    return secrets.token_hex(8)


def build_confirmation_markup(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Confirm", callback_data=f"action:confirm:{token}"),
                InlineKeyboardButton("Cancel", callback_data=f"action:cancel:{token}"),
            ]
        ]
    )


def register_action(kind: str, user_id: int, summary: str, payload: dict[str, Any]) -> str:
    cleanup_expired_actions()
    token = make_action_token()
    PENDING_ACTIONS[token] = {
        "kind": kind,
        "user_id": user_id,
        "summary": summary,
        "payload": payload,
        "expires_at": (now_ist() + PENDING_ACTION_TTL).isoformat(),
    }
    return token


def parse_id_arguments(args: list[str]) -> Optional[list[int]]:
    resolved: list[int] = []
    for raw_value in args:
        meme_id = parse_meme_ref(raw_value)
        if meme_id is None:
            return None
        resolved.append(meme_id)
    return resolved


async def notify_admin_channel(bot: Any, text: str) -> None:
    if not ADMIN_CHANNEL_ID:
        return
    try:
        await bot.send_message(ADMIN_CHANNEL_ID, text)
    except Exception as exc:
        logger.warning("Failed to notify admin channel: %s", exc)


async def compute_next_slot(after_dt: Optional[datetime] = None) -> datetime:
    if after_dt is None:
        after_dt = now_ist()
    else:
        after_dt = _ensure_ist(after_dt)
    today = after_dt.date()
    for slot in SLOTS:
        candidate = _ist_localize(datetime.combine(today, slot))
        if candidate > after_dt:
            return candidate
    next_day = today + timedelta(days=1)
    return _ist_localize(datetime.combine(next_day, SLOTS[0]))


async def create_backup(
    send_document_to: Optional[int] = None,
    bot: Optional[Any] = None,
) -> tuple[Path, int, int]:
    runtime = await init_db()
    path, payload = await runtime.write_backup_file()
    counts = payload["counts"]
    if send_document_to and bot:
        with path.open("rb") as fh:
            await bot.send_document(
                send_document_to,
                InputFile(fh, filename=path.name),
                caption=(
                    "✅ Backup completed\n"
                    f"Source: {runtime.active_key}\n"
                    f"Records: {counts['total']} total · {counts['scheduled']} scheduled"
                ),
            )
    return path, counts["total"], counts["scheduled"]


async def schedule_meme(
    owner_file_id: str,
    mime_type: str,
    caption: Optional[str] = None,
) -> tuple[int, datetime]:
    runtime = await init_db()
    async with runtime.operation_lock:
        backend = await runtime.get_backend()
        last_ts = await backend.get_last_scheduled_ts()
        reference_dt = now_ist() if last_ts is None else datetime.fromtimestamp(last_ts, tz=IST)
        next_dt = await compute_next_slot(reference_dt)
        meme_id = await backend.insert_meme(
            owner_file_id=owner_file_id,
            mime_type=mime_type,
            scheduled_ts=int(next_dt.timestamp()),
            created_ts=int(now_ist().timestamp()),
            preview_file_id=owner_file_id,
            caption=caption,
        )
        await runtime.write_backup_file()
    return meme_id, next_dt


def format_scheduled_page(records: list[Any], page: int) -> tuple[str, InlineKeyboardMarkup]:
    page_count = max(1, (len(records) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, page_count - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    lines = ["Scheduled Memes", ""]
    if not records:
        lines.append("No scheduled memes.")
    else:
        for record in records[start:end]:
            lines.append(
                f"{public_meme_id(record.id)} · {channel_label()} · "
                f"{format_ts_ist(record.scheduled_ts)} · {status_badge(record.posted)}"
            )
        lines.extend(["", f"Page {page + 1}/{page_count} · Total {len(records)}"])
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Prev", callback_data=f"scheduled:page:{max(page - 1, 0)}"),
                InlineKeyboardButton("Refresh", callback_data=f"scheduled:refresh:{page}"),
                InlineKeyboardButton(
                    "Next", callback_data=f"scheduled:page:{min(page + 1, page_count - 1)}"
                ),
            ]
        ]
    )
    return "\n".join(lines), markup


def load_alert_state() -> dict[str, Any]:
    if not ALERT_STATE_PATH.exists():
        return {}
    try:
        return json.loads(ALERT_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_alert_state(payload: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    ALERT_STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def render_storage_report() -> tuple[str, Optional[str]]:
    runtime = await init_db()
    snapshot = await runtime.capture_storage_snapshot()
    history = runtime.storage_history(days=30)

    recent = history[-2:] if len(history) >= 2 else history
    weekly = history[-7:] if len(history) >= 7 else history

    daily_fill = predict_fill_date(
        recent,
        snapshot.total_bytes,
        capacity_bytes=None,
        free_bytes=snapshot.local_free_bytes,
    )

    daily_growth = 0
    if len(recent) >= 2:
        older, newer = recent[0], recent[-1]
        seconds = (
            datetime.fromisoformat(newer.timestamp) - datetime.fromisoformat(older.timestamp)
        ).total_seconds()
        if seconds > 0:
            daily_growth = int((newer.total_bytes - older.total_bytes) * 86400 / seconds)

    weekly_growth = 0
    if len(weekly) >= 2:
        older, newer = weekly[0], weekly[-1]
        seconds = (
            datetime.fromisoformat(newer.timestamp) - datetime.fromisoformat(older.timestamp)
        ).total_seconds()
        if seconds > 0:
            weekly_growth = int((newer.total_bytes - older.total_bytes) * 604800 / seconds)

    lines = [
        "Storage Status",
        f"Active DB: {runtime.active_key}",
        f"Database: {format_bytes(snapshot.db_bytes)}",
        f"Backups: {format_bytes(snapshot.backup_bytes)}",
        f"Cache: {format_bytes(snapshot.cache_bytes)}",
        f"Logs: {format_bytes(snapshot.log_bytes)}",
        f"Runtime: {format_bytes(snapshot.runtime_bytes)}",
        f"Total tracked: {format_bytes(snapshot.total_bytes)}",
        f"Local free space: {format_bytes(snapshot.local_free_bytes)}",
        f"Daily growth: {format_bytes(daily_growth)}",
        f"Weekly growth: {format_bytes(weekly_growth)}",
        f"Predicted local fill date: {daily_fill or 'insufficient history'}",
        "Recommended action: prune old backups or migrate before runway tightens.",
    ]
    return "\n".join(lines), daily_fill


def storage_alert_level(fill_date: Optional[str]) -> Optional[str]:
    if not fill_date:
        return None
    days_left = (datetime.fromisoformat(fill_date).date() - now_ist().date()).days
    if days_left <= 3:
        return "critical"
    if days_left <= 14:
        return "warning"
    return None


async def maybe_send_storage_alert(bot: Any) -> None:
    global LAST_STORAGE_ALERT_CHECK
    now = now_ist()
    if LAST_STORAGE_ALERT_CHECK and now - LAST_STORAGE_ALERT_CHECK < STORAGE_ALERT_CHECK_INTERVAL:
        return
    LAST_STORAGE_ALERT_CHECK = now
    report, fill_date = await render_storage_report()
    level = storage_alert_level(fill_date)
    if not level:
        return
    state = load_alert_state()
    if state.get("level") == level and state.get("fill_date") == fill_date:
        return
    prefix = "⚠️" if level == "warning" else "❌"
    await notify_admin_channel(
        bot,
        f"{prefix} Storage alert · Predicted date: {fill_date}\n"
        "Recommended action: prune backups or switch databases.\n\n"
        f"{report}",
    )
    save_alert_state({"level": level, "fill_date": fill_date, "sent_at": now.isoformat()})


async def preview_record(record: Any, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    caption = f"{public_meme_id(record.id)} · {status_badge(record.posted)}"
    try:
        if record.mime_type and record.mime_type.startswith("video"):
            await context.bot.send_video(chat_id, record.owner_file_id, caption=caption)
            return
        try:
            await context.bot.send_photo(chat_id, record.owner_file_id, caption=caption)
            return
        except Exception:
            await context.bot.send_document(chat_id, record.owner_file_id, caption=caption)
            return
    except Exception:
        file = await context.bot.get_file(record.owner_file_id)
        buffer = io.BytesIO()
        await file.download(out=buffer)
        buffer.seek(0)
        if record.mime_type and record.mime_type.startswith("video"):
            await context.bot.send_video(
                chat_id,
                InputFile(buffer, filename=f"{public_meme_id(record.id)}.mp4"),
                caption=caption,
            )
        else:
            try:
                await context.bot.send_photo(
                    chat_id,
                    InputFile(buffer, filename=f"{public_meme_id(record.id)}.jpg"),
                    caption=caption,
                )
            except Exception:
                buffer.seek(0)
                await context.bot.send_document(
                    chat_id,
                    InputFile(buffer, filename=public_meme_id(record.id)),
                    caption=caption,
                )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "MemeBot is online.\n"
        "Send a photo, video, or GIF in private chat to queue it for the next slot."
    )


async def helpcmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "MemeBot Commands\n"
        "/scheduled · list queued memes\n"
        "/preview MEME-0001 · preview a queued meme\n"
        "/unschedule MEME-0001 · cancel queued memes with confirmation\n"
        "/postnow [MEME-0001] · publish next queued meme or a specific one\n"
        "/scheduleat id: MEME-0001 16:20 · move one meme\n"
        "/backup <password> · export a verified backup\n"
        "/restore <password> · reply to a backup file and confirm restore\n"
        "/database · open migration and switch controls\n"
        "/storage or /status · storage and runway report\n"
        "/logs [action] [YYYY-MM-DD] · admin event log"
    )


async def scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    runtime = await init_db()
    backend = await runtime.get_backend()
    records = await backend.list_pending_memes()
    text, markup = format_scheduled_page(records, 0)
    await update.message.reply_text(text, reply_markup=markup)


async def unschedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    if not context.args:
        await update.message.reply_text("❌ Usage · /unschedule MEME-0001 [MEME-0002 ...]")
        return
    meme_ids = parse_id_arguments(context.args)
    if meme_ids is None:
        await update.message.reply_text("❌ Invalid meme ID · Use MEME-XXXX or a numeric ID")
        return
    token = register_action(
        "unschedule",
        update.effective_user.id,
        ", ".join(public_meme_id(meme_id) for meme_id in meme_ids),
        {"meme_ids": meme_ids},
    )
    await update.message.reply_text(
        "Confirm unschedule\n"
        + "\n".join(f"• {public_meme_id(meme_id)}" for meme_id in meme_ids),
        reply_markup=build_confirmation_markup(token),
    )


async def preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    if not context.args:
        await update.message.reply_text("❌ Usage · /preview MEME-0001")
        return
    meme_id = parse_meme_ref(context.args[0])
    if meme_id is None:
        await update.message.reply_text("❌ Invalid meme ID · Use MEME-XXXX or a numeric ID")
        return
    runtime = await init_db()
    backend = await runtime.get_backend()
    record = await backend.get_meme(meme_id)
    if record is None:
        await update.message.reply_text(
            f"❌ Preview failed · {public_meme_id(meme_id)} not found · Suggested fix: /scheduled"
        )
        return
    await preview_record(record, update, context)


async def logcmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    action_filter = context.args[0] if context.args else None
    date_filter = context.args[1] if len(context.args) > 1 else None
    filtered = []
    for event in load_admin_events():
        if action_filter and event.get("action") != action_filter:
            continue
        if date_filter and not str(event.get("timestamp", "")).startswith(date_filter):
            continue
        filtered.append(event)
    if not filtered:
        await update.message.reply_text("No matching admin events.")
        return
    lines = ["Admin Events"]
    for event in filtered[-10:]:
        lines.append(
            f"{event['timestamp']} · {event['action']} · user={event['user_id']} · "
            f"{event['outcome']} · {event['detail']}"
        )
    await update.message.reply_text("\n".join(lines))


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    if not _verify_backup_password(context.args):
        await update.message.reply_text("❌ Backup denied · Invalid password")
        return
    try:
        path, total_memes, scheduled_memes = await create_backup(
            send_document_to=update.effective_chat.id,
            bot=context.bot,
        )
        log_admin_event(
            "backup",
            update.effective_user.id,
            "success",
            f"{path.name} · total={total_memes} · scheduled={scheduled_memes}",
        )
        await update.message.reply_text(
            "✅ Action completed\n"
            f"Source: {get_db_runtime().active_key}\n"
            f"Records: {total_memes} total · {scheduled_memes} scheduled\n"
            f"File: {path.name}"
        )
    except Exception as exc:
        log_admin_event("backup", update.effective_user.id, "failed", str(exc))
        await update.message.reply_text(
            f"❌ Backup failed · {type(exc).__name__}: {exc} · Suggested fix: verify DB access"
        )


async def restore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    if not _verify_backup_password(context.args):
        await update.message.reply_text("❌ Restore denied · Invalid password")
        return
    replied = update.message.reply_to_message if update.message else None
    if not replied or not replied.document:
        await update.message.reply_text("❌ Restore requires a replied backup JSON document")
        return
    file = await context.bot.get_file(replied.document.file_id)
    buffer = io.BytesIO()
    await file.download(out=buffer)
    buffer.seek(0)
    try:
        payload = json.loads(buffer.read().decode("utf-8"))
        get_db_runtime().verify_payload(payload)
    except Exception as exc:
        await update.message.reply_text(
            f"❌ Restore validation failed · {type(exc).__name__}: {exc}"
        )
        return
    token = register_action(
        "restore",
        update.effective_user.id,
        replied.document.file_name or "backup.json",
        {"payload": payload, "filename": replied.document.file_name or "backup.json"},
    )
    await update.message.reply_text(
        "Confirm restore\n"
        f"Backup: {replied.document.file_name or 'backup.json'}\n"
        f"Records: {payload['counts']['total']} total · {payload['counts']['scheduled']} scheduled",
        reply_markup=build_confirmation_markup(token),
    )


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg: Message = update.message
    if msg.from_user.id not in OWNER_IDS:
        await msg.reply_text("❌ Admin only · Permission denied")
        return
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
        mime = "image"
    else:
        await msg.reply_text("❌ Unsupported media · Send a photo, GIF, or video")
        return
    meme_id, scheduled_dt = await schedule_meme(file_id, mime, caption)
    await msg.reply_text(
        "✅ Action completed · "
        f"ID: {public_meme_id(meme_id)} · Scheduled: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S IST')}"
    )


async def postnow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    runtime = await init_db()
    backend = await runtime.get_backend()
    meme_id: Optional[int] = None
    if context.args:
        meme_id = parse_meme_ref(context.args[0])
        if meme_id is None:
            await update.message.reply_text("❌ Invalid meme ID · Use MEME-XXXX or a numeric ID")
            return
    record = await backend.get_pending_meme(meme_id) if meme_id else await backend.get_next_pending_meme()
    if record is None:
        await update.message.reply_text("❌ No scheduled meme found")
        return
    try:
        if record.mime_type and record.mime_type.startswith("video"):
            await context.bot.send_video(CHANNEL_ID, record.owner_file_id, caption=record.caption)
        else:
            await context.bot.send_photo(CHANNEL_ID, record.owner_file_id, caption=record.caption)
        async with runtime.operation_lock:
            await backend.mark_posted(record.id)
            await runtime.write_backup_file()
        await update.message.reply_text(
            f"✅ Action completed · ID: {public_meme_id(record.id)} · Posted to {channel_label()}"
        )
    except Exception as exc:
        await update.message.reply_text(
            f"❌ Post failed · {type(exc).__name__}: {exc} · Suggested fix: verify media access"
        )


async def scheduleat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "❌ Usage · /scheduleat id: MEME-0001 16:20 or /scheduleat ids: 5-10 2026-05-17"
        )
        return
    argstr = " ".join(context.args)
    m_single = re.match(r"id:\s*([A-Za-z0-9-]+)\s+(\d{2}):(\d{2})$", argstr)
    m_range = re.match(r"ids:\s*(\d+)-(\d+)\s+(\d{4}-\d{2}-\d{2})$", argstr)
    runtime = await init_db()
    backend = await runtime.get_backend()
    if m_single:
        meme_id = parse_meme_ref(m_single.group(1))
        if meme_id is None:
            await update.message.reply_text("❌ Invalid meme ID · Use MEME-XXXX")
            return
        hour = int(m_single.group(2))
        minute = int(m_single.group(3))
        if not (0 <= hour < 24 and 0 <= minute < 60):
            await update.message.reply_text("❌ Invalid time · Use HH:MM in 24-hour format")
            return
        now_local = now_ist()
        scheduled_dt = _ist_localize(
            datetime(now_local.year, now_local.month, now_local.day, hour, minute)
        )
        async with runtime.operation_lock:
            updated = await backend.update_schedule(meme_id, int(scheduled_dt.timestamp()))
            if updated:
                await runtime.write_backup_file()
        if not updated:
            await update.message.reply_text(
                f"❌ Reschedule failed · {public_meme_id(meme_id)} not pending"
            )
            return
        await update.message.reply_text(
            "✅ Action completed · "
            f"ID: {public_meme_id(meme_id)} · Scheduled: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S IST')}"
        )
        return
    if m_range:
        start_id = int(m_range.group(1))
        end_id = int(m_range.group(2))
        base_date = _ist_localize(datetime.strptime(m_range.group(3), "%Y-%m-%d"))
        updates = []
        for index, meme_id in enumerate(range(start_id, end_id + 1)):
            slot = SLOTS[index % len(SLOTS)]
            scheduled_dt = base_date.replace(
                hour=slot.hour,
                minute=slot.minute,
                second=0,
                microsecond=0,
            )
            updates.append((int(scheduled_dt.timestamp()), meme_id))
        async with runtime.operation_lock:
            updated_count = await backend.update_many_schedules(updates)
            if updated_count:
                await runtime.write_backup_file()
        await update.message.reply_text(
            "✅ Action completed · "
            f"IDs: MEME-{start_id:04d}..MEME-{end_id:04d} · Updated: {updated_count}"
        )
        return
    await update.message.reply_text("❌ Invalid format · Suggested fix: /help")


async def pop_due_memes_and_post(context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = await init_db()
    backend = await runtime.get_backend()
    records = await backend.list_due_memes(int(now_ist().timestamp()))
    for record in records:
        try:
            sent = False
            if record.mime_type and record.mime_type.startswith("video"):
                try:
                    await context.bot.send_video(CHANNEL_ID, record.owner_file_id, caption=record.caption)
                    sent = True
                except Exception as exc:
                    logger.warning("send_video failed for %s: %s", public_meme_id(record.id), exc)
            if not sent:
                try:
                    await context.bot.send_photo(CHANNEL_ID, record.owner_file_id, caption=record.caption)
                    sent = True
                except Exception:
                    await context.bot.send_document(CHANNEL_ID, record.owner_file_id, caption=record.caption)
                    sent = True
            if sent:
                async with runtime.operation_lock:
                    await backend.mark_posted(record.id)
                    await runtime.write_backup_file()
                posting_log.append(
                    f"[SUCCESS] {public_meme_id(record.id)} posted at {now_ist().isoformat(sep=' ')}"
                )
                posting_log[:] = posting_log[-100:]
        except Exception as exc:
            logger.exception("Failed to post %s: %s", public_meme_id(record.id), exc)
            posting_log.append(
                f"[FAIL] {public_meme_id(record.id)} at {now_ist().isoformat(sep=' ')}: "
                f"{type(exc).__name__}: {exc}"
            )
            posting_log[:] = posting_log[-100:]


async def database_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    runtime = await init_db()
    buttons = []
    for key, profile in runtime.profiles.items():
        if key == runtime.active_key:
            continue
        buttons.append(
            [
                InlineKeyboardButton(f"Migrate → {profile.label}", callback_data=f"db:migrate:{key}"),
                InlineKeyboardButton(f"Switch → {profile.label}", callback_data=f"db:switch:{key}"),
            ]
        )
    if not buttons:
        buttons = [[InlineKeyboardButton("Refresh", callback_data="db:refresh")]]
    await update.message.reply_text(
        "Database Control\n"
        f"Active: {runtime.active_key}\n"
        "Use the buttons below for verified migrations or emergency switches.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def storage_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await reply_admin_only(update)
        return
    report, _ = await render_storage_report()
    await update.message.reply_text(report)


async def execute_registered_action(
    token: str,
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    cleanup_expired_actions()
    payload = PENDING_ACTIONS.pop(token, None)
    if payload is None:
        await query.edit_message_text("❌ Action expired · Suggested fix: run the command again")
        return
    if payload["user_id"] != query.from_user.id:
        await query.answer("Action belongs to a different admin.", show_alert=True)
        return

    runtime = await init_db()
    kind = payload["kind"]
    details = payload["payload"]
    try:
        if kind == "unschedule":
            backend = await runtime.get_backend()
            async with runtime.operation_lock:
                deleted = await backend.delete_pending(details["meme_ids"])
                if deleted:
                    await runtime.write_backup_file()
            deleted_ids = ", ".join(public_meme_id(meme_id) for meme_id in deleted) or "none"
            log_admin_event(kind, query.from_user.id, "success", deleted_ids)
            await query.edit_message_text(
                f"✅ Action completed · IDs: {deleted_ids} · Status: cancelled"
            )
            return

        if kind == "restore":
            async with runtime.operation_lock:
                pre_restore_path, _ = await runtime.write_backup_file()
                counts = await runtime.import_payload(details["payload"], runtime.active_key)
                post_restore_path, _ = await runtime.write_backup_file()
            log_admin_event(
                kind,
                query.from_user.id,
                "success",
                f"file={details['filename']} · pre={pre_restore_path.name} · post={post_restore_path.name}",
            )
            await query.edit_message_text(
                "✅ Action completed · "
                f"Restore source: {details['filename']} · Records: {counts['total']} total"
            )
            return

        if kind in {"migrate", "switch"}:
            summary = await runtime.migrate_to(
                details["target_key"],
                allow_backup_fallback=(kind == "switch"),
            )
            log_admin_event(kind, query.from_user.id, "success", json.dumps(summary))
            await query.edit_message_text(
                f"✅ Action completed · Active DB: {summary['target_key']} · "
                f"Records: {summary['target_counts']['total']}"
            )
            await notify_admin_channel(
                context.bot,
                (
                    f"Database {kind.title()} Summary\n"
                    f"Source: {summary['source_key']}\n"
                    f"Target: {summary['target_key']}\n"
                    f"Records moved: {summary['target_counts']['total']}\n"
                    f"Scheduled moved: {summary['target_counts']['scheduled']}\n"
                    f"Backup: {Path(summary['backup_path']).name}\n"
                    f"Source mode: {summary['source_mode']}\n"
                    f"Fallback until: {summary['fallback_until']}\n"
                    f"Warnings: {', '.join(summary['warnings']) or 'none'}"
                ),
            )
            return

        await query.edit_message_text("❌ Unsupported action")
    except Exception as exc:
        log_admin_event(kind, query.from_user.id, "failed", str(exc))
        await query.edit_message_text(
            f"❌ {kind} failed · {type(exc).__name__}: {exc} · Suggested fix: review /logs"
        )


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if data.startswith("scheduled:"):
        runtime = await init_db()
        backend = await runtime.get_backend()
        records = await backend.list_pending_memes()
        _, _, raw_page = data.split(":")
        text, markup = format_scheduled_page(records, int(raw_page))
        await query.edit_message_text(text, reply_markup=markup)
        return

    if data.startswith("action:cancel:"):
        token = data.split(":")[-1]
        payload = PENDING_ACTIONS.pop(token, None)
        if payload:
            log_admin_event(payload["kind"], query.from_user.id, "cancelled", payload["summary"])
        await query.edit_message_text("✅ Action cancelled")
        return

    if data.startswith("action:confirm:"):
        await execute_registered_action(data.split(":")[-1], query, context)
        return

    if data == "db:refresh":
        runtime = await init_db()
        buttons = []
        for key, profile in runtime.profiles.items():
            if key == runtime.active_key:
                continue
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"Migrate → {profile.label}",
                        callback_data=f"db:migrate:{key}",
                    ),
                    InlineKeyboardButton(
                        f"Switch → {profile.label}",
                        callback_data=f"db:switch:{key}",
                    ),
                ]
            )
        await query.edit_message_text(
            f"Database Control\nActive: {runtime.active_key}",
            reply_markup=InlineKeyboardMarkup(
                buttons or [[InlineKeyboardButton("Refresh", callback_data="db:refresh")]]
            ),
        )
        return

    if data.startswith("db:migrate:") or data.startswith("db:switch:"):
        _, kind, target_key = data.split(":")
        runtime = await init_db()
        profile = runtime.profiles.get(target_key)
        if profile is None:
            await query.edit_message_text("❌ Unknown database profile")
            return
        token = register_action(
            kind,
            query.from_user.id,
            f"{runtime.active_key} -> {target_key}",
            {"target_key": target_key},
        )
        await query.edit_message_text(
            f"Confirm {kind}\n"
            f"Source: {runtime.active_key}\n"
            f"Target: {profile.label}\n"
            f"Mode: {'verified migration' if kind == 'migrate' else 'emergency switch with backup fallback'}",
            reply_markup=build_confirmation_markup(token),
        )


async def periodic_poster(application: Any) -> None:
    while True:
        try:
            await pop_due_memes_and_post(application)
            await maybe_send_storage_alert(application.bot)
        except Exception:
            logger.exception("Error in poster loop")
        await asyncio.sleep(30)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Please set TELEGRAM_BOT_TOKEN")
    if not OWNER_IDS or 0 in OWNER_IDS:
        raise SystemExit("Please set OWNER_ID to your Telegram user ID")
    if not CHANNEL_ID:
        raise SystemExit("Please set CHANNEL_ID")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", helpcmd))
    app.add_handler(CommandHandler("scheduled", scheduled))
    app.add_handler(CommandHandler("preview", preview))
    app.add_handler(CommandHandler("unschedule", unschedule))
    app.add_handler(CommandHandler("postnow", postnow))
    app.add_handler(CommandHandler("scheduleat", scheduleat))
    app.add_handler(CommandHandler("backup", backup))
    app.add_handler(CommandHandler("restore", restore))
    app.add_handler(CommandHandler("database", database_menu))
    app.add_handler(CommandHandler("db", database_menu))
    app.add_handler(CommandHandler("storage", storage_status))
    app.add_handler(CommandHandler("status", storage_status))
    app.add_handler(CommandHandler("logs", logcmd))
    app.add_handler(CommandHandler("log", logcmd))
    app.add_handler(CallbackQueryHandler(on_callback))

    media_filter = filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.ANIMATION)
    app.add_handler(MessageHandler(media_filter, handle_media))

    async def post_init(application: Any) -> None:
        me = await application.bot.get_me()
        logger.info("Bot connected as @%s (id=%s)", me.username, me.id)
        asyncio.create_task(periodic_poster(application))

    app.post_init = post_init
    logger.info("Starting bot...")
    app.run_polling()


if __name__ == "__main__":
    main()
