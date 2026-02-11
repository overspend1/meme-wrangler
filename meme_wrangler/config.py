"""Centralised configuration loaded from environment variables."""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import time
from pathlib import Path
from typing import Optional, Set
from urllib.parse import urlparse, urlunparse, quote
from zoneinfo import ZoneInfo

try:
    import pytz
except ModuleNotFoundError:
    pytz = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------

if pytz is not None:
    IST = pytz.timezone("Asia/Kolkata")

    def ist_localize(dt) -> "datetime":  # noqa: F821
        """Attach IST to a naive datetime or convert an aware one."""
        from datetime import datetime as _dt  # noqa: F811

        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
else:
    IST = ZoneInfo("Asia/Kolkata")

    def ist_localize(dt) -> "datetime":  # noqa: F821
        from datetime import datetime as _dt  # noqa: F811

        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST)


def ensure_ist(dt) -> "datetime":  # noqa: F821
    """Guarantee *dt* is expressed in IST."""
    if dt.tzinfo is None:
        return ist_localize(dt)
    return dt.astimezone(IST)


# ---------------------------------------------------------------------------
# Scheduling constants
# ---------------------------------------------------------------------------

SLOTS = [time(11, 0), time(16, 0), time(21, 0)]

# ---------------------------------------------------------------------------
# Database URL construction
# ---------------------------------------------------------------------------


def _normalize_database_url(url: str) -> str:
    """Replace localhost hosts with the configured Postgres host for
    container runs, preserving any query-string parameters (e.g.
    ``sslmode=require`` used by Neon)."""

    host_override = os.environ.get("POSTGRES_HOST", "").strip()
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


def build_database_url() -> Optional[str]:
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


def is_neon_url(url: str) -> bool:
    """Return *True* when *url* points to a Neon database host."""
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    return ".neon.tech" in hostname or ".neon." in hostname


# ---------------------------------------------------------------------------
# Config dataclass-like holder
# ---------------------------------------------------------------------------


class Config:
    """Read-once configuration bag populated from env vars."""

    def __init__(self) -> None:
        self.database_url: Optional[str] = build_database_url()
        self.bot_token: Optional[str] = os.environ.get("TELEGRAM_BOT_TOKEN")

        # Owner IDs (comma-separated)
        raw_ids = os.environ.get("OWNER_ID", "0")
        try:
            self.owner_ids: Set[int] = set(
                int(oid.strip()) for oid in raw_ids.split(",") if oid.strip()
            )
        except ValueError as exc:
            logger.error("Failed to parse OWNER_ID '%s': %s", raw_ids, exc)
            self.owner_ids = {0}

        self.channel_id: Optional[str] = os.environ.get("CHANNEL_ID")

        # Backup settings
        self.backup_dir: Path = Path(
            os.environ.get("MEMEBOT_BACKUP_DIR", "backups")
        )
        _hardcoded_hash = (
            "16c5b5ddf1b27f16ad5f801bb83595d00e666cc53085e53a4b1e67b715016251"
        )
        self.backup_password_hash: str = (
            os.environ.get("MEMEBOT_BACKUP_PASSWORD_HASH") or _hardcoded_hash
        )
        self.backup_interval_hours: float = float(
            os.environ.get("MEMEBOT_BACKUP_INTERVAL_HOURS", "6")
        )
        self.backup_retain_count: int = int(
            os.environ.get("MEMEBOT_BACKUP_RETAIN_COUNT", "10")
        )
        self.backup_store_in_db: bool = (
            os.environ.get("MEMEBOT_BACKUP_STORE_IN_DB", "true").lower()
            in {"true", "1", "yes"}
        )

        # Neon awareness
        self.is_neon: bool = (
            is_neon_url(self.database_url) if self.database_url else False
        )

        logger.info("Configured owner IDs: %s", self.owner_ids)
        if self.is_neon:
            logger.info("Neon database detected - SSL and retry logic enabled")

    # ------------------------------------------------------------------
    def validate(self) -> None:
        """Raise *SystemExit* when mandatory settings are missing."""
        if not self.bot_token:
            raise SystemExit(
                "Please set TELEGRAM_BOT_TOKEN environment variable"
            )
        if not self.owner_ids or 0 in self.owner_ids:
            raise SystemExit(
                "Please set OWNER_ID to your Telegram user id"
            )
        if not self.channel_id:
            raise SystemExit(
                "Please set CHANNEL_ID to target channel (username or id)"
            )
        if not self.database_url:
            raise SystemExit(
                "Please set DATABASE_URL (or MEMEBOT_DB) to a PostgreSQL "
                "connection string"
            )

    def verify_backup_password(self, candidate: Optional[str]) -> bool:
        """Check *candidate* against the stored SHA-256 hash."""
        if not candidate:
            return False
        digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
        import hmac

        return hmac.compare_digest(digest, self.backup_password_hash)


# Singleton - created at import time so every module shares the same object.
cfg = Config()
