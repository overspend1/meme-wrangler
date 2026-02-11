"""Neon-aware PostgreSQL connection pool, schema migrations, and health checks."""

from __future__ import annotations

import asyncio
import logging
import ssl as _ssl
from typing import Optional

try:
    import asyncpg  # type: ignore[import-untyped]
except ModuleNotFoundError:
    asyncpg = None  # type: ignore[assignment]

from meme_wrangler.config import cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level pool state
# ---------------------------------------------------------------------------

_pool: Optional["asyncpg.Pool"] = None
_schema_version: int = 0

# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------

# Each migration is a list of SQL statements.  The runner applies every
# migration whose index >= current schema_version.

MIGRATIONS: list[list[str]] = [
    # --- v1: original schema ---
    [
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        )
        """,
        """
        INSERT INTO schema_version (version)
        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version)
        """,
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
        """,
    ],
    # --- v2: indexes for common queries ---
    [
        """
        CREATE INDEX IF NOT EXISTS idx_memes_pending
            ON memes (scheduled_ts)
            WHERE posted = 0
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_memes_scheduled_ts
            ON memes (scheduled_ts)
        """,
    ],
    # --- v3: backups metadata table ---
    [
        """
        CREATE TABLE IF NOT EXISTS backups (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            filename TEXT NOT NULL,
            total_memes INTEGER NOT NULL,
            scheduled_memes INTEGER NOT NULL,
            checksum TEXT NOT NULL,
            payload JSONB,
            size_bytes INTEGER
        )
        """,
    ],
]


async def _run_migrations(conn: "asyncpg.Connection") -> None:
    """Apply pending migrations inside *conn* (should be in a transaction)."""
    global _schema_version

    # Ensure the version table exists (migration 0 creates it).
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        )
        """
    )
    row = await conn.fetchrow("SELECT version FROM schema_version LIMIT 1")
    if row is None:
        await conn.execute("INSERT INTO schema_version (version) VALUES (0)")
        current = 0
    else:
        current = int(row["version"])

    for idx in range(current, len(MIGRATIONS)):
        logger.info("Applying migration v%d -> v%d", idx, idx + 1)
        for stmt in MIGRATIONS[idx]:
            await conn.execute(stmt)
        await conn.execute(
            "UPDATE schema_version SET version = $1", idx + 1
        )

    _schema_version = len(MIGRATIONS)
    logger.info("Schema is at version %d", _schema_version)


# ---------------------------------------------------------------------------
# Pool creation (Neon-aware)
# ---------------------------------------------------------------------------


def _build_ssl_context() -> Optional[_ssl.SSLContext]:
    """Return an SSL context suitable for Neon (or *None* for local PG)."""
    if not cfg.is_neon:
        return None
    ctx = _ssl.create_default_context()
    # Neon endpoints use valid certs; verify them.
    ctx.check_hostname = True
    ctx.verify_mode = _ssl.CERT_REQUIRED
    return ctx


async def create_pool() -> "asyncpg.Pool":
    """Create and return a connection pool with retry logic for Neon cold
    starts.  The pool is stored in the module-level ``_pool`` variable."""
    if asyncpg is None:
        raise RuntimeError(
            "asyncpg must be installed to use database features."
        )
    if not cfg.database_url:
        raise RuntimeError(
            "DATABASE_URL (or MEMEBOT_DB) must point to a PostgreSQL database"
        )

    ssl_ctx = _build_ssl_context()
    max_retries = 3 if cfg.is_neon else 1
    max_size = 3 if cfg.is_neon else 5

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            pool = await asyncpg.create_pool(
                cfg.database_url,
                min_size=1,
                max_size=max_size,
                ssl=ssl_ctx,
                command_timeout=30,
                server_settings={"application_name": "meme-wrangler"},
            )
            # Verify connectivity
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info(
                "Database pool created (max_size=%d, neon=%s)",
                max_size,
                cfg.is_neon,
            )
            return pool
        except (OSError, Exception) as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "DB connect attempt %d/%d failed: %s - retrying in %ds",
                    attempt + 1,
                    max_retries,
                    exc,
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "All %d DB connect attempts failed.", max_retries
                )

    raise RuntimeError(
        f"Could not connect to database after {max_retries} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


async def init_db() -> "asyncpg.Pool":
    """Ensure the pool exists and the schema is up to date."""
    global _pool
    if _pool is None:
        _pool = await create_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await _run_migrations(conn)
    return _pool


async def get_pool() -> "asyncpg.Pool":
    """Return the initialised pool (calls ``init_db`` if needed)."""
    if _pool is None:
        return await init_db()
    return _pool


async def check_pool_health() -> bool:
    """Lightweight liveness check.  Returns *True* when healthy."""
    global _pool
    if _pool is None:
        return False
    try:
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        logger.warning("Pool health check failed: %s - recreating pool", exc)
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None
        try:
            _pool = await create_pool()
            return True
        except Exception:
            return False


async def close_pool() -> None:
    """Gracefully close the pool (for shutdown hooks)."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")
