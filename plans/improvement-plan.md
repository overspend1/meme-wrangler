# Meme Wrangler Bot - Comprehensive Improvement Plan

## Current State Analysis

The bot lives entirely in a single `bot.py` (~900 lines) with raw `asyncpg` calls, no Neon DB awareness, a file-only backup system, massive code duplication, and minimal tests. This plan addresses every weak spot while keeping the existing Telegram workflow intact.

---

## 1. Project Restructuring - Modular Package

**Problem:** Everything is in one file. New features, testing, and maintenance are painful.

**Target structure:**

```
meme_wrangler/
  __init__.py
  config.py          # all env loading, validation, constants
  db.py              # pool management, schema init, migrations, Neon integration
  models.py          # dataclasses for Meme, BackupPayload, etc.
  scheduling.py      # compute_next_slot, slot constants, scheduling logic
  backup.py          # create/restore/rotate/verify backups
  media.py           # send_media_with_fallback helper (deduplicated)
  handlers/
    __init__.py
    admin.py          # owner-only commands: scheduled, unschedule, postnow, scheduleat, log
    backup_cmds.py    # /backup, /restore
    media_intake.py   # handle_media (DM meme intake)
    general.py        # /start, /help
  poster.py           # periodic_poster background loop
  decorators.py       # owner_only decorator, error-boundary decorator
bot.py                # thin entry point: build app, register handlers, run
tests/
  conftest.py
  test_scheduling.py
  test_backup.py
  test_db.py
  test_handlers.py
  test_media.py
```

### Steps

1. Create the `meme_wrangler/` package directory and `__init__.py`.
2. Extract `config.py` - move all `os.environ.get` calls, `_build_database_url`, `_normalize_database_url`, constant definitions (`SLOTS`, `IST`, `BACKUP_DIR`, etc.), and add a `validate_config()` function that fails fast with clear messages.
3. Extract `db.py` - move `init_db`, `DB_POOL`, `_DB_INITIALIZED`, pool creation, schema DDL; add Neon-specific connection parameters (see Section 2).
4. Extract `models.py` - create `@dataclass` for `Meme` (mirrors the DB row) and `BackupPayload`.
5. Extract `scheduling.py` - move `compute_next_slot`, `get_last_scheduled_ts`, `schedule_meme`, `SLOTS`.
6. Extract `backup.py` - move `create_backup`, restore logic, add rotation/verification (see Section 3).
7. Extract `media.py` - create a single `send_media_with_fallback(bot, chat_id, file_id, mime, caption)` that encapsulates the try-video/try-photo/try-document/download-reupload chain used in at least 3 places today.
8. Extract `decorators.py` - create `@owner_only` decorator that checks `update.effective_user.id in OWNER_IDS` and replies with the rejection message, eliminating the copy-pasted guard from every handler.
9. Extract handler modules under `handlers/`.
10. Slim down `bot.py` to just the `main()` entry point: config validation, DB init, handler registration, `run_polling`.

---

## 2. Neon DB Integration

**Problem:** The bot uses raw `asyncpg.create_pool` with no SSL, no retry logic, and no awareness of Neon's serverless architecture (cold starts, connection pooler, `sslmode=require`).

### What Neon Requires

- **SSL mandatory** - all connections must use `sslmode=require` (or `verify-full`).
- **Connection pooler** - Neon provides a PgBouncer-compatible pooler endpoint (port 5432 vs direct port 5433). The pooled endpoint should be the default for bot workloads.
- **Serverless cold starts** - first connection after idle can take 500ms-3s. The bot needs retry logic.
- **Connection limits** - Neon free tier allows ~50 simultaneous connections on the pooler; the bot should keep its pool small.

### Steps

1. **Add `sslmode=require` to connection parameters.** In `db.py`, detect whether the `DATABASE_URL` targets a Neon host (contains `.neon.tech`) and inject `ssl='require'` into the `asyncpg.create_pool` call. For non-Neon Postgres (local Docker), keep SSL optional.

2. **Add connection retry with exponential backoff.** Wrap pool creation in a retry loop (3 attempts, 1s/2s/4s delays) to handle Neon cold starts gracefully. Log each retry.

3. **Tune pool size for Neon.** Set `min_size=1, max_size=3` for Neon (the bot's workload is light) vs `max_size=5` for local Postgres. Detect via the hostname.

4. **Add connection health checks.** Before acquiring from pool, periodically verify with a lightweight `SELECT 1`. asyncpg doesn't have built-in health checks, so add a wrapper that catches `ConnectionDoesNotExistError` / `InterfaceError` and recreates the pool.

5. **Add `server_settings` for Neon.** Pass `server_settings={'application_name': 'meme-wrangler'}` so the bot is identifiable in Neon's dashboard.

6. **Handle `?sslmode=require` in URL.** Neon connection strings often include query params. The `_normalize_database_url` function should preserve these when rewriting the host.

7. **Update `.env.example` files.** Add a `NEON_DATABASE_URL` example showing the full Neon pooler connection string format, and document the difference between pooled vs direct endpoints.

8. **Update `docker-compose.yml`.** Add a `neon` profile that skips the local Postgres service and uses `DATABASE_URL` pointing to Neon. The default profile keeps the local Postgres for development.

### Connection Setup Pseudocode

```python
async def create_pool(database_url: str) -> asyncpg.Pool:
    is_neon = '.neon.tech' in database_url or '.neon.' in database_url
    ssl_ctx = 'require' if is_neon else None
    max_retries = 3 if is_neon else 1

    for attempt in range(max_retries):
        try:
            pool = await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=3 if is_neon else 5,
                ssl=ssl_ctx,
                command_timeout=30,
                server_settings={'application_name': 'meme-wrangler'},
            )
            # verify connectivity
            async with pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return pool
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning('DB connect attempt %d failed: %s - retrying in %ds', attempt + 1, exc, wait)
                await asyncio.sleep(wait)
            else:
                raise
```

---

## 3. Backup System Overhaul

**Problem:** Backups are JSON files on local disk only. No rotation, no scheduled backups, no integrity checks, no cloud persistence.

### 3a. Scheduled Periodic Backups

Add a background task (alongside `periodic_poster`) that runs `create_backup()` on a configurable interval (default: every 6 hours). Controlled by a new env var `MEMEBOT_BACKUP_INTERVAL_HOURS`.

### 3b. Backup Rotation

After creating a backup, delete old files beyond a retention count. New env var `MEMEBOT_BACKUP_RETAIN_COUNT` (default: 10). The rotation function sorts backups by timestamp in the filename and removes the oldest.

### 3c. Backup Integrity Verification

- After writing, compute a SHA-256 checksum of the file and store it in a `.sha256` sidecar file.
- On restore, verify the checksum before importing.
- Add a `/verifybackup` command that checks the most recent backup's integrity.

### 3d. Neon DB Backup Table

Store backup metadata (and optionally the full JSON payload) in a dedicated `backups` table in Neon. This gives cloud-durable backup history even if the local filesystem is lost.

```sql
CREATE TABLE IF NOT EXISTS backups (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filename TEXT NOT NULL,
    total_memes INTEGER NOT NULL,
    scheduled_memes INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    payload JSONB,
    size_bytes INTEGER
);
```

On every backup creation, insert a row. The `payload` column is optional (controlled by `MEMEBOT_BACKUP_STORE_IN_DB`, default true) - for small datasets this is fine; for large ones, store only metadata.

### 3e. Compressed Backups

Use `gzip` compression for backup files. A typical JSON backup compresses 5-10x. Update restore logic to detect and decompress `.json.gz` files.

### 3f. Backup Status Command

Add `/backupstatus` that shows:
- Last backup time
- Number of backups on disk
- Number of backups in DB
- Total memes / scheduled memes in latest backup
- Disk usage of backup directory

### Steps

1. Create `meme_wrangler/backup.py` with the `BackupManager` class encapsulating all backup logic.
2. Add `create_backup()` - writes JSON (gzip-compressed), computes checksum, stores metadata in DB, rotates old files.
3. Add `restore_from_file()` - verifies checksum, decompresses if needed, validates schema, imports to DB.
4. Add `rotate_backups()` - keeps only the N most recent files.
5. Add `get_backup_status()` - returns a summary dict for the `/backupstatus` command.
6. Add `periodic_backup()` background task in `poster.py` or a new `tasks.py`.
7. Register `/backupstatus` and `/verifybackup` handlers.
8. Update `/backup` and `/restore` handlers to use `BackupManager`.
9. Add `MEMEBOT_BACKUP_INTERVAL_HOURS`, `MEMEBOT_BACKUP_RETAIN_COUNT`, `MEMEBOT_BACKUP_STORE_IN_DB` to config and `.env.example`.

---

## 4. Database Layer Improvements

### 4a. Schema Migrations

Instead of a single `CREATE TABLE IF NOT EXISTS`, add a lightweight migration system:

- A `schema_version` table tracking the current version.
- Migration files or a list of migration functions in `db.py`.
- On startup, run any pending migrations in order.

This matters because adding the `backups` table, indexes, or future columns needs to happen without manual intervention.

### 4b. Indexes

Add missing indexes to improve query performance:

```sql
CREATE INDEX IF NOT EXISTS idx_memes_posted_scheduled
    ON memes (posted, scheduled_ts)
    WHERE posted = 0;

CREATE INDEX IF NOT EXISTS idx_memes_scheduled_ts
    ON memes (scheduled_ts);
```

The `WHERE posted = 0` partial index directly supports the most common queries (`pop_due_memes`, `get_last_scheduled_ts`, `/scheduled`).

### 4c. Connection Pool Health

Add a `check_pool_health()` function that runs periodically (every 60s) and recreates the pool if connections are stale. This is critical for Neon where idle connections get dropped after ~5 minutes.

### Steps

1. Add `schema_version` table and migration runner to `db.py`.
2. Define migration 1 (current schema), migration 2 (add indexes), migration 3 (add `backups` table).
3. Add pool health check background task.
4. Add a `db_status()` helper for diagnostics (pool size, active connections, schema version).

---

## 5. Code Quality and DRY Improvements

### 5a. Owner-Only Decorator

Replace the repeated guard clause in every handler:

```python
def owner_only(handler):
    @wraps(handler)
    async def wrapper(update, context):
        if update.effective_user.id not in OWNER_IDS:
            await update.message.reply_text('Only the owner can use this command.')
            return
        return await handler(update, context)
    return wrapper
```

### 5b. Unified Media Sender

Extract the try-video/try-photo/try-document/download-reupload chain into `send_media_with_fallback()`. Currently duplicated in:
- `pop_due_memes_and_post()` (lines 286-307)
- `scheduled()` (lines 360-400)
- `preview()` (lines 453-482)
- `postnow()` (lines 780-784, less robust version)

### 5c. Structured Logging

Replace string-formatted log messages with structured fields. Use `logging.getLogger('meme_wrangler.<module>')` per module for granular control.

### 5d. Error Handling

- Add a top-level error handler on the Application for unhandled exceptions.
- Use specific exception types instead of bare `except Exception`.
- Add user-friendly error messages while logging full tracebacks.

### 5e. Type Safety

- Add `Meme` dataclass and use it throughout instead of raw `asyncpg.Record` dicts.
- Add return type annotations to all public functions.

### 5f. Move Inline Import

The `import re` at line 790 and `from datetime import time as dtime` at line 833 should move to the top of the file / module.

### Steps

1. Create `decorators.py` with `@owner_only`.
2. Create `media.py` with `send_media_with_fallback()`.
3. Create `models.py` with `Meme` and `BackupPayload` dataclasses.
4. Refactor all handlers to use the decorator and helper.
5. Add Application-level error handler.
6. Fix all inline imports.

---

## 6. Testing Improvements

### Current State
- 3 tests in `test_schedule.py`, all for `compute_next_slot`.
- No DB tests, no handler tests, no backup tests.
- Uses deprecated `asyncio.get_event_loop().run_until_complete` instead of `pytest-asyncio`.

### Steps

1. Add `pytest-asyncio` and `asyncpg` to `requirements-dev.txt`.
2. Add test fixtures in `conftest.py`:
   - `db_pool` fixture that connects to a test database (env-configurable, skipped if unavailable).
   - `fake_bot` / `mock_context` fixtures for handler testing without real Telegram calls.
3. Rewrite existing tests to use `@pytest.mark.asyncio` instead of `run_until_complete`.
4. Add `test_scheduling.py` - expanded slot computation tests including edge cases (midnight, exactly on slot time, DST-like boundary).
5. Add `test_backup.py`:
   - Test `create_backup` writes valid JSON with correct structure.
   - Test backup rotation keeps only N files.
   - Test checksum generation and verification.
   - Test compressed backup round-trip.
   - Test restore with corrupted data rejects gracefully.
6. Add `test_db.py`:
   - Test migration runner applies migrations in order.
   - Test pool health check detects dead connections.
   - Test `schedule_meme` inserts correctly and returns expected slot.
7. Add `test_handlers.py`:
   - Test `@owner_only` rejects non-owners.
   - Test `handle_media` schedules and triggers backup.
   - Test `/unschedule` removes correct memes.
8. Add `test_media.py`:
   - Test `send_media_with_fallback` tries video first for video mime.
   - Test fallback chain when send_photo fails.
9. Target: at least 30 tests covering all modules.

---

## 7. Configuration and Environment Updates

### Steps

1. Update `.env.example` and `.ENV.example` with new variables:
   ```
   # Neon DB (use this instead of DATABASE_URL for Neon)
   # DATABASE_URL=postgresql://user:pass@ep-cool-name-123456.us-east-2.aws.neon.tech/neondb?sslmode=require
   
   # Backup settings
   MEMEBOT_BACKUP_INTERVAL_HOURS=6
   MEMEBOT_BACKUP_RETAIN_COUNT=10
   MEMEBOT_BACKUP_STORE_IN_DB=true
   ```

2. Update `docker-compose.yml`:
   - Add a `neon` profile that disables the `postgres` service.
   - Pass new env vars to the bot service.

3. Update `requirements.txt`:
   - Pin `asyncpg>=0.29.0` (already there).
   - No new runtime dependencies needed - `gzip`, `hashlib`, `ssl` are all stdlib.

4. Update `requirements-dev.txt`:
   ```
   pytest
   pytest-asyncio
   ```

---

## 8. Documentation Updates

### Steps

1. Update `README.md` with:
   - Neon DB setup instructions.
   - New backup commands (`/backupstatus`, `/verifybackup`).
   - New project structure overview.
   - Explanation of the `neon` Docker Compose profile.

2. Update `DOCKER_DEPLOY.md` with Neon-specific deployment guidance.

3. Update `AGENTS.md` to reflect the new `meme_wrangler/` package structure.

---

## Dependency and Risk Notes

- **No new runtime dependencies.** Neon integration is purely connection-parameter tuning on top of existing `asyncpg`. Compression uses stdlib `gzip`. SSL uses stdlib `ssl`.
- **Backward compatible.** The local Docker Postgres workflow remains the default. Neon is opt-in via `DATABASE_URL`.
- **Migration safety.** The migration runner only adds tables/indexes, never drops. Existing data is preserved.
- **Backup file format.** New compressed backups (`.json.gz`) are a superset - the restore logic will handle both plain JSON and gzip transparently.

---

## Execution Order

The work should proceed in this sequence to minimize breakage:

1. **Project restructuring** (Section 1) - split into modules first; all other changes target the new structure.
2. **Code quality / DRY** (Section 5) - decorator, media helper, models - these unblock cleaner handler code.
3. **Database layer** (Section 4) - migrations, indexes, health checks.
4. **Neon DB integration** (Section 2) - SSL, retry, pool tuning.
5. **Backup overhaul** (Section 3) - rotation, compression, DB storage, scheduled backups.
6. **Testing** (Section 6) - write tests against the new modular structure.
7. **Config and docs** (Sections 7-8) - final polish.

---

## Architecture Diagram

```mermaid
graph TD
    subgraph Entry
        A[bot.py - main entrypoint]
    end

    subgraph Package [meme_wrangler package]
        B[config.py]
        C[db.py - Neon-aware pool]
        D[models.py]
        E[scheduling.py]
        F[backup.py - BackupManager]
        G[media.py - send helper]
        H[decorators.py]
        I[poster.py - background tasks]
    end

    subgraph Handlers [handlers subpackage]
        J[general.py - start, help]
        K[admin.py - scheduled, unschedule, postnow, scheduleat, log]
        L[backup_cmds.py - backup, restore, backupstatus, verifybackup]
        M[media_intake.py - handle_media]
    end

    subgraph Storage
        N[Neon DB - memes + backups tables]
        O[Local Filesystem - compressed JSON backups]
        P[Local Postgres - dev/Docker fallback]
    end

    A --> B
    A --> C
    A --> J
    A --> K
    A --> L
    A --> M
    A --> I

    K --> E
    K --> G
    K --> H
    L --> F
    M --> E
    M --> F
    M --> G

    C --> N
    C --> P
    F --> O
    F --> C
    I --> C
    I --> F
    E --> C
```

## Backup Flow Diagram

```mermaid
sequenceDiagram
    participant Owner
    participant Bot
    participant BackupManager
    participant NeonDB
    participant Filesystem

    Note over Bot: Trigger: meme intake, /backup cmd, or periodic task

    Bot->>BackupManager: create_backup
    BackupManager->>NeonDB: SELECT all memes
    NeonDB-->>BackupManager: meme rows
    BackupManager->>BackupManager: Build JSON payload
    BackupManager->>BackupManager: Gzip compress
    BackupManager->>BackupManager: Compute SHA-256 checksum
    BackupManager->>Filesystem: Write .json.gz + .sha256 sidecar
    BackupManager->>NeonDB: INSERT INTO backups - metadata + payload
    BackupManager->>BackupManager: Rotate old files beyond retain count
    BackupManager-->>Bot: backup_path, stats

    Note over Owner: /restore flow
    Owner->>Bot: /restore password - reply to file
    Bot->>BackupManager: restore_from_file
    BackupManager->>BackupManager: Detect gzip vs plain JSON
    BackupManager->>BackupManager: Verify checksum if sidecar exists
    BackupManager->>BackupManager: Validate schema
    BackupManager->>NeonDB: TRUNCATE + INSERT memes
    BackupManager-->>Bot: restore stats
    Bot-->>Owner: Restore complete message
```
