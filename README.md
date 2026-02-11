# Meme Wrangler Bot

Telegram bot that accepts memes (photos, GIF animations, videos) from the owner's private messages and schedules them into a channel at the next available slot among 11:00, 16:00, 21:00 IST.

## Project Structure

```
bot.py                          # Thin entry point
meme_wrangler/
  config.py                     # Env-var loading, validation, constants
  db.py                         # Neon-aware pool, migrations, health checks
  models.py                     # Meme / BackupPayload dataclasses
  scheduling.py                 # Slot computation, DB scheduling ops
  backup.py                     # Create / restore / rotate / verify backups
  media.py                      # Unified send-with-fallback helper
  decorators.py                 # @owner_only handler decorator
  poster.py                     # Background tasks (posting, backup, health)
  handlers/
    general.py                  # /start, /help
    admin.py                    # /scheduled, /unschedule, /preview, /postnow, /log, /scheduleat
    backup_cmds.py              # /backup, /restore, /backupstatus, /verifybackup
    media_intake.py             # DM media handler
tests/                          # pytest suite
```

## Setup

### Option 1: Run with Docker (Recommended)

1. **Set up environment variables:**
   ```bash
   cp .ENV.example .ENV
   nano .ENV  # Edit with your bot credentials
   ```
   The compose file looks for `.ENV` by default. Populate `TELEGRAM_BOT_TOKEN`, `OWNER_ID`, `CHANNEL_ID`, and the Postgres settings. Leave `POSTGRES_HOST=postgres` when running inside Docker Compose.

2. **Run with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f
   ```

4. **Stop the bot:**
   ```bash
   docker-compose down
   ```

For detailed Docker deployment instructions, including remote server deployment, see [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md).

### Option 2: Run with Neon DB

Neon is a serverless PostgreSQL service. The bot auto-detects Neon connection strings and enables SSL, connection retry (for cold starts), and smaller pool sizes.

1. Create a Neon project at [neon.tech](https://neon.tech) and copy the **pooled** connection string.
2. Set `DATABASE_URL` in your `.env`:
   ```
   DATABASE_URL=postgresql://user:pass@ep-cool-name-123456.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
3. Clear `POSTGRES_HOST` (or leave it unset) so the bot doesn't try to rewrite the URL.
4. Run `python bot.py` or use Docker Compose (the `postgres` service will be skipped when `DATABASE_URL` points to Neon).

### Option 3: Run Locally

1. Create a virtualenv and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Provision a PostgreSQL database (local Docker or managed instance) and note the connection string.

3. Set environment variables:
   ```bash
   export TELEGRAM_BOT_TOKEN=123:ABC
   export OWNER_ID=123456789
   export CHANNEL_ID=@yourchannel
   export DATABASE_URL=postgresql://meme:meme@localhost:5432/meme_wrangler
   ```

4. Run the bot:
   ```bash
   python bot.py
   ```

## How it works

- Owner sends a photo/video/animation in the bot's DM.
- Bot stores the Telegram file_id and schedules it for the next available slot: **11:00, 16:00, 21:00 IST**. If there's an existing scheduled meme, new ones queue after the last one.
- A background task posts due memes into the configured channel every 30 seconds.
- Every new meme triggers an automatic compressed backup.

## Backup System

The backup system is comprehensive and resilient:

- **Automatic backups** after every meme intake.
- **Periodic backups** every 6 hours (configurable via `MEMEBOT_BACKUP_INTERVAL_HOURS`).
- **Gzip compression** for 5-10x size reduction.
- **SHA-256 checksums** with sidecar files for integrity verification.
- **Backup rotation** keeps only the N most recent files (configurable via `MEMEBOT_BACKUP_RETAIN_COUNT`).
- **Database storage** - backup metadata (and optionally the full payload) is stored in a `backups` table for cloud-durable history.
- **Transparent restore** handles both plain JSON and `.json.gz` formats.

### Backup Commands

| Command | Description |
|---------|-------------|
| `/backup <password>` | Export all memes as a compressed JSON backup |
| `/restore <password>` | Reply to a backup file to restore |
| `/backupstatus` | Show backup statistics (disk/DB counts, sizes) |
| `/verifybackup` | Verify the latest backup file's SHA-256 integrity |

## Neon DB Integration

When `DATABASE_URL` contains a `.neon.tech` hostname, the bot automatically:

- Enables **SSL/TLS** connections (`sslmode=require`).
- Uses **exponential backoff retry** (3 attempts) to handle Neon cold starts.
- Reduces **pool size** to 3 connections (appropriate for serverless).
- Sets `application_name=meme-wrangler` for Neon dashboard visibility.
- Runs a **background health check** every 60 seconds to handle idle connection drops.

## Docker Implementation

- **Dockerfile**: Lightweight Python 3.12 container
- **docker-compose.yml**: Bot + PostgreSQL with coordinated config
- **Persistent storage**: `pgdata` volume for the DB, `./backups` for JSON exports
- **Auto-restart**: Container restarts on crash
- **Logging**: JSON file driver with 10MB rotation

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest -v
```

## Notes

- All times are in **IST (India Standard Time, UTC+5:30)** regardless of server timezone.
- Stored timestamps are Unix timestamps (UTC).
- The bot must be admin in the channel to post messages.
- Schema migrations run automatically on startup - no manual DB setup needed.
