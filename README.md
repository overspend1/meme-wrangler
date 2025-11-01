# Meme Wrangler Bot

Telegram bot that accepts memes (photos, GIF animations, videos) from the owner's private messages and schedules them into a channel at the next available slot among 11:00, 16:00, 21:00.

## Setup

### Option 1: Run with Docker (Recommended)

The easiest way to run the bot is using Docker:

1. **Set up environment variables:**
   ```bash
   cp .env.example .ENV
   nano .ENV  # Edit with your bot credentials
   ```
   By default the compose file reads values from `compose.env` so it can boot without secrets. To keep credentials out of version control, point the stack at your own file by exporting `COMPOSE_ENV_FILE` before running Compose (e.g. `COMPOSE_ENV_FILE=.ENV docker compose up -d`). Populate `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` (defaults provided). Backups are protected by a built-in SHA-256 hash; optionally define `MEMEBOT_BACKUP_PASSWORD_HASH` to replace it.

2. **Run with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f
   ```

   Deploying via Portainer? Upload your `.ENV` file under **Environment variables** and set `COMPOSE_ENV_FILE=.ENV` so the stack loads it automatically.

4. **Stop the bot:**
   ```bash
   docker-compose down
   ```

For detailed Docker deployment instructions, including remote server deployment, see [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md).

### Option 2: Run Locally

1. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Provision a PostgreSQL database (local Docker container or managed instance) and note the connection string.

3. Set environment variables:

```bash
export TELEGRAM_BOT_TOKEN=123:ABC
export OWNER_ID=123456789
export CHANNEL_ID=@yourchannel  # or -1001234567890
export DATABASE_URL=postgresql://meme:meme@localhost:5432/meme_wrangler
# Optional: where JSON backups are written
# export MEMEBOT_BACKUP_DIR=/path/to/backups
# export MEMEBOT_BACKUP_PASSWORD_HASH=<sha256 hash of your backup secret>
```

4. Run the bot:

```bash
python bot.py
```

## How it works

-   Owner sends a photo/video/animation in the bot's DM.
-   Bot stores the Telegram file_id and schedules it for the next available slot: **11:00, 16:00, 21:00 IST (India Standard Time)**. If there's an existing scheduled meme, new ones are scheduled after the last one using the same cycle.
-   A background task posts due memes into the configured channel at the scheduled IST times.

## Docker Implementation

This project includes full Docker support for easy deployment:

- **Dockerfile**: Creates a lightweight Python container with all dependencies
- **docker-compose.yml**: Simplifies running the bot alongside PostgreSQL with coordinated configuration
- **Persistent storage**: `pgdata` volume keeps the database cluster while `./backups` stores exported JSON backups
- **Auto-restart**: Container automatically restarts if it crashes
- **Logging**: Configured with log rotation (10MB max, 3 files)

The Docker implementation ensures consistent behavior across different environments and simplifies deployment to production servers.

## Notes

-   All times are in **IST (India Standard Time, UTC+5:30)** regardless of the server's timezone.
-   Stored timestamps are Unix timestamps (UTC).
-   Make sure the bot is admin in the channel to post messages.
-   When using Docker Compose, PostgreSQL data lives in the `pgdata` volume and JSON backups are written to `./backups/`.
-   Use `/backup <secret>` to export the full meme catalog (scheduled + posted) and `/restore <secret>` (replying to a backup file) to import it again. Validation happens against a baked-in SHA-256 hash; override `MEMEBOT_BACKUP_PASSWORD_HASH` if you need to supply your own hash.
-   Every new meme DM automatically triggers a fresh JSON backup stored under `./backups/` (Compose) or the directory pointed to `MEMEBOT_BACKUP_DIR`.
