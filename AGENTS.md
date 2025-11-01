# Repository Guidelines

## Project Structure & Module Organization
- `bot.py` hosts the Telegram bot entry point, async scheduling logic, and PostgreSQL access; keep new runtime code here or in clearly named helpers under a future `meme_wrangler/` package.
- `tests/` stores pytest suites (`test_schedule.py` shows the current pattern); mirror module names and keep fixtures local.
- Deployment helpers (`docker-compose.yml`, `Dockerfile`, `deploy.sh`) expect environment files beside the repo root; Compose manages a `postgres` service and persists JSON backups under `./backups/`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` sets up a local environment; install app deps via `pip install -r requirements.txt`.
- `python bot.py` runs the bot against the configured Telegram token and channel; provide a `DATABASE_URL` (e.g., `postgresql://meme:meme@localhost:5432/meme_wrangler`) and optionally `MEMEBOT_BACKUP_DIR`.
- `pytest` executes unit tests; add `-k pattern` to target modules while iterating.
- `docker-compose up --build` builds and runs the production-like stack; `docker-compose logs -f` tails runtime output.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation; keep line length ≤ 100 characters to match existing files.
- Preserve async/await flows and granular functions (see `compute_next_slot`) to keep scheduling testable.
- Name new async handlers `on_<event>` and pure helpers `calculate_<outcome>`; prefer snake_case for variables and functions.
- Use type hints for new interfaces and log meaningful context with `logger`.

## Testing Guidelines
- Use pytest with descriptive `test_<behavior>` names; group related cases into modules mirroring source structure.
- For async logic, rely on `pytest.mark.asyncio` or `asyncio.run` helpers; avoid real Telegram calls by injecting fakes.
- For DB-dependent tests, spin up a temporary PostgreSQL instance (Docker or testcontainers) and point `DATABASE_URL` to an isolated schema; clean up tables after each run.
- Document new edge cases in tests before altering scheduling rules or media handling paths.

## Commit & Pull Request Guidelines
- Follow Conventional Commit prefixes seen in history (`feat:`, `docs:`, `security:`); keep subject ≤ 72 characters and body wrapped at 100.
- Each PR should describe behavior changes, note env vars or migrations, and attach `pytest` output or manual bot logs when relevant.
- Link issues or TODOs, flag breaking changes in bold, and include screenshots only when UI-facing Telegram copy changes.

## Deployment & Secret Tips
- Keep `.env` out of version control; copy from `.env.example` and set `TELEGRAM_BOT_TOKEN`, `OWNER_ID`, `CHANNEL_ID`, `POSTGRES_*` credentials, and `DATABASE_URL` for local runs.
- Only override `MEMEBOT_BACKUP_PASSWORD_HASH` if you plan to replace the baked-in SHA-256 hash for backup commands.
- Docker workflows mount backups in `./backups/` and keep the PostgreSQL cluster in the `pgdata` volume; prune carefully when resetting schedules. New memes automatically generate a fresh backup file in that directory.
