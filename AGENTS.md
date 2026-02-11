# Repository Guidelines

## Project Structure & Module Organization
- `bot.py` is a thin entry point that wires up handlers, background tasks, and runs the Telegram polling loop.
- `meme_wrangler/` is the main package containing all business logic:
  - `config.py` loads and validates environment variables; exposes a `cfg` singleton.
  - `db.py` manages the Neon-aware PostgreSQL pool, schema migrations, and health checks.
  - `models.py` defines `Meme`, `BackupPayload`, and `BackupStatus` dataclasses.
  - `scheduling.py` contains slot computation and all scheduling DB operations.
  - `backup.py` handles creating, restoring, rotating, and verifying backups.
  - `media.py` provides `send_media_with_fallback()` - the unified media send chain.
  - `decorators.py` provides `@owner_only` for handler access control.
  - `poster.py` runs background tasks: periodic posting, backup, and health checks.
  - `handlers/` contains Telegram command handlers split by domain: `general.py`, `admin.py`, `backup_cmds.py`, `media_intake.py`.
- `tests/` stores pytest suites; mirror module names and keep fixtures in `conftest.py`.
- Deployment helpers (`docker-compose.yml`, `Dockerfile`, `deploy.sh`) expect environment files beside the repo root.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` sets up a local environment; install app deps via `pip install -r requirements.txt`.
- `python bot.py` runs the bot against the configured Telegram token and channel; provide a `DATABASE_URL` (e.g., `postgresql://meme:meme@localhost:5432/meme_wrangler`) and optionally `MEMEBOT_BACKUP_DIR`.
- `pip install -r requirements-dev.txt && pytest` executes the test suite; add `-k pattern` to target modules while iterating.
- `docker-compose up --build` builds and runs the production-like stack; `docker-compose logs -f` tails runtime output.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation; keep line length <= 100 characters to match existing files.
- Preserve async/await flows and granular functions to keep scheduling testable.
- Name new async handlers `on_<event>` and pure helpers `calculate_<outcome>`; prefer snake_case for variables and functions.
- Use type hints for new interfaces and log meaningful context with `logger`.
- Use the `@owner_only` decorator instead of manual ID checks in handlers.
- Use `send_media_with_fallback()` instead of writing try/except send chains.

## Testing Guidelines
- Use pytest with descriptive `test_<behavior>` names; group related cases into modules mirroring source structure.
- For async logic, use `@pytest.mark.asyncio`; avoid `asyncio.get_event_loop().run_until_complete`.
- For DB-dependent tests, spin up a temporary PostgreSQL instance (Docker or testcontainers) and point `DATABASE_URL` to an isolated schema; clean up tables after each run.
- Document new edge cases in tests before
