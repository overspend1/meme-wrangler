"""Tests for bot lifecycle hooks and background task supervision."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import bot


class DummyBot:
    async def get_me(self):
        return SimpleNamespace(username="meme_test_bot", id=123)


def test_post_init_starts_tasks_and_stores_references(monkeypatch):
    async def scenario():
        started = asyncio.Event()

        async def managed_loop(_application):
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(
            bot,
            "_task_specs",
            lambda: [bot.TaskSpec("managed_loop", managed_loop, restart_on_failure=False)],
        )

        application = SimpleNamespace(bot=DummyBot(), bot_data={})

        await bot.post_init(application)

        task_registry = application.bot_data[bot.BACKGROUND_TASKS_KEY]
        assert "managed_loop" in task_registry
        task = task_registry["managed_loop"]
        await asyncio.wait_for(started.wait(), timeout=1)
        assert task.done() is False

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_post_shutdown_cancels_tasks_and_closes_pool(monkeypatch):
    async def scenario():
        cancelled = asyncio.Event()
        closed = asyncio.Event()

        async def run_forever():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def fake_close_pool():
            closed.set()

        task = asyncio.create_task(run_forever())
        await asyncio.sleep(0)
        app = SimpleNamespace(bot_data={bot.BACKGROUND_TASKS_KEY: {"loop": task}})
        monkeypatch.setattr(bot, "close_pool", fake_close_pool)

        await bot.post_shutdown(app)

        assert cancelled.is_set()
        assert task.done()
        assert closed.is_set()

    asyncio.run(scenario())


def test_supervisor_restarts_after_failure(monkeypatch):
    async def scenario():
        attempts = 0
        blocker = asyncio.Event()

        monkeypatch.setattr(bot, "_BACKOFF_INITIAL_SECONDS", 0)

        async def flaky_loop(_application):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("boom")
            await blocker.wait()

        app = SimpleNamespace(bot=DummyBot(), bot_data={})
        supervisor_task = asyncio.create_task(
            bot._supervise_task(app, bot.TaskSpec("flaky", flaky_loop, restart_on_failure=True))
        )

        await asyncio.sleep(0.05)
        assert attempts >= 2

        supervisor_task.cancel()
        await asyncio.gather(supervisor_task, return_exceptions=True)

    asyncio.run(scenario())
