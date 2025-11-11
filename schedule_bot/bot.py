import asyncio
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties

from schedule_bot.config import load_settings
from schedule_bot.handlers import schedule, start
from schedule_bot.services.deps import storage
from schedule_bot.services.monitor import monitor_updates
from schedule_bot.services.sessions import ensure_sessions_loaded


async def main() -> None:
    settings = load_settings()
    bot = Bot(
        settings.bot.token,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dispatcher = Dispatcher()

    dispatcher.include_router(start.router)
    dispatcher.include_router(schedule.router)

    ensure_sessions_loaded(storage)

    monitor_task = asyncio.create_task(
        monitor_updates(bot, interval_minutes=60)
    )

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dispatcher.start_polling(bot)
    finally:
        monitor_task.cancel()
        with suppress(asyncio.CancelledError):
            await monitor_task


if __name__ == "__main__":
    asyncio.run(main())
