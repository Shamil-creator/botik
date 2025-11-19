import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties

from schedule_bot.config import load_settings
from schedule_bot.handlers import admin, schedule, start
from schedule_bot.logging_config import setup_logging
from schedule_bot.middleware.activity import ActivityMiddleware

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = load_settings()
    setup_logging(settings.logging)

    logger.info("Starting bot initialisation")

    from schedule_bot.services.deps import cache, storage  # noqa: WPS433
    from schedule_bot.services.monitor import (  # noqa: WPS433
        monitor_updates,
    )
    from schedule_bot.services.sessions import (  # noqa: WPS433
        ensure_sessions_loaded,
    )

    logger.info(
        "Dependencies ready initial_watchers=%d",
        len(cache.get_watchers()),
    )

    bot = Bot(
        settings.bot.token,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dispatcher = Dispatcher()
    
    # Регистрируем middleware для отслеживания активности
    dispatcher.message.middleware(ActivityMiddleware())

    dispatcher.include_router(start.router)
    dispatcher.include_router(schedule.router)
    dispatcher.include_router(admin.router)

    try:
        ensure_sessions_loaded(storage)
    except Exception:
        logger.exception("Failed to load session documents")
        raise

    monitor_task = asyncio.create_task(
        monitor_updates(bot, interval_minutes=60)
    )
    logger.info("Background monitor task started")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook removed, starting polling")
        await dispatcher.start_polling(bot)
    except Exception:
        logger.exception("Dispatcher polling terminated with error")
        raise
    finally:
        monitor_task.cancel()
        with suppress(asyncio.CancelledError):
            await monitor_task
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Bot stopped due to unrecoverable error")
        raise
