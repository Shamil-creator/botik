from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

from schedule_bot.services.deps import storage

logger = logging.getLogger(__name__)


class ActivityMiddleware(BaseMiddleware):
    """Middleware для обновления активности пользователей."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        """Обновляет активность пользователя перед обработкой события."""
        if isinstance(event, Message):
            chat_id = event.chat.id
            # Обновляем активность только для зарегистрированных пользователей
            if storage.get_user_group(chat_id) is not None:
                try:
                    # Получаем username из сообщения если он есть
                    username = None
                    if event.from_user:
                        username = event.from_user.username
                    storage.update_user_activity(chat_id, username)
                except Exception:
                    # Не прерываем обработку при ошибке обновления активности
                    logger.debug(
                        "Failed to update activity for chat_id=%s",
                        chat_id,
                        exc_info=True,
                    )
        
        return await handler(event, data)

