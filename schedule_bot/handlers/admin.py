from __future__ import annotations

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from schedule_bot.config import load_settings
from schedule_bot.services.deps import cache, storage

router = Router()
logger = logging.getLogger(__name__)

_settings = load_settings()
ADMIN_ID = _settings.bot.admin_id


def is_admin(chat_id: int) -> bool:
    """Проверяет, является ли пользователь администратором."""
    if ADMIN_ID is None:
        logger.warning("ADMIN_ID not set, admin commands disabled")
        return False
    return chat_id == ADMIN_ID


def format_statistics() -> str:
    """Форматирует статистику бота."""
    total_users = storage.get_total_users()
    active_7d = storage.get_active_users_count(days=7)
    active_30d = storage.get_active_users_count(days=30)
    new_7d = storage.get_new_users_count(days=7)
    new_30d = storage.get_new_users_count(days=30)
    
    group_stats = storage.get_group_statistics(limit=10)
    watchers_count = len(cache.get_watchers())
    
    stats_lines = [
        "<b>📊 Статистика бота</b>",
        "",
        "<b>👥 Пользователи:</b>",
        f"  • Всего: <b>{total_users}</b>",
        f"  • Активных за 7 дней: <b>{active_7d}</b>",
        f"  • Активных за 30 дней: <b>{active_30d}</b>",
        f"  • Новых за 7 дней: <b>{new_7d}</b>",
        f"  • Новых за 30 дней: <b>{new_30d}</b>",
        f"  • Следит за обновлениями: <b>{watchers_count}</b>",
        "",
        "<b>📚 Популярные группы (TOP-10):</b>",
    ]
    
    if group_stats:
        for idx, (group_name, count) in enumerate(group_stats, 1):
            stats_lines.append(f"  {idx}. <b>{group_name}</b> — {count} чел.")
    else:
        stats_lines.append("  (пока нет данных)")
    
    return "\n".join(stats_lines)


@router.message(Command("admin"))
async def handle_admin(message: Message) -> None:
    """Команда /admin — показывает меню админ-панели."""
    if not is_admin(message.chat.id):
        logger.warning(
            "Admin command called by non-admin chat_id=%s",
            message.chat.id,
        )
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return
    
    logger.info("Admin command called chat_id=%s", message.chat.id)
    await message.answer(
        "<b>🔐 Админ-панель</b>\n\n"
        "Доступные команды:\n"
        "  • /stats — статистика бота\n"
        "  • /admin — это меню",
    )


@router.message(Command("stats"))
async def handle_stats(message: Message) -> None:
    """Команда /stats — показывает подробную статистику."""
    if not is_admin(message.chat.id):
        logger.warning(
            "Stats command called by non-admin chat_id=%s",
            message.chat.id,
        )
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return
    
    logger.info("Stats command called chat_id=%s", message.chat.id)
    
    try:
        stats_text = format_statistics()
        await message.answer(stats_text)
    except Exception as e:
        logger.exception("Failed to generate statistics")
        await message.answer(
            f"❌ Ошибка при получении статистики: {type(e).__name__}"
        )
