from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

from schedule_bot.config import load_settings
from schedule_bot.services.deps import cache, storage

router = Router()
logger = logging.getLogger(__name__)


class BroadcastState(StatesGroup):
    waiting_message = State()

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
        # Игнорируем команду без ответа для не-админов
        logger.debug(
            "Admin command ignored (non-admin) chat_id=%s",
            message.chat.id,
        )
        return
    
    logger.info("Admin command called chat_id=%s", message.chat.id)
    await message.answer(
        "<b>🔐 Админ-панель</b>\n\n"
        "Доступные команды:\n"
        "  • /stats — статистика бота\n"
        "  • /users [группа] — список пользователей группы\n"
        "  • /broadcast — отправить сообщение всем пользователям\n"
        "  • /admin — это меню",
    )


@router.message(Command("stats"))
async def handle_stats(message: Message) -> None:
    """Команда /stats — показывает подробную статистику."""
    if not is_admin(message.chat.id):
        # Игнорируем команду без ответа для не-админов
        logger.debug(
            "Stats command ignored (non-admin) chat_id=%s",
            message.chat.id,
        )
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


@router.message(Command("users"))
async def handle_users(message: Message, command: CommandObject) -> None:
    """Команда /users [группа] — показывает список пользователей группы."""
    if not is_admin(message.chat.id):
        # Игнорируем команду без ответа для не-админов
        logger.debug(
            "Users command ignored (non-admin) chat_id=%s",
            message.chat.id,
        )
        return
    
    group_query = (command.args or "").strip()
    
    if not group_query:
        # Показываем список всех групп
        group_stats = storage.get_group_statistics(limit=50)
        if not group_stats:
            await message.answer("❌ Нет зарегистрированных групп.")
            return
        
        groups_text = "<b>📚 Список групп:</b>\n\n"
        groups_text += "Используйте: <code>/users &lt;группа&gt;</code>\n\n"
        groups_text += "<b>Доступные группы:</b>\n"
        for idx, (group_name, count) in enumerate(group_stats, 1):
            groups_text += f"  {idx}. <b>{group_name}</b> — {count} чел.\n"
        
        await message.answer(groups_text)
        logger.info("Users command called without group chat_id=%s", message.chat.id)
        return
    
    # Ищем пользователей указанной группы
    try:
        users = storage.get_users_by_group(group_query)  # Возвращает [(chat_id, username), ...]
        
        if not users:
            await message.answer(
                f"❌ Группа <b>{group_query}</b> не найдена или в ней нет пользователей."
            )
            logger.info(
                "Users command: group not found chat_id=%s group=%s",
                message.chat.id,
                group_query,
            )
            return
        
        # Форматируем список пользователей
        # Telegram ограничивает длину сообщения ~4096 символов
        # Разбиваем на части если нужно
        max_users_per_message = 40  # Уменьшено из-за username
        total_users = len(users)
        
        if total_users <= max_users_per_message:
            # Одно сообщение
            users_text = f"<b>👥 Пользователи группы {group_query}</b>\n\n"
            users_text += f"Всего: <b>{total_users}</b> чел.\n\n"
            users_text += "<b>Пользователи:</b>\n"
            for idx, (user_id, username) in enumerate(users, 1):
                if username:
                    users_text += f"  {idx}. @{username} (<code>{user_id}</code>)\n"
                else:
                    users_text += f"  {idx}. <code>{user_id}</code>\n"
            
            await message.answer(users_text)
        else:
            # Несколько сообщений
            await message.answer(
                f"<b>👥 Пользователи группы {group_query}</b>\n\n"
                f"Всего: <b>{total_users}</b> чел.\n\n"
                f"Список будет отправлен частями..."
            )
            
            for i in range(0, total_users, max_users_per_message):
                chunk = users[i:i + max_users_per_message]
                chunk_text = f"<b>Часть {i // max_users_per_message + 1}</b>\n\n"
                chunk_text += "<b>Пользователи:</b>\n"
                for idx, (user_id, username) in enumerate(chunk, start=i + 1):
                    if username:
                        chunk_text += f"  {idx}. @{username} (<code>{user_id}</code>)\n"
                    else:
                        chunk_text += f"  {idx}. <code>{user_id}</code>\n"
                
                await message.answer(chunk_text)
        
        logger.info(
            "Users command: group found chat_id=%s group=%s count=%d",
            message.chat.id,
            group_query,
            total_users,
        )
    except Exception as e:
        logger.exception("Failed to get users by group")
        await message.answer(
            f"❌ Ошибка при получении списка пользователей: {type(e).__name__}"
        )


@router.message(Command("broadcast"))
async def handle_broadcast(message: Message, state: FSMContext) -> None:
    """Команда /broadcast — начинает процесс рассылки."""
    if not is_admin(message.chat.id):
        logger.debug(
            "Broadcast command ignored (non-admin) chat_id=%s",
            message.chat.id,
        )
        return
    
    logger.info("Broadcast command called chat_id=%s", message.chat.id)
    
    total_users = storage.get_total_users()
    
    # Клавиатура с кнопкой отмены
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отменить")]],
        resize_keyboard=True,
    )
    
    await state.set_state(BroadcastState.waiting_message)
    await message.answer(
        f"<b>📢 Массовая рассылка</b>\n\n"
        f"Всего пользователей: <b>{total_users}</b>\n\n"
        f"Отправь сообщение, которое увидят все пользователи.\n"
        f"Поддерживается HTML-разметка.\n\n"
        f"Нажми \"❌ Отменить\" для отмены.",
        reply_markup=cancel_kb,
    )


@router.message(BroadcastState.waiting_message, F.text == "❌ Отменить")
async def handle_broadcast_cancel(message: Message, state: FSMContext) -> None:
    """Отмена рассылки."""
    await state.clear()
    await message.answer(
        "❌ Рассылка отменена.",
        reply_markup=None,
    )
    logger.info("Broadcast cancelled by admin chat_id=%s", message.chat.id)


@router.message(BroadcastState.waiting_message)
async def handle_broadcast_message(message: Message, state: FSMContext) -> None:
    """Отправляет сообщение всем пользователям."""
    await state.clear()
    
    broadcast_text = message.text or message.caption or ""
    if not broadcast_text:
        await message.answer(
            "❌ Сообщение не может быть пустым.",
            reply_markup=None,
        )
        return
    
    # Получаем список всех пользователей
    all_users = list(storage.iter_chat_ids())
    total = len(all_users)
    
    await message.answer(
        f"📤 Начинаю рассылку для {total} пользователей...",
        reply_markup=None,
    )
    
    # Получаем бота из контекста
    bot: Bot = message.bot
    
    # Статистика
    success_count = 0
    failed_count = 0
    blocked_count = 0
    
    # Отправляем сообщение всем пользователям
    for chat_id in all_users:
        try:
            await bot.send_message(chat_id, broadcast_text, parse_mode="HTML")
            success_count += 1
            # Небольшая задержка чтобы не словить флуд
            await asyncio.sleep(0.05)
        except Exception as e:
            failed_count += 1
            error_msg = str(e).lower()
            if "blocked" in error_msg or "forbidden" in error_msg:
                blocked_count += 1
                # Удаляем пользователя который заблокировал бота
                storage.remove_user(chat_id)
                cache.remove_watcher(chat_id)
                logger.info(
                    "User blocked bot, removed from storage chat_id=%s",
                    chat_id,
                )
            else:
                logger.warning(
                    "Failed to send broadcast to chat_id=%s: %s",
                    chat_id,
                    e,
                )
    
    # Отчет
    report = (
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 Статистика:\n"
        f"  • Всего пользователей: {total}\n"
        f"  • Успешно отправлено: {success_count}\n"
        f"  • Ошибок: {failed_count}\n"
        f"  • Заблокировали бота: {blocked_count}"
    )
    
    await message.answer(report)
    logger.info(
        "Broadcast completed: total=%d success=%d failed=%d blocked=%d",
        total,
        success_count,
        failed_count,
        blocked_count,
    )
