from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.types import Message

from schedule_bot.services.deps import exams_storage, storage
from schedule_bot.services.exams_parser import ExamEntry
from schedule_bot.services.ui import (
    BACK_BUTTON,
    MAIN_BUTTON_CHANGE,
    MAIN_BUTTON_CREDITS,
    MAIN_BUTTON_EXAMS,
    MAIN_BUTTON_SCHEDULE,
    MAIN_BUTTON_SESSION,
    build_main_keyboard,
)

router = Router()
logger = logging.getLogger(__name__)


def _format_user_info(message: Message) -> str:
    """Форматирует информацию о пользователе для логов."""
    chat_id = message.chat.id
    username = message.from_user.username if message.from_user else None
    if username:
        return f"chat_id={chat_id} @{username}"
    return f"chat_id={chat_id}"


def _format_exam_entry(entry: ExamEntry) -> str:
    """Форматирует одну запись экзамена/зачета."""
    lines = []
    
    # Дата и день недели
    date_line = f"📅 {entry.date}"
    if entry.day_of_week:
        date_line += f" ({entry.day_of_week})"
    lines.append(date_line)
    
    # Содержимое
    content_lines = entry.content.split("\n")
    for line in content_lines:
        line = line.strip()
        if line:
            lines.append(f"  {line}")
    
    return "\n".join(lines)


def _format_exam_schedule(entries: list[ExamEntry], title: str) -> str:
    """Форматирует расписание экзаменов/зачетов."""
    if not entries:
        return f"Расписание {title} не найдено для вашей группы."
    
    lines = [f"📋 <b>{title}</b>", ""]
    
    # Группируем по датам
    from collections import defaultdict
    import re
    
    by_date = defaultdict(list)
    
    for entry in entries:
        by_date[entry.date].append(entry)
    
    # Сортируем по дате правильно (год, месяц, день)
    def date_sort_key(date_str):
        # Извлекаем дату в формате dd.mm.yyyy
        date_match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
        if date_match:
            day, month, year = date_match.groups()
            return (year, month, day)  # Сортировка по (год, месяц, день)
        # Если не удалось распарсить, возвращаем строку как есть
        return ("9999", "99", date_str)
    
    sorted_dates = sorted(by_date.keys(), key=date_sort_key)
    
    for date in sorted_dates:
        date_entries = by_date[date]
        for entry in date_entries:
            lines.append(_format_exam_entry(entry))
            lines.append("")  # Пустая строка между записями
    
    # Убираем последнюю пустую строку
    if lines and not lines[-1]:
        lines.pop()
    
    return "\n".join(lines)


@router.message(F.text == MAIN_BUTTON_CREDITS)
async def handle_credits_button(message: Message) -> None:
    """Обработчик кнопки "Зачеты"."""
    group_name = storage.get_user_group(message.chat.id)
    if not group_name:
        await message.answer(
            "Сначала укажи группу через /start",
            reply_markup=build_main_keyboard(),
        )
        logger.warning(
            "Credits button without group %s", _format_user_info(message)
        )
        return
    
    entries = await exams_storage.get_credits_for_group(group_name)
    schedule_text = _format_exam_schedule(entries, "Расписание зачетов")
    
    await message.answer(
        schedule_text,
        reply_markup=build_main_keyboard(),
    )
    logger.info(
        "Credits schedule sent %s group=%s entries=%d",
        _format_user_info(message),
        group_name,
        len(entries),
    )


@router.message(F.text == MAIN_BUTTON_EXAMS)
async def handle_exams_button(message: Message) -> None:
    """Обработчик кнопки "Экзамены"."""
    group_name = storage.get_user_group(message.chat.id)
    if not group_name:
        await message.answer(
            "Сначала укажи группу через /start",
            reply_markup=build_main_keyboard(),
        )
        logger.warning(
            "Exams button without group %s", _format_user_info(message)
        )
        return
    
    entries = await exams_storage.get_exams_for_group(group_name)
    schedule_text = _format_exam_schedule(entries, "Расписание экзаменов")
    
    await message.answer(
        schedule_text,
        reply_markup=build_main_keyboard(),
    )
    logger.info(
        "Exams schedule sent %s group=%s entries=%d",
        _format_user_info(message),
        group_name,
        len(entries),
    )

