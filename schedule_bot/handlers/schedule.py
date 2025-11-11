from __future__ import annotations

import re
from typing import Optional

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
import httpx

from schedule_bot.services.deps import cache, fetcher, storage
from schedule_bot.services.fetcher import ScheduleFile
from schedule_bot.services.formatter import DAY_ORDER, format_lessons
from schedule_bot.services.parser import (
    extract_group_schedule,
    list_groups,
    list_sheets,
    process_workbook,
)
from schedule_bot.services import weeks  # noqa: F401
from schedule_bot.services.ui import build_schedule_keyboard


router = Router()


@router.message(Command("schedule"))
async def handle_schedule(message: Message, command: CommandObject) -> None:
    args = (command.args or "").strip()
    if args:
        tokens = args.split()
        group_query = tokens[0]
        day_query = " ".join(tokens[1:]) if len(tokens) > 1 else None
    else:
        stored_group = storage.get_user_group(message.chat.id)
        if not stored_group:
            await message.answer(
                "Укажи группу: /schedule <группа> [день] или сначала "
                "настрой группу через /start"
            )
            return
        group_query = stored_group
        day_query = None

    day = _normalize_day(day_query) if day_query else None
    if day_query and not day:
        await message.answer(
            "Не понял день недели. Используй, например: понедельник, вт, "
            "ср, пятница."
        )
        return

    cache.add_watcher(message.chat.id)

    await send_schedule_for_group(
        message,
        group_query,
        day,
        reply_markup=build_schedule_keyboard(),
    )


async def _find_group_schedule(
    files: list[ScheduleFile],
    group_query: str,
    day: Optional[str],
    current_week: Optional[int],
) -> Optional[tuple[str, ScheduleFile, str, str]]:
    target = _normalize_group(group_query)
    for file_info in files:
        content = await _get_schedule_file_bytes(file_info)
        if content is None:
            continue
        try:
            sheets = list_sheets(content)
        except Exception:
            continue
        for sheet in sheets:
            try:
                groups = list_groups(content, sheet)
            except Exception:
                continue
            group_name = _match_group(groups, target)
            if not group_name:
                continue
            lessons = extract_group_schedule(
                content,
                sheet_name=sheet,
                group_name=group_name,
                day_filter=day,
                current_week=current_week,
            )
            if not lessons:
                lessons = extract_group_schedule(
                    content,
                    sheet_name=sheet,
                    group_name=group_name,
                    day_filter=day,
                    current_week=None,
                )
            if not lessons:
                continue
            formatted = format_lessons(lessons)
            return formatted, file_info, sheet, group_name
    return None


async def send_schedule_for_group(
    message: Message,
    group_query: str,
    day: Optional[str],
    preview_only: bool = False,
    reply_markup=None,
    suppress_not_found_message: bool = False,
    current_week_info=None,
):
    schedule_files = await _ensure_schedule_files(message, preview_only)
    if not schedule_files:
        return None

    if current_week_info is None:
        current_week_info = weeks.get_current_week()

    current_week_number = (
        current_week_info[0] if current_week_info else None
    )

    match = await _find_group_schedule(
        schedule_files,
        group_query,
        day,
        current_week=current_week_number,
    )
    if not match:
        if not preview_only and not suppress_not_found_message:
            await message.answer(
                f"Не нашёл расписание для группы '{group_query}'. "
                "Проверь код или попробуй позже."
            )
        return None

    if preview_only:
        return match

    schedule_text, file_info, _, group_name = match
    if day:
        schedule_text = _strip_day_heading(schedule_text)

    header_lines = [
        f"📄 {_format_title(file_info.title)}",
        f"👥 Группа: {group_name}",
    ]
    if day:
        header_lines.append(f"🗓 День: {day}")
    if current_week_info:
        header_lines.append(f"📆 {weeks.format_week_info(current_week_info)}")
    header_lines.append("")
    await message.answer(
        "\n".join(header_lines) + schedule_text,
        reply_markup=reply_markup,
    )
    return match


def _normalize_group(name: str) -> str:
    return re.sub(r"\s+", "", name).upper()


def _match_group(groups: list[str], target: str) -> Optional[str]:
    for group in groups:
        if _normalize_group(group) == target:
            return group
    return None


def _normalize_day(day: str) -> Optional[str]:
    if not day:
        return None
    day_lower = day.strip().lower()
    if not day_lower:
        return None
    for option in DAY_ORDER:
        if option.lower().startswith(day_lower):
            return option
    return None


_TITLE_DATE_PATTERN = re.compile(r"от\s+(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)


def _format_title(title: str) -> str:
    match = _TITLE_DATE_PATTERN.search(title)
    if match:
        return f"Расписание (от {match.group(1)})"
    return title


def _strip_day_heading(schedule_text: str) -> str:
    """Удаляет строку вида "📅 Среда" из начала расписания"""
    lines = schedule_text.splitlines()
    if lines and lines[0].startswith("📅 "):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
        # Добавляем пустую строку для визуального отступа
        lines.insert(0, "")
    return "\n".join(lines)  # EOF



async def _get_schedule_file_bytes(file_info: ScheduleFile) -> Optional[bytes]:
    cached = cache.get_file_content(file_info.url)
    if cached is not None:
        return cached

    stored = cache.load_file_from_disk(file_info.url)
    if stored is not None:
        cache.set_file_content(file_info.url, stored, persist=False)
        return stored

    try:
        raw = await fetcher.download(file_info)
    except httpx.HTTPError:
        return None

    processed = process_workbook(raw)
    cache.set_file_content(file_info.url, processed)
    return processed


async def _ensure_schedule_files(
    message: Message,
    preview_only: bool,
) -> Optional[list[ScheduleFile]]:
    schedule_files = cache.get_file_list()
    if schedule_files is None:
        try:
            schedule_files = await fetcher.list_schedule_files()
            cache.update_file_list(schedule_files)
        except httpx.HTTPError as error:
            await message.answer(f"Не удалось получить список файлов: {error}")
            return None

    if not schedule_files:
        if not preview_only:
            await message.answer("На сайте пока нет файлов расписания.")
        return None

    return schedule_files
