from __future__ import annotations

import logging
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
logger = logging.getLogger(__name__)


def _format_user_info(message: Message) -> str:
    """Форматирует информацию о пользователе для логов: chat_id и username (если есть)."""
    chat_id = message.chat.id
    username = message.from_user.username if message.from_user else None
    if username:
        return f"chat_id={chat_id} @{username}"
    return f"chat_id={chat_id}"


@router.message(Command("schedule"))
async def handle_schedule(message: Message, command: CommandObject) -> None:
    args = (command.args or "").strip()
    if args:
        tokens = args.split()
        group_query = tokens[0]
        day_query = " ".join(tokens[1:]) if len(tokens) > 1 else None
        logger.info(
            "Schedule request with args %s group_query=%s day_query=%s",
            _format_user_info(message),
            group_query,
            day_query,
        )
    else:
        stored_group = storage.get_user_group(message.chat.id)
        if not stored_group:
            await message.answer(
                "Укажи группу: /schedule <группа> [день] или сначала "
                "настрой группу через /start"
            )
            logger.info("Schedule request without group %s", _format_user_info(message))
            return
        group_query = stored_group
        day_query = None
        logger.info(
            "Schedule request using stored group %s group=%s",
            _format_user_info(message),
            group_query,
        )

    day = _normalize_day(day_query) if day_query else None
    if day_query and not day:
        await message.answer(
            "Не понял день недели. Используй, например: понедельник, вт, "
            "ср, пятница."
        )
        logger.warning(
            "Failed to parse day %s input=%s", _format_user_info(message), day_query
        )
        return

    cache.add_watcher(message.chat.id)
    logger.debug("Watcher added %s", _format_user_info(message))

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
    
    # Шаг 0: Проверяем кэш расположения группы (быстрая проверка)
    cached_location = cache.get_group_location(group_query)
    if cached_location:
        file_url, sheet_name, actual_group_name = cached_location
        # Находим файл в списке файлов
        file_info = next((f for f in files if f.url == file_url), None)
        if file_info:
            logger.debug(
                "Using cached group location group=%s file=%s sheet=%s",
                group_query,
                file_info.title,
                sheet_name,
            )
            # Загружаем файл и извлекаем расписание
            content = await _get_schedule_file_bytes(file_info)
            if content is not None:
                try:
                    lessons = await extract_group_schedule(
                        content,
                        sheet_name=sheet_name,
                        group_name=actual_group_name,
                        day_filter=day,
                        current_week=current_week,
                    )
                    if not lessons:
                        lessons = await extract_group_schedule(
                            content,
                            sheet_name=sheet_name,
                            group_name=actual_group_name,
                            day_filter=day,
                            current_week=None,
                        )
                    if lessons:
                        formatted = format_lessons(lessons)
                        logger.info(
                            "Schedule found using cache group=%s sheet=%s file=%s",
                            actual_group_name,
                            sheet_name,
                            file_info.title,
                        )
                        return formatted, file_info, sheet_name, actual_group_name
                except Exception:
                    logger.exception(
                        "Failed to extract schedule from cached location group=%s file=%s sheet=%s",
                        group_query,
                        file_info.title,
                        sheet_name,
                    )
                    # Продолжаем поиск в других файлах, если кэш не сработал
    
    # Шаг 1: Ищем группу во всех файлах (если кэш не сработал)
    for file_info in files:
        logger.debug(
            "Searching schedule in file title=%s url=%s target=%s",
            file_info.title,
            file_info.url,
            target,
        )
        
        # Шаг 1: Получаем или загружаем метаданные (листы -> группы)
        metadata = cache.get_file_metadata(file_info.url)
        content = None
        
        if metadata is None:
            # Метаданных нет, загружаем файл и извлекаем их
            content = await _get_schedule_file_bytes(file_info)
            if content is None:
                logger.warning("Failed to get content for file %s", file_info.url)
                continue
            
            try:
                sheets = await list_sheets(content)
            except Exception:
                logger.exception(
                    "Failed to list sheets for file title=%s url=%s",
                    file_info.title,
                    file_info.url,
                )
                continue
            
            # Извлекаем группы для каждого листа и кэшируем метаданные
            metadata = {}
            for sheet in sheets:
                try:
                    groups = await list_groups(content, sheet)
                    metadata[sheet] = groups
                except Exception:
                    logger.exception(
                        "Failed to list groups for sheet=%s file=%s",
                        sheet,
                        file_info.url,
                    )
                    continue
            
            if metadata:
                cache.set_file_metadata(file_info.url, metadata)
        
        # Шаг 2: Ищем группу в метаданных
        target_sheet = None
        target_group_name = None
        
        for sheet, groups in metadata.items():
            group_name = _match_group(groups, target)
            if group_name:
                target_sheet = sheet
                target_group_name = group_name
                break
        
        if not target_group_name:
            logger.debug("Group not found in file %s", file_info.url)
            continue
        
        # Шаг 3: Группа найдена, загружаем файл если еще не загружен
        if content is None:
            content = await _get_schedule_file_bytes(file_info)
            if content is None:
                logger.warning("Failed to get content for file %s", file_info.url)
                continue
        
        # Шаг 4: Извлекаем расписание
        lessons = await extract_group_schedule(
            content,
            sheet_name=target_sheet,
            group_name=target_group_name,
            day_filter=day,
            current_week=current_week,
        )
        if not lessons:
            lessons = await extract_group_schedule(
                content,
                sheet_name=target_sheet,
                group_name=target_group_name,
                day_filter=day,
                current_week=None,
            )
        if not lessons:
            continue
        
        formatted = format_lessons(lessons)
        logger.info(
            "Schedule found group=%s sheet=%s file=%s",
            target_group_name,
            target_sheet,
            file_info.title,
        )
        # Сохраняем расположение группы в кэш для быстрого доступа в будущем
        cache.set_group_location(
            group_query,
            file_info.url,
            target_sheet,
            target_group_name,
        )
        return formatted, file_info, target_sheet, target_group_name
    
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
        logger.warning(
            "Schedule not found %s group=%s day=%s",
            _format_user_info(message),
            group_query,
            day,
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
    logger.info(
        "Schedule sent %s group=%s day=%s",
        _format_user_info(message),
        group_name,
        day,
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
    # Проверяем кэш в памяти
    cached = cache.get_file_content(file_info.url)
    if cached is not None:
        logger.info(
            "✓ Using in-memory cached file title=%s size=%d bytes",
            file_info.title,
            len(cached),
        )
        return cached

    # Проверяем кэш на диске (асинхронно)
    stored = await cache.load_file_from_disk_async(file_info.url)
    if stored is not None:
        # Восстанавливаем файл в кэш памяти для быстрого доступа
        cache.set_file_content(file_info.url, stored, persist=False)
        logger.info(
            "✓ Loaded file from disk cache title=%s size=%d bytes",
            file_info.title,
            len(stored),
        )
        return stored

    # Файл не найден в кэшах, скачиваем с сайта
    logger.warning(
        "⚠ File not in cache, downloading from website title=%s url=%s",
        file_info.title,
        file_info.url,
    )
    try:
        raw = await fetcher.download(file_info)
    except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
        # Временные ошибки подключения
        logger.warning(
            "Failed to download file %s: %s (using cached data if available)",
            file_info.url,
            type(e).__name__,
        )
        return None
    except httpx.HTTPError as e:
        # Другие HTTP ошибки
        logger.error(
            "HTTP error while downloading file %s: %s",
            file_info.url,
            e,
        )
        return None
    except Exception as e:
        # Неожиданные ошибки
        logger.exception(
            "Unexpected error while downloading file %s",
            file_info.url,
        )
        return None

    # Обрабатываем файл асинхронно
    try:
        processed = await process_workbook(raw)
        await cache.set_file_content_async(file_info.url, processed)
        logger.info(
            "✓ Downloaded, processed and cached file title=%s size=%d bytes",
            file_info.title,
            len(processed),
        )
        return processed
    except Exception as e:
        logger.exception(
            "Failed to process file %s",
            file_info.url,
        )
        return None


async def _ensure_schedule_files(
    message: Message,
    preview_only: bool,
) -> Optional[list[ScheduleFile]]:
    # Сначала проверяем актуальный кэш
    schedule_files = cache.get_file_list()
    if schedule_files is not None:
        logger.debug(
            "Using cached file list (count=%d files)",
            len(schedule_files),
        )
        return schedule_files
    
    # Кэш истек, но проверяем устаревший кэш перед запросом на сайт
    # Это позволяет избежать лишних запросов на сайт при каждом обращении
    stale_files = cache.get_file_list_stale()
    if stale_files:
        logger.info(
            "File list cache expired, using stale cache to avoid website request (count=%d files). "
            "Background monitor will update it later.",
            len(stale_files),
        )
        # Обновляем время кэша, чтобы использовать устаревший кэш
        # Это позволит избежать запроса на сайт при каждом обращении
        cache.update_file_list(stale_files)
        return stale_files
    
    # Нет даже устаревшего кэша, запрашиваем с сайта
    logger.warning(
        "⚠ File list not in cache, fetching from website (this may cause delay)"
    )
    try:
        schedule_files = await fetcher.list_schedule_files()
        cache.update_file_list(schedule_files)
        logger.info(
            "✓ Fetched file list from website (count=%d files)",
            len(schedule_files),
        )
    except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
        # Временные ошибки подключения
        if not preview_only:
            await message.answer(
                "⚠️ Не удалось подключиться к серверу. "
                "Попробуйте позже или используйте кэшированные данные."
            )
        logger.warning(
            "Failed to fetch schedule file list: %s (will use cached data if available)",
            type(e).__name__,
        )
        return None
    except httpx.HTTPError as e:
        # Другие HTTP ошибки
        if not preview_only:
            await message.answer(
                f"⚠️ Ошибка при получении списка файлов: {e}. "
                "Попробуйте позже."
            )
        logger.error(
            "HTTP error while fetching schedule file list: %s",
            e,
            exc_info=True,
        )
        return None
    except Exception as e:
        # Неожиданные ошибки
        if not preview_only:
            await message.answer(
                "⚠️ Произошла неожиданная ошибка. "
                "Попробуйте позже."
            )
        logger.exception(
            "Unexpected error while fetching schedule file list: %s",
            type(e).__name__,
        )
        return None

    if not schedule_files:
        if not preview_only:
            await message.answer("На сайте пока нет файлов расписания.")
        logger.warning("Empty schedule file list obtained")
        return None

    return schedule_files
