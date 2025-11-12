from __future__ import annotations

import asyncio
import logging
import re
from typing import Iterable

import httpx
from aiogram import Bot

from schedule_bot.services.deps import cache, fetcher, storage
from schedule_bot.services.parser import process_workbook
from schedule_bot.services.fetcher import ScheduleFile

_TITLE_DATE_PATTERN = re.compile(r"от\s+(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)

logger = logging.getLogger(__name__)


async def monitor_updates(bot: Bot, interval_minutes: int = 60) -> None:
    """Отслеживает обновления расписания."""
    interval_seconds = max(60, int(interval_minutes * 60))
    consecutive_errors = 0
    max_consecutive_errors = 5

    while True:
        try:
            files = await fetcher.list_schedule_files()
            consecutive_errors = 0  # Сбрасываем счетчик ошибок при успехе
            
            # Успешно получен список файлов
            previous_signature = cache.get_file_list_signature()
            changed = cache.update_file_list(files)
            await _preload_files(files, only_missing=not changed)
            if changed and previous_signature is not None:
                await _notify_about_update(bot, files)
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
            # Временные ошибки подключения - логируем без полного traceback
            consecutive_errors += 1
            logger.warning(
                "Failed to fetch schedule file list: %s (will retry later, error count: %d/%d)",
                type(e).__name__,
                consecutive_errors,
                max_consecutive_errors,
            )
            # Если много последовательных ошибок, увеличиваем интервал ожидания
            if consecutive_errors >= max_consecutive_errors:
                extended_wait = min(interval_seconds * 2, 600)  # Максимум 10 минут
                logger.warning(
                    "Too many consecutive errors, extending wait time to %d seconds",
                    extended_wait,
                )
                await asyncio.sleep(extended_wait)
                consecutive_errors = 0  # Сбрасываем после увеличенного ожидания
                continue
        except httpx.HTTPError as e:
            # Другие HTTP ошибки - логируем с контекстом
            consecutive_errors += 1
            logger.error(
                "HTTP error while fetching schedule file list: %s (error count: %d)",
                e,
                consecutive_errors,
                exc_info=True,
            )
        except Exception as e:
            # Неожиданные ошибки - логируем с полным traceback
            consecutive_errors += 1
            logger.exception(
                "Unexpected error while fetching schedule file list: %s (error count: %d)",
                type(e).__name__,
                consecutive_errors,
            )
        
        logger.debug("Monitor sleeping for %s seconds", interval_seconds)
        await asyncio.sleep(interval_seconds)


async def _preload_files(
    files: Iterable[ScheduleFile], *, only_missing: bool
) -> None:
    for file_info in files:
        logger.debug(
            "Preloading schedule file title=%s url=%s only_missing=%s",
            file_info.title,
            file_info.url,
            only_missing,
        )
        stored = await cache.load_file_from_disk_async(file_info.url)
        if stored is not None:
            if not only_missing:
                # Используем синхронную версию, так как persist=False и это просто запись в память
                cache.set_file_content(file_info.url, stored, persist=False)
            continue
        try:
            raw = await fetcher.download(file_info)
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
            # Временные ошибки подключения - пропускаем файл
            logger.warning(
                "Failed to download schedule file title=%s url=%s: %s",
                file_info.title,
                file_info.url,
                type(e).__name__,
            )
            continue
        except httpx.HTTPError as e:
            # Другие HTTP ошибки
            logger.error(
                "HTTP error while downloading schedule file title=%s url=%s: %s",
                file_info.title,
                file_info.url,
                e,
            )
            continue
        except Exception as e:
            # Неожиданные ошибки
            logger.exception(
                "Unexpected error while downloading schedule file title=%s url=%s",
                file_info.title,
                file_info.url,
            )
            continue
        try:
            processed = await process_workbook(raw)
            await cache.set_file_content_async(file_info.url, processed)
            logger.info("Schedule file cached title=%s", file_info.title)
        except Exception as e:
            logger.exception(
                "Failed to process schedule file title=%s url=%s",
                file_info.title,
                file_info.url,
            )
            continue


async def _notify_about_update(bot: Bot, files: Iterable) -> None:
    watchers = cache.get_watchers()
    if not watchers:
        logger.info("No watchers to notify about schedule update")
        return

    title = next(iter(files), None)
    if title is None:
        message = "📢 Расписание было обновлено."
    else:
        message = f"📢 Расписание обновлено: {_format_title(title.title)}"

    for chat_id in watchers:
        try:
            await bot.send_message(chat_id, message)
        except Exception:
            cache.remove_watcher(chat_id)
            storage.remove_user(chat_id)
            logger.exception("Failed to notify watcher chat_id=%s, removed from list", chat_id)


def _format_title(title: str) -> str:
    match = _TITLE_DATE_PATTERN.search(title)
    if match:
        return f"от {match.group(1)}"
    return title
