from __future__ import annotations

import asyncio
import re
from typing import Iterable

import httpx
from aiogram import Bot

from schedule_bot.services.deps import cache, fetcher, storage
from schedule_bot.services.parser import process_workbook
from schedule_bot.services.fetcher import ScheduleFile

_TITLE_DATE_PATTERN = re.compile(r"от\s+(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)


async def monitor_updates(bot: Bot, interval_minutes: int = 60) -> None:
    """Отслеживает обновления расписания."""
    interval_seconds = max(60, int(interval_minutes * 60))

    while True:
        try:
            files = await fetcher.list_schedule_files()
        except httpx.HTTPError:
            pass
        else:
            previous_signature = cache.get_file_list_signature()
            changed = cache.update_file_list(files)
            await _preload_files(files, only_missing=not changed)
            if changed and previous_signature is not None:
                await _notify_about_update(bot, files)
        await asyncio.sleep(interval_seconds)


async def _preload_files(
    files: Iterable[ScheduleFile], *, only_missing: bool
) -> None:
    for file_info in files:
        stored = cache.load_file_from_disk(file_info.url)
        if stored is not None:
            if not only_missing:
                cache.set_file_content(file_info.url, stored, persist=False)
            continue
        try:
            raw = await fetcher.download(file_info)
        except httpx.HTTPError:
            continue
        processed = process_workbook(raw)
        cache.set_file_content(file_info.url, processed)


async def _notify_about_update(bot: Bot, files: Iterable) -> None:
    watchers = cache.get_watchers()
    if not watchers:
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


def _format_title(title: str) -> str:
    match = _TITLE_DATE_PATTERN.search(title)
    if match:
        return f"от {match.group(1)}"
    return title
