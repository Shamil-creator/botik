from __future__ import annotations

from pathlib import Path

from schedule_bot.services.cache import ScheduleCache
from schedule_bot.services.fetcher import ScheduleFetcher
from schedule_bot.services.storage import Storage

BASE_DIR = Path(__file__).resolve().parent.parent

# Настройки кэша оптимизированы для сервера с ограниченной памятью
# TTL для содержимого файлов: 60 минут (для экономии памяти)
# TTL для списка файлов: 240 минут (4 часа) - список файлов меняется реже
# Максимальный размер кэша в памяти: 50 МБ (можно уменьшить до 20-30 МБ при необходимости)
cache = ScheduleCache(
    ttl_minutes=60,  # TTL для содержимого файлов
    storage_dir=BASE_DIR / "schedule_data",
    max_cache_size_mb=50.0,
    file_list_ttl_minutes=240,  # TTL для списка файлов (4 часа) - меньше запросов на сайт
)
fetcher = ScheduleFetcher()
storage = Storage(BASE_DIR / "schedule.db")

for chat_id in storage.iter_chat_ids():
    cache.add_watcher(chat_id)
