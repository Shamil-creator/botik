from __future__ import annotations

from pathlib import Path

from schedule_bot.services.cache import ScheduleCache
from schedule_bot.services.exams_storage import ExamsStorage
from schedule_bot.services.fetcher import ScheduleFetcher
from schedule_bot.services.storage import Storage

BASE_DIR = Path(__file__).resolve().parent.parent

# Настройки кэша оптимизированы для сервера с 1GB RAM
# Стратегия: минимум памяти, максимум работы с диском
# TTL для содержимого файлов: 30 минут (быстрая эвикция)
# TTL для списка файлов: 240 минут (4 часа)
# Максимальный размер кэша в памяти: 20 МБ (для экономии RAM)
cache = ScheduleCache(
    ttl_minutes=30,  # TTL для содержимого файлов (снижено с 60)
    storage_dir=BASE_DIR / "schedule_data",
    max_cache_size_mb=20.0,  # Снижено с 50 МБ до 20 МБ
    file_list_ttl_minutes=240,  # TTL для списка файлов (4 часа)
)
fetcher = ScheduleFetcher()
storage = Storage(BASE_DIR / "schedule.db")
# Оптимизированный кэш: максимум 20 групп в памяти, TTL 2 часа
exams_storage = ExamsStorage(
    BASE_DIR.parent / "зачеты и сессия",
    max_cache_entries=20,
    ttl_minutes=120,
)

for chat_id in storage.iter_chat_ids():
    cache.add_watcher(chat_id)
