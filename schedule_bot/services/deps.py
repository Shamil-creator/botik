from __future__ import annotations

from pathlib import Path

from schedule_bot.services.cache import ScheduleCache
from schedule_bot.services.fetcher import ScheduleFetcher
from schedule_bot.services.storage import Storage

BASE_DIR = Path(__file__).resolve().parent.parent

cache = ScheduleCache(ttl_minutes=120, storage_dir=BASE_DIR / "schedule_data")
fetcher = ScheduleFetcher()
storage = Storage(BASE_DIR / "schedule.db")

for chat_id in storage.iter_chat_ids():
    cache.add_watcher(chat_id)
