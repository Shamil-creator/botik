from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from schedule_bot.services.fetcher import ScheduleFile

logger = logging.getLogger(__name__)


class ScheduleCache:
    """Простой кэш для расписаний в памяти."""

    def __init__(self, ttl_minutes: int = 30, storage_dir: Path | None = None):
        self._ttl = timedelta(minutes=ttl_minutes)
        self._file_list_cache: Optional[Tuple[datetime, list[ScheduleFile]]] = None
        self._file_list_signature: Optional[Tuple[Tuple[str, str], ...]] = None
        self._file_content_cache: Dict[str, Tuple[datetime, bytes]] = {}
        self._watchers: Set[int] = set()
        base_dir = storage_dir or Path(__file__).resolve().parents[2] / 'schedule_data'
        base_dir.mkdir(parents=True, exist_ok=True)
        self._storage_dir = base_dir

    # ----- Работа со списком файлов -----

    def get_file_list(self) -> Optional[list[ScheduleFile]]:
        """Возвращает закэшированный список файлов, если он ещё актуален."""
        if self._file_list_cache is None:
            return None
        cached_time, files = self._file_list_cache
        if datetime.now() - cached_time > self._ttl:
            return None
        return files

    def update_file_list(self, files: list[ScheduleFile]) -> bool:
        """Обновляет кэш и возвращает True, если список изменился."""
        signature = tuple((file.url, file.title) for file in files)
        changed = signature != self._file_list_signature
        self._file_list_signature = signature
        self._file_list_cache = (datetime.now(), files)
        if changed:
            self._prune_storage({file.url for file in files})
            logger.info("File list updated count=%d", len(files))
        return changed

    def get_file_list_signature(self) -> Optional[Tuple[Tuple[str, str], ...]]:
        return self._file_list_signature

    # ----- Работа с файлами на диске -----

    def _hash_url(self, file_url: str) -> str:
        from hashlib import sha256

        return sha256(file_url.encode("utf-8")).hexdigest()

    def _file_path(self, file_url: str) -> Path:
        return self._storage_dir / f"{self._hash_url(file_url)}.xlsx"

    def load_file_from_disk(self, file_url: str) -> Optional[bytes]:
        path = self._file_path(file_url)
        if not path.exists():
            logger.debug("Cache miss on disk for %s", file_url)
            return None
        try:
            return path.read_bytes()
        except OSError:
            logger.exception("Failed to read cached file %s", path)
            return None

    def _persist_file(self, file_url: str, content: bytes) -> None:
        path = self._file_path(file_url)
        try:
            path.write_bytes(content)
        except OSError:
            logger.exception("Failed to persist cache file %s", path)

    def _prune_storage(self, active_urls: Set[str]) -> None:
        active_hashes = {self._hash_url(url) for url in active_urls}
        for file_path in self._storage_dir.glob("*.xlsx"):
            if file_path.stem not in active_hashes:
                try:
                    file_path.unlink()
                except OSError:
                    pass
        for url in list(self._file_content_cache.keys()):
            if url not in active_urls:
                self._file_content_cache.pop(url, None)

    # ----- Работа с содержимым файлов -----

    def get_file_content(self, file_url: str) -> Optional[bytes]:
        if file_url not in self._file_content_cache:
            return None
        cached_time, content = self._file_content_cache[file_url]
        if datetime.now() - cached_time > self._ttl:
            del self._file_content_cache[file_url]
            return None
        return content

    def set_file_content(self, file_url: str, content: bytes, *, persist: bool = True) -> None:
        self._file_content_cache[file_url] = (datetime.now(), content)
        if persist:
            self._persist_file(file_url, content)
        logger.debug(
            "In-memory cache updated for %s persist=%s", file_url, persist
        )

    # ----- Наблюдатели -----

    def add_watcher(self, chat_id: int) -> None:
        self._watchers.add(chat_id)

    def get_watchers(self) -> Set[int]:
        return set(self._watchers)

    def remove_watcher(self, chat_id: int) -> None:
        self._watchers.discard(chat_id)

    # ----- Служебные методы -----

    def clear(self) -> None:
        self._file_list_cache = None
        self._file_list_signature = None
        self._file_content_cache.clear()
        self._watchers.clear()

