from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiofiles

from schedule_bot.services.fetcher import ScheduleFile

logger = logging.getLogger(__name__)


class ScheduleCache:
    """Простой кэш для расписаний в памяти."""

    def __init__(
        self,
        ttl_minutes: int = 30,
        storage_dir: Path | None = None,
        max_cache_size_mb: float = 50.0,
    ):
        self._ttl = timedelta(minutes=ttl_minutes)
        self._file_list_cache: Optional[Tuple[datetime, list[ScheduleFile]]] = None
        self._file_list_signature: Optional[Tuple[Tuple[str, str], ...]] = None
        self._file_content_cache: Dict[str, Tuple[datetime, bytes]] = {}
        self._watchers: Set[int] = set()
        base_dir = storage_dir or Path(__file__).resolve().parents[2] / 'schedule_data'
        base_dir.mkdir(parents=True, exist_ok=True)
        self._storage_dir = base_dir
        # Кэш метаданных (листы и группы для каждого файла)
        self._file_metadata_cache: Dict[str, Tuple[datetime, Dict[str, List[str]]]] = {}
        self._metadata_ttl = timedelta(minutes=ttl_minutes * 2)  # Метаданные кэшируются дольше
        # Ограничение на размер кэша в памяти (в байтах)
        self._max_cache_size = int(max_cache_size_mb * 1024 * 1024)
        self._current_cache_size = 0
        # Кэш для связи группы с файлом и листом (group_name -> (file_url, sheet_name, group_name))
        # Это значительно ускоряет поиск группы при повторных запросах
        self._group_location_cache: Dict[str, Tuple[datetime, str, str, str]] = {}
        self._group_location_ttl = timedelta(minutes=ttl_minutes * 4)  # Кэш расположения группы хранится дольше

    # ----- Работа со списком файлов -----

    def get_file_list(self) -> Optional[list[ScheduleFile]]:
        """Возвращает закэшированный список файлов, если он ещё актуален."""
        if self._file_list_cache is None:
            return None
        cached_time, files = self._file_list_cache
        if datetime.now() - cached_time > self._ttl:
            return None
        return files

    def get_file_list_stale(self) -> Optional[list[ScheduleFile]]:
        """Возвращает закэшированный список файлов, даже если он устарел.
        Используется при ошибках подключения для работы с кэшированными данными."""
        if self._file_list_cache is None:
            return None
        _, files = self._file_list_cache
        return files

    def update_file_list(self, files: list[ScheduleFile]) -> bool:
        """Обновляет кэш и возвращает True, если список изменился."""
        signature = tuple((file.url, file.title) for file in files)
        changed = signature != self._file_list_signature
        self._file_list_signature = signature
        self._file_list_cache = (datetime.now(), files)
        if changed:
            self._prune_storage({file.url for file in files})
            # Очищаем кэш расположения групп, если список файлов изменился
            # (старые группы могут быть в удаленных файлах)
            active_urls = {file.url for file in files}
            groups_to_remove = []
            for group_name, (_, file_url, _, _) in self._group_location_cache.items():
                if file_url not in active_urls:
                    groups_to_remove.append(group_name)
            for group_name in groups_to_remove:
                del self._group_location_cache[group_name]
            if groups_to_remove:
                logger.debug(
                    "Cleared group location cache for %d groups (files removed)",
                    len(groups_to_remove),
                )
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
        """Синхронная версия для обратной совместимости."""
        path = self._file_path(file_url)
        if not path.exists():
            logger.debug("Cache miss on disk for %s", file_url)
            return None
        try:
            return path.read_bytes()
        except OSError:
            logger.exception("Failed to read cached file %s", path)
            return None

    async def load_file_from_disk_async(self, file_url: str) -> Optional[bytes]:
        """Асинхронная версия для чтения файла с диска."""
        path = self._file_path(file_url)
        if not path.exists():
            logger.debug("Cache miss on disk for %s", file_url)
            return None
        try:
            async with aiofiles.open(path, 'rb') as f:
                return await f.read()
        except OSError:
            logger.exception("Failed to read cached file %s", path)
            return None

    def _persist_file(self, file_url: str, content: bytes) -> None:
        """Синхронная версия для обратной совместимости."""
        path = self._file_path(file_url)
        try:
            path.write_bytes(content)
        except OSError:
            logger.exception("Failed to persist cache file %s", path)

    async def _persist_file_async(self, file_url: str, content: bytes) -> None:
        """Асинхронная версия для записи файла на диск."""
        path = self._file_path(file_url)
        try:
            async with aiofiles.open(path, 'wb') as f:
                await f.write(content)
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
        # Очищаем кэш содержимого файлов
        for url in list(self._file_content_cache.keys()):
            if url not in active_urls:
                _, content = self._file_content_cache.pop(url, (None, b""))
                if content:
                    self._current_cache_size -= len(content)
        # Очищаем кэш метаданных
        for url in list(self._file_metadata_cache.keys()):
            if url not in active_urls:
                self._file_metadata_cache.pop(url, None)

    # ----- Работа с содержимым файлов -----

    def get_file_content(self, file_url: str) -> Optional[bytes]:
        if file_url not in self._file_content_cache:
            return None
        cached_time, content = self._file_content_cache[file_url]
        if datetime.now() - cached_time > self._ttl:
            del self._file_content_cache[file_url]
            self._current_cache_size -= len(content)
            return None
        return content

    def _evict_oldest_if_needed(self) -> None:
        """Удаляет старые файлы из кэша, если превышен лимит размера."""
        if self._current_cache_size <= self._max_cache_size:
            return
        
        # Сортируем файлы по времени кэширования (старые первыми)
        sorted_items = sorted(
            self._file_content_cache.items(),
            key=lambda x: x[1][0]  # Сортировка по времени (datetime)
        )
        
        # Удаляем самые старые файлы, пока не уложимся в лимит
        while self._current_cache_size > self._max_cache_size and sorted_items:
            url, (_, content) = sorted_items.pop(0)
            del self._file_content_cache[url]
            self._current_cache_size -= len(content)
            logger.debug(
                "Evicted file from cache url=%s size=%d current_size=%d max_size=%d",
                url,
                len(content),
                self._current_cache_size,
                self._max_cache_size,
            )

    def set_file_content(self, file_url: str, content: bytes, *, persist: bool = True) -> None:
        """Синхронная версия для обратной совместимости."""
        # Удаляем старый файл из кэша, если он там был
        if file_url in self._file_content_cache:
            old_content = self._file_content_cache[file_url][1]
            self._current_cache_size -= len(old_content)
        
        # Добавляем новый файл
        self._file_content_cache[file_url] = (datetime.now(), content)
        self._current_cache_size += len(content)
        
        # Проверяем лимит и удаляем старые файлы при необходимости
        self._evict_oldest_if_needed()
        
        if persist:
            self._persist_file(file_url, content)
        logger.debug(
            "In-memory cache updated for %s persist=%s size=%d current_total=%d",
            file_url,
            persist,
            len(content),
            self._current_cache_size,
        )

    async def set_file_content_async(self, file_url: str, content: bytes, *, persist: bool = True) -> None:
        """Асинхронная версия для кэширования содержимого файла."""
        # Удаляем старый файл из кэша, если он там был
        if file_url in self._file_content_cache:
            old_content = self._file_content_cache[file_url][1]
            self._current_cache_size -= len(old_content)
        
        # Добавляем новый файл
        self._file_content_cache[file_url] = (datetime.now(), content)
        self._current_cache_size += len(content)
        
        # Проверяем лимит и удаляем старые файлы при необходимости
        self._evict_oldest_if_needed()
        
        if persist:
            await self._persist_file_async(file_url, content)
        logger.debug(
            "In-memory cache updated for %s persist=%s size=%d current_total=%d",
            file_url,
            persist,
            len(content),
            self._current_cache_size,
        )

    # ----- Кэширование метаданных (листы и группы) -----

    def get_file_metadata(self, file_url: str) -> Optional[Dict[str, List[str]]]:
        """Возвращает закэшированные метаданные файла (листы -> группы)."""
        if file_url not in self._file_metadata_cache:
            return None
        cached_time, metadata = self._file_metadata_cache[file_url]
        if datetime.now() - cached_time > self._metadata_ttl:
            del self._file_metadata_cache[file_url]
            return None
        return metadata

    def set_file_metadata(self, file_url: str, metadata: Dict[str, List[str]]) -> None:
        """Сохраняет метаданные файла (листы -> группы)."""
        self._file_metadata_cache[file_url] = (datetime.now(), metadata)
        logger.debug("Metadata cached for %s sheets=%d", file_url, len(metadata))

    # ----- Кэширование расположения группы (группа -> файл, лист) -----

    def get_group_location(self, group_name: str) -> Optional[Tuple[str, str, str]]:
        """Возвращает расположение группы (file_url, sheet_name, actual_group_name) если оно закэшировано."""
        normalized = self._normalize_group_name(group_name)
        if normalized not in self._group_location_cache:
            return None
        cached_time, file_url, sheet_name, actual_group_name = self._group_location_cache[normalized]
        if datetime.now() - cached_time > self._group_location_ttl:
            del self._group_location_cache[normalized]
            return None
        return file_url, sheet_name, actual_group_name

    def set_group_location(
        self, group_name: str, file_url: str, sheet_name: str, actual_group_name: str
    ) -> None:
        """Сохраняет расположение группы (группа -> файл, лист)."""
        normalized = self._normalize_group_name(group_name)
        self._group_location_cache[normalized] = (
            datetime.now(),
            file_url,
            sheet_name,
            actual_group_name,
        )
        logger.debug(
            "Group location cached group=%s file=%s sheet=%s",
            normalized,
            file_url,
            sheet_name,
        )

    def _normalize_group_name(self, name: str) -> str:
        """Нормализует имя группы для использования в кэше.
        Использует ту же логику, что и _normalize_group в schedule.py."""
        import re
        # Удаляем все пробелы и преобразуем в верхний регистр
        # Это должно совпадать с логикой _normalize_group в handlers/schedule.py
        return re.sub(r"\s+", "", name).upper()

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

