from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionData:
    group_name: str
    credit_start: Optional[str]
    credit_end: Optional[str]
    credit_text: str
    exam_start: Optional[str]
    exam_end: Optional[str]
    exam_text: str


class Storage:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        logger.debug("Storage initialised path=%s", self._path)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            # Миграция: добавляем поля created_at и last_activity если их нет
            try:
                conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
            except sqlite3.OperationalError:
                pass  # Поле уже существует
            
            try:
                conn.execute("ALTER TABLE users ADD COLUMN last_activity TEXT")
            except sqlite3.OperationalError:
                pass  # Поле уже существует
            
            try:
                conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
            except sqlite3.OperationalError:
                pass  # Поле уже существует
            
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    group_name TEXT NOT NULL,
                    created_at TEXT,
                    last_activity TEXT,
                    username TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    group_name TEXT PRIMARY KEY,
                    credit_start TEXT,
                    credit_end TEXT,
                    credit_text TEXT NOT NULL,
                    exam_start TEXT,
                    exam_end TEXT,
                    exam_text TEXT NOT NULL
                )
                """
            )
        logger.debug("Database schema ensured at %s", self._path)

    def _normalize_group(self, name: str) -> str:
        normalized = re.sub(r"\s+", "", name or "")
        return normalized.upper()

    def set_user_group(self, chat_id: int, group_name: str, username: Optional[str] = None) -> None:
        now = datetime.now().isoformat()
        with self._connect() as conn:
            # Проверяем, существует ли пользователь
            existing = conn.execute(
                "SELECT created_at FROM users WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
            
            if existing:
                # Обновляем существующего пользователя (сохраняем created_at если он есть)
                created_at = existing[0] if existing[0] else now
                conn.execute(
                    """
                    UPDATE users
                    SET group_name = ?, last_activity = ?, created_at = ?, username = ?
                    WHERE chat_id = ?
                    """,
                    (group_name, now, created_at, username, chat_id),
                )
            else:
                # Создаём нового пользователя
                conn.execute(
                    """
                    INSERT INTO users(chat_id, group_name, created_at, last_activity, username)
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (chat_id, group_name, now, now, username),
                )
        logger.info("User group saved chat_id=%s group=%s username=%s", chat_id, group_name, username)
    
    def update_user_activity(self, chat_id: int, username: Optional[str] = None) -> None:
        """Обновляет время последней активности пользователя и username (если указан)."""
        now = datetime.now().isoformat()
        with self._connect() as conn:
            if username is not None:
                conn.execute(
                    "UPDATE users SET last_activity = ?, username = ? WHERE chat_id = ?",
                    (now, username, chat_id),
                )
            else:
                conn.execute(
                    "UPDATE users SET last_activity = ? WHERE chat_id = ?",
                    (now, chat_id),
                )

    def get_user_group(self, chat_id: int) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT group_name FROM users WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        if row:
            logger.debug("User group fetched chat_id=%s group=%s", chat_id, row[0])
            return row[0]
        logger.debug("User group not found chat_id=%s", chat_id)
        return None

    def iter_chat_ids(self) -> Iterable[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT chat_id FROM users").fetchall()
        logger.debug("Iterating %d chat ids", len(rows))
        return (row[0] for row in rows)

    def remove_user(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
        logger.info("User removed chat_id=%s", chat_id)

    def replace_sessions(self, sessions: Iterable[SessionData]) -> None:
        records = [
            (
                self._normalize_group(session.group_name),
                session.credit_start,
                session.credit_end,
                session.credit_text,
                session.exam_start,
                session.exam_end,
                session.exam_text,
            )
            for session in sessions
        ]
        with self._connect() as conn:
            conn.execute("DELETE FROM sessions")
            if records:
                conn.executemany(
                    """
                    INSERT INTO sessions (
                        group_name,
                        credit_start,
                        credit_end,
                        credit_text,
                        exam_start,
                        exam_end,
                        exam_text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    records,
                )
        logger.info("Sessions replaced count=%d", len(records))

    def has_sessions(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM sessions LIMIT 1").fetchone()
        result = row is not None
        logger.debug("Sessions present=%s", result)
        return result

    def get_session(self, group_name: str) -> Optional[SessionData]:
        key = self._normalize_group(group_name)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    group_name,
                    credit_start,
                    credit_end,
                    credit_text,
                    exam_start,
                    exam_end,
                    exam_text
                FROM sessions
                WHERE group_name = ?
                """,
                (key,),
            ).fetchone()
        if row is None:
            logger.debug("Session not found group=%s", group_name)
            return None
        logger.debug("Session retrieved group=%s", group_name)
        return SessionData(*row)
    
    # ----- Методы статистики -----
    
    def get_total_users(self) -> int:
        """Возвращает общее количество зарегистрированных пользователей."""
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        return row[0] if row else 0
    
    def get_group_statistics(self, limit: int = 10) -> list[Tuple[str, int]]:
        """
        Возвращает список групп с количеством пользователей.
        Сортируется по убыванию количества пользователей.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT group_name, COUNT(*) as count
                FROM users
                GROUP BY group_name
                ORDER BY count DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]
    
    def get_new_users_count(self, days: int = 7) -> int:
        """Возвращает количество новых пользователей за последние N дней."""
        cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = cutoff_date - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()
        
        with self._connect() as conn:
            # Считаем только пользователей с датой регистрации >= cutoff_date
            # Если created_at NULL (старые пользователи), не учитываем их
            row = conn.execute(
                """
                SELECT COUNT(*) FROM users
                WHERE created_at >= ?
                """,
                (cutoff_str,),
            ).fetchone()
        return row[0] if row else 0
    
    def get_active_users_count(self, days: int = 7) -> int:
        """Возвращает количество активных пользователей за последние N дней."""
        cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = cutoff_date - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()
        
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT chat_id) FROM users
                WHERE last_activity >= ?
                """,
                (cutoff_str,),
            ).fetchone()
        return row[0] if row else 0
    
    def get_users_by_group(self, group_name: str) -> list[Tuple[int, Optional[str]]]:
        """
        Возвращает список (chat_id, username) пользователей указанной группы.
        Группа нормализуется перед поиском.
        """
        normalized = self._normalize_group(group_name)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT chat_id, username FROM users WHERE group_name = ? ORDER BY chat_id",
                (normalized,),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]
