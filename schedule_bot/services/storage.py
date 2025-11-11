from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    group_name TEXT NOT NULL
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

    def set_user_group(self, chat_id: int, group_name: str) -> None:
        with self._connect() as conn:
            query = (
                "INSERT OR REPLACE INTO "
                "users(chat_id, group_name) "
                "VALUES(?, ?)"
            )
            conn.execute(query, (chat_id, group_name))
        logger.info("User group saved chat_id=%s group=%s", chat_id, group_name)

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
