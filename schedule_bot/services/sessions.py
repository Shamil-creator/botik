from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from docx import Document

from schedule_bot.services.storage import SessionData, Storage


@dataclass(frozen=True)
class ParsedRange:
    start: str | None
    end: str | None
    text: str


def ensure_sessions_loaded(storage: Storage, session_dir: Path | None = None) -> None:
    if storage.has_sessions():
        return

    directory = session_dir or Path(__file__).resolve().parents[1] / "сессия"
    if not directory.exists():
        return

    sessions = _collect_sessions(directory)
    if sessions:
        storage.replace_sessions(sessions.values())


def format_session_message(group_name: str, session: SessionData) -> str:
    lines = [f"🎓 Группа: {group_name}"]
    if session.credit_text and session.credit_text != "—":
        lines.append(f"📚 Зачётная сессия: {session.credit_text}")
    if session.exam_text and session.exam_text != "—":
        lines.append(f"📝 Экзамены: {session.exam_text}")
    if len(lines) == 1:
        lines.append("Нет данных о датах зимней сессии.")
    return "\n".join(lines)


def _collect_sessions(directory: Path) -> Dict[str, SessionData]:
    sessions: Dict[str, SessionData] = {}
    for path in sorted(directory.glob("*.docx")):
        for record in _parse_doc(path):
            sessions[record.group_name] = record
    return sessions


def _parse_doc(path: Path) -> Iterable[SessionData]:
    document = Document(path)
    for table in document.tables:
        if not table.rows:
            continue
        header = [_clean_text(cell.text).lower() for cell in table.rows[0].cells]
        try:
            group_idx = header.index("группа")
            credit_idx = header.index("зачетная сессия")
            exam_idx = header.index("экзаменационная сессия")
        except ValueError:
            continue

        for row in table.rows[1:]:
            cells = [_clean_text(cell.text) for cell in row.cells]
            if len(cells) <= max(group_idx, credit_idx, exam_idx):
                continue

            groups = _split_groups(cells[group_idx])
            if not groups:
                continue

            credit = _parse_range(cells[credit_idx])
            exam = _parse_range(cells[exam_idx])

            for group in groups:
                yield SessionData(
                    group_name=group,
                    credit_start=credit.start,
                    credit_end=credit.end,
                    credit_text=credit.text,
                    exam_start=exam.start,
                    exam_end=exam.end,
                    exam_text=exam.text,
                )


def _clean_text(text: str) -> str:
    text = text.replace("\xa0", " ").strip()
    return " ".join(text.split())


def _split_groups(value: str) -> List[str]:
    if not value:
        return []
    raw_groups = re.split(r"[;,\n]+", value.replace("—", "-").replace("–", "-"))
    result: List[str] = []
    for raw in raw_groups:
        group = re.sub(r"\s+", "", raw.upper())
        if group:
            result.append(group)
    return result


def _parse_range(value: str) -> ParsedRange:
    cleaned = value.replace("\xa0", "")
    compact = cleaned.replace(" ", "")
    dates = re.findall(r"\d{2}\.\d{2}\.\d{4}", compact)
    if len(dates) >= 2:
        return ParsedRange(dates[0], dates[1], f"{dates[0]} – {dates[1]}")
    if len(dates) == 1:
        return ParsedRange(dates[0], None, dates[0])
    fallback = _clean_text(value)
    return ParsedRange(None, None, fallback or "—")

