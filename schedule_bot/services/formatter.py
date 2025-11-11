from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Iterable, List

from schedule_bot.services.parser import Lesson


DAY_ORDER = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]


def format_lessons(lessons: Iterable[Lesson]) -> str:
    lessons_list: List[Lesson] = sorted(
        lessons,
        key=lambda lesson: (_day_index(lesson.day), _time_key(lesson.time)),
    )
    if not lessons_list:
        return "Записей не найдено."

    grouped = defaultdict(list)
    for lesson in lessons_list:
        grouped[lesson.day].append(lesson)

    lines: List[str] = []
    for day in sorted(grouped.keys(), key=_day_index):
        lines.append(f"📅 {day}")
        lines.append("")  # пустая строка после дня недели
        for lesson in grouped[day]:
            lines.extend(_format_lesson(lesson))
        lines.append("")  # пустая строка между днями

    if lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def _format_lesson(lesson: Lesson) -> List[str]:
    description = lesson.description.replace("\r\n", "\n")
    parts = description.split("\n")
    if not parts:
        return [lesson.time]

    lines = [f"{lesson.time} — {parts[0]}"]
    lines.extend(parts[1:])
    lines.append("")  # пустая строка после каждой пары
    return lines


def _day_index(day: str) -> int:
    try:
        return DAY_ORDER.index(day)
    except ValueError:
        return len(DAY_ORDER)


def _time_key(time_range: str) -> datetime:
    try:
        start, *_ = time_range.split("-")
        return datetime.strptime(start.strip(), "%H:%M")
    except Exception:
        return datetime.max
