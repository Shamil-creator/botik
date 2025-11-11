from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple

WeekInfo = Tuple[int, date, date]

WEEKS: List[WeekInfo] = [
    (1, date(2025, 9, 1), date(2025, 9, 6)),
    (2, date(2025, 9, 8), date(2025, 9, 13)),
    (3, date(2025, 9, 15), date(2025, 9, 20)),
    (4, date(2025, 9, 22), date(2025, 9, 27)),
    (5, date(2025, 9, 29), date(2025, 10, 4)),
    (6, date(2025, 10, 6), date(2025, 10, 11)),
    (7, date(2025, 10, 13), date(2025, 10, 18)),
    (8, date(2025, 10, 20), date(2025, 10, 25)),
    (9, date(2025, 10, 27), date(2025, 11, 1)),
    (10, date(2025, 11, 3), date(2025, 11, 8)),
    (11, date(2025, 11, 10), date(2025, 11, 15)),
    (12, date(2025, 11, 17), date(2025, 11, 22)),
    (13, date(2025, 11, 24), date(2025, 11, 29)),
    (14, date(2025, 12, 1), date(2025, 12, 6)),
    (15, date(2025, 12, 8), date(2025, 12, 13)),
    (16, date(2025, 12, 15), date(2025, 12, 20)),
    (17, date(2025, 12, 22), date(2025, 12, 27)),
    (18, date(2025, 12, 29), date(2026, 1, 3)),
]


def get_current_week(today: Optional[date] = None) -> Optional[WeekInfo]:
    today = today or date.today()
    for info in WEEKS:
        _, start, end = info
        if start <= today <= end:
            return info
    return None


def get_week_by_number(number: int) -> Optional[WeekInfo]:
    for info in WEEKS:
        if info[0] == number:
            return info
    return None


def format_week_info(info: WeekInfo) -> str:
    number, start, end = info
    return f"Неделя {number} ({start.strftime('%d.%m')}–{end.strftime('%d.%m')})"
