from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from schedule_bot.services.formatter import DAY_ORDER

MAIN_BUTTON_SCHEDULE = "Расписание"
MAIN_BUTTON_SESSION = "Зимняя сессия"
MAIN_BUTTON_CREDITS = "Зачеты"
MAIN_BUTTON_EXAMS = "Экзамены"
MAIN_BUTTON_CHANGE = "Сменить группу"
BACK_BUTTON = "Назад"

DAY_SELECTION_ORDER = DAY_ORDER[:-1]
DAY_BUTTONS = DAY_SELECTION_ORDER + ["Вся неделя", BACK_BUTTON]


def build_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MAIN_BUTTON_SCHEDULE)],
            [
                KeyboardButton(text=MAIN_BUTTON_CREDITS),
                KeyboardButton(text=MAIN_BUTTON_EXAMS),
            ],
            [KeyboardButton(text=MAIN_BUTTON_CHANGE)],
        ],
        resize_keyboard=True,
    )


def build_schedule_keyboard() -> ReplyKeyboardMarkup:
    rows = []
    for index in range(0, len(DAY_SELECTION_ORDER), 2):
        row_buttons = [KeyboardButton(text=DAY_SELECTION_ORDER[index])]
        if index + 1 < len(DAY_SELECTION_ORDER):
            row_buttons.append(KeyboardButton(text=DAY_SELECTION_ORDER[index + 1]))
        rows.append(row_buttons)
    rows.append([
        KeyboardButton(text="Вся неделя"),
        KeyboardButton(text=BACK_BUTTON),
    ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)
