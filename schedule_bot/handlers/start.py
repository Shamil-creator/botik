from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardRemove

from schedule_bot.services.deps import cache, storage
from schedule_bot.services.sessions import format_session_message
from schedule_bot.services.ui import (
    BACK_BUTTON,
    DAY_BUTTONS,
    MAIN_BUTTON_CHANGE,
    MAIN_BUTTON_SCHEDULE,
    MAIN_BUTTON_SESSION,
    build_main_keyboard,
    build_schedule_keyboard,
)
from schedule_bot.services.weeks import format_week_info, get_current_week

from .schedule import send_schedule_for_group


router = Router()
logger = logging.getLogger(__name__)


def _format_user_info(message: Message) -> str:
    """Форматирует информацию о пользователе для логов: chat_id и username (если есть)."""
    chat_id = message.chat.id
    username = message.from_user.username if message.from_user else None
    if username:
        return f"chat_id={chat_id} @{username}"
    return f"chat_id={chat_id}"


class RegistrationState(StatesGroup):
    waiting_group = State()


@router.message(Command("start"))
async def handle_start(message: Message, state: FSMContext) -> None:
    stored_group = storage.get_user_group(message.chat.id)
    current_week_info = get_current_week()
    if stored_group:
        cache.add_watcher(message.chat.id)
        logger.info(
            "/start called with existing group %s group=%s",
            _format_user_info(message),
            stored_group,
        )
        await state.clear()
        week_line = (
            f"Сейчас {format_week_info(current_week_info)}.\n"
            if current_week_info
            else ""
        )
        message_text = (
            "Привет! Я запомнил твою группу"
            f" <b>{stored_group}</b>.\n"
            f"{week_line}"
            "Нажми «Расписание», чтобы выбрать день, "
            "или «Зимняя сессия», чтобы посмотреть даты экзаменов."
        )
        await message.answer(
            message_text,
            reply_markup=build_main_keyboard(),
        )
        return

    logger.info("/start registration initiated %s", _format_user_info(message))
    await _prompt_for_group(message, state)


@router.message(Command("change_group"))
async def handle_change_group(message: Message, state: FSMContext) -> None:
    logger.info("Change group command %s", _format_user_info(message))
    await _prompt_for_group(message, state)


@router.message(F.text == MAIN_BUTTON_CHANGE)
async def handle_change_group_button(
    message: Message, state: FSMContext
) -> None:
    logger.info("Change group button %s", _format_user_info(message))
    await _prompt_for_group(message, state)


@router.message(RegistrationState.waiting_group)
async def handle_group_input(message: Message, state: FSMContext) -> None:
    group_query = (message.text or "").strip()
    if not group_query:
        await message.answer("Введи, пожалуйста, код группы.")
        logger.warning("Empty group input %s", _format_user_info(message))
        return

    validation_result = await send_schedule_for_group(
        message,
        group_query,
        day=None,
        preview_only=True,
        suppress_not_found_message=True,
        current_week_info=get_current_week(),
    )

    if validation_result is None:
        await message.answer(
            "Не нашёл такую группу. Проверь код и попробуй ещё раз."
        )
        logger.warning(
            "Group validation failed %s input=%s",
            _format_user_info(message),
            group_query,
        )
        return

    _, _, _, group_name = validation_result
    username = message.from_user.username if message.from_user else None
    storage.set_user_group(message.chat.id, group_name, username)
    cache.add_watcher(message.chat.id)
    logger.info(
        "Group stored %s group=%s", _format_user_info(message), group_name
    )

    await state.clear()
    await message.answer(
        (
            f"Группа <b>{group_name}</b> сохранена!\n"
            "Нажми «Расписание», чтобы посмотреть пары, "
            "или «Зимняя сессия», чтобы узнать даты экзаменов."
        ),
        reply_markup=build_main_keyboard(),
    )


@router.message(F.text == MAIN_BUTTON_SESSION)
async def handle_session_button(message: Message) -> None:
    group_name = storage.get_user_group(message.chat.id)
    if not group_name:
        await message.answer(
            "Сначала укажи группу через /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        logger.warning(
            "Session button without group %s", _format_user_info(message)
        )
        return

    session = storage.get_session(group_name)
    if session is None:
        await message.answer(
            (
                "Не нашёл данные о зимней сессии для группы "
                f"<b>{group_name}</b>."
            ),
            reply_markup=build_main_keyboard(),
        )
        logger.warning(
            "Session data missing %s group=%s",
            _format_user_info(message),
            group_name,
        )
        return

    message_text = format_session_message(group_name, session)
    await message.answer(message_text, reply_markup=build_main_keyboard())
    logger.info(
        "Session data sent %s group=%s", _format_user_info(message), group_name
    )


@router.message(F.text == MAIN_BUTTON_SCHEDULE)
async def handle_schedule_button(message: Message) -> None:
    group_name = storage.get_user_group(message.chat.id)
    if not group_name:
        await message.answer(
            "Сначала укажи группу через /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        logger.warning(
            "Schedule button without group %s", _format_user_info(message)
        )
        return

    week_info = get_current_week()
    week_line = (
        f"Сейчас {format_week_info(week_info)}.\n"
        if week_info
        else ""
    )
    await message.answer(
        f"{week_line}Выбери день недели:",
        reply_markup=build_schedule_keyboard(),
    )
    logger.debug(
        "Schedule keyboard shown %s group=%s",
        _format_user_info(message),
        group_name,
    )


@router.message(F.text == BACK_BUTTON)
async def handle_back_button(message: Message) -> None:
    await message.answer(
        "Вернулся в главное меню.",
        reply_markup=build_main_keyboard(),
    )
    logger.debug("Back to main menu %s", _format_user_info(message))


@router.message(F.text.in_(DAY_BUTTONS))
async def handle_day_selection(message: Message) -> None:
    group_name = storage.get_user_group(message.chat.id)
    if not group_name:
        await message.answer(
            "Сначала укажи группу командой /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        logger.warning(
            "Day selection without group %s", _format_user_info(message)
        )
        return

    day_text = message.text
    if day_text == BACK_BUTTON:
        await message.answer(
            "Вернулся в главное меню.",
            reply_markup=build_main_keyboard(),
        )
        logger.debug("Back button pressed in day selection %s", _format_user_info(message))
        return

    day = None if day_text == "Вся неделя" else day_text

    result = await send_schedule_for_group(
        message,
        group_name,
        day=day,
        reply_markup=build_schedule_keyboard(),
        current_week_info=get_current_week(),
    )

    if result is None:
        await message.answer(
            "Не удалось получить расписание. Попробуй позже.",
            reply_markup=build_main_keyboard(),
        )
        logger.error(
            "Failed to send schedule from day selection %s group=%s day=%s",
            _format_user_info(message),
            group_name,
            day,
        )


async def _prompt_for_group(message: Message, state: FSMContext) -> None:
    await state.set_state(RegistrationState.waiting_group)
    await message.answer(
        (
            "Привет! Я помогу следить за расписанием.\n"
            "Напиши код своей группы (например, <code>06-451</code>)."
        ),
        reply_markup=ReplyKeyboardRemove(),
    )
    logger.debug("Prompted for group %s", _format_user_info(message))
