from __future__ import annotations

import logging
import logging.config

from schedule_bot.config import LoggingConfig

_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(config: LoggingConfig) -> None:
    """
    Configures application-wide logging.

    Parameters
    ----------
    config:
        Logging configuration taken from environment variables.
    """
    level = _resolve_level(config.level)

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": _DEFAULT_FORMAT,
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": level,
            }
        },
        "root": {
            "handlers": ["console"],
            "level": level,
        },
    }

    logging.config.dictConfig(logging_config)

    if not config.include_library_logs:
        _mute_library_logs()
    else:
        _unmute_library_logs(level)

    logging.getLogger(__name__).debug(
        "Logging configured. level=%s include_library_logs=%s",
        logging.getLevelName(level),
        config.include_library_logs,
    )


def _resolve_level(level_name: str) -> int:
    if not level_name:
        return logging.INFO
    level = logging.getLevelName(level_name.upper())
    if isinstance(level, int):
        return level
    return logging.INFO


def _mute_library_logs() -> None:
    for logger_name in (
        "aiogram",
        "aiohttp",
        "httpx",
        "openpyxl",
        "asyncio",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def _unmute_library_logs(level: int) -> None:
    for logger_name in (
        "aiogram",
        "aiohttp",
        "httpx",
    ):
        logging.getLogger(logger_name).setLevel(level)

