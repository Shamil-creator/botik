from dataclasses import dataclass
import os
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class BotConfig:
    token: str


@dataclass(frozen=True)
class FetcherConfig:
    schedule_url: str = "https://kpfu.ru/physics/raspisanie-zanyatij"


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    include_library_logs: bool = False


@dataclass(frozen=True)
class Settings:
    bot: BotConfig
    fetcher: FetcherConfig
    logging: LoggingConfig


def _get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


def load_settings() -> Settings:
    bot_token = _get_env("BOT_TOKEN")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    include_library_logs = os.getenv("LOG_INCLUDE_LIBS", "0") in {"1", "true", "TRUE"}
    return Settings(
        bot=BotConfig(token=bot_token),
        fetcher=FetcherConfig(),
        logging=LoggingConfig(
            level=log_level,
            include_library_logs=include_library_logs,
        ),
    )
