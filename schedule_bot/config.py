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
class Settings:
    bot: BotConfig
    fetcher: FetcherConfig


def _get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


def load_settings() -> Settings:
    bot_token = _get_env("BOT_TOKEN")
    return Settings(
        bot=BotConfig(token=bot_token),
        fetcher=FetcherConfig(),
    )
