from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ScheduleFile:
    title: str
    url: str


DEFAULT_SCHEDULE_URL = "https://kpfu.ru/physics/raspisanie-zanyatij"

# Настройки таймаутов: отдельно для подключения и чтения
# Увеличенный таймаут подключения для медленных соединений
DEFAULT_CONNECT_TIMEOUT = 60.0  # 60 секунд на подключение
DEFAULT_READ_TIMEOUT = 120.0  # 120 секунд на чтение данных


class ScheduleFetcher:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
    ) -> None:
        self._base_url = base_url or DEFAULT_SCHEDULE_URL
        self._timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=30.0,
            pool=30.0,
        )
        self._logger = logging.getLogger(self.__class__.__name__)

    async def list_schedule_files(self) -> List[ScheduleFile]:
        self._logger.info("Fetching schedule file list from %s", self._base_url)
        html = await self._load_page(self._base_url)
        files = list(self._parse_excel_links(html))
        self._logger.info("Discovered %d schedule file(s)", len(files))
        return files

    async def download(self, file: ScheduleFile) -> bytes:
        self._logger.info("Downloading schedule file title=%s url=%s", file.title, file.url)
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(file.url)
                response.raise_for_status()
                self._logger.info(
                    "Downloaded %s with status %s and %d bytes",
                    file.url,
                    response.status_code,
                    len(response.content),
                )
                return response.content
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
            self._logger.warning(
                "Connection timeout/error while downloading %s: %s",
                file.url,
                type(e).__name__,
            )
            raise
        except httpx.HTTPError as e:
            self._logger.error(
                "HTTP error while downloading %s: %s",
                file.url,
                e,
            )
            raise

    async def _load_page(self, url: str) -> str:
        self._logger.debug("Loading HTML page %s", url)
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
            self._logger.warning(
                "Connection timeout/error while loading %s: %s",
                url,
                type(e).__name__,
            )
            raise
        except httpx.HTTPError as e:
            self._logger.error(
                "HTTP error while loading %s: %s",
                url,
                e,
            )
            raise

    def _parse_excel_links(self, html: str) -> Iterable[ScheduleFile]:
        soup = BeautifulSoup(html, "lxml")
        anchors = soup.find_all("a", href=True)
        for anchor in anchors:
            href = anchor["href"]
            if not href:
                continue
            href_lower = href.lower()
            if not href_lower.endswith((".xls", ".xlsx")):
                continue
            absolute_url = urljoin(self._base_url, href)
            title = anchor.get_text(strip=True) or href.split("/")[-1]
            self._logger.debug("Found schedule file title=%s url=%s", title, absolute_url)
            yield ScheduleFile(title=title, url=absolute_url)
