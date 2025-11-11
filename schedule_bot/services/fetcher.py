from __future__ import annotations

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


class ScheduleFetcher:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url or DEFAULT_SCHEDULE_URL
        self._timeout = timeout

    async def list_schedule_files(self) -> List[ScheduleFile]:
        html = await self._load_page(self._base_url)
        return list(self._parse_excel_links(html))

    async def download(self, file: ScheduleFile) -> bytes:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(file.url)
            response.raise_for_status()
            return response.content

    async def _load_page(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

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
            yield ScheduleFile(title=title, url=absolute_url)
