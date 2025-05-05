import time
import aiohttp
from aiohttp import ClientSession
import asyncio

class CDXPagination:
    def __init__(self, session: ClientSession, max_retries: int, backoff_factor: float):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    async def fetch_page(self, url: str, retries: int = 0) -> str:
        """
        Пытается получить страницу с URL. В случае ошибки 429 (слишком много запросов),
        пробует повторить запрос с экспоненциальной задержкой.
        """
        try:
            async with self.session.get(url) as response:
                if response.status == 429:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status
                    )
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientResponseError as e:
            if retries < self.max_retries:
                # Экспоненциальная задержка при повторе
                wait_time = self.backoff_factor ** retries
                print(f"Rate limit hit, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await self.fetch_page(url, retries + 1)
            else:
                raise e

    async def get_cdx_page(self, base_url: str, page_number: int, limit: int) -> str:
        """
        Получение страницы с данными из CDX.
        """
        url = f"{base_url}?page={page_number}&limit={limit}"
        return await self.fetch_page(url)
