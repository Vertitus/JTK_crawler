import aiohttp
import random
import asyncio
import logging
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError
from typing import List, Tuple
from .utils import rotate_user_agent
from crawler.utils import is_valid_mime_type

class Fetcher:
    def __init__(self, cfg):
        self.cfg = cfg
        self.user_agents = self._load_user_agents(cfg.user_agents_file)
        self.session: ClientSession = None
        self.rate_limit = cfg.get("rate_limit", 1)
        self.semaphore = asyncio.Semaphore(cfg.get("max_concurrent", 8))  # Ограничение одновременных запросов

    def _load_user_agents(self, user_agents_file: str) -> List[str]:
        try:
            with open(user_agents_file, 'r') as f:
                return [line.strip() for line in f.readlines()]
        except Exception as e:
            logging.error(f"Failed to load user agents from {user_agents_file}: {e}")
            return []

    async def _create_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def fetch(self, url: str) -> Tuple[str, str]:
        await self._create_session()

        async with self.semaphore:
            try:
                headers = {'User-Agent': rotate_user_agent(self.user_agents)}
                async with self.session.get(url, headers=headers) as response:
                    # Если ошибка, сразу выходим
                    if response.status != 200:
                        logging.warning(f"Request to {url} failed with status {response.status}")
                        return None, url

                    content = await response.text()
                    final_url = str(response.url)

                    await asyncio.sleep(self.rate_limit)  # Задержка между запросами для предотвращения перегрузки сервера

                    return content, final_url

            except ClientError as e:
                logging.error(f"Network error while fetching {url}: {e}")
                return None, url
            if not is_valid_mime_type(response.headers.get("Content-Type", "")):
                return None, url

    async def close(self):
        if self.session:
            await self.session.close()
