# crawler/fetcher.py

import aiohttp
import random
import asyncio
import logging
from aiohttp import ClientSession, ClientError
from typing import List, Tuple
from .utils import rotate_user_agent  # если у вас есть такая утилита

class Fetcher:
    def __init__(self, cfg):
        """
        cfg — это инстанс FetchConfig, в котором есть:
          - user_agents_file: str
          - rate_limit: float
          # При необходимости можно добавить другие поля в FetchConfig 
          # и обращаться к ним через cfg.<field>
        """
        self.cfg = cfg
        self.user_agents = self._load_user_agents(cfg.user_agents_file)
        self.rate_limit = cfg.rate_limit
        # если вы хотите ограничивать максимальное число одновременных запросов
        # можно передать max_concurrent из основного конфига через аргумент
        # self.semaphore = asyncio.Semaphore(cfg.max_concurrent)
        self.session: ClientSession | None = None

    def _load_user_agents(self, user_agents_file: str) -> List[str]:
        """
        Загружает список User-Agent из файла, по одному на строку.
        Если файл не найден или при ошибке чтения — возвращает пустой список.
        """
        try:
            with open(user_agents_file, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            logging.error(f"Failed to load user agents from {user_agents_file}: {e}")
            return []

    async def _ensure_session(self):
        """
        Ленивая инициализация aiohttp.ClientSession
        """
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def fetch(self, url: str) -> Tuple[str | None, str]:
        """
        Выполняет GET-запрос по URL.
        Возвращает кортеж (content, final_url).
        Если запрос не удался — content будет None.
        """
        await self._ensure_session()

        # Ограничение числа одновременных запросов, если нужно.
        # Если semaphore не нужен, можно удалить этот блок.
        # async with self.semaphore:
        try:
            headers = {'User-Agent': rotate_user_agent(self.user_agents)}
            async with self.session.get(url, headers=headers) as response:
                if response.status != 200:
                    logging.warning(f"Request to {url} failed with status {response.status}")
                    return None, str(response.url)

                content = await response.text()
                final_url = str(response.url)

                # Задержка между запросами, чтобы не перегружать сервер
                if self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)

                return content, final_url

        except ClientError as e:
            logging.error(f"Network error while fetching {url}: {e}")
            return None, url
        
        if not response or response.status != 200:
            logger.warning(f"Bad response for {domain}: {response}")
            return None

    async def close(self):
        """
        Закрывает сессию при завершении работы.
        """
        if self.session:
            await self.session.close()
