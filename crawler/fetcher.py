import aiohttp
import asyncio
import logging
from aiohttp import ClientSession, ClientError
from typing import List, Tuple
from .utils import rotate_user_agent
import chardet  # для определения кодировки

logger = logging.getLogger("Fetcher")

class Fetcher:
    def __init__(self, cfg):
        """
        cfg — это инстанс FetchConfig, в котором есть:
          - user_agents_file: str
          - rate_limit: float
        """
        self.cfg = cfg
        self.user_agents = self._load_user_agents(cfg.user_agents_file)
        self.rate_limit = cfg.rate_limit
        self.session: ClientSession | None = None
        self.logger = logger

    def _load_user_agents(self, user_agents_file: str) -> List[str]:
        try:
            with open(user_agents_file, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.logger.error(f"Failed to load user agents from {user_agents_file}: {e}")
            return []

    async def _ensure_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def fetch(self, url: str) -> Tuple[str | None, str]:
        """
        Выполняет GET-запрос по URL.
        Возвращает кортеж (content, final_url).
        Если запрос не удался — content будет None.
        """
        self.logger.debug(f"Fetching URL: {url}")
        await self._ensure_session()

        try:
            headers = {'User-Agent': rotate_user_agent(self.user_agents)}
            async with self.session.get(url, headers=headers) as response:
                if response.status != 200:
                    self.logger.warning(f"Request to {url} failed with status {response.status}")
                    return None, str(response.url)

                raw = await response.read()
                # Определяем кодировку
                detected = chardet.detect(raw)
                encoding = detected.get('encoding') or response.charset or 'latin-1'
                try:
                    content = raw.decode(encoding, errors='strict')
                except (LookupError, UnicodeDecodeError):
                    content = raw.decode(encoding, errors='replace')

                final_url = str(response.url)
                self.logger.debug(f"Decoded content length: {len(content)} chars from {final_url}")

                if self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)

                return content, final_url

        except ClientError as e:
            self.logger.error(f"Network error while fetching {url}: {e}")
            return None, url

    async def close(self):
        """
        Закрывает сессию при завершении работы.
        """
        if self.session:
            await self.session.close()
