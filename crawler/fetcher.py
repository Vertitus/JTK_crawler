# fetcher.py (замена/обновление)
import aiohttp
import asyncio
import logging
from aiohttp import ClientSession, ClientError, InvalidURL, ClientConnectorError
from typing import List, Tuple, Optional
from .utils import rotate_user_agent
import chardet  # для определения кодировки
from urllib.parse import urlparse
from typing import Tuple, Optional
from aiohttp_socks import ProxyConnector

logger = logging.getLogger("Fetcher")

def normalize_url(raw: str) -> Optional[str]:
    """Пытаемся привести к валидному URL.
       Возвращаем None если URL некорректен / явно шаблонный.
    """
    if not raw:
        return None
    raw = raw.strip()
    # пропускаем явные шаблоны
    if "*" in raw:
        return None
    parsed = urlparse(raw)
    # если уже есть схема — принимаем
    if parsed.scheme in ("http", "https"):
        return raw
    # если это web.archive.org — добавим схему если не задана
    if raw.startswith("web.archive.org") or raw.startswith("http://web.archive.org") or raw.startswith("https://web.archive.org"):
        if not parsed.scheme:
            return "https://" + raw
        return raw
    # если просто домен или путь без схемы — добавим http://
    if "." in raw and parsed.scheme == "":
        return "http://" + raw
    return None

class Fetcher:
    def __init__(self, cfg):
        self.cfg = cfg
        self.user_agents = self._load_user_agents(cfg.user_agents_file)
        self.rate_limit = getattr(cfg, "rate_limit", 0)
        self.session: Optional[ClientSession] = None
        self.logger = logger
        self._timeout_seconds = getattr(cfg, "request_timeout", 30)
        self.network_cfg = getattr(cfg, "network", None)

    def _load_user_agents(self, user_agents_file: str) -> List[str]:
        try:
            with open(user_agents_file, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.logger.error(f"Failed to load user agents from {user_agents_file}: {e}")
            return []

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
            if self.network_cfg and self.network_cfg.use_tor:
                connector = ProxyConnector.from_url(self.network_cfg.tor_socks_url)
                self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
                self.logger.info(f"Fetcher: using Tor SOCKS proxy {self.network_cfg.tor_socks_url}")
            else:
                raise RuntimeError("Tor must be enabled — direct connection is blocked for this environment.")



    async def fetch(self, url: str) -> Tuple[Optional[str], str, Optional[int]]:
        """Fetch page safely. Returns (content_or_None, final_url_or_input, status_or_None)."""

        self.logger.debug(f"Fetching URL: {url}")
        await self._ensure_session()

        normalized = normalize_url(url)
        if not normalized:
            self.logger.warning("URL not normalized / skipped: %s", url)
            return None, url, None                     # ✅ всегда три значения

        try:
            headers = {'User-Agent': rotate_user_agent(self.user_agents) or "JTK-Crawler/1.0"}
            async with self.session.get(normalized, headers=headers, timeout=30) as response:  # ✅ добавлен таймаут
                if response.status != 200:
                    self.logger.warning(f"Request to {normalized} failed with status {response.status}")
                    return None, str(response.url), response.status                            # ✅ три значения

                raw = await response.read()
                detected = chardet.detect(raw)
                encoding = detected.get('encoding') or response.charset or 'latin-1'
                try:
                    content = raw.decode(encoding, errors='strict')
                except (LookupError, UnicodeDecodeError):
                    content = raw.decode(encoding, errors='replace')

                final_url = str(response.url)
                status = response.status
                self.logger.debug(f"Decoded content length: {len(content)} chars from {final_url}")

                if self.rate_limit and self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)

                return content, final_url, status

        # ---------- [⚙️ Обработка ошибок без трассировок] ----------
        except InvalidURL:
            self.logger.warning(f"❌ Invalid URL skipped: {url}")
            return None, url, None

        except (ClientConnectorError, asyncio.TimeoutError):
            self.logger.warning(f"⏳ Timeout / Connection error fetching {url}")
            return None, url, None

        except ClientError:
            self.logger.warning(f"⚠️ ClientError fetching {url}")
            return None, url, None

        except Exception as e:
            # Только при DEBUG включаем трассировку
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception(f"Unexpected error while fetching {url}: {e}")
            else:
                self.logger.error(f"❌ Fetcher failed for {url}: {type(e).__name__}")
            return None, url, None

    async def close(self):
        if self.session:
            try:
                await self.session.close()
            except Exception:
                self.logger.exception("Error closing session")
            finally:
                self.session = None
