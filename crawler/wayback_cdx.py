import aiohttp
import asyncio
import json
import logging
from urllib.parse import quote
from typing import List, Optional, Tuple
from urllib.parse import unquote, quote


class WaybackCDXClient:
    CDX_API = "https://web.archive.org/cdx/search/cdx"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        request_timeout: int = 30,
        max_pages: int = 0,
        page_size: int = 1000,
        rate_limiter=None
    ):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.request_timeout = request_timeout
        self.max_pages = max_pages
        self.page_size = page_size
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = rate_limiter

    async def _handle_errors(self, response: aiohttp.ClientResponse):
        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            self.logger.warning(f"Rate limited. Retrying after {retry_after}s")
            await asyncio.sleep(retry_after)
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message="Rate limit exceeded",
            )
        if response.status != 200:
            text = await response.text()
            self.logger.error(f"HTTP error {response.status}: {text[:200]}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"HTTP error {response.status}",
            )

    async def fetch_snapshots(
        self,
        domain: str,
        from_date: str = "20050101000000",
        to_date: str = "20051231235959",
    ) -> List[str]:
        """
        Возвращает список уникальных URL снапшотов Wayback Machine для домена.
        """
        base_url = self.CDX_API
        params = {
            "url": f"http://{domain}/*",
            "from": "20050101000000",
            "to": "20051231235959",
            "output": "json",
            "limit": 10,  # ← было 1000, уменьшаем для теста
            # "showResumeKey": "true",  # ← можно отключить временно
            "matchType": "prefix",
            "fl": "timestamp,original,statuscode,mimetype",
        }
        headers = {"User-Agent": "JTK-Crawler/1.0 (+mailto:youremail@example.com)"}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        results: List[str] = []
        attempt = 0

        while attempt <= self.max_retries:
            try:
                snapshots, resume_key = await self._page_request(base_url, params, timeout, headers)
                results.extend(snapshots)

                page = 1
                # пагинация
                while resume_key and (self.max_pages == 0 or page < self.max_pages):
                    page += 1
                    params["resumeKey"] = resume_key
                    snapshots, resume_key = await self._page_request(base_url, params, timeout)
                    results.extend(snapshots)

                # уникализация
                unique = list(dict.fromkeys(results))
                self.logger.info(f"Total unique snapshots for {domain}: {len(unique)}")
                return unique

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self.max_retries:
                    self.logger.error(f"Max retries reached for {domain}: {type(e).__name__}: {e}")
                    return []
                delay = self.backoff_factor ** attempt
                self.logger.warning(f"Retry {attempt+1} for {domain} in {delay}s: {type(e).__name__}: {e}")
                await asyncio.sleep(delay)
                attempt += 1

        return []

    async def _page_request(self, url: str, params: dict, timeout: aiohttp.ClientTimeout, headers: dict) -> Tuple[List[str], Optional[str]]:
        try:
            self.logger.debug(f"CDX request to {url} params={params}")

            async with self.rate_limiter:
                async with self.session.get(url, params=params, timeout=timeout, headers=headers) as resp:
                    self.logger.debug(f"CDX HTTP {resp.status} for {params.get('url')}")
                    await self._handle_errors(resp)

                    text = await resp.text()
                    self.logger.debug(f"CDX raw response snippet: {text[:300]}")

                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        self.logger.warning("Response is not valid JSON, maybe HTML or empty.")
                        data = []
        except asyncio.TimeoutError:
            self.logger.exception("Timeout while requesting CDX")
            raise
        except aiohttp.ClientError:
            self.logger.exception("ClientError while requesting CDX")
            raise

        if not data:
            self.logger.info("No data in CDX response.")
            return [], None

        resume_key = None
        if isinstance(data[-1], str) and data[-1].startswith("resumeKey:"):
            resume_key = data.pop().split(":", 1)[1]

        snapshots = []
        for entry in data[1:]:
            if not isinstance(entry, list) or len(entry) < 5:
                continue

            _, ts, original, mime, status, *_ = entry
            if "*" in original:
                continue

            url = self._build_wayback_url(ts, original)
            if url:
                snapshots.append(url)

        self.logger.info(f"Page fetched: {len(snapshots)} entries, resumeKey={resume_key}")
        return snapshots, resume_key


    def _build_wayback_url(self, timestamp: str, original: str) -> Optional[str]:
        # Раздекодируем исходный original (если он закодирован)
        try:
            orig = unquote(original)
        except Exception:
            orig = original

        # не строим URL если остались шаблонные символы
        if "*" in orig:
            return None

        # Сохраняем корректную кодировку для вставки в путь web.archive.org
        # quote с безопасными символами :/ сохраняет схему и слеши
        encoded = quote(orig, safe=":/")
        return f"https://web.archive.org/web/{timestamp}id_/{encoded}"

class CDXManager:
    def __init__(self, cfg, storage, rate_limiter=None):
        self.cfg = cfg
        self.storage = storage
        self.rate_limiter = rate_limiter
        self.client: Optional[WaybackCDXClient] = None
        self.logger = logging.getLogger("CDXManager")

    async def initialize(self, session: aiohttp.ClientSession):
        self.client = WaybackCDXClient(
            session=session,
            max_retries=self.cfg.max_retries,
            backoff_factor=self.cfg.backoff_factor,
            request_timeout=self.cfg.request_timeout,
            max_pages=self.cfg.max_pages,
            page_size=self.cfg.page_size
        )

    async def get_seed_urls(self) -> List[str]:
        if not self.client:
            raise RuntimeError("CDXClient not initialized")

        domains = self._load_domains()
        self.logger.info(f"Will bootstrap seeds for {len(domains)} domains")

        all_urls: List[str] = []
        for domain in domains:
            try:
                self.logger.info(f"Fetching CDX for {domain}")
                urls = await self.client.fetch_snapshots(domain)
                self.logger.info(f"  → raw snapshots: {len(urls)}")

                filtered = await self._filter_new_urls(urls)
                self.logger.info(f"  → new (unvisited): {len(filtered)}")

                await self.storage.stats.add_snapshots(
                    total=len(urls),
                    new=len(filtered)
                )

                all_urls.extend(filtered)

            except Exception as e:
                self.logger.error(f"Failed to process domain {domain}: {str(e)}")
                await self.storage.stats.add_failed_domain(domain)
                continue

        return all_urls

    def _load_domains(self) -> List[str]:
        try:
            with open(self.cfg.target_domains_file, "r") as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.logger.error("Domains file not found at %s", self.cfg.target_domains_file)
            return []

    async def _filter_new_urls(self, urls: List[str]) -> List[str]:
        return [url for url in urls if not self.storage.is_visited(url)]
