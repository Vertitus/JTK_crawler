import aiohttp
import asyncio
import logging
from urllib.parse import quote
from typing import List, Optional
from datetime import datetime


class WaybackCDXClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        request_timeout: int = 30,
        max_pages: int = 0,
        page_size: int = 1000
    ):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.request_timeout = request_timeout
        self.max_pages = max_pages
        self.page_size = page_size
        self.logger = logging.getLogger("CDXClient")

    async def fetch_snapshots(
        self,
        domain: str,
        from_date: str = "20050101000000",
        to_date: str = "20051231235959"
    ) -> List[str]:
        base_url = "http://web.archive.org/cdx/search/cdx"
        results: List[str] = []
        params = {
            "url": f"{domain}/*",
            "matchType": "domain",
            "from": from_date,
            "to": to_date,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype",
            # фильтры и collapse отключены для полного покрытия
            # "filter": ["statuscode:200", "mimetype:text/html"],
            # "collapse": "urlkey",
            "limit": self.page_size,
            "showResumeKey": "true",
        }

        timeout = aiohttp.ClientTimeout(total=self.request_timeout)

        for attempt in range(self.max_retries + 1):
            try:
                async with self.session.get(
                    base_url,
                    params=params,
                    timeout=timeout
                ) as response:
                    await self._handle_errors(response)

                    # Читаем Resume-Key
                    resume_key = response.headers.get("Resume-Key")
                    self.logger.info(f"[Page 1] resumeKey={resume_key}")

                    try:
                        data = await response.json(content_type=None)
                        if not isinstance(data, list):
                            raise ValueError(f"Non-list JSON: {data}")
                    except Exception as e:
                        text = await response.text()
                        self.logger.error(f"Invalid JSON from CDX API for {domain}: {e}")
                        self.logger.debug(f"Raw response: {text}")
                        return []

                    page_count = 1
                    links_count = len(data) - 1
                    self.logger.info(f"  → page={page_count}, links={links_count}")
                    results.extend(self._process_cdx_response(data))

                    # пагинация
                    while resume_key and (self.max_pages == 0 or page_count < self.max_pages):
                        params["resumeKey"] = resume_key
                        page_count += 1

                        async with self.session.get(
                            base_url,
                            params=params,
                            timeout=timeout
                        ) as paginated_response:
                            await self._handle_errors(paginated_response)

                            resume_key = paginated_response.headers.get("Resume-Key")
                            self.logger.info(f"[Page {page_count}] resumeKey={resume_key}")

                            try:
                                data = await paginated_response.json(content_type=None)
                                if not isinstance(data, list):
                                    raise ValueError(f"Non-list JSON: {data}")
                            except Exception as e:
                                text = await paginated_response.text()
                                self.logger.error(f"Invalid JSON during pagination for {domain}: {e}")
                                self.logger.debug(f"Raw response: {text}")
                                break

                            links_count = len(data) - 1
                            self.logger.info(f"  → page={page_count}, links={links_count}")
                            results.extend(self._process_cdx_response(data))

                    unique = list(dict.fromkeys(results))
                    self.logger.info(f"Total unique snapshots for {domain}: {len(unique)}")
                    return unique

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self.max_retries:
                    self.logger.error(f"Max retries reached for {domain}: {e}")
                    return []
                delay = self.backoff_factor ** attempt
                self.logger.warning(f"Retry {attempt+1} for {domain} in {delay}s: {e}")
                await asyncio.sleep(delay)

        self.logger.error(f"Failed to fetch CDX for {domain} after {self.max_retries} retries")
        return []

    def _process_cdx_response(self, data: list) -> List[str]:
        if not data or len(data) < 2:
            return []

        urls: List[str] = []
        for entry in data[1:]:
            if len(entry) >= 4:
                timestamp, original, status, mime = entry[0], entry[1], entry[2], entry[3]
                wayback_url = self._build_wayback_url(timestamp, original)
                self.logger.debug(f"[CDX] {status} | {mime} → {wayback_url}")
                urls.append(wayback_url)
        return urls

    def _build_wayback_url(self, timestamp: str, original_url: str) -> str:
        encoded = quote(original_url, safe=":/")
        return f"http://web.archive.org/web/{timestamp}id_/{encoded}" 

    async def _handle_errors(self, response: aiohttp.ClientResponse):
        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            self.logger.warning(f"Rate limited. Retrying after {retry_after}s")
            await asyncio.sleep(retry_after)
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message="Rate limit exceeded"
            )

        if response.status != 200:
            text = await response.text()
            self.logger.error(f"HTTP error {response.status}: {text[:200]}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"HTTP error {response.status}"
            )


class CDXManager:
    def __init__(self, cfg, storage):
        self.cfg = cfg
        self.storage = storage
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
