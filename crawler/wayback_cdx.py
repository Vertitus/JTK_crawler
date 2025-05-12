# crawler/wayback_cdx.py
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
        max_pages: int = 100,
        page_size: int = 5000
    ):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.request_timeout = request_timeout
        self.max_pages = max_pages    # 0 = no limit on pages
        self.page_size = page_size    # number of URLs per request
        self.logger = logging.getLogger("CDXClient")

    async def fetch_snapshots(
        self,
        domain: str,
        from_date: str = "20040101000000",
        to_date: str = "20041231235959"
    ) -> List[str]:
        base_url = "https://web.archive.org/cdx/search/cdx"
        results: List[str] = []
        params = {
            "url": f"{domain}/*",
            "matchType": "domain",
            "from": from_date,
            "to": to_date,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype",
            "filter": ["statuscode:200", "mimetype:text/html"],
            "collapse": "urlkey",
            "limit": self.page_size,
            "showResumeKey": "true",
        }

        try:
            for attempt in range(self.max_retries + 1):
                try:
                    async with self.session.get(
                        base_url,
                        params=params,
                        timeout=self.request_timeout
                    ) as response:
                        await self._handle_errors(response)

                        try:
                            data = await response.json(content_type=None)
                            if not isinstance(data, list):
                                raise ValueError(f"Non-list JSON response: {data}")
                        except Exception as e:
                            text = await response.text()
                            self.logger.error(f"Invalid JSON response from CDX API for {domain}: {e}")
                            self.logger.debug(f"Raw response: {text}")
                            return []

                        results.extend(self._process_cdx_response(data))

                        # Pagination: follow Resume-Key until no more or page limit reached
                        page = 1
                        while "Resume-Key" in response.headers and (self.max_pages == 0 or page < self.max_pages):
                            params["resumeKey"] = response.headers["Resume-Key"]
                            async with self.session.get(
                                base_url,
                                params=params,
                                timeout=self.request_timeout
                            ) as paginated_response:
                                await self._handle_errors(paginated_response)

                                try:
                                    data = await paginated_response.json(content_type=None)
                                    if not isinstance(data, list):
                                        raise ValueError(f"Non-list JSON response: {data}")
                                except Exception as e:
                                    text = await paginated_response.text()
                                    self.logger.error(f"Invalid JSON response during pagination for {domain}: {e}")
                                    self.logger.debug(f"Raw response: {text}")
                                    break

                                results.extend(self._process_cdx_response(data))
                                page += 1

                        # Deduplicate and apply optional total limit
                        unique = list(dict.fromkeys(results))  # preserve order
                        if self.max_pages > 0:
                            unique = unique[: self.max_pages * self.page_size]

                        self.logger.info(f"Fetched {len(unique)} snapshots for domain {domain}")
                        return unique

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == self.max_retries:
                        raise
                    delay = self.backoff_factor ** attempt
                    self.logger.warning(f"Retry {attempt+1} for {domain} in {delay}s")
                    await asyncio.sleep(delay)

        except Exception as e:
            self.logger.error(f"Failed to fetch CDX for {domain}: {str(e)}")
            self.logger.debug(f"Params: {params}")
            return []

    def _process_cdx_response(self, data: list) -> List[str]:
        if not data or len(data) < 2:
            return []

        urls: List[str] = []
        for entry in data[1:]:
            if len(entry) >= 2:
                timestamp, original = entry[0], entry[1]
                urls.append(self._build_wayback_url(timestamp, original))
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
            self.logger.error("Domains file not found")
            return []

    async def _filter_new_urls(self, urls: List[str]) -> List[str]:
        return [url for url in urls if not self.storage.is_visited(url)]
