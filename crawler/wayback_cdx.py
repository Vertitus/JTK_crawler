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
        max_pages: int = 100
    ):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.request_timeout = request_timeout
        self.max_pages = max_pages
        self.logger = logging.getLogger("CDXClient")
        self.logger = logging.getLogger("CDXManager")

    async def fetch_snapshots(
        self,
        domain: str,
        from_date: str = "20040101000000",
        to_date: str = "20041231235959"
    ) -> List[str]:
        """Fetch all archived URLs for domain within date range"""
        base_url = "https://web.archive.org/cdx/search/cdx"
        results = []
        collapse = "urlkey"
        page_size = 5000
        params = {
            "url": f"{domain}/*",
            "matchType": "domain",
            "filter": "statuscode:200",
            "mimetype": "text/html",
            "from": from_date,
            "to": to_date,
            "collapse": collapse,
            "showResumeKey": "true",
            "limit": page_size,
            "output": "json"
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
                        data = await response.json()
                        results.extend(self._process_cdx_response(data))
                        
                        # Pagination handling
                        while "Resume-Key" in response.headers and len(results) < self.max_pages * page_size:
                            params["resumeKey"] = response.headers["Resume-Key"]
                            async with self.session.get(
                                base_url,
                                params=params,
                                timeout=self.request_timeout
                            ) as paginated_response:
                                await self._handle_errors(paginated_response)
                                data = await paginated_response.json()
                                results.extend(self._process_cdx_response(data))
                                
                        return list(set(results))[:self.max_pages * page_size]

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == self.max_retries:
                        raise
                    delay = self.backoff_factor ** attempt
                    self.logger.warning(f"Retry {attempt+1} for {domain} in {delay}s")
                    await asyncio.sleep(delay)

        except Exception as e:
            self.logger.error(f"Failed to fetch CDX for {domain}: {str(e)}")
            return []

    def _process_cdx_response(self, data: list) -> List[str]:
        """Process raw CDX API response"""
        if not data or len(data) < 2:
            return []

        urls = []
        for entry in data[1:]:  # Skip header
            if len(entry) >= 5:
                timestamp = entry[1]
                original = entry[2]
                url = self._build_wayback_url(timestamp, original)
                urls.append(url)
        return urls

    def _build_wayback_url(self, timestamp: str, original_url: str) -> str:
        """Build normalized Wayback Machine URL"""
        encoded = quote(original_url, safe=":/")
        return f"http://web.archive.org/web/{timestamp}id_/{encoded}"

    async def _handle_errors(self, response: aiohttp.ClientResponse):
        """Handle HTTP errors and rate limits"""
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

    async def initialize(self, session: aiohttp.ClientSession):
        self.client = WaybackCDXClient(
            session=session,
            max_retries=self.cfg.max_retries,
            backoff_factor=self.cfg.backoff_factor,
            request_timeout=self.cfg.request_timeout,
            max_pages=self.cfg.max_pages
        )

    async def get_seed_urls(self) -> List[str]:
        """Get all seed URLs from Wayback Machine"""
        if not self.client:
            raise RuntimeError("CDXClient not initialized")

        domains = self._load_domains()
        all_urls = []
        
        for domain in domains:
            self.logger.info(f"Fetching CDX for {domain}")
            urls = await self.client.fetch_snapshots(domain)
            filtered = await self._filter_new_urls(urls)
            all_urls.extend(filtered)
            
        return all_urls

    def _load_domains(self) -> List[str]:
        """Load target domains from file"""
        try:
            with open(self.cfg.target_domains_file, "r") as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.logger.error("Domains file not found")
            return []

    async def _filter_new_urls(self, urls: List[str]) -> List[str]:
        """Filter URLs using Bloom filter"""
        return [url for url in urls if not self.storage.is_visited(url)]