import asyncio
import logging
from asyncio import PriorityQueue
from dataclasses import dataclass, field
from crawler.wayback_cdx import CDXManager

@dataclass(order=True)
class PrioritizedItem:
    priority: int
    depth: int
    url: str = field(compare=False)

class Scheduler:
    def __init__(self, cfg, storage, fetcher, parser, stats):
        self.cfg = cfg
        self.storage = storage
        self.fetcher = fetcher
        self.parser = parser
        self.stats = stats
        self.queue = PriorityQueue(maxsize=cfg.get("queue_size", 10000))
        self.workers = []
        self.is_running = True
        self.poison_pill = cfg.get("poison_pill", "STOP")
        self.max_depth = cfg.get("max_depth", 3)

    async def run(self):
        await self._bootstrap_seeds()

        for _ in range(self.cfg.get("max_concurrent", 8)):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)

        await asyncio.gather(*self.workers)
        logging.info("All workers shut down.")

    async def _bootstrap_seeds(self):
        # 1. Инициализация CDXManager
        cdx_manager = CDXManager(
            cfg=self.cfg.cdx,  # Конфиг из секции cdx
            storage=self.storage
        )
        
        # 2. Передача сессии из Fetcher
        await cdx_manager.initialize(self.fetcher.session)
        
        # 3. Получение URL
        seed_urls = await cdx_manager.get_seed_urls()
        
        # 4. Добавление в очередь
        for url in seed_urls:
            await self.enqueue_url(url, priority=0, depth=0)

    async def enqueue_url(self, url, priority=5, depth=0):
        if depth > self.max_depth:
            return
        async with self.storage.visited_lock:
            if self.storage.is_visited(url):
                return
            self.storage.mark_visited(url)

        item = PrioritizedItem(priority, depth, url)
        await self.queue.put(item)

    async def _worker_loop(self):
        while self.is_running:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=5)
            except asyncio.TimeoutError:
                continue

            if item.url == self.poison_pill:
                logging.info("Received poison pill, stopping worker.")
                break

            await self._process_url(item.url, item.depth)
            self.queue.task_done()

    async def _process_url(self, url, depth):
        try:
            content, final_url = await self.fetcher.fetch(url)
            if not content:
                return

            matches, discovered_urls = self.parser.parse(content, final_url)
            if matches:
                await self.storage.save_matches(final_url, matches)

            self.stats.record_match_count(len(matches))
            self.stats.record_url_processed()

            for new_url in discovered_urls:
                await self.enqueue_url(new_url, priority=10, depth=depth + 1)

        except Exception as e:
            self.stats.record_error(e)
            logging.exception(f"Failed to process {url}: {e}")

    async def shutdown(self):
        if not self.is_running:
            return
        logging.info("Shutting down scheduler...")
        self.is_running = False

        for _ in self.workers:
            await self.queue.put(PrioritizedItem(100, 0, self.poison_pill))

        await asyncio.gather(*self.workers, return_exceptions=True)
        await self.storage.persist()
        await self.stats.persist()
        logging.info("Scheduler shutdown complete.")
