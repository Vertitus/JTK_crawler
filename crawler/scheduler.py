import asyncio
import logging
from asyncio import PriorityQueue
from dataclasses import dataclass, field
from crawler.wayback_cdx import CDXManager
from typing import List, Optional
from crawler.fetcher import normalize_url

@dataclass(order=True)
class PrioritizedItem:
    priority: int
    depth: int
    url: str = field(compare=False)

class Scheduler:
    def __init__(
        self,
        scheduler_cfg,
        cdx_cfg,
        storage,
        fetcher,
        parser,
        stats
    ):
        # Разделение конфигураций
        self.scheduler_cfg = scheduler_cfg
        self.cdx_cfg = cdx_cfg
        self.storage = storage
        self.fetcher = fetcher
        self.parser = parser
        self.stats = stats
        self.semaphore = asyncio.Semaphore(self.config.concurrency)
        self.logger = logging.getLogger("Scheduler")

        # Очередь приоритетов
        self.queue = PriorityQueue()
        self.workers: List[asyncio.Task] = []
        self.is_running = True
        self.poison_pill = scheduler_cfg.poison_pill
        self.max_depth = scheduler_cfg.max_depth

    async def run(self):
        """
        Запускает процесс планировщика: инициализация семян, запуск воркеров и ожидание их завершения.
        """
        # Bootstrap начальных URL
        await self._bootstrap_seeds()

        # Запуск воркеров
        for _ in range(self.scheduler_cfg.max_concurrent):
            task = asyncio.create_task(self._worker_loop())
            self.workers.append(task)

        self.logger.info("Started all workers")

        try:
            # Ожидание завершения всех воркеров
            await asyncio.gather(*self.workers)
        except asyncio.CancelledError:
            self.logger.info("Workers cancelled.")
        finally:
            self.logger.info("All workers shut down.")

    async def _bootstrap_seeds(self):
        """
        Загружает начальные URL: сначала из Wayback Machine, затем из конфигурации.
        """
        try:
            cdx = CDXManager(self.cdx_cfg, self.storage)
            await cdx.initialize(self.fetcher.session)
            self.logger.info("Bootstrapping seeds from CDX...")
            seed_urls = await cdx.get_seed_urls()
            self.logger.info(f"Total seed URLs from CDX: {len(seed_urls)}")

            # Устанавливаем общее число URL для прогресса (CDX + static seeds)
            total_seeds = len(seed_urls) + len(self.scheduler_cfg.seeds)
            await self.stats.set_total_urls(total_seeds)

            # Добавляем URL из CDX
            for url in seed_urls:
                await self.enqueue_url(url, priority=0, depth=0)

        except Exception as e:
            self.logger.error(f"Failed to bootstrap from CDX: {e}")

        # Добавляем статические семена (после CDX)
        self.logger.info(f"Adding {len(self.scheduler_cfg.seeds)} static seed URLs")
        for url in self.scheduler_cfg.seeds:
            await self.enqueue_url(url, priority=0, depth=0)

    async def enqueue_url(self, url: str, priority: int = 5, depth: int = 0):
        if depth > self.max_depth:
            return

        norm = normalize_url(url)
        if not norm:
            self.logger.debug(f"Skipping invalid/unnormalizable URL: {url}")
            return

        self.logger.debug(f"→ Adding to queue: {norm} depth={depth}")

        async with self.storage.visited_lock:
            if self.storage.is_visited(norm):
                return
            self.storage.add_visited(norm)

        item = PrioritizedItem(priority, depth, norm)
        await self.queue.put(item)

    async def _worker_loop(self):
        """
        Основной цикл воркера: извлекает URL из очереди и обрабатывает их.
        """
        worker_name = asyncio.current_task().get_name()
        while self.is_running:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=5)
                self.logger.info(f"[{worker_name}] Dequeued URL: {item.url} (depth={item.depth})")
            except asyncio.TimeoutError:
                continue

            if item.url == self.poison_pill:
                self.logger.info(f"[{worker_name}] Received poison pill, stopping.")
                break

            async with self.semaphore:
                await self._process_url(item.url, item.depth)
            self.queue.task_done()

    async def _process_url(self, url: str, depth: int):

        async with self.semaphore:
            """
            Обрабатывает один URL: скачивает контент, парсит, сохраняет результаты и добавляет новые URL.
            """
            try:
                content, final_url, status = await self.fetcher.fetch(url)
                if not content:
                    self.logger.warning(f"No content for {url}, skipping.")
                    return

                self.logger.info(f"Fetched {len(content)} bytes from {final_url}")

                matches, discovered_urls = self.parser.parse(content, final_url, depth, http_status=status)
                if matches:
                    await self.storage.save_matches(final_url, matches)
                    self.logger.info(f"  → {len(matches)} keyword matches at {final_url}")

                # Обновляем статистику
                await self.stats.increment("processed_urls")
                processed = await self.stats.get("processed_urls")
                total = await self.stats.get_total_urls()
                pct = (processed / total * 100) if total else 0
                self.logger.info(f"Progress: {processed}/{total} URLs ({pct:.2f}%)")

                # Фиксируем количество совпадений
                await self.stats.increment("match_count", len(matches))

                # Добавляем обнаруженные URL в очередь
                for new_url in discovered_urls:
                    self.logger.debug(f"Discovered URL: {new_url}")
                    await self.enqueue_url(new_url, priority=depth + 1, depth=depth + 1)

            except Exception as e:
                await self.stats.increment("error_count")
                self.logger.exception(f"Error processing {url}: {e}")

    async def shutdown(self):
        """
        Корректное завершение работы планировщика.
        """
        if not self.is_running:
            return
        self.is_running = False
        self.logger.info("Shutting down scheduler...")

        # Отправляем poison-pill каждому воркеру
        for _ in range(self.scheduler_cfg.max_concurrent):
            await self.queue.put(PrioritizedItem(priority=100, depth=0, url=self.poison_pill))

        # Ожидаем завершения задач
        await asyncio.gather(*self.workers, return_exceptions=True)

        # Закрываем соединения и сохраняем результаты
        await self.fetcher.close()
        await self.storage.persist_matches()

# Точка входа
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from config import SchedulerConfig, CDXConfig  # адаптируйте импорт
    from fetcher import Fetcher
    from parser import Parser
    from storage import Storage
    from stats import Stats

    # Инициализация компонентов
    scheduler_cfg = SchedulerConfig()
    cdx_cfg = CDXConfig()
    storage = Storage()
    fetcher = Fetcher()
    parser = Parser()
    stats = Stats()

    scheduler = Scheduler(scheduler_cfg, cdx_cfg, storage, fetcher, parser, stats)
    asyncio.run(scheduler.run())
