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
        self.cdx_cfg       = cdx_cfg
        self.storage       = storage
        self.fetcher       = fetcher
        self.parser        = parser
        self.stats         = stats

        self.queue        = PriorityQueue(maxsize=scheduler_cfg.queue_size)
        self.workers      = []
        self.is_running   = True
        self.poison_pill  = scheduler_cfg.poison_pill
        self.max_depth    = scheduler_cfg.max_depth

    async def run(self):
        """
        Запускает процесс планировщика: инициализация семян, запуск воркеров и ожидание их завершения.
        """
        await self._bootstrap_seeds()

        # Создание и запуск воркеров
        for _ in range(self.scheduler_cfg.max_concurrent):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)

        # Ожидание завершения всех воркеров
        await asyncio.gather(*self.workers)
        logging.info("All workers shut down.")

    async def _bootstrap_seeds(self):
        """
        Загружает начальные URL: сначала из Wayback Machine, затем из конфигурации.
        """
        # Семена из Wayback CDX по target_domains_file за 2004 год
        try:
            cdx = CDXManager(self.cdx_cfg, self.storage)
            await cdx.initialize(self.fetcher.session)
            seed_urls = await cdx.get_seed_urls()
            for url in seed_urls:
                await self.enqueue_url(url, priority=0, depth=0)
        except Exception as e:
            logging.error(f"Failed to bootstrap from CDX: {e}")

        # Обычные семена из конфигурации
        for url in self.scheduler_cfg.seeds:
            await self.enqueue_url(url, priority=0, depth=0)

    async def enqueue_url(self, url: str, priority: int = 5, depth: int = 0):
        """
        Добавляет URL в очередь, если глубина не превышена и URL ещё не посещён.
        """
        if depth > self.max_depth:
            return
        # Защита от повторного посещения
        async with self.storage.visited_lock:
            if self.storage.is_visited(url):
                return
            self.storage.add_visited(url)

        item = PrioritizedItem(priority, depth, url)
        await self.queue.put(item)

    async def _worker_loop(self):
        """
        Основной цикл воркера: извлекает URL из очереди и обрабатывает их.
        """
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

    async def _process_url(self, url: str, depth: int):
        """
        Обрабатывает один URL: скачивает контент, парсит, сохраняет результаты и добавляет новые URL.
        """
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
        """
        Корректное завершение работы: отправка poison-pill воркерам, сохранение данных.
        """
        if not self.is_running:
            return
        logging.info("Shutting down scheduler...")
        self.is_running = False

        # Отправляем poison-pill каждому воркеру
        for _ in self.workers:
            await self.queue.put(PrioritizedItem(priority=100, depth=0, url=self.poison_pill))

        # Ждём завершения
        await asyncio.gather(*self.workers, return_exceptions=True)

        # Сохраняем состояния
        await self.storage.persist()
        await self.stats.persist()
        logging.info("Scheduler shutdown complete.")
