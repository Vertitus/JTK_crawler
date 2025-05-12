import asyncio
import logging
from asyncio import PriorityQueue
from dataclasses import dataclass, field
from crawler.wayback_cdx import CDXManager
from typing import List, Dict, Optional

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

        import logging
        self.logger = logging.getLogger("Scheduler")

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
        try:
            cdx = CDXManager(self.cdx_cfg, self.storage)
            await cdx.initialize(self.fetcher.session)
            self.logger.info("Bootstrapping seeds from CDX...")
            seed_urls = await cdx.get_seed_urls()
            self.logger.info(f"Total seed URLs from CDX: {len(seed_urls)}")
            
            # Устанавливаем общее число URL для прогресса
            await self.stats.set_total_urls(len(seed_urls))
            
            for url in seed_urls:
                await self.enqueue_url(url, priority=0, depth=0)
        except Exception as e:
            self.logger.error(f"Failed to bootstrap from CDX: {e}")

        # Обычные семена из конфигурации
        self.logger.info(f"Adding {len(self.scheduler_cfg.seeds)} static seed URLs")
        for url in self.scheduler_cfg.seeds:
            await self.enqueue_url(url, priority=0, depth=0)

    async def enqueue_url(self, url: str, priority: int = 5, depth: int = 0):
            """
            Добавляет URL в очередь, если глубина не превышена и URL ещё не посещён.
            """
            # Ограничение глубины
            if depth > self.max_depth:
                return
    
            # Защита от повторного посещения
            async with self.storage.visited_lock:
                if self.storage.is_visited(url):
                    return
                self.storage.add_visited(url)
    
            # Помещаем в очередь
            item = PrioritizedItem(priority, depth, url)
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

            await self._process_url(item.url, item.depth)
            self.queue.task_done()


    async def _process_url(self, url: str, depth: int):
        """
        Обрабатывает один URL: скачивает контент, парсит, сохраняет результаты и добавляет новые URL.
        """
        try:
            content, final_url = await self.fetcher.fetch(url)
            if not content:
                self.logger.warning(f"No content for {url}, skipping.")
                return

            self.logger.info(f"Fetched {len(content)} bytes from {final_url}")

            matches, discovered_urls = self.parser.parse(content, final_url)
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

            # Логируем найденные ссылки и ставим их в очередь
            for new_url in discovered_urls:
                self.logger.debug(f"Discovered URL: {new_url}")
                await self.enqueue_url(new_url, priority=depth + 1, depth=depth + 1)

        except Exception as e:
            # Учёт ошибок
            await self.stats.increment("error_count")
            self.logger.exception(f"Error processing {url}: {e}")


    # crawler/scheduler.py
    async def shutdown(self):  # <-- Добавьте этот метод
        """
        Корректное завершение работы планировщика.
        """
        if not self.is_running:
            return
        
        self.is_running = False
        logging.info("Shutting down scheduler...")
        
        # Отправляем poison-pill каждому воркеру
        for _ in range(self.scheduler_cfg.max_concurrent):
            await self.queue.put(PrioritizedItem(priority=100, depth=0, url=self.poison_pill))
        
        # Ожидаем завершения задач
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        # Выводим статистику
        total_snapshots = self.storage.stats.total_snapshots
        new_snapshots = self.storage.stats.new_snapshots
        processed = await self.stats.get("processed_urls")
        matches = await self.stats.get("match_count")
        failed_domains = await self.storage.stats.get_failed_domains()
        
        logging.info("\n=== Final Statistics ===")
        logging.info(f"Total snapshots found:     {total_snapshots}")
        logging.info(f"New snapshots processed:   {new_snapshots}")
        logging.info(f"URLs crawled:              {processed}")
        logging.info(f"Keyword matches found:     {matches}")
        
        if failed_domains:
            logging.info("\n=== Problem Domains ===")
            for domain in failed_domains:
                logging.info(f" - {domain}")
        
        # Закрываем соединения
        await self.fetcher.close()
        await self.storage.persist_matches()
