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
            await self.stats.set_total_urls(len(seed_urls))
            print("Seed URLs:", seed_urls)
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
            # Скачиваем контент
            content, final_url = await self.fetcher.fetch(url)
            if not content:
                return

            # Парсим HTML
            matches, discovered_urls = self.parser.parse(content, final_url)

            # Сохраняем совпадения ключевых слов
            if matches:
                await self.storage.save_matches(final_url, matches)

            # Обновляем статистику и прогресс
            await self.stats.increment_processed()
            progress = await self.stats.get_progress()
            current_processed = await self.stats.get("processed_urls")
            total_urls = await self.stats.get("total_urls")

            logging.info(
                f"Progress: {progress:.2f}% | "
                f"Processed: {current_processed}/{total_urls} | "
                f"URL: {final_url}"
            )

            # Фиксируем метрики
            await self.stats.increment("match_count", len(matches))
            await self.stats.increment("processed_urls")

            # Добавляем новые URL в очередь
            for new_url in discovered_urls:
                await self.enqueue_url(new_url, priority=10, depth=depth + 1)

        except Exception as e:
            # Фиксируем ошибку и логируем
            await self.stats.increment("error_count")
            logging.exception(f"Failed to process {url}: {str(e)}")
            if self.scheduler_cfg.debug:
                logging.debug(f"Error context - Content: {content[:200]}...")

        except Exception as e:
            self.stats.record_error(e)
            logging.exception(f"Failed to process {url}: {e}")

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
