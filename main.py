import logging
import asyncio
from config import load_config
from crawler.logger import init_logger
from crawler.signals import setup_signal_handlers
from crawler.scheduler import Scheduler
from crawler.fetcher import Fetcher
from crawler.parser import Parser
from crawler.storage import Storage
from crawler.stats import Stats

async def log_progress(stats: Stats):
    while True:
        progress = await stats.get_progress()
        logging.info(f"[Progress] {progress:.2f}%")
        await asyncio.sleep(10)

async def main():
    try:
        print("[1/5] Loading config...")
        cfg = load_config('config.yaml')
        
        print("[2/5] Initializing logger...")
        init_logger(cfg.log)
        
        print("[3/5] Creating core components...")
        stats = Stats()
        storage = Storage(cfg.storage, stats)
        fetcher = Fetcher(cfg.fetch)
        
        print("[4/5] Initializing fetcher session...")
        await fetcher._ensure_session()
        print(f"Fetcher session: {fetcher.session}")
        
        parser = Parser(cfg.parser)
        
        print("[5/5] Starting scheduler...")
        scheduler = Scheduler(cfg.scheduler, cfg.cdx, storage, fetcher, parser, stats)
        setup_signal_handlers(scheduler.shutdown)
        
        # Запуск задачи прогресса
        progress_task = asyncio.create_task(log_progress(stats))
        
        print("=== Starting crawler ===")
        await scheduler.run()
        
        # Остановка задачи прогресса
        progress_task.cancel()
        await asyncio.sleep(1)
        print("=== Crawler finished ===")

    except Exception as e:
        logging.error(f"!!! Critical error: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        print("Process terminated")