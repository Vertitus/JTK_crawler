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
from crawler.rate_limiter import RateLimiter

async def log_progress(stats: Stats):
    while True:
        progress = await stats.get_progress()
        logging.info(f"[Progress] {progress:.2f}%")
        await asyncio.sleep(10)



async def main():
    try:
        logging.basicConfig(level=logging.DEBUG)
        print("[1/5] Loading config...")
        cfg = load_config('config.yaml')
        
        print("[2/5] Initializing logger...")
        init_logger(cfg.log)
        
        print("[3/5] Creating core components...")
        stats = Stats()
        storage = Storage(cfg.storage, stats)
        rate_limiter = RateLimiter(min_interval=1.5)
        
        cfg.fetch.network = cfg.network  
        fetcher = Fetcher(cfg.fetch)
        
        print("[4/5] Initializing fetcher session...")
        await fetcher._ensure_session()
        print(f"Fetcher session: {fetcher.session}")
        
        if not fetcher.session:
            raise RuntimeError("Fetcher session not initialized")

        parser = Parser(cfg.parser)
        
        print("[5/5] Starting scheduler...")

        print("Testing Tor connection...")
        test_url = "https://check.torproject.org/"
        content, final_url, status = await fetcher.fetch(test_url)
        if content and "Congratulations. This browser is configured to use Tor" in content:
            print("✅ Tor connection verified!")
        else:
            print("⚠️ Tor not detected! Check if Tor is running and socks URL is correct.")
            return

        scheduler = Scheduler(cfg.scheduler, cfg.cdx, storage, fetcher, parser, stats)
        setup_signal_handlers(scheduler.shutdown)
        
        
        # Запуск задачи прогресса
        progress_task = asyncio.create_task(log_progress(stats))
        
        print("=== Starting crawler ===")
        await scheduler.run()
        
        # Остановка задачи прогресса
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass
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