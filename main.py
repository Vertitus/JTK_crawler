import asyncio
from config import load_config
from crawler.logger import init_logger
from crawler.signals import setup_signal_handlers
from crawler.scheduler import Scheduler
from crawler.fetcher import Fetcher
from crawler.parser import Parser
from crawler.storage import Storage
from crawler.stats import Stats

async def main():
    cfg = load_config('config.yaml')
    init_logger(cfg.log)
    stats = Stats()
    storage = Storage(cfg.storage)
    fetcher = Fetcher(cfg.fetch)
    parser = Parser(cfg.parser)
    scheduler = Scheduler(cfg.scheduler, storage, fetcher, parser, stats)

    setup_signal_handlers(scheduler.shutdown)
    await scheduler.run()

if __name__ == '__main__':
    asyncio.run(main())