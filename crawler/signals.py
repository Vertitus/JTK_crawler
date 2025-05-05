# crawler/signals.py
import asyncio
import signal
import logging

def setup_signal_handlers(shutdown_callback):
    loop = asyncio.get_event_loop()

    def _handler(signame):
        logging.info(f"Received {signame}, initiating shutdown...")
        asyncio.create_task(shutdown_callback())

    for signame in ('SIGINT', 'SIGTERM'):
        try:
            loop.add_signal_handler(getattr(signal, signame), lambda: _handler(signame))
        except NotImplementedError:
            # Windows fallback
            signal.signal(getattr(signal, signame), lambda s, f: asyncio.create_task(shutdown_callback()))
