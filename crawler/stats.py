# crawler/stats.py
import asyncio
from collections import defaultdict
from typing import Dict
import logging

class Stats:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._counters: Dict[str, int] = defaultdict(int)

    async def increment(self, key: str, amount: int = 1):
        async with self._lock:
            self._counters[key] += amount
            logging.debug(f"Stats incremented: {key} -> {self._counters[key]}")

    async def get(self, key: str) -> int:
        async with self._lock:
            return self._counters.get(key, 0)

    async def snapshot(self) -> Dict[str, int]:
        async with self._lock:
            return dict(self._counters)

    def export_sync(self) -> Dict[str, int]:
        """Безопасный экспорт без лока для логгирования в момент завершения."""
        return dict(self._counters)

    async def log_summary(self):
        snapshot = await self.snapshot()
        logging.info("Crawler statistics summary:")
        for key, value in snapshot.items():
            logging.info(f"  {key}: {value}")
