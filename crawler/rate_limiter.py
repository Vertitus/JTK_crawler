# crawler/rate_limiter.py
import asyncio
import time

class RateLimiter:
    def __init__(self, min_interval: float = 1.0):
        self._min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def __aenter__(self):
        await self._lock.acquire()
        now = time.monotonic()
        wait = self._min_interval - (now - self._last)
        if wait > 0:
            await asyncio.sleep(wait)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._last = time.monotonic()
        self._lock.release()
        return False
