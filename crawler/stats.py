# crawler/stats.py
import asyncio
from collections import defaultdict
from typing import Dict, List, Set
import logging

class Stats:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self.total_snapshots: int = 0
        self.new_snapshots: int = 0
        self.failed_domains: Set[str] = set()

    async def add_snapshots(self, total: int, new: int):
        async with self._lock:
            self.total_snapshots += total
            self.new_snapshots += new

    async def add_failed_domain(self, domain: str):
        async with self._lock:
            self.failed_domains.add(domain)

    async def get_failed_domains(self) -> List[str]:
        async with self._lock:
            return sorted(self.failed_domains)

    async def increment(self, key: str, amount: int = 1):
        async with self._lock:
            self._counters[key] += amount

    async def get(self, key: str) -> int:
        async with self._lock:
            return self._counters.get(key, 0)

    async def snapshot(self) -> Dict[str, int]:
        async with self._lock:
            return dict(self._counters)