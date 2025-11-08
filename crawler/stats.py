# crawler/stats.py
import asyncio
from collections import defaultdict
from typing import Dict, List, Set
import logging
import time

class Stats:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self.total_snapshots: int = 0
        self.new_snapshots: int = 0
        self.failed_domains: Set[str] = set()
        self.total_urls: int = 0  # общее число URL для обработки
        self.time_series_processed = []  # list of (ts, processed_count)
        self.time_series_matches = []
        self.time_series_errors = []

    async def snapshot_time_series(self):
        async with self._lock:
            ts = int(time.time())
            processed = self._counters.get("processed_urls", 0)
            matches = self._counters.get("match_count", 0)
            errors = self._counters.get("error_count", 0)
            self.time_series_processed.append((ts, processed))
            self.time_series_matches.append((ts, matches))
            self.time_series_errors.append((ts, errors))

    async def export_csv(self, path="stats_timeseries.csv"):
        import csv
        async with self._lock:
            rows = []
            # assume same lengths and aligned by index
            length = max(len(self.time_series_processed), len(self.time_series_matches), len(self.time_series_errors))
            for i in range(length):
                p = self.time_series_processed[i] if i < len(self.time_series_processed) else (None, None)
                m = self.time_series_matches[i] if i < len(self.time_series_matches) else (None, None)
                e = self.time_series_errors[i] if i < len(self.time_series_errors) else (None, None)
                rows.append([p[0], p[1], m[1], e[1]])
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["ts", "processed", "matches", "errors"])
                writer.writerows(rows)

    async def get_progress(self) -> float:
        """
        Возвращает процент обработанных URL: processed_urls / total_urls * 100
        """
        async with self._lock:
            processed = self._counters.get("processed_urls", 0)
            if self.total_urls:
                return processed / self.total_urls * 100
            return 0.0

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

    async def set_total_urls(self, total: int):
        """
        Асинхронно устанавливает общее количество URL, ожидающих обработки.
        """
        async with self._lock:
            self.total_urls = total

    async def get_total_urls(self) -> int:
        """Возвращает общее число URL для обработки."""
        async with self._lock:
            return self.total_urls

    async def snapshot(self) -> Dict[str, int]:
        async with self._lock:
            # Формируем снимок всех счетчиков и метрик
            snapshot = dict(self._counters)
            snapshot.update({
                'total_snapshots': self.total_snapshots,
                'new_snapshots': self.new_snapshots,
                'total_urls': self.total_urls
            })
            return snapshot
