import asyncio
import json
import os
import time
import hashlib
from collections import defaultdict, deque
from typing import List, Deque, Optional, Set, DefaultDict
from pybloom_live import BloomFilter


class Storage:
    def __init__(self, cfg, stats, logger=None):
        self.cfg = cfg
        self.stats = stats
        self.logger = logger
        self.cache_dir = cfg.cache_dir
        self.bloom_capacity = cfg.bloom_capacity
        self.bloom_error_rate = cfg.bloom_error_rate
        self.cache_ttl_days = cfg.cache_ttl_days

        # --- результат и метрики ---
        self.result_path = getattr(cfg, "result_path", "result.json")

        # --- память и блокировки ---
        self._matches: DefaultDict[str, List[str]] = defaultdict(list)
        self.visited_lock = asyncio.Lock()
        self.lock = asyncio.Lock()

        # --- bloom ---
        self.bloom = BloomFilter(capacity=self.bloom_capacity,
                                 error_rate=self.bloom_error_rate)
        self.cache_queue: Deque[str] = deque(maxlen=self.bloom_capacity)

        # --- авто-сброс ---
        self.last_save = time.time()
        self.auto_save_interval = 5 * 60  # каждые 5 минут

        # --- фоновая задача для периодического сброса ---
        self._auto_task = asyncio.create_task(self._auto_persist_loop())

        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    # ============================================================
    # --- Сохранение совпадений ---
    # ============================================================

    async def save_matches(self, url: str, matches: list):
        """Сохраняет найденные совпадения и инициирует авто-сброс."""
        if not matches:
            return

        self._matches[url] = matches
        now = time.time()

        # Быстрый сброс, если интервал истёк
        if now - self.last_save >= self.auto_save_interval:
            await self.persist_matches()
            self.last_save = now

    async def persist_matches(self):
        """Сбрасывает все накопленные совпадения в result.json (в читаемом виде)."""
        try:
            if not self._matches:
                return

            os.makedirs(os.path.dirname(self.result_path) or ".", exist_ok=True)

            with open(self.result_path, "w", encoding="utf-8") as f:
                json.dump(self._matches, f, indent=2, ensure_ascii=False)

            if self.logger:
                self.logger.info(f"💾 Saved {len(self._matches)} URLs to {self.result_path}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving results: {e}")
            else:
                print(f"[Storage] Error saving results: {e}")

    async def _auto_persist_loop(self):
        """Фоновая задача, которая каждые 5 минут сбрасывает данные."""
        try:
            while True:
                await asyncio.sleep(self.auto_save_interval)
                await self.persist_matches()
        except asyncio.CancelledError:
            # когда задача отменяется (например при Ctrl+C) — просто выходим
            pass

    # ============================================================
    # --- Завершение и безопасный сброс ---
    # ============================================================

    async def shutdown(self):
        """Останавливает фоновые задачи и сохраняет всё перед выходом."""
        if self.logger:
            self.logger.info("🧹 Shutting down storage, saving final results...")
        if self._auto_task:
            self._auto_task.cancel()
        await self.persist_matches()
        if self.logger:
            self.logger.info("✅ Results saved successfully.")

    # ============================================================
    # --- Остальной код (bloom / cache) ---
    # ============================================================

    def is_visited(self, url: str) -> bool:
        return url in self.bloom

    def add_visited(self, url: str):
        if not self.is_visited(url):
            self.bloom.add(url)
            self.cache_queue.append(url)
            self._save_bloom_filter()

    def _save_bloom_filter(self):
        with open(os.path.join(self.cache_dir, 'bloom_filter.json'), 'w') as f:
            json.dump(list(self.cache_queue), f)

    def load_bloom_filter(self):
        bloom_filter_file = os.path.join(self.cache_dir, 'bloom_filter.json')
        if os.path.exists(bloom_filter_file):
            with open(bloom_filter_file, 'r') as f:
                visited_urls = json.load(f)
                for url in visited_urls:
                    self.bloom.add(url)

    def is_cache_valid(self, cache_file: str) -> bool:
        if not os.path.exists(cache_file):
            return False
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age > self.cache_ttl_days * 86400:
            os.remove(cache_file)
            return False
        return True

    def get_from_cache(self, url: str) -> Optional[str]:
        cache_file = self._get_cache_filename(url)
        if self.is_cache_valid(cache_file):
            with open(cache_file, 'r') as f:
                return f.read()
        return None

    def save_to_cache(self, url: str, content: str):
        cache_file = self._get_cache_filename(url)
        with open(cache_file, 'w') as f:
            f.write(content)

    def _get_cache_filename(self, url: str) -> str:
        hash_url = hashlib.sha256(url.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_url}.html")
