import os
import hashlib
import json
import time
from pybloom_live import BloomFilter
from collections import deque
from typing import Set, Optional

class Storage:
    def __init__(self, cfg):
        self.cache_dir = cfg.get('cache_dir', 'cache')
        self.bloom_capacity = cfg.get('bloom_capacity', 1000000)
        self.bloom_error_rate = cfg.get('bloom_error_rate', 0.001)
        self.cache_ttl_days = cfg.get('cache_ttl_days', 7)
        
        # Инициализация Bloom filter
        self.bloom = BloomFilter(capacity=self.bloom_capacity, error_rate=self.bloom_error_rate)
        
        # Директория для кэша
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # Очередь для кэширования старых записей
        self.cache_queue = deque(maxlen=self.bloom_capacity)
        
    def is_visited(self, url: str) -> bool:
        """
        Проверяет, был ли URL уже посещён с помощью Bloom filter.
        """
        return url in self.bloom

    def add_visited(self, url: str):
        """
        Добавляет URL в список посещённых.
        """
        if not self.is_visited(url):
            self.bloom.add(url)
            self.cache_queue.append(url)
            self._save_bloom_filter()

    def _save_bloom_filter(self):
        """
        Сохраняет Bloom filter в файл.
        """
        with open(os.path.join(self.cache_dir, 'bloom_filter.json'), 'w') as f:
            json.dump([url for url in self.cache_queue], f)

    def load_bloom_filter(self):
        """
        Загружает Bloom filter из файла, если он существует.
        """
        bloom_filter_file = os.path.join(self.cache_dir, 'bloom_filter.json')
        if os.path.exists(bloom_filter_file):
            with open(bloom_filter_file, 'r') as f:
                visited_urls = json.load(f)
                for url in visited_urls:
                    self.bloom.add(url)
        
    def is_cache_valid(self, cache_file: str) -> bool:
        """
        Проверяет, действителен ли кэш. Если срок действия истёк, то кэш удаляется.
        """
        if not os.path.exists(cache_file):
            return False
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age > self.cache_ttl_days * 86400:  # 86400 секунд в день
            os.remove(cache_file)
            return False
        return True

    def get_from_cache(self, url: str) -> Optional[str]:
        """
        Получает контент из кэша, если он ещё действителен.
        """
        cache_file = self._get_cache_filename(url)
        if self.is_cache_valid(cache_file):
            with open(cache_file, 'r') as f:
                return f.read()
        return None

    def save_to_cache(self, url: str, content: str):
        """
        Сохраняет контент в кэш.
        """
        cache_file = self._get_cache_filename(url)
        with open(cache_file, 'w') as f:
            f.write(content)

    def _get_cache_filename(self, url: str) -> str:
        """
        Генерирует имя файла для кэширования контента URL.
        """
        hash_url = hashlib.sha256(url.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_url}.html")
