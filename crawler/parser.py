# crawler/parser.py
import json
import os
import time
import re
import logging
from typing import List, Tuple, Dict, Any
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, Comment

class Parser:
    def __init__(self, cfg):
        """
        Инициализация парсера.
        cfg — это инстанс ParserConfig с полем:
          - patterns_file: str
        """
        self.logger = logging.getLogger(__name__)
        self.metrics_file = "metrics.json"  # Путь к файлу с метриками
        self.keyword_patterns = self._compile_patterns(cfg.patterns_file)

        # Проверка и создание файла с метриками, если его нет
        if not os.path.exists(self.metrics_file):
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=4)

    def _save_metrics(self, url, matches, depth, status):
        """Сохранить метрики в JSON файл."""
        timestamp = time.time()  # Текущее время в формате UNIX
        new_data = {
            "matches": [{"match": m['value'], "type": m['type'], "context": m['context']} for m in matches],
            "depth": depth,
            "timestamp": timestamp,
            "status": status,
        }
        try:
            # Чтение текущих метрик
            with open(self.metrics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Обновление данных для текущего URL
            data[url] = new_data

            # Запись обновленных данных
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            self.logger.error(f"Error saving metrics for {url}: {e}")

    def get_status(self, url):
        """Возвращает HTTP статус код для указанного URL."""
        try:
            response = requests.get(url)
            return response.status_code  # Вернёт HTTP статус код (например, 200)
        except requests.RequestException:
            return "Ошибка"

    def _compile_patterns(self, patterns_file: str) -> List[re.Pattern]:
        """Загружает и компилирует регулярные выражения для поиска ключевых слов."""
        patterns = []
        try:
            with open(patterns_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    pat = re.escape(line).replace(r'\ ', r'\s+')
                    patterns.append(re.compile(rf"\b{pat}\b", re.IGNORECASE))
        except Exception as e:
            self.logger.error(f"Failed to load patterns: {e}")
        return patterns

    def parse(self, html: str, base_url: str, depth: int) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Парсит HTML:
          - возвращает список найденных совпадений (ключевых слов и ссылок)
          - возвращает список URL для дальнейшего обхода
        """
        matches: List[Dict[str, Any]] = []
        discovered_urls: List[str] = []

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # 1) Ищем в тексте страницы
            text = soup.get_text(separator=' ')
            for pat in self.keyword_patterns:
                for m in pat.findall(text):
                    matches.append({'value': m, 'type': 'text', 'context': ''})

            # 2) Ищем в атрибутах тегов
            for tag in soup.find_all(True):
                for attr, val in tag.attrs.items():
                    if isinstance(val, str):
                        hay = val
                    elif isinstance(val, list):
                        hay = " ".join(val)
                    else:
                        continue
                    for pat in self.keyword_patterns:
                        for m in pat.findall(hay):
                            matches.append({
                                'value': m,
                                'type': 'attr',
                                'context': f"<{tag.name} {attr}={val}>"
                            })

            # 3) Извлекаем ссылки и ищем в URL
            for tag in soup.find_all(['a', 'img', 'script', 'iframe', 'link']):
                for attr in ('href', 'src'):
                    url = tag.get(attr)
                    if not url: 
                        continue
                    full = urljoin(base_url, url)
                    discovered_urls.append(full)
                    for pat in self.keyword_patterns:
                        for m in pat.findall(full):
                            matches.append({
                                'value': m,
                                'type': 'link' if tag.name == 'a' else 'img',
                                'context': full
                            })

        except Exception as e:
            self.logger.error(f"Parsing error at {base_url}: {e}")

        # убираем дубли
        seen = set()
        unique_matches = []
        for hit in matches:
            key = (hit['value'], hit['type'], hit['context'])
            if key not in seen:
                seen.add(key)
                unique_matches.append(hit)

        discovered_urls = list(dict.fromkeys(discovered_urls))

        # Сохраняем метрики
        status = self.get_status(base_url)
        self._save_metrics(base_url, unique_matches, depth, status)

        return unique_matches, discovered_urls
