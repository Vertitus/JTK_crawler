# crawler/parser.py

import re
import logging
from typing import List, Tuple
from bs4 import BeautifulSoup

class Parser:
    def __init__(self, cfg):
        """
        cfg — это инстанс ParserConfig с полем:
          - patterns_file: str
        """
        # Создаем логгер
        self.logger = logging.getLogger(__name__)

        # Компилируем шаблоны ключевых слов
        self.keyword_patterns = self._compile_patterns(cfg.patterns_file)

    def _compile_patterns(self, patterns_file: str) -> List[re.Pattern]:
        """
        Загружает файл с ключевыми словами (одна строка = один шаблон)
        и компилирует их в регулярные выражения.
        """
        patterns = []
        try:
            with open(patterns_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    # Экранируем строку, чтобы искать буквальное совпадение
                    pat = re.escape(line).replace(r'\ ', r'\s+')
                    patterns.append(re.compile(rf"\b{pat}\b", re.IGNORECASE))
        except Exception as e:
            self.logger.error(f"Failed to load patterns from {patterns_file}: {e}")
        return patterns

    def parse(self, html: str, base_url: str) -> Tuple[List[str], List[str]]:
        """
        Парсит HTML:
          - возвращает список найденных совпадений ключевых слов
          - список новых URL для обхода
        """
        matches = []
        discovered_urls = []

        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text(separator=' ')
            
            # Ищем совпадения в тексте
            for pat in self.keyword_patterns:
                for m in pat.findall(text):
                    matches.append(m)

            # Извлекаем ссылки
            for tag in soup.find_all('a', href=True):
                url = tag['href']
                if url.startswith('http'):
                    discovered_urls.append(url)

        except Exception as e:
            self.logger.error(f"Parsing error at {base_url}: {e}")

        return matches, discovered_urls
