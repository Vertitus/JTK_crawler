# crawler/parser.py

import re
import logging
from typing import List, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Comment

class Parser:
    def __init__(self, cfg):
        """
        cfg — это инстанс ParserConfig с полем:
          - patterns_file: str
        """
        self.logger = logging.getLogger(__name__)
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
        matches: List[str] = []
        discovered_urls: List[str] = []

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # 1) Собираем текст для поиска:
            parts: List[str] = []

            # 1.1) Видимый текст
            parts.append(soup.get_text(separator=' '))

            # 1.2) <title>
            if soup.title and soup.title.string:
                parts.append(soup.title.string)

            # 1.3) <meta name="description"> и другие meta[name/...]
            for meta in soup.find_all('meta', attrs={'content': True}):
                content = meta.get('content', '').strip()
                if content:
                    parts.append(content)

            # 1.4) Атрибуты alt, title, aria-*, data-* и другие
            for tag in soup.find_all(True):
                for attr, value in tag.attrs.items():
                    if isinstance(value, str):
                        parts.append(value)
                    elif isinstance(value, list):
                        parts.extend(value)

            # 1.5) Содержимое <script> (например, JSON-LD)
            for script in soup.find_all('script'):
                if script.string:
                    parts.append(script.string)

            # 1.6) HTML-комментарии
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                parts.append(comment)

            # Объединяем всё в один большой текст
            full_text = " ".join(parts)

            # 2) Ищем совпадения по ключевым шаблонам
            for pat in self.keyword_patterns:
                for m in pat.findall(full_text):
                    matches.append(m)

            # 3) Извлекаем ссылки из href и src всех релевантных тегов
            for tag in soup.find_all(['a', 'img', 'script', 'iframe', 'link'], href=True):
                url = tag.get('href')
                if url:
                    discovered_urls.append(urljoin(base_url, url))
            for tag in soup.find_all(['img', 'script', 'iframe', 'link'], src=True):
                url = tag.get('src')
                if url:
                    discovered_urls.append(urljoin(base_url, url))

        except Exception as e:
            self.logger.error(f"Parsing error at {base_url}: {e}")

        # Убираем дубли и возвращаем списки
        matches = list(dict.fromkeys(matches))
        discovered_urls = list(dict.fromkeys(discovered_urls))

        return matches, discovered_urls
