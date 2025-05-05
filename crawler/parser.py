# crawler/parser.py
import re
import logging
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse

class Parser:
    def __init__(self, cfg):
        self.case_sensitive = cfg.get("case_sensitive", False)
        self.keyword_patterns = self._compile_patterns(cfg)
        self.url_patterns = self._compile_url_patterns(cfg)
        self.logger = logging.getLogger("Parser")

    def _compile_patterns(self, cfg) -> List[re.Pattern]:
        patterns = []
        try:
            with open(cfg['keywords_file'], 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    
                    # Специальная обработка для японских символов
                    if any(ord(c) > 127 for c in line):
                        pattern = re.compile(line)
                    else:
                        # Автоматическое преобразование пробелов в \s+
                        pattern = line.replace(" ", r"\s+")
                        flags = 0 if self.case_sensitive else re.IGNORECASE
                        pattern = re.compile(rf"\b{pattern}\b", flags=flags)
                    
                    patterns.append(pattern)
        except Exception as e:
            self.logger.error(f"Failed to load patterns: {e}")
        return patterns

    def _compile_url_patterns(self, cfg) -> List[re.Pattern]:
        url_patterns = []
        try:
            with open(cfg.get('url_filters', 'url_filters.txt'), 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        url_patterns.append(re.compile(line))
        except FileNotFoundError:
            self.logger.warning("URL filters file not found, using defaults")
            url_patterns = [re.compile(r"\.(jpg|jpeg|png|gif|pdf)$", re.I)]
        return url_patterns

    def parse(self, content: str, base_url: str) -> Tuple[List[str], List[str]]:
        soup = BeautifulSoup(content, 'html.parser')
        
        # Поиск ключевых слов в тексте
        text = soup.get_text(separator=' ', strip=True)
        content_matches = self._find_content_matches(text)
        
        # Извлечение и фильтрация ссылок
        raw_links = self._extract_links(soup, base_url)
        filtered_links = self._filter_links(raw_links)
        
        return filtered_links, content_matches

    def _find_content_matches(self, text: str) -> List[str]:
        matches = []
        for pattern in self.keyword_patterns:
            try:
                if pattern.search(text):
                    matches.append(pattern.pattern)
            except Exception as e:
                self.logger.error(f"Error in pattern {pattern.pattern}: {e}")
        return list(set(matches))

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links = set()
        for tag in soup.find_all(['a', 'img', 'script', 'link'], href=True):
            try:
                url = urljoin(base_url, tag['href'])
                if URLTools.is_archive(url):
                    links.add(URLTools.normalize(url))
            except KeyError:
                continue
        return list(links)

    def _filter_links(self, links: List[str]) -> List[str]:
        return [
            link for link in links
            if not any(p.search(link) for p in self.url_patterns)
        ]

class URLTools:
    @staticmethod
    def normalize(url: str) -> str:
        return url.split("#")[0].rstrip("/").lower()

    @staticmethod
    def is_archive(url: str) -> bool:
        parsed = urlparse(url)  # Должен использовать импортированный urlparse
        return (
            parsed.netloc == "web.archive.org" and 
            re.match(r"^/web/\d+/", parsed.path)
        )