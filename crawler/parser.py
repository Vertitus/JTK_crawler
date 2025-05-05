import re
from typing import List
from bs4 import BeautifulSoup
import logging

class Parser:
    def __init__(self, cfg):
        self.patterns = self._load_patterns(cfg.get('patterns_file', 'keywords.txt'))

    def _load_patterns(self, patterns_file: str) -> List[str]:
        try:
            with open(patterns_file, 'r') as f:
                return [line.strip() for line in f.readlines()]
        except Exception as e:
            logging.error(f"Failed to load patterns from {patterns_file}: {e}")
            return []

    def parse_html(self, content: str, url: str) -> List[str]:
        """
        Парсит HTML контент, извлекает все ссылки и фильтрует их по ключевым словам.
        """
        soup = BeautifulSoup(content, 'html.parser')
        links = self._extract_links(soup)
        filtered_links = self._filter_links_by_patterns(links)
        
        logging.info(f"Found {len(filtered_links)} filtered links in {url}")
        return filtered_links

    def _extract_links(self, soup: BeautifulSoup) -> List[str]:
        """
        Извлекает все уникальные ссылки из HTML.
        """
        links = set()
        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            if link.startswith('http'):
                links.add(link)
        return list(links)

    def _filter_links_by_patterns(self, links: List[str]) -> List[str]:
        """
        Фильтрует ссылки по ключевым словам, заданным в patterns.
        """
        filtered_links = []
        for link in links:
            if any(re.search(pattern, link) for pattern in self.patterns):
                filtered_links.append(link)
        return filtered_links
