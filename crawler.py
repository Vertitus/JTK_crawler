import asyncio
import aiohttp
import async_timeout
from bs4 import BeautifulSoup, Comment
import re
import logging
import os
import hashlib
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import json
import datetime

# Конфигурация
KEYWORDS = [
    "white face", "pale face", "ghostly smile", "eerie smile",
    "horrific grin", "haunted face", "bloodless smile",
    "sinister smile", "dead white face", "prettyFace",
    "creepy face", "faceless horror", "night smile",
    "white horror", "japan ghost face", "cursed_photo",
    "lost horror pic", "白い顔", "不気味な笑顔", "恐怖の笑顔",
    "白粉", "おしろい", "ホラー画像", "閲覧注意画像",
    "怪しいスマイル", "昔の怖い写真", "2004怪しい画像"
]

REGEX_PATTERNS = [
    re.compile(r"j+e+f+f+\s*t+h+e+\s*k+i+l+l+e+r", re.IGNORECASE),
]

TARGET_DOMAINS = [
    "fileman.n1e.jp", "2ch.net", "0ch.net",
    "geocities.jp", "yaplog.jp", "nifty.com",
    "ocn.ne.jp", "biglobe.ne.jp"
]

MAX_CONCURRENT = 8
MAX_RETRIES = 3
MAX_TOTAL_PAGES = 10000
CACHE_DIR = "cache"
LOG_DIR = "logs"
RESULTS_FILE = os.path.join(LOG_DIR, "results.json")
STATS_FILE = os.path.join(LOG_DIR, "stats.json")

# Инициализация директорий
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка логирования
logger = logging.getLogger("Crawler")
logger.setLevel(logging.DEBUG)

results_logger = logging.getLogger("Results")
results_logger.setLevel(logging.INFO)

# Хэндлеры
main_handler = logging.FileHandler(os.path.join(LOG_DIR, "crawler.log"))
main_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

results_handler = logging.FileHandler(RESULTS_FILE)
results_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

logger.addHandler(main_handler)
results_logger.addHandler(results_handler)

# Глобальные состояния
visited = set()
stats = defaultdict(int)
matches = defaultdict(list)

class URLTools:
    @staticmethod
    def normalize_url(url):
        url = url.split("#")[0].rstrip("/")
        parsed = urlparse(url)
        return parsed._replace(
            path=parsed.path.rstrip("/"),
            query="",
            fragment=""
        ).geturl().lower()

    @staticmethod
    def is_valid_archive_url(url):
        return "web.archive.org/web/" in url and any(
            f"/{domain}/" in url for domain in TARGET_DOMAINS
        )

async def fetch_cdx(session, domain):
    cdx_url = (
        f"http://web.archive.org/cdx/search/cdx?"
        f"url={domain}/*&"
        "matchType=domain&"
        "filter=statuscode:200&"
        "from=20040101000000&to=20041231235959&"
        "collapse=timestamp:6&"
        "output=json"
    )
    
    try:
        async with async_timeout.timeout(30):
            async with session.get(cdx_url) as response:
                return await response.json()
    except Exception as e:
        logger.error(f"CDX Error for {domain}: {str(e)}")
        return []

async def get_snapshots(session):
    snapshots = set()
    for domain in TARGET_DOMAINS:
        try:
            data = await fetch_cdx(session, domain)
            if not data or len(data) < 2:
                continue
                
            for entry in data[1:]:  # Пропускаем заголовок
                timestamp = entry[1]
                original = entry[2]
                url = f"http://web.archive.org/web/{timestamp}/{original}"
                snapshots.add(url)
                
        except Exception as e:
            logger.error(f"Snapshot Error: {str(e)}")
            
    return snapshots

async def fetch_page(session, url):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_path = os.path.join(CACHE_DIR, f"{url_hash}.html")
    
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()
            
    for attempt in range(MAX_RETRIES):
        try:
            async with async_timeout.timeout(30):
                async with session.get(url) as response:
                    content = await response.text(errors="ignore")
                    with open(cache_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    return content
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Failed to fetch {url}: {str(e)}")
            await asyncio.sleep(2 ** attempt)
    return ""

def keyword_search(text):
    text = re.sub(r'\s+', ' ', text.lower().strip())
    found = []
    
    # Поиск по ключевым словам
    for kw in KEYWORDS:
        if kw.lower() in text:
            found.append(kw)
    
    # Поиск по регулярным выражениям
    for pattern in REGEX_PATTERNS:
        if pattern.search(text):
            found.append(pattern.pattern)
    
    return found

async def process_page(session, url, queue):
    global stats, matches
    
    if stats['total_pages'] >= MAX_TOTAL_PAGES:
        return
        
    normalized = URLTools.normalize_url(url)
    if normalized in visited:
        return
        
    visited.add(normalized)
    stats['total_pages'] += 1
    logger.info(f"Processing: {normalized}")
    
    try:
        content = await fetch_page(session, url)
        if not content:
            stats['errors'] += 1
            return
            
        soup = BeautifulSoup(content, 'lxml')
        found_keywords = False
        
        # Поиск в текстовых элементах
        def check_text(text, source_type):
            nonlocal found_keywords
            if not text or len(text.strip()) < 3:
                return
                
            found = keyword_search(text)
            if found:
                found_keywords = True
                match_data = {
                    "url": normalized,
                    "text": text[:500] + "..." if len(text) > 500 else text,
                    "keywords": found,
                    "source": source_type,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                matches[normalized].append(match_data)
                results_logger.info(json.dumps(match_data, ensure_ascii=False))
                stats['matches'] += 1
        
        # Обработка различных элементов
        for element in soup.find_all(text=True):
            if isinstance(element, Comment):
                check_text(element.string, 'comment')
            else:
                check_text(element.get_text(), 'text')
                
        for meta in soup.find_all('meta', {"content": True}):
            check_text(meta['content'], 'meta')
            
        for img in soup.find_all('img'):
            for attr in ['src', 'alt', 'title']:
                if img.has_attr(attr):
                    check_text(img[attr], f'img_{attr}')
        
        # Сбор ссылок
        if found_keywords:
            stats['priority_pages'] += 1
            
        for link in soup.find_all('a', href=True):
            href = urljoin(url, link['href'])
            normalized_href = URLTools.normalize_url(href)
            
            if (URLTools.is_valid_archive_url(href) and
                normalized_href not in visited and
                stats['total_pages'] < MAX_TOTAL_PAGES):
                
                priority = 1 if found_keywords else 0
                await queue.put((priority, normalized_href))
                
    except Exception as e:
        stats['errors'] += 1
        logger.error(f"Error processing {normalized}: {str(e)}")

async def worker(session, queue):
    while stats['total_pages'] < MAX_TOTAL_PAGES:
        try:
            priority, url = await queue.get()
            await process_page(session, url, queue)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
        finally:
            queue.task_done()

async def main():
    async with aiohttp.ClientSession(
        headers={
            "User-Agent": "ArchiveResearchBot/1.0 (+http://example.com)",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate"
        }
    ) as session:
        # Приоритетная очередь (выше приоритет = раньше обрабатывается)
        queue = asyncio.PriorityQueue()
        
        # Получение начальных URL
        snapshots = await get_snapshots(session)
        for url in snapshots:
            await queue.put((0, URLTools.normalize_url(url)))
        
        # Запуск воркеров
        workers = [
            asyncio.create_task(worker(session, queue))
            for _ in range(MAX_CONCURRENT)
        ]
        
        # Ожидание завершения
        try:
            await queue.join()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            # Сохранение результатов
            with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    "stats": dict(stats),
                    "matches": dict(matches)
                }, f, indent=2, ensure_ascii=False)
                
            with open(STATS_FILE, 'w') as f:
                json.dump(dict(stats), f, indent=2)
                
            # Остановка воркеров
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())