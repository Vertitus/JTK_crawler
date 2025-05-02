import asyncio
import aiohttp
import async_timeout
import aiofiles
from bs4 import BeautifulSoup, Comment, Doctype
import re
import logging
import os
import hashlib
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import json
import datetime
import random
import time

# Конфигурация
KEYWORDS = [
    "white face", "pale face", "ghostly smile", "eerie smile", "horrific grin",
    "haunted face", "bloodless smile", "sinister smile", "dead white face",
    "pale grimace", "spectral face", "frozen smile", "white powder",
    "powdered face", "frozen grin", "deformed smile", "distorted grin",
    "creepy grin", "grimacing face", "demonic smile", "horror smile",
    "smiling death", "白い顔", "不気味な笑顔", "恐怖の笑顔", "白粉", 
    "霊の笑顔", "異形の笑顔", "7-24h2659b", "iup71138", "1123087197322", "プリティフェイス",
    "綺麗な顔", "可愛い顔", "かわいいかお", "きれいなかお", "びがん", "美顔", "ホワイトパウダ", 
    "白い粉", "しろいこな", "白粉", "おしろい", "おたふく", "白い化粧", "しろいけしょう", 
    "白粉", "おしろい", "おばけのきゅうたろう", "フェイスセット", "普通の姿", "ふつうのすがた"
    "お多福の面", "prettyFace", "creepy face", "blank stare", "faceless horror", "night smile", "white horror",
    "white ghost", "death smile", "scary smile", "twisted grin", "melted face",
    "paranormal smile", "slasher grin", "bloodless face", "emotionless smile", "doll face",
    "plastic smile", "painted face", "frozen expression", "paralyzed smile", "cursed smile",
    "smiling monster", "killer smile", "deathly grin", "ghoul face", "freak face",
    "unholy smile", "smiling corpse", "smiling demon", "smile of death", "facial terror",
    "dead grin", "noir smile", "creepyphoto.jpg", "unknown_face", "smile_killer",
    "japan ghost face", "lost_face_image", "cursed_photo", "weird_face", "smiling entity",
    "笑ってる顔", "変な顔", "恐い写真", "呪いの写真", "笑顔の幽霊", "死神の笑顔", "血の気のない顔",
    "恐怖の画像", "不気味な顔", "変顔", "消せない画像", "恐怖スレ", "気持ち悪い笑顔", "怖いスレ",
    "忘れられない顔", "夜の笑顔", "ホラー画像", "呪われた笑顔", "おかしな顔", "笑う死体",
    "笑ってる化け物", "しろいかお", "こわいえがお", "ばけもののかお", "ぞっとする顔",
    "わらってるひと", "意味不明な画像", "名前のない画像", "無名の顔", "殺人鬼の顔",
    "名無しの笑顔", "未確認笑顔", "白塗り顔", "笑う幽霊", "画像削除願います",
    "過去スレ画像", "2004怪しい画像", "怖画像", "閲覧注意画像", "japanese creepypasta",
    "笑顔の殺人鬼", "闇の笑顔", "見たら終わり", "白い女", "顔のない笑顔", "人形のような顔",
    "古い恐怖画像", "怪しいスマイル", "昔の怖い写真", "lost horror pic"
]
REGEX_PATTERNS = [
    re.compile(r"j+e+f+f+\s*t+h+e+\s*k+i+l+l+e+r", re.IGNORECASE),
]
TARGET_DOMAINS = [
    "fileman.n1e.jp", "2ch.net", "0ch.net", "geocities.jp",
    "yaplog.jp", "nifty.com", "ocn.ne.jp", "biglobe.ne.jp",
    "pya.cc"
]  # Ваши целевые домены

MAX_CONCURRENT = 8
MAX_RETRIES = 3
MAX_TOTAL_PAGES = 10000
MAX_DEPTH = 3
MAX_QUEUE_SIZE = 10000
AUTO_SAVE_INTERVAL = 300  # 5 минут
CACHE_DIR = "cache"
LOG_DIR = "logs"
RESULTS_FILE = os.path.join(LOG_DIR, "results.json")
STATS_FILE = os.path.join(LOG_DIR, "stats.json")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка логирования
logger = logging.getLogger("Crawler")
logger.setLevel(logging.DEBUG)

results_logger = logging.getLogger("Results")
results_logger.setLevel(logging.INFO)

main_handler = logging.FileHandler(
    os.path.join(LOG_DIR, "crawler.log"), 
    encoding='utf-8'
)
main_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

results_handler = logging.FileHandler(
    RESULTS_FILE,
    encoding='utf-8'
)
results_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

logger.addHandler(main_handler)
results_logger.addHandler(results_handler)

# Состояние
visited = set()
stats = defaultdict(int)
matches = defaultdict(list)
performance_metrics = {
    'processing_times': [],
    'start_time': time.time()
}

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
        "filter=statuscode:200&mimetype:text/html&"
        "from=20040101000000&to=20041231235959&"
        "collapse=timestamp:6&"
        "output=json"
    )
    
    try:
        async with async_timeout.timeout(30):
            async with session.get(cdx_url) as response:
                data = await response.json()
                if isinstance(data, list):
                    logger.info(f"Found {len(data)-1} snapshots for {domain}")
                    return data
                return []
    except Exception as e:
        logger.error(f"CDX Error: {str(e)}")
        return []

async def get_snapshots(session):
    snapshots = set()
    for domain in TARGET_DOMAINS:
        try:
            data = await fetch_cdx(session, domain)
            if isinstance(data, list) and len(data) >= 2:
                for entry in data[1:]:
                    if len(entry) >= 3:
                        timestamp = entry[1]
                        original = entry[2]
                        url = f"http://web.archive.org/web/{timestamp}/{original}"
                        snap_url = URLTools.normalize_url(url)
                        snapshots.add(snap_url)
        except Exception as e:
            logger.error(f"Snapshot Error: {str(e)}")
    
    logger.info(f"Total snapshots: {len(snapshots)}")
    return snapshots

async def fetch_page(session, url):
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    cache_path = os.path.join(CACHE_DIR, f"{url_hash}.html")
    
    if os.path.exists(cache_path):
        async with aiofiles.open(cache_path, "r", encoding="utf-8") as f:
            return await f.read()
    
    for attempt in range(MAX_RETRIES):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            proxy = os.getenv("HTTP_PROXY")
            
            async with async_timeout.timeout(30):
                async with session.get(url, headers=headers, proxy=proxy) as response:
                    content = await response.text(errors="ignore")
                    async with aiofiles.open(cache_path, "w", encoding="utf-8") as f:
                        await f.write(content)
                    return content
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Failed to fetch {url}: {str(e)}")
            await asyncio.sleep(2 ** attempt)
    return ""

def calculate_priority(match_count):
    if match_count == 0:
        return 0
    elif 1 <= match_count <= 2:
        return 1
    elif 3 <= match_count <= 5:
        return 2
    else:
        return 3

def keyword_search(text):
    """Ищет ключевые слова и регулярные выражения в тексте."""
    text_lower = text.lower()
    found_keywords = [kw for kw in KEYWORDS if kw.lower() in text_lower]
    
    for pattern in REGEX_PATTERNS:
        if pattern.search(text_lower):
            found_keywords.append(pattern.pattern)
    
    return found_keywords if found_keywords else None

async def process_page(session, url, queue, depth):
    global stats, matches, performance_metrics
    
    if stats['total_pages'] >= MAX_TOTAL_PAGES or depth > MAX_DEPTH:
        return
    
    start_time = time.time()
    normalized = URLTools.normalize_url(url)
    
    if normalized in visited:
        return
    
    visited.add(normalized)
    stats['total_pages'] += 1
    
    logger.info(f"Processing: {normalized} (Depth: {depth})")
    
    try:
        content = await fetch_page(session, url)
        if not content:
            stats['errors'] += 1
            return
        
        parse_start = time.time()
        try:
            soup = BeautifulSoup(content, 'lxml')
        except Exception as e:
            logger.error(f"Parse error: {str(e)}")
            return
        
        match_count = 0
        
        def check_text(text, source_type):
            nonlocal match_count
            if not text or len(text.strip()) < 3:
                return
            
            found = keyword_search(text)
            if found:
                match_count += len(found)
                match_data = {
                    "url": normalized,
                    "text": text[:500] + "..." if len(text) > 500 else text,
                    "keywords": found,
                    "source": source_type,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                matches[normalized].append(match_data)
                results_logger.info(json.dumps(match_data, ensure_ascii=False))
                stats['matches'] += len(found)
        
        # Обработка элементов
        for element in soup.find_all(string=True):
            if isinstance(element, Doctype):
                continue
            try:
                text = str(element).strip()
                if isinstance(element, Comment):
                    check_text(text, 'comment')
                else:
                    check_text(text, 'text')
            except Exception as e:
                logger.error(f"Element error: {str(e)}")
        
        # Обработка метаданных и изображений
        for meta in soup.find_all('meta', {"content": True}):
            check_text(meta['content'], 'meta')
        
        for img in soup.find_all('img'):
            for attr in ['src', 'alt', 'title']:
                if img.has_attr(attr):
                    check_text(img[attr], f'img_{attr}')
        
        processing_time = time.time() - start_time
        performance_metrics['processing_times'].append(processing_time)
        
        logger.info(f"Processed: {normalized} | Matches: {match_count} | Time: {processing_time:.2f}s")
        
        # Добавление новых ссылок с приоритетом
        if queue.qsize() < MAX_QUEUE_SIZE:
            for link in soup.find_all('a', href=True):
                href = urljoin(url, link['href'])
                normalized_href = URLTools.normalize_url(href)
                
                if (URLTools.is_valid_archive_url(href) 
                    and normalized_href not in visited 
                    and stats['total_pages'] < MAX_TOTAL_PAGES):
                    
                    priority = calculate_priority(match_count)
                    await queue.put((-priority, depth + 1, normalized_href))
        else:
            logger.warning("Queue size limit reached, skipping new links")
            
    except Exception as e:
        stats['errors'] += 1
        logger.error(f"Processing error: {str(e)}")

async def worker(session, queue):
    while stats['total_pages'] < MAX_TOTAL_PAGES:
        try:
            priority, depth, url = await queue.get()
            await process_page(session, url, queue, depth)
            queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")

async def auto_saver():
    while True:
        await asyncio.sleep(AUTO_SAVE_INTERVAL)
        try:
            async with aiofiles.open(RESULTS_FILE, 'w', encoding='utf-8') as f:
                await f.write(json.dumps({
                    "stats": dict(stats),
                    "matches": dict(matches)
                }, indent=2, ensure_ascii=False))
            
            async with aiofiles.open(STATS_FILE, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(dict(stats), indent=2, ensure_ascii=False))
            
            logger.info("Auto-save completed")
        except Exception as e:
            logger.error(f"Auto-save failed: {str(e)}")

async def metrics_logger():
    while True:
        await asyncio.sleep(600)  # 10 минут
        try:
            if performance_metrics['processing_times']:
                avg_time = sum(performance_metrics['processing_times']) / len(performance_metrics['processing_times'])
                logger.info(f"Metrics | Avg: {avg_time:.2f}s | Total: {stats['total_pages']} | Matches: {stats['matches']}")
        except Exception as e:
            logger.error(f"Metrics error: {str(e)}")

async def main():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"Accept-Encoding": "gzip, deflate"}
    ) as session:
        queue = asyncio.PriorityQueue()
        
        logger.info("Initializing snapshots...")
        snapshots = await get_snapshots(session)
        logger.info(f"Total initial snapshots: {len(snapshots)}")
        
        for url in snapshots:
            await queue.put((0, 0, URLTools.normalize_url(url)))
        
        workers = [asyncio.create_task(worker(session, queue)) for _ in range(MAX_CONCURRENT)]
        saver = asyncio.create_task(auto_saver())
        metrics = asyncio.create_task(metrics_logger())
        
        try:
            await queue.join()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            for task in workers:
                task.cancel()
            saver.cancel()
            metrics.cancel()
            
            await asyncio.gather(*workers, saver, metrics, return_exceptions=True)
            
            async with aiofiles.open(RESULTS_FILE, 'w', encoding='utf-8') as f:
                await f.write(json.dumps({
                    "stats": dict(stats),
                    "matches": dict(matches)
                }, indent=2, ensure_ascii=False))
            
            logger.info("Final results saved")

if __name__ == "__main__":
    asyncio.run(main())