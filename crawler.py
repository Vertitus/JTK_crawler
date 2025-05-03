import asyncio
import aiohttp
import async_timeout
import aiofiles
from aiohttp.client_exceptions import ClientPayloadError, ContentTypeError
from bs4 import BeautifulSoup
import re
import logging
import os
import hashlib
from urllib.parse import urljoin, urlparse, quote
from collections import defaultdict
import json
import random
import time

# === Настройки ===
MAX_CONCURRENT     = 8
MAX_RETRIES        = 3
MAX_TOTAL_PAGES    = 10000
MAX_DEPTH          = 3
MAX_QUEUE_SIZE     = 10000
AUTO_SAVE_INTERVAL = 300
BATCH_SIZE         = 1000  # размер пакета для обработки
CACHE_DIR          = "cache"
LOG_DIR            = "logs"
RESULTS_FILE       = os.path.join(LOG_DIR, "results.json")
STATS_FILE         = os.path.join(LOG_DIR, "stats.json")
USER_AGENTS        = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    "Mozilla/5.0 (X11; Linux x86_64)..."
]
TARGET_DOMAINS     = [
    "fileman.n1e.jp","2ch.net","0ch.net","geocities.jp",
    "yaplog.jp","nifty.com","ocn.ne.jp","biglobe.ne.jp","pya.cc"
]
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

# Создать папки
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Логирование
logger = logging.getLogger("Crawler")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.join(LOG_DIR, "crawler.log"), encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)

# Состояние
visited = set()
stats   = defaultdict(int)
matches = defaultdict(list)

# Подготовка паттернов
ALL_PATTERNS = [re.compile(r"j+e+f+f+\s*t+h+e+\s*k+i+l+l+e+r", re.IGNORECASE)]
for kw in KEYWORDS:
    esc = re.escape(kw)
    if all(ord(c) < 128 for c in kw):
        pat = esc.replace(r'\ ', r'\s+')
        pat = rf"\b{pat}\b"
        ALL_PATTERNS.append(re.compile(pat, re.IGNORECASE))
    else:
        ALL_PATTERNS.append(re.compile(esc))

class URLTools:
    @staticmethod
    def normalize(u: str) -> str:
        u = u.split("#")[0].rstrip("/")
        p = urlparse(u)
        return p._replace(path=p.path.rstrip("/"), query="", fragment="").geturl().lower()

    @staticmethod
    def is_archive(u: str) -> bool:
        p = urlparse(u)
        if p.netloc != "web.archive.org":
            return False
        m = re.match(r"^/web/\d+/(.+)$", p.path)
        if not m:
            return False
        orig = m.group(1)
        return any(orig.startswith(f"http://{d}") or orig.startswith(f"https://{d}") for d in TARGET_DOMAINS)

async def fetch_cdx(session, domain):
    query = quote(domain + '/*', safe='')
    url = (
        f"https://web.archive.org/cdx/search/cdx?url={query}"
        "&matchType=domain&filter=statuscode:200&mimetype=text/html"
        "&from=20040101000000&to=20041231235959&collapse=urlkey&output=json"
    )
    for attempt in range(MAX_RETRIES):
        try:
            hdr = {"User-Agent": random.choice(USER_AGENTS)}
            async with async_timeout.timeout(60):
                resp = await session.get(url, headers=hdr)
            if resp.status != 200:
                logger.warning(f"CDX {domain} HTTP {resp.status}")
                await asyncio.sleep(2 ** attempt)
                continue
            ct = resp.headers.get("Content-Type", "")
            if "application/json" not in ct:
                logger.error(f"CDX {domain} returned non-json: {ct}")
                return []
            try:
                data = await resp.json()
            except (ClientPayloadError, ContentTypeError, asyncio.IncompleteReadError) as e:
                logger.warning(f"CDX {domain} payload error on attempt {attempt+1}: {e}")
                await asyncio.sleep(2 ** attempt)
                continue
            if not isinstance(data, list):
                logger.error(f"CDX {domain} returned unexpected type")
                return []
            logger.info(f"{domain}: snapshots = {len(data)-1}")
            return [entry for entry in data[1:] if len(entry) >= 3]
        except asyncio.TimeoutError:
            logger.warning(f"CDX {domain} timed out on attempt {attempt+1}")
        except Exception as e:
            logger.exception(f"CDX error {domain} on attempt {attempt+1}: {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error(f"CDX {domain} failed after {MAX_RETRIES} attempts")
    return []

async def collect_all_snapshots(session):
    all_snaps = []
    for domain in TARGET_DOMAINS:
        entries = await fetch_cdx(session, domain)
        for entry in entries:
            t, orig = entry[1], entry[2]
            u = URLTools.normalize(f"http://web.archive.org/web/{t}/{orig}")
            all_snaps.append(u)
    logger.info(f"Collected total snapshots: {len(all_snaps)}")
    return all_snaps

async def fetch_page(session, url):
    stats['total_pages'] += 1
    h = hashlib.md5(url.encode()).hexdigest()
    cache_path = os.path.join(CACHE_DIR, f"{h}.html")
    if os.path.exists(cache_path):
        async with aiofiles.open(cache_path, "r", encoding="utf-8") as f:
            return await f.read()
    for attempt in range(MAX_RETRIES):
        try:
            hdr = {"User-Agent": random.choice(USER_AGENTS)}
            async with async_timeout.timeout(30):
                resp = await session.get(url, headers=hdr)
            b = await resp.read()
            txt = b.decode(resp.get_encoding() or 'utf-8', errors='ignore')
            if resp.status == 200:
                async with aiofiles.open(cache_path, "w", encoding="utf-8") as f:
                    await f.write(txt)
                return txt
            logger.warning(f"HTTP {resp.status} at {url}")
            return ""
        except Exception:
            logger.exception(f"Fetch error {url} [{attempt+1}/{MAX_RETRIES}]")
            await asyncio.sleep(2 ** attempt)
    stats['errors'] += 1
    return ""

async def process_page(session, url, queue, depth):
    html = await fetch_page(session, url)
    if not html:
        return
    found = 0
    for pat in ALL_PATTERNS:
        ms = pat.findall(html)
        if ms:
            found += len(ms)
            matches[url].extend(ms)
    if found:
        stats['matches'] += found
        logger.info(f"Found {found} hits at {url}")
    if depth < MAX_DEPTH:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            u = URLTools.normalize(urljoin(url, a["href"]))
            if URLTools.is_archive(u) and u not in visited:
                await queue.put((depth + 1, u))

async def worker(session, queue):
    while not queue.empty() and stats['total_pages'] < MAX_TOTAL_PAGES:
        depth, url = await queue.get()
        if url not in visited:
            visited.add(url)
            await process_page(session, url, queue, depth)
        queue.task_done()

async def auto_save():
    while True:
        await asyncio.sleep(AUTO_SAVE_INTERVAL)
        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
        logger.info("Auto-saved results & stats")

async def main():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        all_snaps = await collect_all_snapshots(session)
        total = len(all_snaps)
        for i in range(0, total, BATCH_SIZE):
            batch = all_snaps[i:i+BATCH_SIZE]
            logger.info(f"Processing batch {i//BATCH_SIZE+1} of { (total-1)//BATCH_SIZE+1 }")
            queue = asyncio.Queue(MAX_QUEUE_SIZE)
            for u in batch:
                queue.put_nowait((0, u))
            workers = [asyncio.create_task(worker(session, queue)) for _ in range(MAX_CONCURRENT)]
            await queue.join()
            for w in workers:
                w.cancel()
        # финальное сохранение
        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
        logger.info("Crawl completed")

if __name__ == "__main__":
    asyncio.run(main())
