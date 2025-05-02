import os
import requests
import time
import random
import logging
from logging.handlers import RotatingFileHandler
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm
from typing import List, Tuple, Set
from stem import Signal
from stem.control import Controller
import matplotlib.pyplot as plt
from collections import Counter


# ========== НАСТРОЙКИ ========== #
TOR_PROXY = "socks5h://127.0.0.1:9150"
BRIDGES = [
    "obfs4 185.177.207.6:63133 EDEF8A9E6AC0E564B9AE5C43AE8CE8B6C7006A85 cert=p9L6+25s8bnfkye1ZxFeAE4mAGY7DH4Gaj7dxngIIzP9BtqrHHwZXdjMK0RVIQ34C7aqZw iat-mode=2",
    "obfs4 49.156.17.160:443 241D37FB8297DCF7E9411FF8E6B44C294A20DE6B cert=R3gdf7HUiVGlj5mRmHCEMaFMnRwUwqf2KjgNV/W8HweVSahHe/1wXxglGth+up86X2GYSA iat-mode=0"
]
DOMAIN = "pya.cc"
YEAR = "2003"
TARGETS = [
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
DELAY = 5
MAX_DEPTH = 3
SAVE_DIR = "downloaded_images"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
# =============================== #

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("ArchiveSearch")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    file_handler = RotatingFileHandler("archive_search.log", maxBytes=10*1024*1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

class TorManager:
    def __init__(self):
        self.proxies = {"http": TOR_PROXY, "https": TOR_PROXY}
        self.session = self.create_session()
    
    def create_session(self) -> requests.Session:
        session = requests.Session()
        session.proxies = self.proxies
        session.headers.update({"User-Agent": self.random_ua()})
        return session
    
    def random_ua(self) -> str:
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        ]) + " ArchiveResearchBot/1.0"
    
    def renew_connection(self):
        try:
            with Controller.from_port(port=9151) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                logger.info("IP-адрес изменен")
        except Exception as e:
            logger.error(f"Ошибка смены IP: {str(e)}")

    def check_tor(self) -> bool:
        try:
            response = self.session.get("https://check.torproject.org", timeout=15)
            return "Congratulations" in response.text
        except Exception as e:
            logger.error(f"Tor check failed: {str(e)}")
            return False

class ArchiveParser:
    def __init__(self, tor_manager: TorManager):
        self.tor = tor_manager
        self.found_matches = []
        self.total_scanned = 0
        self.visited_urls: Set[str] = set()
        self.save_dir = SAVE_DIR
        self._prepare_save_directory()
    
    def _prepare_save_directory(self):
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
            logger.info(f"Создана папка для сохранения: {self.save_dir}")

    def _download_image(self, url: str, referer: str) -> bool:
        try:
            response = self.tor.session.get(
                url,
                stream=True,
                headers={'Referer': referer},
                timeout=20,
                allow_redirects=True
            )
            
            content_size = int(response.headers.get('Content-Length', 0))
            if content_size > MAX_FILE_SIZE:
                logger.warning(f"Файл слишком большой: {url} [{content_size} bytes]")
                return False

            content_type = response.headers.get('Content-Type', '').split('/')[-1]
            ext = content_type if content_type in ['jpeg', 'png', 'gif'] else 'bin'
            filename = f"{hash(url)}_{int(time.time())}.{ext}"
            save_path = os.path.join(self.save_dir, filename)

            with open(save_path, 'wb') as f, tqdm(
                total=content_size,
                unit='B',
                unit_scale=True,
                desc=f"Скачиваю {filename[:15]}..",
                leave=False
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            logger.info(f"Сохранено изображение: {save_path}")
            return True

        except Exception as e:
            logger.error(f"Ошибка скачивания {url}: {str(e)}")
            if isinstance(e, (requests.ConnectionError, requests.Timeout)):
                self.tor.renew_connection()
            return False

    def fetch_snapshots(self) -> List[Tuple[str, str]]:
        url = "https://web.archive.org/cdx/search/cdx"
        params = {
            "url": DOMAIN,
            "from": f"{YEAR}0101000000",
            "to": f"{YEAR}1231235959",
            "output": "json",
            "fl": "timestamp,original",
            "filter": "statuscode:200",
            "pageSize": 5000
        }
        try:
            response = self.tor.session.get(url, params=params, timeout=30)
            return [(item[0], item[1]) for item in response.json()[1:]]
        except Exception as e:
            logger.error(f"Ошибка получения снимков: {str(e)}")
            return []

    def process_snapshot(self, timestamp: str, original_url: str):
        base_snapshot_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
        before = len(self.found_matches)
        self._crawl_snapshot(base_snapshot_url, depth=0)
        after = len(self.found_matches)
        found_here = after - before
        logger.info(f"Обработан снимок {timestamp} → {original_url}")
        logger.info(f"Найдено совпадений в этом снимке: {found_here}")

    def _crawl_snapshot(self, url: str, depth: int):
        if url in self.visited_urls or depth > MAX_DEPTH:
            return
        self.visited_urls.add(url)
        try:
            response = self.tor.session.get(url, timeout=20)
            if response.status_code == 200:
                self.total_scanned += 1
                soup = BeautifulSoup(response.text, "html.parser")
                self.search_content(soup, url)
                if depth < MAX_DEPTH:
                    for a in soup.find_all("a", href=True):
                        link = urljoin(url, a["href"])
                        parsed = urlparse(link)
                        if parsed.netloc in ["web.archive.org", ""]:
                            self._crawl_snapshot(link, depth + 1)
        except Exception as e:
            logger.warning(f"Ошибка обработки {url}: {str(e)}")
            self.tor.renew_connection()

    def search_content(self, soup: BeautifulSoup, base_url: str):
        found = False
        for tag in soup.find_all(["img", "a", "link"]):
            url = tag.get("src") or tag.get("href")
            if url:
                full_url = urljoin(base_url, url).lower()
                for target in TARGETS:
                    if target.lower() in full_url:
                        status = "Ошибка скачивания"
                        if tag.name == "img":
                            if self._download_image(full_url, base_url):
                                status = "Скачано"
                        self.found_matches.append((full_url, target, "URL", status))
                        logger.info(f"НАЙДЕНО: [Таргет: '{target}'] → {full_url} (в URL) [{status}]")
                        found = True
        page_text = soup.get_text().lower()
        for target in TARGETS:
            if target.lower() in page_text:
                self.found_matches.append((base_url, target, "Текст", "-"))
                logger.info(f"НАЙДЕНО: [Таргет: '{target}'] → {base_url} (в тексте)")
                found = True
        if not found:
            logger.debug(f"Совпадений нет: {base_url}")

def save_visualization(matches: List[Tuple[str, str, str]]):
    if not matches:
        logger.warning("Нет данных для визуализации.")
        return
    types = [match_type for _, _, match_type, _ in matches]
    targets = [target for _, target, _, _ in matches]
    type_counter = Counter(types)
    target_counter = Counter(targets)
    fig, axs = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Результаты поиска по архиву", fontsize=16)
    axs[0].bar(type_counter.keys(), type_counter.values(), color=['skyblue', 'salmon'])
    axs[0].set_title("Типы совпадений")
    axs[0].set_ylabel("Количество")
    axs[0].set_xlabel("Тип")
    top_targets = target_counter.most_common(10)
    targets_labels, targets_counts = zip(*top_targets)
    axs[1].barh(targets_labels, targets_counts, color='lightgreen')
    axs[1].invert_yaxis()
    axs[1].set_title("Топ-10 таргетов")
    axs[1].set_xlabel("Количество упоминаний")
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("search_results.png", dpi=300)
    logger.info("График сохранён как search_results.png")

def main():
    logger.info("=== ЗАПУСК ПОИСКА ===")
    tor = TorManager()
    if not tor.check_tor():
        logger.error("Tor не работает!")
        return
    parser = ArchiveParser(tor)
    snapshots = parser.fetch_snapshots()
    if not snapshots:
        logger.error("Нет снимков для анализа")
        return
    logger.info(f"Найдено снимков: {len(snapshots)}")
    progress = tqdm(snapshots, desc="Анализ", unit="снимок")
    for i, (timestamp, url) in enumerate(progress, start=1):
        logger.info(f"--- [{i}/{len(snapshots)}] Начата обработка снимка: {timestamp} → {url}")
        parser.process_snapshot(timestamp, url)
        percent = (i / len(snapshots)) * 100
        logger.info(f"Прогресс: {percent:.2f}% ({i}/{len(snapshots)})")
        logger.info(f"Общее количество совпадений: {len(parser.found_matches)}\n")
        progress.set_postfix({
            "Проверено": parser.total_scanned,
            "Найдено": len(parser.found_matches)
        })
        time.sleep(DELAY * random.uniform(0.8, 1.2))

    logger.info("\n=== РЕЗУЛЬТАТЫ ===")
    logger.info(f"Всего проверено: {parser.total_scanned}")
    logger.info(f"Всего совпадений: {len(parser.found_matches)}")
    
    if parser.found_matches:
        logger.info("\nДетализация:")
        for item in parser.found_matches:
            url, target, match_type, status = item
            logger.info(f"• Таргет: {target}")
            logger.info(f"  URL: {url}")
            logger.info(f"  Тип: {match_type}")
            if match_type == "URL":
                logger.info(f"  Статус скачивания: {status}")
            logger.info(f"{'-'*50}")
        save_visualization(parser.found_matches)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Прервано пользователем!")
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")