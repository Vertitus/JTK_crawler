# test_tor_cdx.py (ФИНАЛЬНАЯ ВЕРСИЯ)
import aiohttp
import asyncio
import json
import logging
from urllib.parse import quote, unquote, urlencode
from typing import List, Optional, Tuple
from aiohttp_socks import ProxyConnector

# --- НАЧАЛО КЛАССА WAYBACKCDXCLIENT (ИСПРАВЛЕННАЯ ВЕРСИЯ) ---
class WaybackCDXClient:
    CDX_API = "https://web.archive.org/cdx/search/cdx"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        request_timeout: int = 30,
        max_pages: int = 0,
        page_size: int = 1000,
        rate_limiter=None,
        # proxy_url убран, так как это должно управляться сессией
    ):
        self.session = session
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        # self.request_timeout = request_timeout
        self.max_pages = max_pages
        self.page_size = page_size
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = rate_limiter
        
    

    async def _handle_errors(self, response: aiohttp.ClientResponse):
        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            self.logger.warning(f"Rate limited. Retrying after {retry_after}s")
            await asyncio.sleep(retry_after)
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message="Rate limit exceeded",
            )
        if response.status != 200:
            text = await response.text()
            self.logger.error(f"HTTP error {response.status}: {text[:200]}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"HTTP error {response.status}",
            )

    async def fetch_snapshots(
        self,
        domain: str,
        from_date: str = "20050101000000",
        to_date: str = "20051231235959",
    ) ->     List[str]:
        """
        Попытка 1: запрос с параметрами fl/matchType (более точный).
        Если ответ пустой или невалидный — fallback (запрос без fl и без matchType).
        Возвращает список уникальных wayback URL'ов.
        """
        base_url = self.CDX_API

        # Первичный набор параметров (тот, который иногда даёт пустой ответ)
        params_primary = {
            "url": f"http://{domain}/*",
            "from": from_date,
            "to": to_date,
            "output": "json",
            "limit": self.page_size if self.page_size else 1000,
            "matchType": "prefix",
            "fl": "timestamp,original,statuscode,mimetype",
        }

        # Фоллбек-набор (более совместимый)
        params_fallback = {
            "url": f"http://{domain}/*",
            "from": from_date,
            "to": to_date,
            "output": "json",
            "limit": self.page_size if self.page_size else 1000,
            # без 'fl' и 'matchType'
        }

        headers = {"User-Agent": "JTK-Crawler/1.0 (+mailto:youremail@example.com)"}
        attempt = 0
        results: List[str] = []

        # Вспомогательная внутренняя функция для одной логической попытки с набором params
        async def try_with_params(params: dict) -> List[str]:
            collected: List[str] = []
            local_params = params.copy()
            page = 0
            resume_key = None

            while True:
                # если есть resumeKey — добавляем его
                if resume_key:
                    local_params["resumeKey"] = resume_key

                snaps, resume_key = await self._page_request(base_url, local_params, headers)
                collected.extend(snaps)

                page += 1
                # Если max_pages задан и достигнут — выходим
                if self.max_pages and page >= self.max_pages:
                    break
                # Без resume_key — значит страницы закончились
                if not resume_key:
                    break

            return collected

        # Попробовать первичный запрос
        try:
            self.logger.info(f"CDX: primary attempt for domain {domain} (with fl & matchType)")
            primary_results = await try_with_params(params_primary)

            # Если первичный вернул данные — используем их
            if primary_results:
                unique = list(dict.fromkeys(primary_results))
                self.logger.info(f"Primary query returned {len(unique)} snapshots for {domain}")
                return unique

            # Иначе — лог и fall back
            self.logger.info(f"Primary query returned 0 results for {domain}, trying fallback (without fl/matchType)")

            fallback_results = await try_with_params(params_fallback)
            unique_f = list(dict.fromkeys(fallback_results))
            self.logger.info(f"Fallback query returned {len(unique_f)} snapshots for {domain}")
            return unique_f

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.logger.error(f"Error fetching snapshots for {domain}: {e}")
            return []

    async def _page_request(self, url: str, params: dict, headers: dict) -> Tuple[List[str], Optional[str]]:
        try:
            query_string = urlencode(params)
            full_url = f"{url}?{query_string}"
            self.logger.debug(f"CDX request to: {full_url}")
    
            async with self.session.get(full_url, headers=headers) as resp:
                self.logger.debug(f"CDX HTTP {resp.status} for {params.get('url')}")
                text = await resp.text()
                await self._handle_errors(resp)
    
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                self.logger.warning("CDX response is not valid JSON (maybe HTML or empty).")
                return [], None
    
            if not data or len(data) <= 1:
                return [], None
    
            resume_key = None
            if isinstance(data[-1], str) and data[-1].startswith("resumeKey:"):
                resume_key = data.pop().split(":", 1)[1]
    
            snapshots = []
            # обрабатываем строки (первая — заголовки)
            for entry in data[1:]:
                # entry может иметь разную длину в зависимости от fl — обработаем гибко
                if not isinstance(entry, list) or len(entry) < 3:
                    continue
                
                # Возможные форматы:
                # 1) [timestamp, original, statuscode, mimetype, ...]  (если fl указан)
                # 2) [urlkey, timestamp, original, mimetype, statuscode, digest, length] (без fl)
                # Попытаемся достать timestamp и original в обеих формах:
    
                ts = None
                original = None
    
                # Если entry длина >= 7 и первый — urlkey
                if len(entry) >= 7 and isinstance(entry[0], str) and entry[0].find(',') != -1:
                    # формат с urlkey первым
                    _, ts, original, *rest = entry
                else:
                    # обычный формат: timestamp, original, statuscode, mimetype
                    ts = entry[0] if len(entry) > 0 else None
                    original = entry[1] if len(entry) > 1 else None
    
                if not ts or not original:
                    continue
                
                # Пропускаем шаблонные урлы
                if "*" in original:
                    continue
                
                # Формируем итоговый wayback URL
                try:
                    wayback_url = self._build_wayback_url(ts, original)
                    if wayback_url:
                        snapshots.append(wayback_url)
                except Exception:
                    continue
                
            self.logger.debug(f"Page fetched: {len(snapshots)} entries, resumeKey={resume_key}")
            return snapshots, resume_key
    
        except asyncio.TimeoutError:
            self.logger.exception("Timeout while requesting CDX")
            raise
        except aiohttp.ClientError:
            self.logger.exception("ClientError while requesting CDX")
            raise

    def _build_wayback_url(self, timestamp: str, original: str) -> Optional[str]:
        try:
            orig = unquote(original)
        except Exception:
            orig = original

        if "*" in orig:
            return None

        encoded = quote(orig, safe=":/")
        return f"https://web.archive.org/web/{timestamp}id_/{encoded}"


    def _build_wayback_url(self, timestamp: str, original: str) -> Optional[str]:
        # Раздекодируем исходный original (если он закодирован)
        try:
            orig = unquote(original)
        except Exception:
            orig = original

        # не строим URL если остались шаблонные символы
        if "*" in orig:
            return None

        # Сохраняем корректную кодировку для вставки в путь web.archive.org
        # quote с безопасными символами :/ сохраняет схему и слеши
        encoded = quote(orig, safe=":/")
        return f"https://web.archive.org/web/{timestamp}id_/{encoded}"




async def test_tor_request():
    logging.basicConfig(level=logging.DEBUG)

    proxy = "socks5://127.0.0.1:9050"
    connector = ProxyConnector.from_url(proxy, rdns=True)

    # Создаем объект таймаута
    timeout = aiohttp.ClientTimeout(total=90) # 90 секунд

    # Передаем коннектор и таймаут в СЕССИЮ
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # В WaybackCDXClient больше не нужно передавать ничего, кроме сессии
        client = WaybackCDXClient(
            session=session,
            max_retries=2
        )

        domain = "pya.cc"
        print(f"🔍 Тестируем домен {domain} через Tor...")

        try:
            snapshots = await client.fetch_snapshots(
                domain,
                from_date="20050101000000",
                to_date="20051231235959"
            )
            print(f"✅ Получено {len(snapshots)} снимков")
            if snapshots:
                print("Пример:", snapshots[0])
        except Exception as e:
            print("❌ Ошибка:", e)

if __name__ == "__main__":
    asyncio.run(test_tor_request())