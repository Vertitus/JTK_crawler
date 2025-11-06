import asyncio
import aiohttp
import logging
from wayback_cdx import WaybackCDXClient
from rate_limiter import RateLimiter


async def test_domain(domain: str):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    async with aiohttp.ClientSession() as session:
        rate_limiter = RateLimiter(min_interval=1.5)
        client = WaybackCDXClient(
            session=session,
            max_retries=3,
            backoff_factor=2,
            request_timeout=20,
            max_pages=0,         # Ограничим до 2 страниц для быстрой отладки
            page_size=1000,       # Можно уменьшить/увеличить при необходимости
            rate_limiter=rate_limiter
        )

        print(f"🔍 Запрашиваем снапшоты для: {domain}")
        snapshots = await client.fetch_snapshots(
            domain=domain,
            from_date="20050101000000",
            to_date="20051231235959"
        )

        print(f"\n✅ Найдено: {len(snapshots)} снимков")
        print("\n🔗 Примеры ссылок:")
        for url in snapshots[:10]:
            print("  ", url)


if __name__ == "__main__":
    # 💡 Вставь сюда нужный домен для теста
    domain_to_test = "pya.cc"  # ← замени на свой домен
    asyncio.run(test_domain(domain_to_test))
