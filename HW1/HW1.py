
import asyncio
import logging
import time
from typing import Optional

import aiohttp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть не меньше 1")

        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_requests = 0
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        """Создаёт HTTP-сессию, если она ещё не открыта."""
        if self.session is not None and not self.session.closed:
            logging.info("HTTP-сессия уже открыта")
            return

        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=5,
            sock_read=5
        )

        self.session = aiohttp.ClientSession(timeout=timeout)
        logging.info("HTTP-сессия создана")

    async def close(self) -> None:
        """Безопасно закрывает HTTP-сессию."""
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logging.info("HTTP-сессия закрыта")

    async def fetch_url(self, url: str) -> Optional[str]:
        """Загружает страницу. При сетевой ошибке возвращает None."""
        if self.session is None or self.session.closed:
            raise RuntimeError(
                "HTTP-сессия не открыта. Сначала вызови start()."
            )

        async with self.semaphore:
            self.active_requests += 1

            logging.info(
                f"Начинаем загрузку: {url}. "
                f"Активных запросов: {self.active_requests}"
            )

            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    html = await response.text()

                    logging.info(
                        f"Сайт {url} загружен. "
                        f"HTTP-статус: {response.status}"
                    )

                    return html

            except asyncio.TimeoutError as e:
                logging.warning(
                    f"Тайм-аут при загрузке {url}. "
                    f"Тип ошибки: {type(e).__name__}"
                )
                return None

            except aiohttp.ClientResponseError as e:
                logging.error(
                    f"Ошибка HTTP при загрузке {url}. "
                    f"Статус: {e.status}. "
                    f"Тип ошибки: {type(e).__name__}"
                )
                return None

            except aiohttp.ClientError as e:
                logging.error(
                    f"Сетевая ошибка при загрузке {url}. "
                    f"Тип ошибки: {type(e).__name__}"
                )
                return None

            finally:
                self.active_requests -= 1

                logging.info(
                    f"Запрос завершён: {url}. "
                    f"Активных запросов: {self.active_requests}"
                )

    async def fetch_urls(
        self,
        urls: list[str]
    ) -> dict[str, Optional[str]]:
        """Конкурентно загружает список страниц."""
        results = await asyncio.gather(
            *(self.fetch_url(url) for url in urls)
        )

        return dict(zip(urls, results))


async def main() -> None:
    urls = [
        "https://example.com/nonexistent-page-12345",
        "https://python.org",
        "https://httpbin.org/html",
        "https://www.wikipedia.org",
        "https://www.iana.org",
        "https://httpbin.org/delay/8"
    ]

    crawler = AsyncCrawler(max_concurrent=2)

    try:
        await crawler.start()

        # 1. Последовательная загрузка
        logging.info("Начинаем последовательную загрузку")

        start_time = time.perf_counter()

        for url in urls:
            await crawler.fetch_url(url)

        sequential_time = time.perf_counter() - start_time

        print(
            f"\nПоследовательная загрузка: "
            f"{sequential_time:.2f} секунд"
        )

        # 2. Конкурентная загрузка
        logging.info("Начинаем конкурентную загрузку")

        start_time = time.perf_counter()

        results = await crawler.fetch_urls(urls)

        concurrent_time = time.perf_counter() - start_time

        print(
            f"Конкурентная загрузка: "
            f"{concurrent_time:.2f} секунд"
        )

        # 3. Результаты загрузки
        print("\nРезультаты:")

        for url, html in results.items():
            if html is not None:
                print(f"{url} — успешно")
            else:
                print(f"{url} — ошибка")

        successful = sum(
            1 for html in results.values()
            if html is not None
        )

        print(
            f"\nУспешно загружено: "
            f"{successful} из {len(urls)} страниц"
        )

        # 4. Сравнение времени
        if concurrent_time > 0:
            speedup = sequential_time / concurrent_time
            print(f"Ускорение: {speedup:.2f}x")

            if concurrent_time < sequential_time:
                print("Конкурентная загрузка оказалась быстрее.")
            else:
                print(
                    "В этом запуске конкурентная загрузка "
                    "не оказалась быстрее."
                )

    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())