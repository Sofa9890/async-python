"""Задание 2. Таймауты, три попытки и независимая обработка ошибок.

На одну попытку отводится 3 секунды целиком через asyncio.timeout.
Дополнительно HTTPX ограничивает ожидание отдельных сетевых операций.
Три попытки означают две паузы: 1 и 2 секунды. Для пауз 1, 2, 4 нужны 4 попытки.
Повторяем сетевые ошибки, таймауты, HTTP 429 и 5xx. Остальные HTTP-ошибки
(например 404) повторять бессмысленно, поэтому возвращаем их без ретраев.
gather(return_exceptions=True) дожидается остальных запросов при одной ошибке.
Результат robust_main: (список успешных результатов, список ошибок), с URL.
CancelledError не перехватывается: внешняя отмена корректно распространяется.

Замер 31.10.2026, Python 3.12.14: 12.025 с на 22 URL.
20 картинок получены; несуществующий домен дал ConnectError,
httpbin.org/delay/10 — TimeoutError. Все три попытки выполнены.
Полные результаты: measurements/latest.json.
"""

import asyncio

import httpx

from common import URLS, timed

REQUEST_TIMEOUT = 3.0
ATTEMPTS = 3
BROKEN_URLS = ["https://no-such-site.invalid", "https://httpbin.org/delay/10"]


async def robust_fetch(
    client: httpx.AsyncClient,
    url: str,
    *,
    attempts: int = ATTEMPTS,
    timeout: float = REQUEST_TIMEOUT,
    backoff: float = 1.0,
) -> int:
    if attempts < 1:
        raise ValueError("attempts должно быть не меньше 1")
    if timeout <= 0 or backoff < 0:
        raise ValueError("timeout должен быть > 0, backoff должен быть >= 0")

    for attempt in range(attempts):
        try:
            async with asyncio.timeout(timeout):
                response = await client.get(url, timeout=timeout)
                response.raise_for_status()
                return response.status_code
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status != 429 and status < 500:
                raise
            if attempt == attempts - 1:
                raise
        except (httpx.RequestError, TimeoutError):
            if attempt == attempts - 1:
                raise

        delay = backoff * 2**attempt
        print(f"Повтор {attempt + 2}/{attempts} через {delay:g} с: {url}")
        await asyncio.sleep(delay)


async def robust_main(urls=None):
    urls = list(URLS + BROKEN_URLS if urls is None else urls)
    # Неверный домен проверяем через DNS напрямую, без возможного HTTP-прокси.
    async with httpx.AsyncClient(follow_redirects=True, trust_env=False) as client:
        items = await asyncio.gather(
            *(robust_fetch(client, url) for url in urls), return_exceptions=True
        )

    results, errors = [], []
    for url, item in zip(urls, items, strict=True):
        if isinstance(item, BaseException):
            errors.append({"url": url, "type": type(item).__name__, "message": str(item)})
        else:
            results.append({"url": url, "status": item})
    return results, errors


def run_demo():
    (results, errors), elapsed = timed("robust async", lambda: asyncio.run(robust_main()))
    print(f"Успешно: {len(results)}; ошибок: {len(errors)}")
    for error in errors:
        print(f"  {error['type']}: {error['url']}")
    return {"seconds": elapsed, "results": results, "errors": errors}


if __name__ == "__main__":
    run_demo()
