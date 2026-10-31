"""Задание 0. Почему async не ускоряет последовательный цикл.

В slow_main следующий запрос начинается только после завершения предыдущего.
await приостанавливает текущую корутину, но сам по себе не создаёт другие задачи.
В fast_main сначала создаются объекты корутин; gather планирует их как Task.
Пока один запрос ждёт сеть, event loop может выполнять остальные задачи.
Имя tasks в первом варианте обозначает список корутин, а не готовые Task.
Вариант fast_main_inline запускает те же запросы с той же конкурентностью.
follow_redirects=True нужен, чтобы скачать картинку после ответа 302.

Замер 31.10.2026, Python 3.12.14, 20 URL:
slow_main 6.489 с; fast_main 1.060 с; fast_main_inline 1.339 с.
Все версии получили 20 успешных ответов. Это один сетевой запуск;
разница между gather-вариантами здесь не доказывает влияние синтаксиса.
Полные результаты: measurements/latest.json и README.md.
"""

import asyncio

import httpx

from common import NETWORK_TIMEOUT, URLS, timed


async def fetch(client: httpx.AsyncClient, url: str) -> int:
    response = await client.get(url)
    response.raise_for_status()
    return response.status_code


async def slow_main(urls=URLS):
    async with httpx.AsyncClient(follow_redirects=True, timeout=NETWORK_TIMEOUT) as client:
        results = []
        for url in urls:
            results.append(await fetch(client, url))
        return results


async def fast_main(urls=URLS):
    async with httpx.AsyncClient(follow_redirects=True, timeout=NETWORK_TIMEOUT) as client:
        tasks = [fetch(client, url) for url in urls]
        return await asyncio.gather(*tasks)


async def fast_main_inline(urls=URLS):
    async with httpx.AsyncClient(follow_redirects=True, timeout=NETWORK_TIMEOUT) as client:
        return await asyncio.gather(*[fetch(client, url) for url in urls])


def run_demo():
    measurements = {}
    for name, fn in [
        ("slow", slow_main),
        ("gather_list", fast_main),
        ("gather_inline", fast_main_inline),
    ]:
        statuses, elapsed = timed(name, lambda: asyncio.run(fn()))
        measurements[name] = {"seconds": elapsed, "successful": len(statuses)}
    return measurements


if __name__ == "__main__":
    run_demo()
