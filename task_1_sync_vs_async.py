"""Задание 1. requests и httpx.AsyncClient на одинаковых 20 картинках.

Синхронный цикл ждёт завершения каждого запроса. gather позволяет перекрыть
ожидания сети. Это конкурентность, а не выполнение Python-кода в 20 потоках.
Обе версии скачивают тела ответов, следуют редиректам и проверяют HTTP-статус.
Session у requests и один AsyncClient у httpx переиспользуют соединения.
Разовый замер зависит от сети, сервера, DNS, TLS и кеширования.

Замер 31.10.2026, Python 3.12.14, 20 URL:
requests 5.942 с; httpx async 1.178 с; отношение времени 5.04.
В обоих случаях получено 20 успешных ответов.
Полные результаты: measurements/latest.json и README.md.
"""

import asyncio

import httpx
import requests

from common import NETWORK_TIMEOUT, URLS, timed


def sync_main(urls=URLS):
    statuses = []
    with requests.Session() as client:
        for url in urls:
            response = client.get(url, timeout=NETWORK_TIMEOUT)
            response.raise_for_status()
            statuses.append(response.status_code)
    return statuses


async def fetch(client: httpx.AsyncClient, url: str) -> int:
    response = await client.get(url)
    response.raise_for_status()
    return response.status_code


async def fast_main(urls=URLS):
    async with httpx.AsyncClient(follow_redirects=True, timeout=NETWORK_TIMEOUT) as client:
        return await asyncio.gather(*(fetch(client, url) for url in urls))


def run_demo():
    sync_results, sync_time = timed("requests sync", sync_main)
    async_results, async_time = timed("httpx async", lambda: asyncio.run(fast_main()))
    print(f"Отношение sync / async в этом запуске: {sync_time / async_time:.2f}")
    return {
        "sync": {"seconds": sync_time, "successful": len(sync_results)},
        "async": {"seconds": async_time, "successful": len(async_results)},
        "speedup": sync_time / async_time,
    }


if __name__ == "__main__":
    run_demo()
