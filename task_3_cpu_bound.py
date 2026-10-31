"""Задание 3 со звёздочкой. CPU-вычисления и живой event loop.

Сначала асинхронно скачиваем 20 картинок. Одни и те же байты используем в двух
сравнениях: прямой вызов heavy и run_in_executor с ProcessPoolExecutor.
ticker печатает точку каждые 0.1 с и измеряет максимальный промежуток между ними.
Прямой вызов heavy занимает поток event loop: даже при освобождении GIL
внутри hashlib этот же поток не возвращается к планировщику asyncio.
Отдельные процессы оставляют event loop возможность исполнять ticker.
Пул ограничен двумя процессами, входной фрагмент — 64 КиБ, чтобы не раздувать RAM.
Защита __main__ обязательна для запуска процессов на macOS и Windows.

Замер 31.10.2026, Python 3.12.14, 20 картинок:
загрузка 1.915 с; прямой CPU-вызов 2.619 с, пауза ticker 2.619 с;
пул процессов 1.457 с, максимальная пауза ticker 0.101 с.
Хеши двух вариантов совпали. Полные результаты: measurements/latest.json.
"""

import asyncio
import hashlib
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress

import httpx

from common import NETWORK_TIMEOUT, URLS, timed


def heavy(body: bytes) -> str:
    # До 64 МиБ на процесс. Повторяем хеширование, чтобы пауза была заметна.
    expanded = body[:65536] * 1000
    digest = hashlib.sha256()
    for _ in range(8):
        digest.update(expanded)
    return digest.hexdigest()


async def ticker(gaps):
    previous = time.perf_counter()
    while True:
        print(".", end="", flush=True)
        await asyncio.sleep(0.1)
        now = time.perf_counter()
        gaps.append(now - previous)
        previous = now


async def download_bodies(urls=URLS):
    async def download(client, url):
        response = await client.get(url)
        response.raise_for_status()
        return response.content

    async with httpx.AsyncClient(follow_redirects=True, timeout=NETWORK_TIMEOUT) as client:
        return await asyncio.gather(*(download(client, url) for url in urls))


async def compare_cpu(bodies, use_processes):
    gaps = []
    pulse = asyncio.create_task(ticker(gaps))
    await asyncio.sleep(0)  # Дать ticker запуститься перед тяжёлой работой.
    start = time.perf_counter()
    try:
        if use_processes:
            loop = asyncio.get_running_loop()
            with ProcessPoolExecutor(max_workers=2) as pool:
                hashes = await asyncio.gather(
                    *(loop.run_in_executor(pool, heavy, body) for body in bodies)
                )
        else:
            hashes = [heavy(body) for body in bodies]
        elapsed = time.perf_counter() - start
        # Дать ticker измерить и записать задержку после синхронной блокировки.
        await asyncio.sleep(0.12)
    finally:
        pulse.cancel()
        with suppress(asyncio.CancelledError):
            await pulse
        print()
    return hashes, {"seconds": elapsed, "max_tick_gap_seconds": max(gaps, default=0.0)}


async def cpu_main():
    start = time.perf_counter()
    bodies = await download_bodies()
    download_time = time.perf_counter() - start
    print(f"Загружено картинок: {len(bodies)}; сеть: {download_time:.3f} с")
    print("Прямой вызов heavy; наблюдай за точками:")
    direct_hashes, direct = await compare_cpu(bodies, use_processes=False)
    print("ProcessPoolExecutor; наблюдай за точками:")
    process_hashes, processes = await compare_cpu(bodies, use_processes=True)
    if direct_hashes != process_hashes:
        raise AssertionError("Хеши двух вариантов должны совпасть")
    print(f"Прямой вызов: {direct['seconds']:.3f} с; пауза {direct['max_tick_gap_seconds']:.3f} с")
    print(
        f"Процессы: {processes['seconds']:.3f} с; пауза {processes['max_tick_gap_seconds']:.3f} с"
    )
    return {
        "download_seconds": download_time,
        "direct": direct,
        "processes": processes,
        "hashes_equal": True,
        "images": len(bodies),
    }


def run_demo():
    result, _ = timed("CPU demo целиком", lambda: asyncio.run(cpu_main()))
    return result


if __name__ == "__main__":
    run_demo()
