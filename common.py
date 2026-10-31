"""Общие адреса и замер времени. Здесь нет запуска сетевых запросов при импорте."""

import time

URLS = [f"https://picsum.photos/seed/{i}/800/600" for i in range(20)]
NETWORK_TIMEOUT = 30.0


def timed(label, fn, *args):
    """Вернуть результат функции и время. fn должна быть обычной функцией."""
    start = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.3f} с")
    return result, elapsed
