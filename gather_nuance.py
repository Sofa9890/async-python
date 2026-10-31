"""Разбор двух записей gather без влияния сети.

Это моделирование ожидания через Event, НЕ замер загрузки URL.
Обе версии создают N корутин и N задач. Отдельный список в локальной переменной
сохраняется на время await; временный список inline-варианта в CPython уже удалён.
Измеряем удерживаемый список напрямую, а память Python — через tracemalloc.
tracemalloc добавляет накладные расходы; это не чистый тест скорости HTTP.
Генератор под * тоже полностью распаковывается перед вызовом gather.
"""

import argparse
import asyncio
import gc
import statistics
import sys
import time
import tracemalloc


async def fetch(gate: asyncio.Event, url: int) -> int:
    await gate.wait()
    return url


async def with_list(gate, urls):
    tasks = [fetch(gate, url) for url in urls]
    return await asyncio.gather(*tasks)


async def inline(gate, urls):
    return await asyncio.gather(*[fetch(gate, url) for url in urls])


async def generator(gate, urls):
    return await asyncio.gather(*(fetch(gate, url) for url in urls))


async def measure_case(fn, count):
    gate = asyncio.Event()
    parent = asyncio.create_task(fn(gate, range(count)))
    # 1: родитель создаст gather; 2: все дочерние задачи встанут на Event.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    # cr_frame используем только для наблюдения в учебном эксперименте.
    local_list = parent.get_coro().cr_frame.f_locals.get("tasks")
    retained_list_bytes = sys.getsizeof(local_list) if local_list is not None else 0
    live_children = len(asyncio.all_tasks()) - 2  # Без измерителя и parent.
    waiting_bytes, _ = tracemalloc.get_traced_memory()
    gate.set()
    results = await parent
    if results != list(range(count)):
        raise AssertionError("Результаты должны сохранять порядок URL")
    return {
        "retained_list_bytes": retained_list_bytes,
        "live_children": live_children,
        "waiting_mib": waiting_bytes / 1024**2,
    }


def run_demo(counts=(100, 10000, 50000), repeats=3):
    rows = []
    for count in counts:
        samples = {"list": [], "inline": [], "generator": []}
        variants = [("list", with_list), ("inline", inline), ("generator", generator)]
        for repeat in range(repeats):
            # Меняем порядок, чтобы первая версия не оказывалась всегда первой.
            order = variants[repeat % 3 :] + variants[: repeat % 3]
            for name, fn in order:
                gc.collect()
                tracemalloc.start()
                start = time.perf_counter()
                try:
                    sample = asyncio.run(measure_case(fn, count))
                    sample["seconds"] = time.perf_counter() - start
                    sample["peak_mib"] = tracemalloc.get_traced_memory()[1] / 1024**2
                finally:
                    tracemalloc.stop()
                samples[name].append(sample)
        for name, items in samples.items():
            row = {"count": count, "variant": name, "repeats": repeats}
            for key in items[0]:
                row[key] = statistics.median(item[key] for item in items)
            rows.append(row)
            print(
                f"N={count:>6}, {name:9}: {row['seconds']:.3f} с; "
                f"задач {row['live_children']}; список {row['retained_list_bytes']} байт; "
                f"ожидание {row['waiting_mib']:.2f} МиБ; пик {row['peak_mib']:.2f} МиБ"
            )
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сравнение gather; сеть не используется")
    parser.add_argument("--count", type=int, nargs="+", default=[100, 10000, 50000])
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if min(args.count) < 1 or args.repeats < 1:
        parser.error("count и repeats должны быть положительными")
    run_demo(tuple(args.count), args.repeats)
