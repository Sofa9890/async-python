"""Запустить задания 0–3 и эксперимент gather, сохранить фактические замеры."""

import json
import platform
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import gather_nuance
import task_0_fix_async
import task_1_sync_vs_async
import task_2_robust_fetch
import task_3_cpu_bound


def main():
    results = {
        "measured_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "system": platform.system(),
        "url_count": 20,
        "note": "Сетевые времена — один реальный запуск; gather_nuance — модель без сети.",
    }
    demos = [
        ("task_0", task_0_fix_async.run_demo),
        ("task_1", task_1_sync_vs_async.run_demo),
        ("task_2", task_2_robust_fetch.run_demo),
        ("task_3", task_3_cpu_bound.run_demo),
        ("gather_nuance", gather_nuance.run_demo),
    ]
    destination = Path(__file__).parent / "measurements" / "latest.json"
    destination.parent.mkdir(exist_ok=True)
    for name, demo in demos:
        print(f"\n{name}")
        results[name] = demo()
        # Сохраняем и промежуточный результат после каждого выполненного задания.
        destination.write_text(
            json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(f"\nЗамеры сохранены: {destination}")


if __name__ == "__main__":
    main()
