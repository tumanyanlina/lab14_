import asyncio
import json
import os
import random
import time
from pathlib import Path

import psutil

DATA_DIR = Path(__file__).parent.parent / "data"
NUM_WINDOWS = 8
EMIT_INTERVAL = 2.0   # секунды между тиками
WINDOW_DUR = 30       # секунды tumbling window
RUN_DURATION = 60     # секунды работы сборщика
OUTPUT_FILE = DATA_DIR / "python_collector_metrics.json"

SERVICES = ["passport", "registration", "social_benefit", "tax", "property"]


class WindowState:
    def __init__(self, window_id: int):
        self.window_id = window_id
        self.service_type = SERVICES[window_id % len(SERVICES)]
        self.queue_len = random.randint(0, 4)

    def update(self) -> None:
        self.queue_len += random.randint(-2, 2)
        self.queue_len = max(0, min(30, self.queue_len))

    def estimate_wait(self) -> int:
        result = self.queue_len * 240 + random.randint(-30, 30)
        return max(0, result)

class Aggregator:
    def __init__(self, window_dur: int):
        self.window_dur = window_dur
        self.buckets: dict = {}

    def add(self, record: dict) -> None:
        ts = record["timestamp"]
        bucket_id = int(ts) // self.window_dur
        key = (record["window_id"], bucket_id)

        if key not in self.buckets:
            self.buckets[key] = {
                "service_type": record["service_type"],
                "sum_queue": 0,
                "max_queue": 0,
                "min_queue": record["queue_length"],
                "sum_wait": 0,
                "max_wait": 0,
                "count": 0,
            }

        acc = self.buckets[key]
        acc["sum_queue"] += record["queue_length"]
        acc["sum_wait"] += record["wait_time_sec"]
        acc["count"] += 1
        acc["max_queue"] = max(acc["max_queue"], record["queue_length"])
        acc["min_queue"] = min(acc["min_queue"], record["queue_length"])
        acc["max_wait"] = max(acc["max_wait"], record["wait_time_sec"])

    def flush(self, now: float) -> list[dict]:
        current_bucket = int(now) // self.window_dur
        results = []
        done_keys = []

        for (window_id, bucket_id), acc in self.buckets.items():
            if bucket_id >= current_bucket:
                continue
            results.append({
                "window_id": window_id,
                "service_type": acc["service_type"],
                "avg_queue_length": acc["sum_queue"] / acc["count"],
                "max_queue_length": acc["max_queue"],
                "min_queue_length": acc["min_queue"],
                "avg_wait_time_sec": acc["sum_wait"] / acc["count"],
                "max_wait_time_sec": acc["max_wait"],
                "sample_count": acc["count"],
                "window_start": bucket_id * self.window_dur,
                "window_end": (bucket_id + 1) * self.window_dur,
            })
            done_keys.append((window_id, bucket_id))

        for key in done_keys:
            del self.buckets[key]

        return results

async def emit_window(state: WindowState, queue: asyncio.Queue) -> None:
    """Корутина для одного окошка — эмитирует записи каждые EMIT_INTERVAL сек."""
    while True:
        state.update()
        await queue.put({
            "window_id": state.window_id,
            "service_type": state.service_type,
            "queue_length": state.queue_len,
            "wait_time_sec": state.estimate_wait(),
            "timestamp": time.time(),
        })
        await asyncio.sleep(EMIT_INTERVAL)


async def run_collector(duration: int) -> dict:
    """Запускает все корутины окошек и агрегатор, возвращает метрики."""
    process = psutil.Process(os.getpid())
    queue: asyncio.Queue = asyncio.Queue(maxsize=NUM_WINDOWS * 10)
    aggregator = Aggregator(WINDOW_DUR)
    windows = [WindowState(i + 1) for i in range(NUM_WINDOWS)]

    records_raw = 0
    records_agg = 0
    start_time = time.perf_counter()
    start_cpu = process.cpu_times()
    start_mem = process.memory_info().rss

    tasks = [
        asyncio.create_task(emit_window(w, queue))
        for w in windows
    ]

    flush_interval = 5.0
    last_flush = time.time()
    deadline = time.time() + duration

    print(f"[INFO] Python-сборщик запущен: {NUM_WINDOWS} окошек, "
          f"интервал={EMIT_INTERVAL}s, окно={WINDOW_DUR}s")

    while time.time() < deadline:
        try:
            record = await asyncio.wait_for(queue.get(), timeout=1.0)
            aggregator.add(record)
            records_raw += 1
        except asyncio.TimeoutError:
            pass

        if time.time() - last_flush >= flush_interval:
            flushed = aggregator.flush(time.time())
            records_agg += len(flushed)
            if flushed:
                print(f"[INFO] flush: {len(flushed)} агрегатов "
                      f"(всего raw={records_raw}, agg={records_agg})")
            last_flush = time.time()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.perf_counter() - start_time
    end_cpu = process.cpu_times()
    end_mem = process.memory_info().rss

    return {
        "duration_sec": round(elapsed, 2),
        "records_raw": records_raw,
        "records_agg": records_agg,
        "throughput_rps": round(records_raw / elapsed, 2),
        "cpu_user_sec": round(end_cpu.user - start_cpu.user, 3),
        "cpu_sys_sec": round(end_cpu.system - start_cpu.system, 3),
        "mem_delta_mb": round((end_mem - start_mem) / 1024 / 1024, 2),
        "mem_peak_mb": round(end_mem / 1024 / 1024, 2),
    }

def main() -> None:
    print(f"[INFO] запуск на {RUN_DURATION} секунд...")
    metrics = asyncio.run(run_collector(RUN_DURATION))

    print("\n=== Метрики Python-сборщика ===")
    for key, val in metrics.items():
        print(f"  {key:25s}: {val}")

    DATA_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[INFO] метрики сохранены в {OUTPUT_FILE}")


if __name__ == "__main__":
    main()