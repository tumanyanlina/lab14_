"""
nats_consumer.py — Обработка потоковых данных через NATS
Читает AggregatedRecord из subject mfc.queue.metrics в реальном времени,
применяет скользящее окно 5 минут, выводит агрегированную статистику.
"""

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timezone

import nats


NATS_URL = "nats://localhost:4222"
NATS_SUBJECT = "mfc.queue.metrics"
SLIDING_WINDOW_SEC = 300  # 5 минут


def sliding_window_stats(window: deque) -> dict:
    if not window:
        return {}

    by_service: dict[str, list[float]] = {}
    for ts, rec in window:
        svc = rec["service_type"]
        if svc not in by_service:
            by_service[svc] = []
        by_service[svc].append(rec["avg_queue_length"])

    return {
        svc: {
            "avg_queue": round(sum(q) / len(q), 2),
            "max_queue": round(max(q), 2),
            "count": len(q),
        }
        for svc, q in by_service.items()
    }


async def main() -> None:
    print(f"[INFO] подключение к NATS: {NATS_URL}")
    print(f"[INFO] subject: {NATS_SUBJECT}")
    print(f"[INFO] скользящее окно: {SLIDING_WINDOW_SEC} сек")
    print("[INFO] ожидание сообщений... (Ctrl+C для остановки)\n")

    nc = await nats.connect(NATS_URL)
    print("[INFO] подключено к NATS")

    window: deque = deque()
    total_messages = 0
    last_stats_time = time.time()

    async def message_handler(msg):
        nonlocal total_messages, last_stats_time

        try:
            rec = json.loads(msg.data.decode())
        except Exception:
            return

        now = time.time()
        window.append((now, rec))
        total_messages += 1

        print(f"[MSG] window_id={rec['window_id']} "
              f"service={rec['service_type']} "
              f"avg_queue={rec['avg_queue_length']:.1f}")

        # Удалить старые записи из скользящего окна.
        while window and now - window[0][0] > SLIDING_WINDOW_SEC:
            window.popleft()

        # Каждые 15 секунд выводить статистику.
        if now - last_stats_time >= 15:
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[{ts}] === Скользящее окно {SLIDING_WINDOW_SEC}s "
                  f"({len(window)} записей, всего={total_messages}) ===")
            for svc, s in sorted(sliding_window_stats(window).items()):
                print(f"  {svc:20s}: avg_queue={s['avg_queue']:5.1f} "
                      f"max={s['max_queue']:5.1f} count={s['count']}")
            last_stats_time = now

    await nc.subscribe(NATS_SUBJECT, cb=message_handler)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[INFO] остановлено. Всего: {total_messages} сообщений")
    finally:
        await nc.drain()
        print("[INFO] nats consumer закрыт")


if __name__ == "__main__":
    asyncio.run(main())