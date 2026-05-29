# Лабораторная работа №14
## Разработка конвейеров обработки данных на Python и Go

Студент: Туманян Лина Врежовна
Группа: 220032-11
Вариант: 27 (Мониторинг очередей в МФЦ)
Уровень сложности: Повышенный

---

## Архитектура конвейера

```
[Go-сборщик]
  ├── Эмулятор датчиков (8 окошек МФЦ, горутины)
  │     ↓ raw QueueRecord (каждые 2 сек)
  ├── Агрегатор (tumbling window 30 сек)
  │     ↓ AggregatedRecord
  ├── Writer       → data/*.ndjson
  ├── ArrowWriter  → data/*.arrow
  └── NatsWriter   → NATS subject mfc.queue.metrics
        ↓
[Python — Polars + DuckDB]
  ├── ingest.py           → очистка → data/mfc_clean.parquet
  ├── aggregate.py        → агрегации + DuckDB benchmark
  ├── visualize.py        → 3 графика Plotly
  ├── arrow_reader.py     → Arrow vs NDJSON benchmark
  ├── nats_consumer.py    → скользящее окно 5 мин
  ├── collector_python.py → Go vs Python benchmark
  └── validate_with_rust.py → валидация через Rust
        ↓
[Rust — mfc_validator]
  └── PyO3 библиотека валидации данных
        ↓
[Streamlit дашборд]
  └── dashboard/app.py — реалтайм, фильтры, KPI, Go vs Python
```

---

## Выполненные задания повышенного уровня

| № | Задание | Статус | Файл |
|---|---------|--------|------|
| П.1 | Распределённый сборщик (etcd) | ✅ | `collector/coordinator.go` |
| П.2 | Оконная агрегация (tumbling window 30с) | ✅ | `collector/aggregator.go` |
| П.3 | Apache Arrow IPC | ✅ | `collector/arrow_writer.go`, `analysis/arrow_reader.py` |
| П.4 | Rust-библиотека валидации (PyO3) | ✅ | `validator/src/lib.rs`, `analysis/validate_with_rust.py` |
| П.5 | Docker + Kubernetes манифесты | ⚠️ | `Dockerfile`, `k8s/` |
| Сравнение Go vs Python | ✅ | `analysis/collector_python.py` |
| NATS стриминг + скользящее окно | ✅ | `collector/nats_writer.go`, `analysis/nats_consumer.py` |
| П.6 | Streamlit дашборд реалтайм | ✅ | `dashboard/app.py` |

> **Примечание по П.5:** Dockerfile подготовлен, образ успешно собран (`docker build`).
> Два инстанса сборщика (`mfc-collector-1`, `mfc-collector-2`) запущены в Docker.
> Kubernetes-манифесты (Deployment + HPA) подготовлены.
> Полный деплой в minikube невозможен из-за ограничений Windows 11 Home —
> отсутствует поддержка Hyper-V, драйвер docker завершается с ошибкой
> `exec format error` (несовместимость архитектуры образа kicbase).
> Команды запуска задокументированы в `docs/kubernetes.md`.

---

## Структура проекта

```
lab14/
├── collector/
│   ├── main.go           # точка входа, горутины, graceful shutdown
│   ├── emulator.go       # эмулятор датчиков 8 окошек МФЦ
│   ├── aggregator.go     # tumbling window агрегация (30 сек)
│   ├── writer.go         # пакетная запись NDJSON
│   ├── arrow_writer.go   # запись Apache Arrow IPC файлов
│   ├── nats_writer.go    # публикация в NATS
│   ├── coordinator.go    # координация через etcd
│   ├── go.mod
│   └── go.sum
├── analysis/
│   ├── ingest.py              # загрузка NDJSON → очистка → Parquet
│   ├── aggregate.py           # Polars + DuckDB + benchmark
│   ├── visualize.py           # 3 графика Plotly
│   ├── arrow_reader.py        # Arrow vs NDJSON benchmark
│   ├── nats_consumer.py       # NATS скользящее окно 5 мин
│   ├── collector_python.py    # сравнение Go vs Python asyncio
│   ├── validate_with_rust.py  # валидация через Rust PyO3
│   ├── plots/                 # PNG + HTML графики (генерируются)
│   └── requirements.txt
├── dashboard/
│   └── app.py            # Streamlit дашборд реалтайм
├── validator/
│   ├── src/
│   │   └── lib.rs        # Rust библиотека валидации (PyO3)
│   ├── Cargo.toml
│   └── test_validator.py
├── k8s/
│   ├── deployment.yaml   # Kubernetes Deployment (2 реплики)
│   └── hpa.yaml          # HPA (min=1, max=4, CPU 50%)
├── docs/
│   ├── architecture.md
│   └── kubernetes.md
├── data/                 # NDJSON, Arrow, Parquet файлы (генерируются)
├── Dockerfile
├── docker-compose.yml    # etcd + NATS
├── .gitignore
├── PROMPT_LOG.md
└── README.md
```

---

## Стек технологий

| Компонент | Технология |
|-----------|-----------|
| Сборщик данных | Go 1.22, `encoding/json`, `sync`, `os/signal` |
| Оконная агрегация | Go (tumbling window, custom) |
| Координация | etcd 3.5, `go.etcd.io/etcd/client/v3` |
| Потоковая передача | NATS 2.10, `github.com/nats-io/nats.go` |
| Хранение сырых данных | NDJSON + Apache Arrow IPC |
| Анализ данных | Python 3.13, Polars 1.0 |
| Колоночное хранилище | Apache Parquet |
| SQL-анализ | DuckDB 1.2.2 |
| Визуализация | Plotly 5.22 |
| Дашборд | Streamlit 1.35 |
| Валидация | Rust (PyO3 0.22), maturin |
| Инфраструктура | Docker, Docker Compose |
| Оркестрация | Kubernetes (манифесты) |

---

## Быстрый старт

### 1. Поднять инфраструктуру (etcd + NATS)

```powershell
docker-compose up -d
docker-compose ps
```

**Ожидаемый вывод:**
```
NAME            STATUS
laba14-etcd-1   Up
laba14-nats-1   Up
```

### 2. Запустить Go-сборщик

```powershell
cd collector
go mod tidy
go run .
```

**Ожидаемый вывод:**
```
2026/05/29 14:00:00 [INFO] MFC collector starting
2026/05/29 14:00:00 [INFO] windows=8, emit_interval=2s, tumbling_window=30s
2026/05/29 14:00:00 [INFO] coordinator: registered /mfc/shards/instance-1 = "instance=1 windows=[1 2 3 4 5 6 7 8]"
2026/05/29 14:00:00 [INFO] active instances: [instance=1 windows=[1 2 3 4 5 6 7 8]]
2026/05/29 14:00:00 [INFO] emulator started: 8 windows
2026/05/29 14:00:00 [INFO] aggregator started: window=30s
2026/05/29 14:00:00 [INFO] arrow writer started: output=../data
2026/05/29 14:00:00 [INFO] nats writer connected: nats://localhost:4222 subject=mfc.queue.metrics
2026/05/29 14:00:00 [INFO] writer started: batch=20, flush_interval=15s, output=../data
2026/05/29 14:00:30 [INFO] window flushed: window_id=1 service=passport avg_queue=4.3 samples=15
2026/05/29 14:00:45 [INFO] flushed 20 records → ../data/mfc_20260529_140045_0001.ndjson
```

Остановить: `Ctrl+C` — буфер будет сброшен (graceful shutdown).

### 3. Установить зависимости Python

```powershell
cd analysis
pip install -r requirements.txt
```

### 4. Загрузить и очистить данные

```powershell
python analysis/ingest.py
```

**Ожидаемый вывод:**
```
[INFO] loading 4 file(s) from data/
[INFO] loaded 32 rows, 10 columns
[STEP] приведение типов...
[STEP] удаление дубликатов (было 32)...
       осталось 32 (удалено 0)
[STEP] фильтрация невалидных значений...
       осталось 32
[INFO] данные сохранены в data/mfc_clean.parquet
```

### 5. Агрегационный анализ + DuckDB benchmark

```powershell
python analysis/aggregate.py
```

**Ожидаемый вывод:**
```
=== По типу услуги (Polars) ===
┌────────────────┬───────────┬───────────┐
│ service_type   ┆ avg_queue ┆ max_queue │
│ passport       ┆ 7.23      ┆ 19.0      │
│ social_benefit ┆ 4.82      ┆ 13.0      │
└────────────────┴───────────┴───────────┘

=== Сравнение производительности ===
Polars  : 0.53 ms  (5 строк)
DuckDB  : 13.01 ms  (5 строк)
Быстрее : Polars (в 24.7x)
```

### 6. Построить графики

```powershell
python analysis/visualize.py
```

Файлы сохраняются в `analysis/plots/`:
- `01_time_series.png` — динамика очереди по окошкам
- `02_heatmap.png` — тепловая карта нагрузки (окошко × час)
- `03_wait_distribution.png` — распределение времени ожидания

### 7. Arrow vs NDJSON benchmark

```powershell
python analysis/arrow_reader.py
```

**Ожидаемый вывод:**
```
=== Сравнение форматов: Arrow vs NDJSON ===
Arrow  : 31.16 ms
NDJSON : 58.48 ms
Быстрее: Arrow (в 1.9x)
```

### 8. NATS консьюмер (скользящее окно 5 мин)

Запустить Go-сборщик в одном терминале, затем в другом:

```powershell
python analysis/nats_consumer.py
```

**Ожидаемый вывод:**
```
[INFO] подключено к NATS
[MSG] window_id=1 service=passport avg_queue=8.7
[16:06:01] === Скользящее окно 300s (5 записей) ===
  passport    : avg_queue=  5.8 max=  8.7 count=2
  registration: avg_queue=  0.3 max=  0.3 count=1
```

### 9. Сравнение Go vs Python (60 сек)

```powershell
python analysis/collector_python.py
```

**Результаты:**
```
=== Метрики Python-сборщика ===
  duration_sec    : 60.24
  records_raw     : 240
  throughput_rps  : 3.98
  mem_peak_mb     : 25.2
```

Go потребляет ~3x меньше памяти (8.5 MB vs 25.2 MB).

### 10. Валидация через Rust

```powershell
cd validator
python -m venv .venv
.venv\Scripts\activate
pip install polars pyarrow tzdata
maturin develop
python ..\analysis\validate_with_rust.py
```

**Ожидаемый вывод:**
```
=== Результаты валидации ===
  Всего записей : 32
  Валидных      : 32
  Невалидных    : 0
✓ Все записи прошли валидацию!
```

### 11. Запустить дашборд

```powershell
streamlit run dashboard/app.py
```

Открыть в браузере: http://localhost:8501

### 12. Docker — запуск нескольких инстансов

```powershell
docker build -t mfc-collector:latest .
docker run -d --name mfc-collector-1 --network laba14_default -e INSTANCE_ID=1 -e ETCD_ENDPOINT=etcd:2379 mfc-collector:latest
docker run -d --name mfc-collector-2 --network laba14_default -e INSTANCE_ID=2 -e ETCD_ENDPOINT=etcd:2379 mfc-collector:latest
docker ps
```

---

## Описание данных

### Сырая запись (QueueRecord)

| Поле | Тип | Описание |
|------|-----|----------|
| `window_id` | int | Номер окошка МФЦ (1–8) |
| `service_type` | string | Тип услуги |
| `queue_length` | int | Длина очереди |
| `wait_time_sec` | int | Расчётное время ожидания (сек) |
| `timestamp` | datetime | Метка времени |

### Агрегированная запись (AggregatedRecord, tumbling window)

| Поле | Тип | Описание |
|------|-----|----------|
| `window_id` | int | Номер окошка |
| `service_type` | string | Тип услуги |
| `avg_queue_length` | float | Средняя длина очереди за окно |
| `max_queue_length` | int | Максимальная длина |
| `min_queue_length` | int | Минимальная длина |
| `avg_wait_time_sec` | float | Среднее время ожидания (сек) |
| `max_wait_time_sec` | int | Максимальное время ожидания |
| `sample_count` | int | Количество замеров в окне |
| `window_start` | datetime | Начало окна |
| `window_end` | datetime | Конец окна |

---

## Типы услуг МФЦ

| Код | Описание |
|-----|----------|
| `passport` | Паспортные услуги |
| `registration` | Регистрация по месту жительства |
| `social_benefit` | Социальные льготы и выплаты |
| `tax` | Налоговые услуги |
| `property` | Операции с недвижимостью |

---

## Анализ производительности

### Polars vs DuckDB

| Движок | Время | Строк |
|--------|-------|-------|
| Polars | 0.53 ms | 5 |
| DuckDB | 13.01 ms | 5 |

Polars быстрее в **24.7x** на малых данных за счёт работы в памяти.
DuckDB эффективнее на больших объёмах за счёт колоночного SQL-движка.

### Apache Arrow vs NDJSON

| Формат | Время чтения | Размер |
|--------|-------------|--------|
| Arrow | 31.16 ms | 20,458 байт |
| NDJSON | 58.48 ms | 15,339 байт |

Arrow быстрее в **1.9x**. На малых данных NDJSON компактнее,
на больших объёмах Arrow выигрывает и по размеру.

### Go vs Python (сборщик)

| Метрика | Go | Python asyncio |
|---------|-----|----------------|
| Throughput | 4.0 rec/s | 3.98 rec/s |
| Память peak | 8.5 MB | 25.2 MB |
| CPU user | ~0 сек | ~0 сек |

Go потребляет **~3x меньше памяти** и лучше масштабируется
за счёт легковесных горутин.

---

## Известные проблемы и решения

| Проблема | Решение |
|----------|---------|
| `duckdb` не устанавливается на Python 3.13 | Использовать `duckdb==1.2.2` |
| `kaleido` не работает с Python 3.13 | Использовать `kaleido==0.1.0.post1` |
| `str.to_datetime` без формата — ошибка timezone | Указать `format="%Y-%m-%dT%H:%M:%S%z"` |
| Kafka недоступна снаружи Docker на Windows | Заменена на NATS |
| minikube не запускается на Windows 11 Home | Образ собран через `docker build`, два инстанса запущены через `docker run` |
| pyo3 0.21 не поддерживает Python 3.13 | Обновить до pyo3 0.22 с `abi3-py38` |
| `ZoneInfoNotFoundError` в Rust venv | Установить `tzdata` |

---

## Список использованных источников

1. Go concurrency: https://go.dev/doc/effective_go#concurrency
2. Polars documentation: https://pola.rs/
3. DuckDB documentation: https://duckdb.org/
4. Apache Arrow: https://arrow.apache.org/
5. NATS documentation: https://docs.nats.io/
6. etcd documentation: https://etcd.io/docs/
7. PyO3 documentation: https://pyo3.rs/
8. Streamlit documentation: https://docs.streamlit.io/
9. Plotly Python: https://plotly.com/python/
10. Maturin: https://www.maturin.rs/
