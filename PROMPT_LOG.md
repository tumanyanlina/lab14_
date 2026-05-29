Лабораторная работа №14

Студент: Туманян Лина Врежовна
Группа: 220032-11
Сложность: повышенная
ИИ-инструмент: Claude Sonnet 4.6 (claude.ai)

Шаг 1 — Инициализация проекта: .gitignore

Промпт: Создай .gitignore для проекта на Go и Python (Polars, DuckDB, Streamlit). Исключить: бинарники Go, __pycache__, .venv, сгенерированные data/*.ndjson, data/*.parquet, папку analysis/plots/, кэш Streamlit, папки IDE (.vscode, .idea),системные файлы ОС.

Результат: Создан файл .gitignore с секциями Go, Python, Data files,
Plots, Streamlit cache, IDE, OS.

.gitignore:

# Go
collector/collector.exe
collector/*.exe

# Python
__pycache__/
*.pyc
.venv/
venv/

# Data files (generated)
data/*.ndjson
data/*.parquet

# Plots (generated)
analysis/plots/

# Streamlit cache
.streamlit/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db

Шаг 2 — Go-сборщик: эмулятор датчиков

Промпт: Напиши Go-файл emulator.go для эмуляции датчиков очередей МФЦ. 8 окошек, каждое — отдельная горутина. Типы услуг: passport, registration, social_benefit, tax, property. Очередь меняется случайным блужданием (±2 в тик). Время ожидания = queue_length × 240 сек jitter. Остановка через chan struct{} (done).

Результат: файл emulator.go — структуры QueueRecord,
WindowState, Emulator; методы Run, updateState, estimateWait.

go.mod:
module github.com/lab14/collector

go 1.22

emulator.go:
package main

import (
	"math/rand"
	"time"
)

// ServiceType represents a type of service provided at MFC.
type ServiceType string

const (
	ServicePassport      ServiceType = "passport"
	ServiceRegistration  ServiceType = "registration"
	ServiceSocialBenefit ServiceType = "social_benefit"
	ServiceTax           ServiceType = "tax"
	ServiceProperty      ServiceType = "property"
)

var allServices = []ServiceType{
	ServicePassport,
	ServiceRegistration,
	ServiceSocialBenefit,
	ServiceTax,
	ServiceProperty,
}

// QueueRecord is a single raw observation from one MFC window sensor.
type QueueRecord struct {
	WindowID    int         `json:"window_id"`
	ServiceType ServiceType `json:"service_type"`
	QueueLength int         `json:"queue_length"`
	WaitTimeSec int         `json:"wait_time_sec"`
	Timestamp   time.Time   `json:"timestamp"`
}

// WindowState holds the current simulated state for one MFC window.
type WindowState struct {
	WindowID    int
	ServiceType ServiceType
	queueLen    int
}

// Emulator simulates MFC queue sensors for multiple windows.
type Emulator struct {
	windows  []*WindowState
	interval time.Duration
	rng      *rand.Rand
}

// NewEmulator creates an Emulator for numWindows windows with the given tick interval.
func NewEmulator(numWindows int, interval time.Duration) *Emulator {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	windows := make([]*WindowState, numWindows)
	for i := range windows {
		windows[i] = &WindowState{
			WindowID:    i + 1,
			ServiceType: allServices[i%len(allServices)],
			queueLen:    rng.Intn(5),
		}
	}
	return &Emulator{windows: windows, interval: interval, rng: rng}
}

// Run starts emitting QueueRecords into ch until done is closed.
func (e *Emulator) Run(ch chan<- QueueRecord, done <-chan struct{}) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			for _, w := range e.windows {
				e.updateState(w)
				ch <- QueueRecord{
					WindowID:    w.WindowID,
					ServiceType: w.ServiceType,
					QueueLength: w.queueLen,
					WaitTimeSec: e.estimateWait(w.queueLen),
					Timestamp:   t,
				}
			}
		}
	}
}

// updateState applies a random walk to the queue length.
func (e *Emulator) updateState(w *WindowState) {
	delta := e.rng.Intn(5) - 2 // -2..+2
	w.queueLen += delta
	if w.queueLen < 0 {
		w.queueLen = 0
	}
	if w.queueLen > 30 {
		w.queueLen = 30
	}
}

// estimateWait returns wait time estimate based on queue length (avg 4 min per person).
func (e *Emulator) estimateWait(queueLen int) int {
	baseSeconds := queueLen * 240
	jitter := e.rng.Intn(60) - 30
	result := baseSeconds + jitter
	if result < 0 {
		result = 0
	}
	return result
}

Шаг 3 — Go-сборщик: агрегатор tumbling window

Промпт: Напиши Go-файл aggregator.go — tumbling window агрегатор для QueueRecord. Окно 30 секунд. Ключ корзины: (window_id, unix_timestamp / window_sec). Считать avg/max/min длины очереди и времени ожидания, количество замеров. Метод Add() добавляет запись, Flush(now) возвращает закрытые окна и удаляет их.

Результат: файл aggregator.go — структуры AggregatedRecord,
windowKey, accumulator, Aggregator; методы Add, Flush.

aggregator.go:
package main

import (
	"time"
)

// AggregatedRecord is the result of aggregating raw QueueRecords within a tumbling window.
type AggregatedRecord struct {
	WindowID       int         `json:"window_id"`
	ServiceType    ServiceType `json:"service_type"`
	AvgQueueLength float64     `json:"avg_queue_length"`
	MaxQueueLength int         `json:"max_queue_length"`
	MinQueueLength int         `json:"min_queue_length"`
	AvgWaitTimeSec float64     `json:"avg_wait_time_sec"`
	MaxWaitTimeSec int         `json:"max_wait_time_sec"`
	SampleCount    int         `json:"sample_count"`
	WindowStart    time.Time   `json:"window_start"`
	WindowEnd      time.Time   `json:"window_end"`
}

// windowKey uniquely identifies a tumbling window bucket per MFC window.
type windowKey struct {
	windowID int
	bucket   int64 // unix seconds / windowSec
}

// accumulator holds running totals for one window bucket.
type accumulator struct {
	serviceType ServiceType
	sumQueue    int
	maxQueue    int
	minQueue    int
	sumWait     int
	maxWait     int
	count       int
}

// Aggregator collects raw records and emits AggregatedRecords every windowDur.
type Aggregator struct {
	windowDur time.Duration
	buckets   map[windowKey]*accumulator
}

// NewAggregator creates an Aggregator with the given tumbling window duration.
func NewAggregator(windowDur time.Duration) *Aggregator {
	return &Aggregator{
		windowDur: windowDur,
		buckets:   make(map[windowKey]*accumulator),
	}
}

// Add ingests a raw QueueRecord into the current bucket.
func (a *Aggregator) Add(r QueueRecord) {
	bucketID := r.Timestamp.Unix() / int64(a.windowDur.Seconds())
	key := windowKey{windowID: r.WindowID, bucket: bucketID}

	acc, ok := a.buckets[key]
	if !ok {
		acc = &accumulator{
			serviceType: r.ServiceType,
			minQueue:    r.QueueLength,
		}
		a.buckets[key] = acc
	}

	acc.sumQueue += r.QueueLength
	acc.sumWait += r.WaitTimeSec
	acc.count++

	if r.QueueLength > acc.maxQueue {
		acc.maxQueue = r.QueueLength
	}
	if r.QueueLength < acc.minQueue {
		acc.minQueue = r.QueueLength
	}
	if r.WaitTimeSec > acc.maxWait {
		acc.maxWait = r.WaitTimeSec
	}
}

// Flush emits all completed (past) buckets and removes them from internal state.
func (a *Aggregator) Flush(now time.Time) []AggregatedRecord {
	currentBucket := now.Unix() / int64(a.windowDur.Seconds())
	var results []AggregatedRecord

	for key, acc := range a.buckets {
		if key.bucket >= currentBucket {
			continue // bucket still open
		}

		windowStart := time.Unix(key.bucket*int64(a.windowDur.Seconds()), 0)
		windowEnd := windowStart.Add(a.windowDur)

		results = append(results, AggregatedRecord{
			WindowID:       key.windowID,
			ServiceType:    acc.serviceType,
			AvgQueueLength: float64(acc.sumQueue) / float64(acc.count),
			MaxQueueLength: acc.maxQueue,
			MinQueueLength: acc.minQueue,
			AvgWaitTimeSec: float64(acc.sumWait) / float64(acc.count),
			MaxWaitTimeSec: acc.maxWait,
			SampleCount:    acc.count,
			WindowStart:    windowStart,
			WindowEnd:      windowEnd,
		})

		delete(a.buckets, key)
	}

	return results
}

Шаг 4 — Go-сборщик: Writer с пакетной записью

Промпт: Напиши Go-файл writer.go — буферизованная запись AggregatedRecord в NDJSON. Флаш по двум триггерам: накоплено batchSize записей ИЛИ прошло flushInterval секунд. При graceful shutdown (done закрыт) — сбросить буфер перед выходом. Каждый флаш → новый файл вида mfc_YYYYMMDD_HHMMSS_0001.ndjson. Защита буфера через sync.Mutex.

Результат: файл writer.go — структура Writer, методы
NewWriter, Run, flushBuffer.

writer.go:
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Writer buffers AggregatedRecords and writes them to NDJSON files in batches.
// A flush is triggered when the buffer reaches batchSize or flushInterval elapses.
type Writer struct {
	outputDir     string
	batchSize     int
	flushInterval time.Duration

	mu      sync.Mutex
	buffer  []AggregatedRecord
	fileSeq int
}

// NewWriter creates a Writer that stores files in outputDir.
func NewWriter(outputDir string, batchSize int, flushInterval time.Duration) (*Writer, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	return &Writer{
		outputDir:     outputDir,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}, nil
}

// Run reads from ch, buffers records, and flushes on batch-size or timer trigger.
// Stops when ch is closed or done is closed.
func (w *Writer) Run(ch <-chan AggregatedRecord, done <-chan struct{}) {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				// Channel closed — flush remaining buffer and exit.
				w.flushBuffer()
				return
			}
			w.mu.Lock()
			w.buffer = append(w.buffer, rec)
			shouldFlush := len(w.buffer) >= w.batchSize
			w.mu.Unlock()

			if shouldFlush {
				w.flushBuffer()
			}

		case <-ticker.C:
			w.flushBuffer()

		case <-done:
			// Graceful shutdown — write whatever is buffered.
			w.flushBuffer()
			return
		}
	}
}

// flushBuffer writes the current buffer to a new NDJSON file.
func (w *Writer) flushBuffer() {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return
	}
	batch := w.buffer
	w.buffer = nil
	w.fileSeq++
	seq := w.fileSeq
	w.mu.Unlock()

	filename := filepath.Join(
		w.outputDir,
		fmt.Sprintf("mfc_%s_%04d.ndjson", time.Now().Format("20060102_150405"), seq),
	)

	f, err := os.Create(filename)
	if err != nil {
		log.Printf("[ERROR] create file %s: %v", filename, err)
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, rec := range batch {
		if err := enc.Encode(rec); err != nil {
			log.Printf("[ERROR] encode record: %v", err)
		}
	}

	log.Printf("[INFO] flushed %d records → %s", len(batch), filename)
}

Шаг 5 — Go-сборщик: main.go

Промпт: Напиши Go-файл main.go — точка входа конвейера мониторинга очередей МФЦ.

Требования:
- Связать компоненты в цепочку: Emulator → rawCh → Aggregator → aggCh → Writer
- Каждый компонент запускать в отдельной горутине через go func()
- Graceful shutdown: слушать os.Signal (SIGINT, SIGTERM) через отдельную горутину, при получении сигнала закрыть канал done — это останавливает все компоненты
- При завершении Writer должен сбросить буфер (дописать незаконченный файл)
- Вынести все настройки в константы вверху файла: numWindows=8, emitInterval=2s, windowDur=30s, batchSize=20,
flushInterval=15s, outputDir="../data"
- Функцию runAggregator вынести отдельно: читает из rawCh, каждые 5 сек вызывает Aggregator.Flush(), при завершении делает финальный flush всех незакрытых окон и закрывает aggCh
- Логировать все ключевые события: старт, каждый flush окна, graceful shutdown, остановку
- Использовать только стандартную библиотеку Go: log, os, os/signal, syscall, time.

Результат: файл main.go — константы, функция main() с инициализацией
всех компонентов и горутин, функция runAggregator() с ticker для периодического
flush и финальным flush при завершении.

main.go:
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	numWindows    = 8
	emitInterval  = 2 * time.Second
	windowDur     = 30 * time.Second
	batchSize     = 20
	flushInterval = 15 * time.Second
	outputDir     = "../data"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("[INFO] MFC collector starting")
	log.Printf("[INFO] windows=%d, emit_interval=%s, tumbling_window=%s",
		numWindows, emitInterval, windowDur)

	// Graceful shutdown: listen for SIGINT / SIGTERM.
	done := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[INFO] received signal %s — shutting down gracefully", sig)
		close(done)
	}()

	// Raw records channel (buffered to absorb bursts).
	rawCh := make(chan QueueRecord, numWindows*10)

	// Aggregated records channel.
	aggCh := make(chan AggregatedRecord, 200)

	// Start emulator.
	emulator := NewEmulator(numWindows, emitInterval)
	go emulator.Run(rawCh, done)
	log.Printf("[INFO] emulator started: %d windows", numWindows)

	// Start aggregator loop.
	aggregator := NewAggregator(windowDur)
	go runAggregator(aggregator, rawCh, aggCh, done)
	log.Printf("[INFO] aggregator started: window=%s", windowDur)

	// Start writer.
	writer, err := NewWriter(outputDir, batchSize, flushInterval)
	if err != nil {
		log.Fatalf("[FATAL] writer init: %v", err)
	}
	log.Printf("[INFO] writer started: batch=%d, flush_interval=%s, output=%s",
		batchSize, flushInterval, outputDir)

	// Writer runs in the main goroutine and blocks until done.
	writer.Run(aggCh, done)

	log.Println("[INFO] MFC collector stopped")
}

// runAggregator reads raw records, feeds them into the aggregator,
// and periodically flushes completed windows into aggCh.
func runAggregator(agg *Aggregator, in <-chan QueueRecord, out chan<- AggregatedRecord, done <-chan struct{}) {
	flushTicker := time.NewTicker(5 * time.Second)
	defer func() {
		flushTicker.Stop()
		// Final flush before exit.
		for _, rec := range agg.Flush(time.Now().Add(windowDur)) {
			out <- rec
		}
		close(out)
		log.Println("[INFO] aggregator stopped")
	}()

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				return
			}
			agg.Add(rec)

		case t := <-flushTicker.C:
			flushed := agg.Flush(t)
			for _, rec := range flushed {
				out <- rec
				log.Printf("[INFO] window flushed: window_id=%d service=%s avg_queue=%.1f samples=%d",
					rec.WindowID, rec.ServiceType, rec.AvgQueueLength, rec.SampleCount)
			}

		case <-done:
			return
		}
	}
}

Шаг 6 — Python: ingest.py (задания 4, 5, 7)

Промпт:мНапиши Python-скрипт ingest.py с использованием Polars.
Задание 4: загрузить все *.ndjson файлы из папки data/ в один DataFrame через pl.read_ndjson + pl.concat, вывести первые 5 строк, схему, количество строк и пропуски по колонкам.
Задание 5: очистка в 4 шага:
1) привести window_start и window_end к Datetime с форматом "%Y-%m-%dT%H:%M:%S%z" (данные содержат timezone +03:00),service_type привести к Categorical
2) удалить дубликаты по (window_id, window_start), вывести сколько удалено
3) отфильтровать строки где avg_queue_length < 0, avg_wait_time_sec < 0, sample_count == 0
4) заполнить пропуски в числовых колонках медианой колонки
Задание 7: сохранить очищенный DataFrame в data/mfc_clean.parquet.
Логировать каждый шаг через print с префиксом [STEP] / [INFO].

Результат: Создан файл ingest.py — функции load_ndjson(),
show_overview(), clean(), main().

ingest.py:
import glob
import sys
from pathlib import Path

import polars as pl


DATA_DIR = Path(__file__).parent.parent / "data"
PARQUET_PATH = DATA_DIR / "mfc_clean.parquet"


def load_ndjson(data_dir: Path) -> pl.DataFrame:
    """Read all NDJSON files from data_dir into a single Polars DataFrame."""
    files = sorted(glob.glob(str(data_dir / "*.ndjson")))
    if not files:
        print(f"[ERROR] no .ndjson files found in {data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] loading {len(files)} file(s) from {data_dir}")
    frames = [pl.read_ndjson(f) for f in files]
    df = pl.concat(frames)
    print(f"[INFO] loaded {df.height} rows, {df.width} columns")
    return df


def show_overview(df: pl.DataFrame) -> None:
    """Print first 5 rows and schema overview."""
    print("\n--- первые 5 строк ---")
    print(df.head(5))
    print("\n--- схема ---")
    print(df.schema)
    print(f"\nВсего строк : {df.height}")
    print(f"Всего колонок: {df.width}")
    print("\n--- пропуски по колонкам ---")
    print(df.null_count())


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """
    Задание 5 — очистка и валидация:
    1. Привести типы (timestamp → Datetime).
    2. Убрать дубликаты по (window_id, window_start).
    3. Удалить строки с отрицательными значениями.
    4. Заполнить пропуски медианой.
    """
    print("\n[STEP] приведение типов...")
    df = df.with_columns([
        pl.col("window_start").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("window_end").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("service_type").cast(pl.Categorical),
    ])

    before = df.height
    print(f"[STEP] удаление дубликатов (было {before})...")
    df = df.unique(subset=["window_id", "window_start"])
    print(f"       осталось {df.height} (удалено {before - df.height})")

    print("[STEP] фильтрация невалидных значений...")
    df = df.filter(
        (pl.col("avg_queue_length") >= 0) &
        (pl.col("avg_wait_time_sec") >= 0) &
        (pl.col("sample_count") > 0)
    )
    print(f"       осталось {df.height}")

    numeric_cols = [
        "avg_queue_length", "max_queue_length", "min_queue_length",
        "avg_wait_time_sec", "max_wait_time_sec",
    ]
    print("[STEP] заполнение пропусков медианой...")
    for col in numeric_cols:
        median_val = df[col].median()
        df = df.with_columns(pl.col(col).fill_null(median_val))

    print(f"[INFO] очистка завершена: {df.height} строк")
    return df


def main() -> None:
    df_raw = load_ndjson(DATA_DIR)
    show_overview(df_raw)
    df_clean = clean(df_raw)

    DATA_DIR.mkdir(exist_ok=True)
    df_clean.write_parquet(PARQUET_PATH)
    print(f"\n[INFO] данные сохранены в {PARQUET_PATH}")

    print("\n--- очищенные данные (первые 5 строк) ---")
    print(df_clean.head(5))


if __name__ == "__main__":
    main()