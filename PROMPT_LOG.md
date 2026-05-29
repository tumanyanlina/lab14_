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