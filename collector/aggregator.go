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