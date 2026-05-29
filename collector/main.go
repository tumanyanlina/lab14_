package main

import (
	"context"
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

	// Coordinator: register this instance in etcd.
	instanceID := 1
	coord, err := NewCoordinator(instanceID)
	if err != nil {
		log.Printf("[WARN] etcd unavailable, running standalone: %v", err)
	} else {
		coordCtx, coordCancel := context.WithCancel(context.Background())
		defer coordCancel()
		assignedWindows := AssignShards(instanceID, 1, numWindows)
		if err := coord.Register(coordCtx, instanceID, assignedWindows); err != nil {
			log.Printf("[WARN] coordinator register failed: %v", err)
		} else {
			defer coord.Deregister(coordCtx)
			instances, _ := coord.ListInstances(coordCtx)
			log.Printf("[INFO] active instances: %v", instances)
		}
	}

	// Raw records channel (buffered to absorb bursts).
	rawCh := make(chan QueueRecord, numWindows*10)

	// Aggregated records channels — fan-out to Writer and ArrowWriter.
	aggCh := make(chan AggregatedRecord, 200)
	aggChArrow := make(chan AggregatedRecord, 200)

	// Start emulator.
	emulator := NewEmulator(numWindows, emitInterval)
	go emulator.Run(rawCh, done)
	log.Printf("[INFO] emulator started: %d windows", numWindows)

	// Start aggregator loop.
	aggregator := NewAggregator(windowDur)
	go runAggregator(aggregator, rawCh, aggCh, done)
	log.Printf("[INFO] aggregator started: window=%s", windowDur)

	// Fan-out: forward aggregated records to both writers.
	go func() {
		for rec := range aggCh {
			aggChArrow <- rec
		}
		close(aggChArrow)
	}()

	// Arrow writer.
	arrowWriter, arrowErr := NewArrowWriter(outputDir)
	if arrowErr != nil {
		log.Printf("[WARN] arrow writer init failed: %v", arrowErr)
	} else {
		log.Printf("[INFO] arrow writer started: output=%s", outputDir)
		go runArrowWriter(arrowWriter, aggChArrow, done)
	}

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

// runArrowWriter buffers AggregatedRecords and writes them to Arrow IPC files.
func runArrowWriter(aw *ArrowWriter, ch <-chan AggregatedRecord, done <-chan struct{}) {
	var buf []AggregatedRecord
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := aw.WriteBatch(buf); err != nil {
			log.Printf("[ERROR] arrow flush: %v", err)
		}
		buf = nil
	}

	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				flush()
				return
			}
			buf = append(buf, rec)
			if len(buf) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-done:
			flush()
			return
		}
	}
}