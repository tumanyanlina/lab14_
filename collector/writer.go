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
