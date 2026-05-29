package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ArrowWriter writes AggregatedRecords to Apache Arrow IPC files.
type ArrowWriter struct {
	outputDir string
	fileSeq   int
}

// NewArrowWriter creates an ArrowWriter that stores .arrow files in outputDir.
func NewArrowWriter(outputDir string) (*ArrowWriter, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create arrow output dir: %w", err)
	}
	return &ArrowWriter{outputDir: outputDir}, nil
}

// schema returns the Arrow schema for AggregatedRecord.
func arrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "window_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "service_type", Type: arrow.BinaryTypes.String},
		{Name: "avg_queue_length", Type: arrow.PrimitiveTypes.Float64},
		{Name: "max_queue_length", Type: arrow.PrimitiveTypes.Int64},
		{Name: "min_queue_length", Type: arrow.PrimitiveTypes.Int64},
		{Name: "avg_wait_time_sec", Type: arrow.PrimitiveTypes.Float64},
		{Name: "max_wait_time_sec", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sample_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "window_start", Type: arrow.PrimitiveTypes.Int64}, // unix seconds
		{Name: "window_end", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

// WriteBatch writes a slice of AggregatedRecords to a new .arrow file.
func (w *ArrowWriter) WriteBatch(records []AggregatedRecord) error {
	if len(records) == 0 {
		return nil
	}

	w.fileSeq++
	filename := filepath.Join(
		w.outputDir,
		fmt.Sprintf("mfc_%s_%04d.arrow",
			time.Now().Format("20060102_150405"), w.fileSeq),
	)

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create arrow file: %w", err)
	}
	defer f.Close()

	schema := arrowSchema()
	pool := memory.NewGoAllocator()

	// Build columns.
	b0 := array.NewInt64Builder(pool)
	b1 := array.NewStringBuilder(pool)
	b2 := array.NewFloat64Builder(pool)
	b3 := array.NewInt64Builder(pool)
	b4 := array.NewInt64Builder(pool)
	b5 := array.NewFloat64Builder(pool)
	b6 := array.NewInt64Builder(pool)
	b7 := array.NewInt64Builder(pool)
	b8 := array.NewInt64Builder(pool)
	b9 := array.NewInt64Builder(pool)

	for _, r := range records {
		b0.Append(int64(r.WindowID))
		b1.Append(string(r.ServiceType))
		b2.Append(r.AvgQueueLength)
		b3.Append(int64(r.MaxQueueLength))
		b4.Append(int64(r.MinQueueLength))
		b5.Append(r.AvgWaitTimeSec)
		b6.Append(int64(r.MaxWaitTimeSec))
		b7.Append(int64(r.SampleCount))
		b8.Append(r.WindowStart.Unix())
		b9.Append(r.WindowEnd.Unix())
	}

	rec := array.NewRecord(schema, []arrow.Array{
		b0.NewArray(), b1.NewArray(), b2.NewArray(),
		b3.NewArray(), b4.NewArray(), b5.NewArray(),
		b6.NewArray(), b7.NewArray(), b8.NewArray(),
		b9.NewArray(),
	}, int64(len(records)))
	defer rec.Release()

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
if err != nil {
    return fmt.Errorf("create arrow file writer: %w", err)
}
	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("write arrow record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close arrow writer: %w", err)
	}

	log.Printf("[INFO] arrow: wrote %d records → %s", len(records), filename)
	return nil
}