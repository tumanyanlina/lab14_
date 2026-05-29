package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

const (
	natsURL     = "nats://localhost:4222"
	natsSubject = "mfc.queue.metrics"
)

// NatsWriter publishes AggregatedRecords to a NATS subject.
type NatsWriter struct {
	conn *nats.Conn
}

// NewNatsWriter connects to NATS and returns a NatsWriter.
func NewNatsWriter() (*NatsWriter, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	log.Printf("[INFO] nats writer connected: %s subject=%s", natsURL, natsSubject)
	return &NatsWriter{conn: nc}, nil
}

// Publish sends a batch of AggregatedRecords to NATS.
func (nw *NatsWriter) Publish(records []AggregatedRecord) error {
	for _, r := range records {
		data, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}
		if err := nw.conn.Publish(natsSubject, data); err != nil {
			return fmt.Errorf("nats publish: %w", err)
		}
	}
	log.Printf("[INFO] nats: published %d records to %s", len(records), natsSubject)
	return nil
}

// Close shuts down the NATS connection.
func (nw *NatsWriter) Close() {
	nw.conn.Drain()
	log.Println("[INFO] nats writer closed")
}
