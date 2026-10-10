package wal

import (
	"context"
	"time"
)

// Record represents a single telemetry event frame
type Record struct {
	SequenceID uint64    `json:"sequence_id"`
	SensorID   string    `json:"sensor_id"`
	StreamType string    `json:"stream_type"`
	Timestamp  time.Time `json:"timestamp"`
	Payload    []byte    `json:"payload"`
}

// Engine defines append-only write-ahead log operations
type Engine interface {
	Append(ctx context.Context, record *Record) (uint64, error)
	Sync() error
	Close() error
}
