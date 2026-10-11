package telemetry

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCorruptPacket     = errors.New("corrupt telemetry packet: CRC-32 checksum mismatch")
	ErrInvalidFillLevel  = errors.New("invalid fill level: must be between 0.00% and 100.00%")
	ErrInvalidStreamType = errors.New("invalid waste stream type")
	ErrSensorNotFound    = errors.New("sensor not found or no telemetry recorded")
	ErrDuplicateSequence = errors.New("duplicate telemetry sequence ID for sensor")
)

// Valid waste stream types matching Postgres enum waste_stream_type
var ValidStreamTypes = map[string]bool{
	"EWASTE":    true,
	"PLASTICS":  true,
	"METALS":    true,
	"ORGANIC":   true,
	"HAZARDOUS": true,
	"PAPER":     true,
	"GLASS":     true,
}

// SensorReading represents a durable telemetry observation
type SensorReading struct {
	ID                 uuid.UUID `json:"id"`
	SensorID           string    `json:"sensor_id"`
	SequenceID         uint64    `json:"sequence_id"`
	StreamType         string    `json:"stream_type"`
	FillLevelPercent   float64   `json:"fill_level_percent"` // 0.00% - 100.00%
	BatteryMillivolts  int       `json:"battery_millivolts"`
	TemperatureCelsius float64   `json:"temperature_celsius"`
	Timestamp          time.Time `json:"timestamp"`
	AppliedAt          time.Time `json:"applied_at"`
}

type IngestRequest struct {
	SensorID           string  `json:"sensor_id"`
	SequenceID         uint64  `json:"sequence_id"`
	StreamType         string  `json:"stream_type"`
	FillLevelPercent   float64 `json:"fill_level_percent"`
	BatteryMillivolts  int     `json:"battery_millivolts"`
	TemperatureCelsius float64 `json:"temperature_celsius"`
	TimestampUnixMs    int64   `json:"timestamp_unix_ms,omitempty"`
}

type LatestReadingResponse struct {
	SensorID   string         `json:"sensor_id"`
	Reading    *SensorReading `json:"reading"`
	IsAlerting bool           `json:"is_alerting"` // True if fill level >= 80%
}
