package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	SaveReading(ctx context.Context, reading *SensorReading) error
	GetLatestReading(ctx context.Context, sensorID string) (*SensorReading, error)
	ListReadings(ctx context.Context, sensorID string, limit int) ([]SensorReading, error)
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) SaveReading(ctx context.Context, reading *SensorReading) error {
	query := `
		INSERT INTO sensor_events (
			id, sensor_id, sequence_id, stream_type,
			fill_level_percent, battery_millivolts, temperature_celsius,
			timestamp, applied_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (sensor_id, sequence_id) DO NOTHING
	`
	if reading.ID == uuid.Nil {
		reading.ID = uuid.New()
	}
	if reading.AppliedAt.IsZero() {
		reading.AppliedAt = time.Now()
	}

	_, err := r.pool.Exec(ctx, query,
		reading.ID, reading.SensorID, int64(reading.SequenceID), reading.StreamType,
		reading.FillLevelPercent, reading.BatteryMillivolts, reading.TemperatureCelsius,
		reading.Timestamp, reading.AppliedAt,
	)
	if err != nil {
		return fmt.Errorf("failed saving sensor event to postgres: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetLatestReading(ctx context.Context, sensorID string) (*SensorReading, error) {
	query := `
		SELECT id, sensor_id, sequence_id, stream_type,
		       fill_level_percent, battery_millivolts, temperature_celsius,
		       timestamp, applied_at
		FROM sensor_events
		WHERE sensor_id = $1
		ORDER BY timestamp DESC, sequence_id DESC
		LIMIT 1
	`
	var s SensorReading
	var seq int64
	var batt *int
	var temp *float64
	err := r.pool.QueryRow(ctx, query, sensorID).Scan(
		&s.ID, &s.SensorID, &seq, &s.StreamType,
		&s.FillLevelPercent, &batt, &temp,
		&s.Timestamp, &s.AppliedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSensorNotFound
	}
	if err != nil {
		return nil, err
	}
	s.SequenceID = uint64(seq)
	if batt != nil {
		s.BatteryMillivolts = *batt
	}
	if temp != nil {
		s.TemperatureCelsius = *temp
	}
	return &s, nil
}

func (r *PostgresRepository) ListReadings(ctx context.Context, sensorID string, limit int) ([]SensorReading, error) {
	if limit <= 0 {
		limit = 20
	}
	query := `
		SELECT id, sensor_id, sequence_id, stream_type,
		       fill_level_percent, battery_millivolts, temperature_celsius,
		       timestamp, applied_at
		FROM sensor_events
		WHERE sensor_id = $1
		ORDER BY timestamp DESC, sequence_id DESC
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, sensorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SensorReading
	for rows.Next() {
		var s SensorReading
		var seq int64
		var batt *int
		var temp *float64
		if err := rows.Scan(
			&s.ID, &s.SensorID, &seq, &s.StreamType,
			&s.FillLevelPercent, &batt, &temp,
			&s.Timestamp, &s.AppliedAt,
		); err != nil {
			return nil, err
		}
		s.SequenceID = uint64(seq)
		if batt != nil {
			s.BatteryMillivolts = *batt
		}
		if temp != nil {
			s.TemperatureCelsius = *temp
		}
		results = append(results, s)
	}
	return results, nil
}

type MemoryRepository struct {
	mu       sync.RWMutex
	readings map[string][]SensorReading
	seenKeys map[string]bool // "sensorID:sequenceID" for duplicate avoidance
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		readings: make(map[string][]SensorReading),
		seenKeys: make(map[string]bool),
	}
}

func (m *MemoryRepository) SaveReading(ctx context.Context, reading *SensorReading) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%d", reading.SensorID, reading.SequenceID)
	if m.seenKeys[key] {
		return nil // Idempotent duplicate: skip
	}

	copyR := *reading
	if copyR.ID == uuid.Nil {
		copyR.ID = uuid.New()
	}
	if copyR.AppliedAt.IsZero() {
		copyR.AppliedAt = time.Now()
	}

	m.seenKeys[key] = true
	m.readings[copyR.SensorID] = append(m.readings[copyR.SensorID], copyR)
	return nil
}

func (m *MemoryRepository) GetLatestReading(ctx context.Context, sensorID string) (*SensorReading, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list, exists := m.readings[sensorID]
	if !exists || len(list) == 0 {
		return nil, ErrSensorNotFound
	}

	// Latest is the last appended entry
	latest := list[len(list)-1]
	return &latest, nil
}

func (m *MemoryRepository) ListReadings(ctx context.Context, sensorID string, limit int) ([]SensorReading, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list, exists := m.readings[sensorID]
	if !exists {
		return []SensorReading{}, nil
	}

	if limit <= 0 || limit > len(list) {
		limit = len(list)
	}

	res := make([]SensorReading, limit)
	start := len(list) - limit
	copy(res, list[start:])

	// Return in reverse chronological order
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res, nil
}
