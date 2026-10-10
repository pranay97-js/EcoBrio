package telemetry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ecobrio/internal/telemetry/wal"
	"github.com/google/uuid"
)

type Service struct {
	repo            Repository
	walEngine       *wal.SegmentedEngine
	checkpointStore wal.CheckpointStore
	recoveryEngine  *wal.RecoveryEngine
}

func NewService(repo Repository, walEngine *wal.SegmentedEngine, checkpointStore wal.CheckpointStore) *Service {
	svc := &Service{
		repo:            repo,
		walEngine:       walEngine,
		checkpointStore: checkpointStore,
	}
	svc.recoveryEngine = wal.NewRecoveryEngine(walEngine, checkpointStore, svc)
	return svc
}

func (s *Service) WALEngine() *wal.SegmentedEngine {
	return s.walEngine
}

func (s *Service) CheckpointStore() wal.CheckpointStore {
	return s.checkpointStore
}

// IngestReading validates and durably logs a telemetry observation through the WAL before committing to DB
func (s *Service) IngestReading(ctx context.Context, req IngestRequest) (*SensorReading, error) {
	sensorID := strings.TrimSpace(req.SensorID)
	if sensorID == "" {
		return nil, fmt.Errorf("sensor_id is required")
	}

	streamType := strings.ToUpper(strings.TrimSpace(req.StreamType))
	if !ValidStreamTypes[streamType] {
		return nil, fmt.Errorf("%w: %s", ErrInvalidStreamType, streamType)
	}

	if req.FillLevelPercent < 0 || req.FillLevelPercent > 100 {
		return nil, ErrInvalidFillLevel
	}

	ts := time.Now()
	if req.TimestampUnixMs > 0 {
		ts = time.UnixMilli(req.TimestampUnixMs)
	}

	reading := &SensorReading{
		ID:                 uuid.New(),
		SensorID:           sensorID,
		SequenceID:         req.SequenceID,
		StreamType:         streamType,
		FillLevelPercent:   req.FillLevelPercent,
		BatteryMillivolts:  req.BatteryMillivolts,
		TemperatureCelsius: req.TemperatureCelsius,
		Timestamp:          ts,
		AppliedAt:          time.Now(),
	}

	// 1. Hardware Packet Binary Encoding & CRC-32 generation
	packetBytes, err := EncodePacket(reading)
	if err != nil {
		return nil, fmt.Errorf("failed binary encoding packet: %w", err)
	}

	// 2. Durably Append to Write-Ahead Log (WAL)
	walRecord := &wal.Record{
		SequenceID: reading.SequenceID,
		SensorID:   reading.SensorID,
		StreamType: reading.StreamType,
		Timestamp:  reading.Timestamp,
		Payload:    packetBytes,
	}

	if _, err := s.walEngine.Append(ctx, walRecord); err != nil {
		return nil, fmt.Errorf("failed appending to write-ahead log: %w", err)
	}

	// 3. Commit to Primary Database
	if err := s.repo.SaveReading(ctx, reading); err != nil {
		return nil, fmt.Errorf("failed persisting sensor event: %w", err)
	}

	return reading, nil
}

// IngestBinaryPacket decodes raw hardware frames, validates CRC-32, logs to WAL, and persists
func (s *Service) IngestBinaryPacket(ctx context.Context, raw []byte) (*SensorReading, error) {
	pkt, err := DecodePacket(raw)
	if err != nil {
		return nil, err
	}

	reading := pkt.ToReading()

	walRecord := &wal.Record{
		SequenceID: reading.SequenceID,
		SensorID:   reading.SensorID,
		StreamType: reading.StreamType,
		Timestamp:  reading.Timestamp,
		Payload:    raw,
	}

	if _, err := s.walEngine.Append(ctx, walRecord); err != nil {
		return nil, fmt.Errorf("failed appending raw binary packet to wal: %w", err)
	}

	if err := s.repo.SaveReading(ctx, reading); err != nil {
		return nil, fmt.Errorf("failed persisting sensor event: %w", err)
	}

	return reading, nil
}

// ApplyRecord implements wal.Applier for deterministic crash recovery
func (s *Service) ApplyRecord(ctx context.Context, rec *wal.Record) error {
	var reading *SensorReading

	if len(rec.Payload) > 0 {
		pkt, err := DecodePacket(rec.Payload)
		if err == nil {
			reading = pkt.ToReading()
		}
	}

	if reading == nil {
		reading = &SensorReading{
			ID:                 uuid.New(),
			SensorID:           rec.SensorID,
			SequenceID:         rec.SequenceID,
			StreamType:         rec.StreamType,
			Timestamp:          rec.Timestamp,
			AppliedAt:          time.Now(),
		}
	}

	return s.repo.SaveReading(ctx, reading)
}

// RunCrashRecovery replays uncommitted WAL segments on system boot
func (s *Service) RunCrashRecovery(ctx context.Context) (int, error) {
	return s.recoveryEngine.ReplayUncommitted(ctx)
}

// GetLatestReading retrieves the most recent reading and evaluates capacity thresholds
func (s *Service) GetLatestReading(ctx context.Context, sensorID string) (*LatestReadingResponse, error) {
	reading, err := s.repo.GetLatestReading(ctx, sensorID)
	if err != nil {
		return nil, err
	}

	return &LatestReadingResponse{
		SensorID:   sensorID,
		Reading:    reading,
		IsAlerting: reading.FillLevelPercent >= 80.0, // High-fill alarm threshold
	}, nil
}

// CheckpointCurrentWAL forces a checkpoint of the active WAL state
func (s *Service) CheckpointCurrentWAL(ctx context.Context) (*wal.Checkpoint, error) {
	activeFile := s.walEngine.ActiveSegmentFilename()
	latestCP, _ := s.checkpointStore.GetLatestCheckpoint(ctx)

	var lastSeq uint64 = 0
	if latestCP != nil {
		lastSeq = latestCP.LastSequenceID
	}

	cp := &wal.Checkpoint{
		ID:             uuid.New(),
		SegmentFile:    activeFile,
		LastSequenceID: lastSeq,
		CheckpointedAt: time.Now(),
	}

	if err := s.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	return cp, nil
}
