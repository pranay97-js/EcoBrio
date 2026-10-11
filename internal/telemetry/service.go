package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"ecobrio/internal/telemetry/snapshots"
	"ecobrio/internal/telemetry/wal"
	"github.com/google/uuid"
)

type CompactionResult struct {
	SnapshotID     uuid.UUID `json:"snapshot_id"`
	LastSequenceID uint64    `json:"last_sequence_id"`
	ActiveSegment  string    `json:"active_segment"`
	SensorCount    int       `json:"sensor_count"`
	PrunedSegments []string  `json:"pruned_segments"`
	Timestamp      time.Time `json:"timestamp"`
}

type Service struct {
	repo            Repository
	walEngine       *wal.SegmentedEngine
	checkpointStore wal.CheckpointStore
	snapshotMgr     *snapshots.Manager
	recoveryEngine  *wal.RecoveryEngine
}

func NewService(repo Repository, walEngine *wal.SegmentedEngine, checkpointStore wal.CheckpointStore, snapshotMgr ...*snapshots.Manager) *Service {
	var sm *snapshots.Manager
	if len(snapshotMgr) > 0 {
		sm = snapshotMgr[0]
	}
	svc := &Service{
		repo:            repo,
		walEngine:       walEngine,
		checkpointStore: checkpointStore,
		snapshotMgr:     sm,
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

func (s *Service) SnapshotManager() *snapshots.Manager {
	return s.snapshotMgr
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
			ID:         uuid.New(),
			SensorID:   rec.SensorID,
			SequenceID: rec.SequenceID,
			StreamType: rec.StreamType,
			Timestamp:  rec.Timestamp,
			AppliedAt:  time.Now(),
		}
	}

	return s.repo.SaveReading(ctx, reading)
}

// RunCrashRecovery replays uncommitted WAL segments on system boot
func (s *Service) RunCrashRecovery(ctx context.Context) (int, error) {
	return s.recoveryEngine.ReplayUncommitted(ctx)
}

// RunCrashRecoveryWithSnapshot restores state from the latest snapshot and replays subsequent WAL segments
func (s *Service) RunCrashRecoveryWithSnapshot(ctx context.Context) (int, int, error) {
	restoredFromSnapshot := 0

	if s.snapshotMgr != nil {
		snap, err := s.snapshotMgr.LoadLatestSnapshot(ctx)
		if err == nil && snap != nil {
			for _, st := range snap.SensorStates {
				r := &SensorReading{
					ID:                 uuid.New(),
					SensorID:           st.SensorID,
					SequenceID:         st.SequenceID,
					StreamType:         st.StreamType,
					FillLevelPercent:   st.FillLevelPercent,
					BatteryMillivolts:  st.BatteryMillivolts,
					TemperatureCelsius: st.TemperatureCelsius,
					Timestamp:          st.Timestamp,
					AppliedAt:          time.Now(),
				}
				_ = s.repo.SaveReading(ctx, r)
				restoredFromSnapshot++
			}

			// Advance checkpoint to snapshot boundary if not already past it
			existingCP, _ := s.checkpointStore.GetLatestCheckpoint(ctx)
			if existingCP == nil || existingCP.LastSequenceID < snap.LastSequenceID {
				_ = s.checkpointStore.SaveCheckpoint(ctx, &wal.Checkpoint{
					ID:             uuid.New(),
					SegmentFile:    snap.SegmentFile,
					LastSequenceID: snap.LastSequenceID,
					CheckpointedAt: time.Now(),
				})
			}
		}
	}

	replayedFromWAL, err := s.recoveryEngine.ReplayUncommitted(ctx)
	if err != nil {
		return restoredFromSnapshot, replayedFromWAL, err
	}

	return restoredFromSnapshot, replayedFromWAL, nil
}

// CompactAndSnapshot creates a point-in-time sensor state snapshot and prunes obsolete sealed WAL segments
func (s *Service) CompactAndSnapshot(ctx context.Context) (*CompactionResult, error) {
	if s.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager is not configured")
	}

	readings, err := s.repo.ListAllLatestSensors(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed fetching latest sensor states: %w", err)
	}

	states := make(map[string]snapshots.CompactedSensorState)
	var highestSeq uint64 = 0

	for _, r := range readings {
		states[r.SensorID] = snapshots.CompactedSensorState{
			SensorID:           r.SensorID,
			SequenceID:         r.SequenceID,
			StreamType:         r.StreamType,
			FillLevelPercent:   r.FillLevelPercent,
			BatteryMillivolts:  r.BatteryMillivolts,
			TemperatureCelsius: r.TemperatureCelsius,
			Timestamp:          r.Timestamp,
		}
		if r.SequenceID > highestSeq {
			highestSeq = r.SequenceID
		}
	}

	activeSegment := s.walEngine.ActiveSegmentFilename()

	// 1. Commit immutable snapshot
	snap, err := s.snapshotMgr.SaveSnapshot(ctx, highestSeq, activeSegment, states)
	if err != nil {
		return nil, fmt.Errorf("failed saving snapshot: %w", err)
	}

	// 2. Prune sealed historical WAL segments whose records are <= highestSeq
	pruned, err := s.walEngine.PruneSegmentsOlderThan(highestSeq)
	if err != nil {
		slog.Warn("WAL segment pruning encountered notice", "error", err)
	}

	// 3. Purge older snapshots, keeping the last 3
	_ = s.snapshotMgr.PurgeOldSnapshots(ctx, 3)

	// 4. Update checkpoint
	cp := &wal.Checkpoint{
		ID:             uuid.New(),
		SegmentFile:    activeSegment,
		LastSequenceID: highestSeq,
		CheckpointedAt: time.Now(),
	}
	_ = s.checkpointStore.SaveCheckpoint(ctx, cp)

	return &CompactionResult{
		SnapshotID:     snap.ID,
		LastSequenceID: highestSeq,
		ActiveSegment:  activeSegment,
		SensorCount:    len(states),
		PrunedSegments: pruned,
		Timestamp:      snap.CreatedAt,
	}, nil
}

// GetLatestSnapshot retrieves the current snapshot on disk
func (s *Service) GetLatestSnapshot(ctx context.Context) (*snapshots.Snapshot, error) {
	if s.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager is not configured")
	}
	return s.snapshotMgr.LoadLatestSnapshot(ctx)
}

// StartBackgroundCompactor launches a background compaction worker loop
func (s *Service) StartBackgroundCompactor(ctx context.Context, interval time.Duration) {
	if s.snapshotMgr == nil {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				slog.Info("WAL background compactor loop stopped")
				return
			case <-ticker.C:
				if res, err := s.CompactAndSnapshot(ctx); err == nil && res.SensorCount > 0 {
					slog.Debug("Background snapshot compaction completed",
						"sensor_count", res.SensorCount,
						"pruned_segments", len(res.PrunedSegments),
					)
				}
			}
		}
	}()
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
