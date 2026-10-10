package wal

import (
	"context"
	"fmt"
	"path/filepath"
	"time"
)

// Applier applies recovered WAL records into the target persistence engine
type Applier interface {
	ApplyRecord(ctx context.Context, record *Record) error
}

type RecoveryEngine struct {
	walEngine       *SegmentedEngine
	checkpointStore CheckpointStore
	applier         Applier
}

func NewRecoveryEngine(walEngine *SegmentedEngine, checkpointStore CheckpointStore, applier Applier) *RecoveryEngine {
	return &RecoveryEngine{
		walEngine:       walEngine,
		checkpointStore: checkpointStore,
		applier:         applier,
	}
}

// ReplayUncommitted discovers uncommitted WAL records from the latest checkpoint and applies them
func (r *RecoveryEngine) ReplayUncommitted(ctx context.Context) (int, error) {
	cp, err := r.checkpointStore.GetLatestCheckpoint(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed fetching latest wal checkpoint: %w", err)
	}

	segments, err := r.walEngine.ListSegments()
	if err != nil {
		return 0, fmt.Errorf("failed listing wal segments: %w", err)
	}

	if len(segments) == 0 {
		return 0, nil
	}

	startIdx := 0
	var minSequenceID uint64 = 0

	if cp != nil {
		minSequenceID = cp.LastSequenceID
		// Find the index of the segment containing the checkpoint
		for i, segPath := range segments {
			if filepath.Base(segPath) == filepath.Base(cp.SegmentFile) {
				startIdx = i
				break
			}
		}
	}

	replayedCount := 0
	var maxSequenceID uint64 = minSequenceID
	var lastSegmentFile string

	for i := startIdx; i < len(segments); i++ {
		segPath := segments[i]
		lastSegmentFile = filepath.Base(segPath)

		records, err := ReadSegment(segPath)
		if err != nil {
			return replayedCount, fmt.Errorf("error reading wal segment %s: %w", segPath, err)
		}

		for _, rec := range records {
			if rec.SequenceID <= minSequenceID {
				continue // Already committed prior to checkpoint
			}

			if err := r.applier.ApplyRecord(ctx, &rec); err != nil {
				return replayedCount, fmt.Errorf("failed applying wal record seq %d: %w", rec.SequenceID, err)
			}

			replayedCount++
			if rec.SequenceID > maxSequenceID {
				maxSequenceID = rec.SequenceID
			}
		}
	}

	// Update checkpoint if new records were processed
	if replayedCount > 0 && lastSegmentFile != "" {
		newCP := &Checkpoint{
			SegmentFile:    lastSegmentFile,
			LastSequenceID: maxSequenceID,
			CheckpointedAt: time.Now(),
		}
		_ = r.checkpointStore.SaveCheckpoint(ctx, newCP)
	}

	return replayedCount, nil
}
