package wal_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"ecobrio/internal/telemetry/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockApplier struct {
	mu      sync.Mutex
	applied []wal.Record
}

func (m *mockApplier) ApplyRecord(ctx context.Context, record *wal.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, *record)
	return nil
}

func (m *mockApplier) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.applied)
}

func TestRecoveryEngine_ReplayFromScratch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "recovery_scratch_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.Config{
		Dir:             tempDir,
		MaxSegmentSize:  300, // Rotate across multiple segments
		FsyncEveryWrite: true,
	}

	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Append 15 records
	for i := uint64(1); i <= 15; i++ {
		_, err := engine.Append(ctx, &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-RECOVERY",
			StreamType: "EWASTE",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("data-%d", i)),
		})
		require.NoError(t, err)
	}

	require.NoError(t, engine.Close())

	// Reopen engine and run crash recovery
	reopened, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer reopened.Close()

	cpStore := wal.NewMemoryCheckpointStore()
	applier := &mockApplier{}
	recovery := wal.NewRecoveryEngine(reopened, cpStore, applier)

	replayed, err := recovery.ReplayUncommitted(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, replayed)
	assert.Equal(t, 15, applier.count())

	// Verify checkpoint was advanced
	cp, err := cpStore.GetLatestCheckpoint(ctx)
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, uint64(15), cp.LastSequenceID)

	// Re-running recovery must replay 0 records because checkpoint is up to date
	replayedSecond, err := recovery.ReplayUncommitted(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, replayedSecond)
}

func TestRecoveryEngine_ReplayFromMidCheckpoint(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "recovery_mid_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.Config{
		Dir:             tempDir,
		MaxSegmentSize:  1024 * 1024,
		FsyncEveryWrite: true,
	}

	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Write records 1..10
	for i := uint64(1); i <= 10; i++ {
		_, err := engine.Append(ctx, &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-MID",
			StreamType: "METALS",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("data-%d", i)),
		})
		require.NoError(t, err)
	}

	// 2. Set checkpoint at sequence 10
	cpStore := wal.NewMemoryCheckpointStore()
	err = cpStore.SaveCheckpoint(ctx, &wal.Checkpoint{
		SegmentFile:    engine.ActiveSegmentFilename(),
		LastSequenceID: 10,
		CheckpointedAt: time.Now(),
	})
	require.NoError(t, err)

	// 3. Write records 11..25
	for i := uint64(11); i <= 25; i++ {
		_, err := engine.Append(ctx, &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-MID",
			StreamType: "METALS",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("data-%d", i)),
		})
		require.NoError(t, err)
	}

	require.NoError(t, engine.Close())

	// 4. Simulate restart and recovery
	reopened, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer reopened.Close()

	applier := &mockApplier{}
	recovery := wal.NewRecoveryEngine(reopened, cpStore, applier)

	replayed, err := recovery.ReplayUncommitted(ctx)
	require.NoError(t, err)

	// Only records 11..25 should be replayed (15 records)
	assert.Equal(t, 15, replayed)
	assert.Equal(t, 15, applier.count())
	assert.Equal(t, uint64(11), applier.applied[0].SequenceID)
	assert.Equal(t, uint64(25), applier.applied[14].SequenceID)

	// Checkpoint is now at sequence 25
	cp, err := cpStore.GetLatestCheckpoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(25), cp.LastSequenceID)
}
