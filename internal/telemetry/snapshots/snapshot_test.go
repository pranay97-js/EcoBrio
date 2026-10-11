package snapshots_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ecobrio/internal/telemetry/snapshots"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_SaveAndLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snap_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mgr, err := snapshots.NewManager(tempDir)
	require.NoError(t, err)

	ctx := context.Background()

	states := map[string]snapshots.CompactedSensorState{
		"SENSOR-01": {
			SensorID:           "SENSOR-01",
			SequenceID:         50,
			StreamType:         "EWASTE",
			FillLevelPercent:   75.5,
			BatteryMillivolts:  3300,
			TemperatureCelsius: 22.1,
			Timestamp:          time.Now(),
		},
		"SENSOR-02": {
			SensorID:           "SENSOR-02",
			SequenceID:         52,
			StreamType:         "PLASTICS",
			FillLevelPercent:   42.0,
			BatteryMillivolts:  3250,
			TemperatureCelsius: 19.8,
			Timestamp:          time.Now(),
		},
	}

	snap, err := mgr.SaveSnapshot(ctx, 52, "wal_0000000000000002.log", states)
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, uint64(52), snap.LastSequenceID)
	assert.Equal(t, "wal_0000000000000002.log", snap.SegmentFile)
	assert.Len(t, snap.SensorStates, 2)

	// Load latest snapshot
	loaded, err := mgr.LoadLatestSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, snap.ID, loaded.ID)
	assert.Equal(t, uint64(52), loaded.LastSequenceID)
	assert.Equal(t, "wal_0000000000000002.log", loaded.SegmentFile)
	assert.Len(t, loaded.SensorStates, 2)
	assert.Equal(t, 75.5, loaded.SensorStates["SENSOR-01"].FillLevelPercent)
}

func TestSnapshot_CorruptionDetection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snap_corrupt_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mgr, err := snapshots.NewManager(tempDir)
	require.NoError(t, err)

	ctx := context.Background()

	states := map[string]snapshots.CompactedSensorState{
		"BIN-01": {
			SensorID:         "BIN-01",
			SequenceID:       10,
			FillLevelPercent: 50.0,
		},
	}

	snap, err := mgr.SaveSnapshot(ctx, 10, "wal_0000000000000001.log", states)
	require.NoError(t, err)

	// Corrupt file on disk
	filePath := filepath.Join(tempDir, "snapshot_0000000000000010.json")
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Mutate payload byte without updating checksum
	data[len(data)/2] ^= 0xFF
	err = os.WriteFile(filePath, data, 0644)
	require.NoError(t, err)

	// Load should detect checksum mismatch
	_, err = mgr.LoadLatestSnapshot(ctx)
	require.Error(t, err)
	assert.True(t, err == snapshots.ErrCorruptSnapshot)
	_ = snap
}

func TestSnapshot_PurgeOldSnapshots(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snap_purge_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mgr, err := snapshots.NewManager(tempDir)
	require.NoError(t, err)

	ctx := context.Background()

	// Create 5 snapshots
	for i := uint64(1); i <= 5; i++ {
		_, err := mgr.SaveSnapshot(ctx, i*10, "wal_0000000000000001.log", map[string]snapshots.CompactedSensorState{})
		require.NoError(t, err)
	}

	// Purge to keep only last 2
	err = mgr.PurgeOldSnapshots(ctx, 2)
	require.NoError(t, err)

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Latest snapshot must still be loadable (sequence 50)
	latest, err := mgr.LoadLatestSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(50), latest.LastSequenceID)
}
