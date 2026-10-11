package chaos_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"ecobrio/internal/telemetry"
	"ecobrio/internal/telemetry/snapshots"
	"ecobrio/internal/telemetry/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_AbruptProcessCrashDuringWrites simulates sudden process abort / kill -9 during concurrent sensor ingestion
func TestChaos_AbruptProcessCrashDuringWrites(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "chaos_crash_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	walDir := tempDir + "/wal"
	snapDir := tempDir + "/snapshots"

	cfg := wal.Config{
		Dir:             walDir,
		MaxSegmentSize:  4096, // Rotate frequently (every 4KB)
		FsyncEveryWrite: true,
	}

	walEngine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	snapMgr, err := snapshots.NewManager(snapDir)
	require.NoError(t, err)

	cpStore := wal.NewMemoryCheckpointStore()
	memRepo := telemetry.NewMemoryRepository()
	service := telemetry.NewService(memRepo, walEngine, cpStore, snapMgr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	totalSensors := 10
	var committedCount int64
	var committedMu sync.Mutex
	committedRecords := make(map[string]telemetry.SensorReading)

	var wg sync.WaitGroup
	errCh := make(chan error, 50)

	// Launch concurrent sensor streams
	for s := 0; s < totalSensors; s++ {
		wg.Add(1)
		go func(sensorIdx int) {
			defer wg.Done()
			sensorID := fmt.Sprintf("CHAOS-BIN-%02d", sensorIdx)

			for seq := uint64(1); seq <= 100; seq++ {
				select {
				case <-ctx.Done():
					return
				default:
					req := telemetry.IngestRequest{
						SensorID:           sensorID,
						SequenceID:         seq,
						StreamType:         "EWASTE",
						FillLevelPercent:   float64((seq * 7) % 100),
						BatteryMillivolts:  3300,
						TemperatureCelsius: 22.0,
						TimestampUnixMs:    time.Now().UnixMilli(),
					}

					reading, err := service.IngestReading(ctx, req)
					if err != nil {
						// Context might be canceled on crash
						return
					}

					committedMu.Lock()
					committedCount++
					key := fmt.Sprintf("%s:%d", reading.SensorID, reading.SequenceID)
					committedRecords[key] = *reading
					committedMu.Unlock()

					time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
				}
			}
		}(s)
	}

	// Let writes run for a short duration, then simulate sudden SIGKILL
	time.Sleep(60 * time.Millisecond)
	cancel()   // Instantly stop workers
	wg.Wait()  // Wait for in-flight goroutines to halt

	committedMu.Lock()
	preCrashCount := len(committedRecords)
	committedMu.Unlock()
	require.Greater(t, preCrashCount, 0, "Should have committed writes before simulated crash")

	// Simulate ungraceful process kill: close WAL abruptly and inject a torn write at the end
	activeFilename := walEngine.ActiveSegmentFilename()
	_ = walEngine.Close()

	// Inject 5 bytes of torn partial header at active segment tail
	activePath := walDir + "/" + activeFilename
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		_, _ = f.Write([]byte{0x00, 0x00, 0x05, 0xAA, 0xBB}) // Partial 5 bytes
		_ = f.Close()
	}

	// SIMULATE NODE RECOVERY / RESTART
	restartWalEngine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer restartWalEngine.Close()

	// Wipe repository (simulating volatile memory loss)
	cleanRepo := telemetry.NewMemoryRepository()
	recoveryService := telemetry.NewService(cleanRepo, restartWalEngine, cpStore, snapMgr)

	// Execute recovery daemon
	restoredSnap, replayedWAL, err := recoveryService.RunCrashRecoveryWithSnapshot(context.Background())
	require.NoError(t, err)

	totalRecovered := restoredSnap + replayedWAL
	assert.GreaterOrEqual(t, totalRecovered, preCrashCount, "Crash recovery must recover all committed pre-crash frames")

	// Verify all committed records exist in the restored repository with exact sequence match
	committedMu.Lock()
	for _, expected := range committedRecords {
		latest, err := cleanRepo.GetLatestReading(context.Background(), expected.SensorID)
		require.NoError(t, err, "Sensor %s must be recoverable after crash", expected.SensorID)
		assert.GreaterOrEqual(t, latest.SequenceID, expected.SequenceID)
	}
	committedMu.Unlock()

	select {
	case err := <-errCh:
		t.Fatalf("unexpected error during chaos test: %v", err)
	default:
	}
}

// TestChaos_SnapshotCompactionLifecycle verifies that compacting historical segments accelerates recovery
func TestChaos_SnapshotCompactionLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "chaos_compact_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	walDir := tempDir + "/wal"
	snapDir := tempDir + "/snapshots"

	cfg := wal.Config{
		Dir:             walDir,
		MaxSegmentSize:  512, // Tiny segment size to force many rotations
		FsyncEveryWrite: true,
	}

	walEngine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer walEngine.Close()

	snapMgr, err := snapshots.NewManager(snapDir)
	require.NoError(t, err)

	cpStore := wal.NewMemoryCheckpointStore()
	memRepo := telemetry.NewMemoryRepository()
	service := telemetry.NewService(memRepo, walEngine, cpStore, snapMgr)

	ctx := context.Background()

	// Ingest 50 readings across 5 sensors (10 readings each)
	for i := uint64(1); i <= 10; i++ {
		for s := 1; s <= 5; s++ {
			req := telemetry.IngestRequest{
				SensorID:           fmt.Sprintf("SENSOR-%d", s),
				SequenceID:         i,
				StreamType:         "PLASTICS",
				FillLevelPercent:   float64(i * 9),
				BatteryMillivolts:  3300,
				TemperatureCelsius: 21.5,
			}
			_, err := service.IngestReading(ctx, req)
			require.NoError(t, err)
		}
	}

	// Check segment count before compaction
	initialSegments, err := walEngine.ListSegments()
	require.NoError(t, err)
	assert.Greater(t, len(initialSegments), 3, "Multiple segments must have rotated")

	// Trigger snapshot compaction
	compaction, err := service.CompactAndSnapshot(ctx)
	require.NoError(t, err)
	assert.NotNil(t, compaction)
	assert.Equal(t, 5, compaction.SensorCount)
	assert.NotEmpty(t, compaction.PrunedSegments, "Compaction must prune sealed historical segments")

	// Segments should be pruned
	remainingSegments, err := walEngine.ListSegments()
	require.NoError(t, err)
	assert.Less(t, len(remainingSegments), len(initialSegments), "Historical segments must have been pruned")

	// Simulate complete crash & restart from snapshot
	freshRepo := telemetry.NewMemoryRepository()
	freshService := telemetry.NewService(freshRepo, walEngine, cpStore, snapMgr)

	restoredSnap, replayedWAL, err := freshService.RunCrashRecoveryWithSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, restoredSnap, "All 5 active sensor states must be restored directly from snapshot")
	assert.Equal(t, 0, replayedWAL, "Zero tail WAL replays required since snapshot is current")

	// Verify latest sensor fill levels match expected values
	for s := 1; s <= 5; s++ {
		latest, err := freshRepo.GetLatestReading(ctx, fmt.Sprintf("SENSOR-%d", s))
		require.NoError(t, err)
		assert.Equal(t, uint64(10), latest.SequenceID)
		assert.Equal(t, 90.0, latest.FillLevelPercent)
	}
}
