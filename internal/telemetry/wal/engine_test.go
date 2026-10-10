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

func TestWAL_AppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.DefaultConfig(tempDir)
	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Append 5 records
	for i := uint64(1); i <= 5; i++ {
		rec := &wal.Record{
			SequenceID: i,
			SensorID:   fmt.Sprintf("SENSOR-%d", i),
			StreamType: "EWASTE",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("payload-%d", i)),
		}
		seq, err := engine.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, i, seq)
	}

	segments, err := engine.ListSegments()
	require.NoError(t, err)
	require.Len(t, segments, 1)

	// Read records from segment
	records, err := wal.ReadSegment(segments[0])
	require.NoError(t, err)
	assert.Len(t, records, 5)

	for i, r := range records {
		expectedSeq := uint64(i + 1)
		assert.Equal(t, expectedSeq, r.SequenceID)
		assert.Equal(t, fmt.Sprintf("SENSOR-%d", expectedSeq), r.SensorID)
		assert.Equal(t, fmt.Sprintf("payload-%d", expectedSeq), string(r.Payload))
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_rot_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Configure small segment size to trigger rotation after every ~2 records
	cfg := wal.Config{
		Dir:             tempDir,
		MaxSegmentSize:  250, // 250 bytes
		FsyncEveryWrite: true,
	}

	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	totalRecords := uint64(10)

	for i := uint64(1); i <= totalRecords; i++ {
		rec := &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-ROT",
			StreamType: "METALS",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("measurement-data-%d", i)),
		}
		_, err := engine.Append(ctx, rec)
		require.NoError(t, err)
	}

	segments, err := engine.ListSegments()
	require.NoError(t, err)
	assert.Greater(t, len(segments), 1, "Should have created multiple segment files due to rotation")

	// Read across all segments and verify all 10 records are present
	var allRead []wal.Record
	for _, seg := range segments {
		recs, err := wal.ReadSegment(seg)
		require.NoError(t, err)
		allRead = append(allRead, recs...)
	}

	assert.Len(t, allRead, int(totalRecords))
	for i, r := range allRead {
		assert.Equal(t, uint64(i+1), r.SequenceID)
	}
}

func TestWAL_TornWriteHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_torn_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.DefaultConfig(tempDir)
	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Write 3 valid records
	for i := uint64(1); i <= 3; i++ {
		rec := &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-TORN",
			StreamType: "PLASTICS",
			Timestamp:  time.Now(),
			Payload:    []byte(fmt.Sprintf("valid-data-%d", i)),
		}
		_, err := engine.Append(ctx, rec)
		require.NoError(t, err)
	}

	require.NoError(t, engine.Close())

	segments, err := engine.ListSegments()
	require.NoError(t, err)
	require.Len(t, segments, 1)

	// Simulate sudden power loss / SIGKILL by appending partial junk bytes at the end of the file
	segPath := segments[0]
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00, 0x00, 0x01}) // Truncated 3 bytes of header
	require.NoError(t, err)
	_ = f.Close()

	// Reading the segment must return the 3 valid records and gracefully truncate the torn tail
	recovered, err := wal.ReadSegment(segPath)
	require.NoError(t, err)
	assert.Len(t, recovered, 3)
	assert.Equal(t, uint64(3), recovered[2].SequenceID)
}

func TestWAL_ConcurrentAppends(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_race_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.DefaultConfig(tempDir)
	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	concurrency := 10
	recordsPerWorker := 30
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < recordsPerWorker; i++ {
				seq := uint64(workerID*1000 + i + 1)
				rec := &wal.Record{
					SequenceID: seq,
					SensorID:   fmt.Sprintf("WORKER-%d", workerID),
					StreamType: "EWASTE",
					Timestamp:  time.Now(),
					Payload:    []byte("concurrent-metric"),
				}
				_, err := engine.Append(ctx, rec)
				assert.NoError(t, err)
			}
		}(w)
	}

	wg.Wait()

	segments, err := engine.ListSegments()
	require.NoError(t, err)

	var totalRead int
	for _, seg := range segments {
		recs, err := wal.ReadSegment(seg)
		require.NoError(t, err)
		totalRead += len(recs)
	}

	assert.Equal(t, concurrency*recordsPerWorker, totalRead)
}
