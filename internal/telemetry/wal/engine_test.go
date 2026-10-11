package wal_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
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

func TestWAL_CorruptPayload_ReturnsErrSegmentCorrupt(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_corrupt_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := wal.DefaultConfig(tempDir)
	engine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	// Write 2 valid records
	for i := uint64(1); i <= 2; i++ {
		rec := &wal.Record{
			SequenceID: i,
			SensorID:   "SENSOR-CORRUPT",
			StreamType: "METALS",
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

	// Corrupt a byte in the middle of the second record payload
	segPath := segments[0]
	data, err := os.ReadFile(segPath)
	require.NoError(t, err)

	// Flip a bit in the file near the end
	data[len(data)-10] ^= 0xFF
	require.NoError(t, os.WriteFile(segPath, data, 0644))

	// Reading the corrupted segment should return at least record 1 and ErrSegmentCorrupt
	recs, err := wal.ReadSegment(segPath)
	assert.ErrorIs(t, err, wal.ErrSegmentCorrupt)
	assert.GreaterOrEqual(t, len(recs), 1, "Should recover at least the valid first record")
}

func TestWAL_MaxPayloadSize_BoundsProtection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_overflow_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a segment file with an impossibly large length header (0xFFFFFFFF = 4GB)
	segPath := filepath.Join(tempDir, "wal_0000000000000001.log")
	bogusFrame := make([]byte, 8)
	binary.BigEndian.PutUint32(bogusFrame[0:4], 0xFFFFFFFF) // 4GB payload length
	binary.BigEndian.PutUint32(bogusFrame[4:8], 0x12345678) // dummy CRC
	require.NoError(t, os.WriteFile(segPath, bogusFrame, 0644))

	// Must not panic or allocate 4GB OOM
	recs, err := wal.ReadSegment(segPath)
	assert.NoError(t, err)
	assert.Empty(t, recs)
}
