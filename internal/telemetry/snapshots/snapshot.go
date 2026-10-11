package snapshots

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCorruptSnapshot = errors.New("snapshot file is corrupted: checksum mismatch")
	ErrNoSnapshotFound = errors.New("no valid snapshot found")
)

// CompactedSensorState holds the consolidated point-in-time state of a single smart bin
type CompactedSensorState struct {
	SensorID           string    `json:"sensor_id"`
	SequenceID         uint64    `json:"sequence_id"`
	StreamType         string    `json:"stream_type"`
	FillLevelPercent   float64   `json:"fill_level_percent"`
	BatteryMillivolts  int       `json:"battery_millivolts"`
	TemperatureCelsius float64   `json:"temperature_celsius"`
	Timestamp          time.Time `json:"timestamp"`
}

// Snapshot represents an immutable point-in-time consolidated checkpoint of all active sensors
type Snapshot struct {
	ID             uuid.UUID                       `json:"id"`
	LastSequenceID uint64                          `json:"last_sequence_id"`
	SegmentFile    string                          `json:"segment_file"`
	CreatedAt      time.Time                       `json:"created_at"`
	SensorStates   map[string]CompactedSensorState `json:"sensor_states"`
	Checksum       uint32                          `json:"checksum"`
}

type snapshotPayload struct {
	ID             uuid.UUID                       `json:"id"`
	LastSequenceID uint64                          `json:"last_sequence_id"`
	SegmentFile    string                          `json:"segment_file"`
	CreatedAt      time.Time                       `json:"created_at"`
	SensorStates   map[string]CompactedSensorState `json:"sensor_states"`
}

type Manager struct {
	dir string
	mu  sync.RWMutex
}

func NewManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating snapshot directory: %w", err)
	}
	return &Manager{dir: dir}, nil
}

// SaveSnapshot atomically writes a new snapshot file with CRC-32 checksum verification
func (m *Manager) SaveSnapshot(ctx context.Context, lastSeq uint64, segFile string, states map[string]CompactedSensorState) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	snapID := uuid.New()

	payload := snapshotPayload{
		ID:             snapID,
		LastSequenceID: lastSeq,
		SegmentFile:    segFile,
		CreatedAt:      now,
		SensorStates:   states,
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling snapshot payload: %w", err)
	}

	checksum := crc32.ChecksumIEEE(rawPayload)

	snapshot := &Snapshot{
		ID:             snapID,
		LastSequenceID: lastSeq,
		SegmentFile:    segFile,
		CreatedAt:      now,
		SensorStates:   states,
		Checksum:       checksum,
	}

	envelopeBytes, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling snapshot envelope: %w", err)
	}

	// Write atomically via temporary file and rename
	tmpFilename := fmt.Sprintf("snapshot_%016d.tmp", lastSeq)
	finalFilename := fmt.Sprintf("snapshot_%016d.json", lastSeq)
	tmpPath := filepath.Join(m.dir, tmpFilename)
	finalPath := filepath.Join(m.dir, finalFilename)

	if err := os.WriteFile(tmpPath, envelopeBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed writing temporary snapshot file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("failed committing atomic snapshot rename: %w", err)
	}

	return snapshot, nil
}

// LoadLatestSnapshot scans for the newest valid snapshot, verifying CRC-32 integrity
func (m *Manager) LoadLatestSnapshot(ctx context.Context) (*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("failed reading snapshot dir: %w", err)
	}

	var filenames []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "snapshot_") && strings.HasSuffix(e.Name(), ".json") {
			filenames = append(filenames, e.Name())
		}
	}

	if len(filenames) == 0 {
		return nil, ErrNoSnapshotFound
	}

	// Sort descending by sequence ID
	sort.Slice(filenames, func(i, j int) bool {
		return extractSnapshotSeq(filenames[i]) > extractSnapshotSeq(filenames[j])
	})

	// Try loading from newest to oldest in case latest is corrupt
	for _, fn := range filenames {
		path := filepath.Join(m.dir, fn)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}

		// Verify CRC-32 checksum
		payload := snapshotPayload{
			ID:             snap.ID,
			LastSequenceID: snap.LastSequenceID,
			SegmentFile:    snap.SegmentFile,
			CreatedAt:      snap.CreatedAt,
			SensorStates:   snap.SensorStates,
		}
		rawPayload, err := json.Marshal(payload)
		if err != nil {
			continue
		}

		if crc32.ChecksumIEEE(rawPayload) != snap.Checksum {
			// Checksum mismatch, try previous snapshot
			continue
		}

		return &snap, nil
	}

	return nil, ErrCorruptSnapshot
}

// PurgeOldSnapshots retains only the most recent N snapshots to bound disk usage
func (m *Manager) PurgeOldSnapshots(ctx context.Context, keepLast int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if keepLast <= 0 {
		keepLast = 3
	}

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	var filenames []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "snapshot_") && strings.HasSuffix(e.Name(), ".json") {
			filenames = append(filenames, e.Name())
		}
	}

	if len(filenames) <= keepLast {
		return nil
	}

	sort.Slice(filenames, func(i, j int) bool {
		return extractSnapshotSeq(filenames[i]) > extractSnapshotSeq(filenames[j])
	})

	for i := keepLast; i < len(filenames); i++ {
		_ = os.Remove(filepath.Join(m.dir, filenames[i]))
	}

	return nil
}

func extractSnapshotSeq(filename string) uint64 {
	trimmed := strings.TrimPrefix(filename, "snapshot_")
	trimmed = strings.TrimSuffix(trimmed, ".json")
	seq, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0
	}
	return seq
}
