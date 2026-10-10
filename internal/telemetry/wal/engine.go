package wal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrSegmentCorrupt = errors.New("wal segment contains corrupted or torn frame")
	ErrClosed         = errors.New("wal engine is closed")
)

type Config struct {
	Dir              string
	MaxSegmentSize   int64 // e.g. 10 * 1024 * 1024 (10MB)
	FsyncEveryWrite  bool
}

func DefaultConfig(dir string) Config {
	return Config{
		Dir:             dir,
		MaxSegmentSize:  10 * 1024 * 1024, // 10MB default
		FsyncEveryWrite: true,
	}
}

// SegmentedEngine provides a crash-resilient append-only write-ahead log
type SegmentedEngine struct {
	cfg            Config
	mu             sync.Mutex
	activeFile     *os.File
	activeSegIndex uint64
	currentOffset  int64
	isClosed       bool
}

// NewSegmentedEngine initializes or opens an existing segmented WAL directory
func NewSegmentedEngine(cfg Config) (*SegmentedEngine, error) {
	if cfg.MaxSegmentSize <= 0 {
		cfg.MaxSegmentSize = 10 * 1024 * 1024
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating wal directory: %w", err)
	}

	engine := &SegmentedEngine{
		cfg: cfg,
	}

	// Discover existing segments
	segments, err := engine.ListSegments()
	if err != nil {
		return nil, err
	}

	var latestIndex uint64 = 1
	if len(segments) > 0 {
		latestPath := segments[len(segments)-1]
		latestIndex = extractSegmentIndex(latestPath)
	}

	// Open or create the active segment file
	activePath := filepath.Join(cfg.Dir, formatSegmentFilename(latestIndex))
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed opening active wal segment: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	engine.activeFile = file
	engine.activeSegIndex = latestIndex
	engine.currentOffset = info.Size()

	// If the existing latest segment is already beyond max size, rotate immediately
	if engine.currentOffset >= cfg.MaxSegmentSize {
		if err := engine.Rotate(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return engine, nil
}

// Append writes an immutable framed record: [Length: 4B][CRC32: 4B][Binary Payload]
func (e *SegmentedEngine) Append(ctx context.Context, record *Record) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isClosed {
		return 0, ErrClosed
	}

	// Serialize record into JSON or payload bytes
	payload, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("failed encoding wal record payload: %w", err)
	}

	payloadLen := uint32(len(payload))
	checksum := crc32.ChecksumIEEE(payload)

	frameLen := int64(4 + 4 + len(payload))

	// Rotate segment if adding this frame exceeds max segment size
	if e.currentOffset+frameLen > e.cfg.MaxSegmentSize && e.currentOffset > 0 {
		if err := e.rotateLocked(); err != nil {
			return 0, fmt.Errorf("wal rotation failed: %w", err)
		}
	}

	frameBuf := make([]byte, frameLen)
	binary.BigEndian.PutUint32(frameBuf[0:4], payloadLen)
	binary.BigEndian.PutUint32(frameBuf[4:8], checksum)
	copy(frameBuf[8:], payload)

	written, err := e.activeFile.Write(frameBuf)
	if err != nil {
		return 0, fmt.Errorf("failed writing frame to wal: %w", err)
	}

	e.currentOffset += int64(written)

	if e.cfg.FsyncEveryWrite {
		if err := e.activeFile.Sync(); err != nil {
			return 0, fmt.Errorf("failed syncing wal to disk: %w", err)
		}
	}

	return record.SequenceID, nil
}

func (e *SegmentedEngine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isClosed || e.activeFile == nil {
		return nil
	}
	return e.activeFile.Sync()
}

func (e *SegmentedEngine) Rotate() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rotateLocked()
}

func (e *SegmentedEngine) rotateLocked() error {
	if e.activeFile != nil {
		_ = e.activeFile.Sync()
		_ = e.activeFile.Close()
	}

	e.activeSegIndex++
	newPath := filepath.Join(e.cfg.Dir, formatSegmentFilename(e.activeSegIndex))
	file, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed creating rotated wal segment: %w", err)
	}

	e.activeFile = file
	e.currentOffset = 0
	return nil
}

func (e *SegmentedEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isClosed {
		return nil
	}
	e.isClosed = true

	if e.activeFile != nil {
		_ = e.activeFile.Sync()
		return e.activeFile.Close()
	}
	return nil
}

// ActiveSegmentFilename returns the base filename of the currently active log file
func (e *SegmentedEngine) ActiveSegmentFilename() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return formatSegmentFilename(e.activeSegIndex)
}

// ListSegments scans the WAL directory and returns all segment files sorted by sequence index
func (e *SegmentedEngine) ListSegments() ([]string, error) {
	entries, err := os.ReadDir(e.cfg.Dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "wal_") && strings.HasSuffix(entry.Name(), ".log") {
			segments = append(segments, filepath.Join(e.cfg.Dir, entry.Name()))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return extractSegmentIndex(segments[i]) < extractSegmentIndex(segments[j])
	})

	return segments, nil
}

// ReadSegment reads and validates all non-corrupted frames from a segment file
// Torn writes at the end of the log are gracefully truncated
func ReadSegment(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []Record
	headerBuf := make([]byte, 8) // 4B Length + 4B CRC32

	for {
		_, err := io.ReadFull(f, headerBuf)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Clean EOF or torn partial header at tail: end of valid stream
			break
		}
		if err != nil {
			return records, err
		}

		payloadLen := binary.BigEndian.Uint32(headerBuf[0:4])
		storedCRC := binary.BigEndian.Uint32(headerBuf[4:8])

		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(f, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Torn write: payload truncated before EOF
			break
		}
		if err != nil {
			return records, err
		}

		// Validate CRC32
		if crc32.ChecksumIEEE(payload) != storedCRC {
			// Corrupted frame detected: halt playback at boundary
			break
		}

		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}

		records = append(records, rec)
	}

	return records, nil
}

func formatSegmentFilename(idx uint64) string {
	return fmt.Sprintf("wal_%016d.log", idx)
}

func extractSegmentIndex(path string) uint64 {
	base := filepath.Base(path)
	trimmed := strings.TrimPrefix(base, "wal_")
	trimmed = strings.TrimSuffix(trimmed, ".log")
	idx, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0
	}
	return idx
}
