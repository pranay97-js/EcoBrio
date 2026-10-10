package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Checkpoint struct {
	ID             uuid.UUID `json:"id"`
	SegmentFile    string    `json:"segment_file"`
	LastSequenceID uint64    `json:"last_sequence_id"`
	CheckpointedAt time.Time `json:"checkpointed_at"`
}

type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, cp *Checkpoint) error
	GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error)
}

type PostgresCheckpointStore struct {
	pool *pgxpool.Pool
}

func NewPostgresCheckpointStore(pool *pgxpool.Pool) *PostgresCheckpointStore {
	return &PostgresCheckpointStore{pool: pool}
}

func (s *PostgresCheckpointStore) SaveCheckpoint(ctx context.Context, cp *Checkpoint) error {
	query := `
		INSERT INTO wal_checkpoints (id, segment_file, last_sequence_id, checkpointed_at)
		VALUES ($1, $2, $3, $4)
	`
	if cp.ID == uuid.Nil {
		cp.ID = uuid.New()
	}
	if cp.CheckpointedAt.IsZero() {
		cp.CheckpointedAt = time.Now()
	}

	_, err := s.pool.Exec(ctx, query, cp.ID, cp.SegmentFile, int64(cp.LastSequenceID), cp.CheckpointedAt)
	if err != nil {
		return fmt.Errorf("failed saving wal checkpoint: %w", err)
	}
	return nil
}

func (s *PostgresCheckpointStore) GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error) {
	query := `
		SELECT id, segment_file, last_sequence_id, checkpointed_at
		FROM wal_checkpoints
		ORDER BY checkpointed_at DESC, last_sequence_id DESC
		LIMIT 1
	`
	var cp Checkpoint
	var lastSeq int64
	err := s.pool.QueryRow(ctx, query).Scan(&cp.ID, &cp.SegmentFile, &lastSeq, &cp.CheckpointedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // No checkpoint yet: fresh start
	}
	if err != nil {
		return nil, err
	}
	cp.LastSequenceID = uint64(lastSeq)
	return &cp, nil
}

type MemoryCheckpointStore struct {
	mu     sync.RWMutex
	latest *Checkpoint
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{}
}

func (m *MemoryCheckpointStore) SaveCheckpoint(ctx context.Context, cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	copyCP := *cp
	if copyCP.ID == uuid.Nil {
		copyCP.ID = uuid.New()
	}
	if copyCP.CheckpointedAt.IsZero() {
		copyCP.CheckpointedAt = time.Now()
	}
	m.latest = &copyCP
	return nil
}

func (m *MemoryCheckpointStore) GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.latest == nil {
		return nil, nil
	}
	copyCP := *m.latest
	return &copyCP, nil
}
