package outbox

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

type Repository interface {
	SaveEvent(ctx context.Context, event *Event) error
	FetchPending(ctx context.Context, limit int) ([]Event, error)
	MarkPublished(ctx context.Context, id uuid.UUID) error
	IsProcessed(ctx context.Context, consumer string, eventID uuid.UUID) (bool, error)
	MarkProcessed(ctx context.Context, consumer string, eventID uuid.UUID) error
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) SaveEvent(ctx context.Context, event *Event) error {
	query := `
		INSERT INTO outbox_events (id, event_type, schema_version, aggregate_type, aggregate_id, correlation_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.pool.Exec(ctx, query,
		event.ID, event.EventType, event.SchemaVersion,
		event.AggregateType, event.AggregateID, event.CorrelationID,
		event.Payload, event.OccurredAt,
	)
	return err
}

func (r *PostgresRepository) FetchPending(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}

	// SKIP LOCKED prevents multiple relay workers from blocking or contending
	query := `
		WITH selected AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		SELECT o.id, o.event_type, o.schema_version, o.created_at, o.aggregate_type, o.aggregate_id, o.correlation_id, o.payload, o.attempts
		FROM outbox_events o
		JOIN selected s ON o.id = s.id
	`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed fetching pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var corrID *string
		if err := rows.Scan(
			&e.ID, &e.EventType, &e.SchemaVersion, &e.OccurredAt,
			&e.AggregateType, &e.AggregateID, &corrID, &e.Payload, &e.Attempts,
		); err != nil {
			return nil, err
		}
		if corrID != nil {
			e.CorrelationID = *corrID
		}
		events = append(events, e)
	}
	return events, nil
}

func (r *PostgresRepository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET published_at = NOW(), attempts = attempts + 1
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

func (r *PostgresRepository) IsProcessed(ctx context.Context, consumer string, eventID uuid.UUID) (bool, error) {
	query := `
		SELECT 1 FROM processed_events
		WHERE consumer_name = $1 AND event_id = $2
	`
	var dummy int
	err := r.pool.QueryRow(ctx, query, consumer, eventID).Scan(&dummy)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *PostgresRepository) MarkProcessed(ctx context.Context, consumer string, eventID uuid.UUID) error {
	query := `
		INSERT INTO processed_events (consumer_name, event_id, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`
	_, err := r.pool.Exec(ctx, query, consumer, eventID)
	return err
}

// MemoryRepository provides a thread-safe in-memory implementation for tests
type MemoryRepository struct {
	mu        sync.RWMutex
	events    map[uuid.UUID]Event
	processed map[string]map[uuid.UUID]bool // consumer -> set of eventIDs
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		events:    make(map[uuid.UUID]Event),
		processed: make(map[string]map[uuid.UUID]bool),
	}
}

func (m *MemoryRepository) SaveEvent(ctx context.Context, event *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[event.ID] = *event
	return nil
}

func (m *MemoryRepository) FetchPending(ctx context.Context, limit int) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Event
	for _, e := range m.events {
		if e.PublishedAt == nil {
			result = append(result, e)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MemoryRepository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.events[id]
	if !ok {
		return nil
	}
	now := time.Now()
	e.PublishedAt = &now
	e.Attempts++
	m.events[id] = e
	return nil
}

func (m *MemoryRepository) IsProcessed(ctx context.Context, consumer string, eventID uuid.UUID) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	set, ok := m.processed[consumer]
	if !ok {
		return false, nil
	}
	return set[eventID], nil
}

func (m *MemoryRepository) MarkProcessed(ctx context.Context, consumer string, eventID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	set, ok := m.processed[consumer]
	if !ok {
		set = make(map[uuid.UUID]bool)
		m.processed[consumer] = set
	}
	set[eventID] = true
	return nil
}
