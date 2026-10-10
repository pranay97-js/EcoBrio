package incidents

import (
	"context"
	"errors"
	"sync"
	"time"

	"ecobrio/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository manages incident storage operations
type Repository interface {
	Create(ctx context.Context, inc *Incident) error
	GetByID(ctx context.Context, id uuid.UUID) (*Incident, error)
	List(ctx context.Context, filter Filter) ([]Incident, error)
	Update(ctx context.Context, inc *Incident) error
}

// PostgresRepository implements Repository using PostgreSQL
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Create(ctx context.Context, inc *Incident) error {
	query := `
		INSERT INTO incidents (id, reporter_id, stream_type, description, latitude, longitude, severity, status, assignee_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err := r.pool.Exec(ctx, query,
		inc.ID, inc.ReporterID, inc.StreamType, inc.Description,
		inc.Latitude, inc.Longitude, inc.Severity, inc.Status,
		inc.AssigneeID, inc.CreatedAt, inc.UpdatedAt,
	)
	return err
}

func (r *PostgresRepository) GetByID(ctx context.Context, id uuid.UUID) (*Incident, error) {
	query := `
		SELECT id, reporter_id, stream_type, description, latitude, longitude, severity, status, assignee_id, created_at, updated_at
		FROM incidents
		WHERE id = $1
	`
	var inc Incident
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&inc.ID, &inc.ReporterID, &inc.StreamType, &inc.Description,
		&inc.Latitude, &inc.Longitude, &inc.Severity, &inc.Status,
		&inc.AssigneeID, &inc.CreatedAt, &inc.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &inc, nil
}

func (r *PostgresRepository) List(ctx context.Context, filter Filter) ([]Incident, error) {
	query := `
		SELECT id, reporter_id, stream_type, description, latitude, longitude, severity, status, assignee_id, created_at, updated_at
		FROM incidents
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR stream_type = $2)
		ORDER BY created_at DESC
		LIMIT $3
	`
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	rows, err := r.pool.Query(ctx, query, filter.Status, filter.StreamType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(
			&inc.ID, &inc.ReporterID, &inc.StreamType, &inc.Description,
			&inc.Latitude, &inc.Longitude, &inc.Severity, &inc.Status,
			&inc.AssigneeID, &inc.CreatedAt, &inc.UpdatedAt,
		); err != nil {
			return nil, err
		}
		incidents = append(incidents, inc)
	}
	return incidents, nil
}

func (r *PostgresRepository) Update(ctx context.Context, inc *Incident) error {
	query := `
		UPDATE incidents
		SET status = $1, assignee_id = $2, updated_at = $3
		WHERE id = $4
	`
	res, err := r.pool.Exec(ctx, query, inc.Status, inc.AssigneeID, inc.UpdatedAt, inc.ID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return api.ErrNotFound
	}
	return nil
}

// MemoryRepository implements Repository in-memory for testing
type MemoryRepository struct {
	mu        sync.RWMutex
	incidents map[uuid.UUID]Incident
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		incidents: make(map[uuid.UUID]Incident),
	}
}

func (m *MemoryRepository) Create(ctx context.Context, inc *Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incidents[inc.ID] = *inc
	return nil
}

func (m *MemoryRepository) GetByID(ctx context.Context, id uuid.UUID) (*Incident, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inc, ok := m.incidents[id]
	if !ok {
		return nil, api.ErrNotFound
	}
	return &inc, nil
}

func (m *MemoryRepository) List(ctx context.Context, filter Filter) ([]Incident, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Incident
	for _, inc := range m.incidents {
		if filter.Status != "" && inc.Status != filter.Status {
			continue
		}
		if filter.StreamType != "" && inc.StreamType != filter.StreamType {
			continue
		}
		result = append(result, inc)
	}
	return result, nil
}

func (m *MemoryRepository) Update(ctx context.Context, inc *Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.incidents[inc.ID]; !ok {
		return api.ErrNotFound
	}
	inc.UpdatedAt = time.Now()
	m.incidents[inc.ID] = *inc
	return nil
}
