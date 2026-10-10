package credits

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	CreateSubmission(ctx context.Context, sub *RecyclingSubmission) error
	GetSubmissionByID(ctx context.Context, id uuid.UUID) (*RecyclingSubmission, error)
	GetSubmissionByQR(ctx context.Context, qr string) (*RecyclingSubmission, error)
	UpdateSubmission(ctx context.Context, sub *RecyclingSubmission) error
	ListSubmissionsByCitizen(ctx context.Context, citizenID uuid.UUID) ([]RecyclingSubmission, error)
}

func CalculatePoints(material string, weightKg float64) int64 {
	rate, ok := MaterialRewardRates[material]
	if !ok {
		rate = 10
	}
	pts := int64(math.Round(float64(rate) * weightKg))
	if pts <= 0 {
		return 1
	}
	return pts
}

// PostgresRepository implements Repository using PostgreSQL
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) CreateSubmission(ctx context.Context, sub *RecyclingSubmission) error {
	query := `
		INSERT INTO recycling_submissions (id, qr_identity, citizen_id, hub_id, material_type, weight_kg, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query,
		sub.ID, sub.QRIdentity, sub.CitizenID, sub.HubID,
		sub.Material, sub.WeightKg, string(sub.State),
		sub.CreatedAt, sub.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicateQR
		}
		return fmt.Errorf("failed creating recycling submission: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetSubmissionByID(ctx context.Context, id uuid.UUID) (*RecyclingSubmission, error) {
	query := `
		SELECT id, qr_identity, citizen_id, hub_id, material_type, weight_kg, status, inspector_id, inspection_notes, created_at, updated_at
		FROM recycling_submissions
		WHERE id = $1
	`
	var sub RecyclingSubmission
	var stateStr string
	var notes *string
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&sub.ID, &sub.QRIdentity, &sub.CitizenID, &sub.HubID,
		&sub.Material, &sub.WeightKg, &stateStr,
		&sub.InspectorID, &notes, &sub.CreatedAt, &sub.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSubmissionNotFound
	}
	if err != nil {
		return nil, err
	}
	sub.State = SubmissionState(stateStr)
	if notes != nil {
		sub.InspectionNotes = *notes
	}
	if sub.State == StateCredited {
		sub.PointsAwarded = CalculatePoints(sub.Material, sub.WeightKg)
	}
	return &sub, nil
}

func (r *PostgresRepository) GetSubmissionByQR(ctx context.Context, qr string) (*RecyclingSubmission, error) {
	query := `
		SELECT id, qr_identity, citizen_id, hub_id, material_type, weight_kg, status, inspector_id, inspection_notes, created_at, updated_at
		FROM recycling_submissions
		WHERE qr_identity = $1
	`
	var sub RecyclingSubmission
	var stateStr string
	var notes *string
	err := r.pool.QueryRow(ctx, query, qr).Scan(
		&sub.ID, &sub.QRIdentity, &sub.CitizenID, &sub.HubID,
		&sub.Material, &sub.WeightKg, &stateStr,
		&sub.InspectorID, &notes, &sub.CreatedAt, &sub.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSubmissionNotFound
	}
	if err != nil {
		return nil, err
	}
	sub.State = SubmissionState(stateStr)
	if notes != nil {
		sub.InspectionNotes = *notes
	}
	if sub.State == StateCredited {
		sub.PointsAwarded = CalculatePoints(sub.Material, sub.WeightKg)
	}
	return &sub, nil
}

func (r *PostgresRepository) UpdateSubmission(ctx context.Context, sub *RecyclingSubmission) error {
	query := `
		UPDATE recycling_submissions
		SET status = $1, inspector_id = $2, inspection_notes = $3, updated_at = NOW()
		WHERE id = $4
	`
	tag, err := r.pool.Exec(ctx, query, string(sub.State), sub.InspectorID, sub.InspectionNotes, sub.ID)
	if err != nil {
		return fmt.Errorf("failed updating recycling submission: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSubmissionNotFound
	}
	return nil
}

func (r *PostgresRepository) ListSubmissionsByCitizen(ctx context.Context, citizenID uuid.UUID) ([]RecyclingSubmission, error) {
	query := `
		SELECT id, qr_identity, citizen_id, hub_id, material_type, weight_kg, status, inspector_id, inspection_notes, created_at, updated_at
		FROM recycling_submissions
		WHERE citizen_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, citizenID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []RecyclingSubmission
	for rows.Next() {
		var sub RecyclingSubmission
		var stateStr string
		var notes *string
		if err := rows.Scan(
			&sub.ID, &sub.QRIdentity, &sub.CitizenID, &sub.HubID,
			&sub.Material, &sub.WeightKg, &stateStr,
			&sub.InspectorID, &notes, &sub.CreatedAt, &sub.UpdatedAt,
		); err != nil {
			return nil, err
		}
		sub.State = SubmissionState(stateStr)
		if notes != nil {
			sub.InspectionNotes = *notes
		}
		if sub.State == StateCredited {
			sub.PointsAwarded = CalculatePoints(sub.Material, sub.WeightKg)
		}
		results = append(results, sub)
	}
	return results, nil
}

// MemoryRepository provides a thread-safe in-memory store for unit tests and local dev
type MemoryRepository struct {
	mu     sync.RWMutex
	byID   map[uuid.UUID]RecyclingSubmission
	byQR   map[string]uuid.UUID
	byUser map[uuid.UUID][]uuid.UUID
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		byID:   make(map[uuid.UUID]RecyclingSubmission),
		byQR:   make(map[string]uuid.UUID),
		byUser: make(map[uuid.UUID][]uuid.UUID),
	}
}

func (m *MemoryRepository) CreateSubmission(ctx context.Context, sub *RecyclingSubmission) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.byQR[sub.QRIdentity]; exists {
		return ErrDuplicateQR
	}

	m.byID[sub.ID] = *sub
	m.byQR[sub.QRIdentity] = sub.ID
	m.byUser[sub.CitizenID] = append(m.byUser[sub.CitizenID], sub.ID)
	return nil
}

func (m *MemoryRepository) GetSubmissionByID(ctx context.Context, id uuid.UUID) (*RecyclingSubmission, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sub, exists := m.byID[id]
	if !exists {
		return nil, ErrSubmissionNotFound
	}
	copySub := sub
	if copySub.State == StateCredited && copySub.PointsAwarded == 0 {
		copySub.PointsAwarded = CalculatePoints(copySub.Material, copySub.WeightKg)
	}
	return &copySub, nil
}

func (m *MemoryRepository) GetSubmissionByQR(ctx context.Context, qr string) (*RecyclingSubmission, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, exists := m.byQR[qr]
	if !exists {
		return nil, ErrSubmissionNotFound
	}
	sub := m.byID[id]
	copySub := sub
	if copySub.State == StateCredited && copySub.PointsAwarded == 0 {
		copySub.PointsAwarded = CalculatePoints(copySub.Material, copySub.WeightKg)
	}
	return &copySub, nil
}

func (m *MemoryRepository) UpdateSubmission(ctx context.Context, sub *RecyclingSubmission) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.byID[sub.ID]; !exists {
		return ErrSubmissionNotFound
	}
	sub.UpdatedAt = time.Now()
	m.byID[sub.ID] = *sub
	return nil
}

func (m *MemoryRepository) ListSubmissionsByCitizen(ctx context.Context, citizenID uuid.UUID) ([]RecyclingSubmission, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := m.byUser[citizenID]
	var results []RecyclingSubmission
	for _, id := range ids {
		sub := m.byID[id]
		if sub.State == StateCredited && sub.PointsAwarded == 0 {
			sub.PointsAwarded = CalculatePoints(sub.Material, sub.WeightKg)
		}
		results = append(results, sub)
	}
	return results, nil
}
