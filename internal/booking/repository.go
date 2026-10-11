package booking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository implements Repository using PostgreSQL transactions
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) GetSlotCapacity(ctx context.Context, slotID uuid.UUID) (total, booked, version int, err error) {
	query := `
		SELECT total_capacity, booked_count, version
		FROM collection_slots
		WHERE id = $1
	`
	err = r.pool.QueryRow(ctx, query, slotID).Scan(&total, &booked, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, 0, ErrSlotNotFound
	}
	return total, booked, version, err
}

func (r *PostgresRepository) CommitBooking(ctx context.Context, b *Booking, fingerprint string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Layer 2: Atomic SQL Decrement / Capacity Invariant Check
	decrementQuery := `
		UPDATE collection_slots
		SET booked_count = booked_count + 1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $1 AND booked_count < total_capacity
		RETURNING booked_count, total_capacity
	`
	var newBooked, total int
	err = tx.QueryRow(ctx, decrementQuery, b.SlotID).Scan(&newBooked, &total)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCapacityExceeded
	}
	if err != nil {
		return fmt.Errorf("failed capacity update: %w", err)
	}

	// 2. Insert Booking Record
	insertBookingQuery := `
		INSERT INTO bookings (id, slot_id, citizen_id, stream_type, idempotency_key, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	if _, err := tx.Exec(ctx, insertBookingQuery,
		b.ID, b.SlotID, b.CitizenID, b.StreamType, b.IdempotencyKey, b.Status, b.CreatedAt, b.UpdatedAt,
	); err != nil {
		return fmt.Errorf("failed inserting booking: %w", err)
	}

	// 3. Persist Idempotency Outcome in PostgreSQL
	bookingJSON, _ := json.Marshal(b)
	insertIdempotencyQuery := `
		INSERT INTO idempotency_records (id, scope, key, request_fingerprint, status, response_code, response_body, created_at, updated_at)
		VALUES ($1, 'BOOKING', $2, $3, 'COMMITTED', 201, $4, NOW(), NOW())
		ON CONFLICT (scope, key) DO UPDATE
		SET status = 'COMMITTED', response_code = 201, response_body = $4, updated_at = NOW()
	`
	if _, err := tx.Exec(ctx, insertIdempotencyQuery, uuid.New(), b.IdempotencyKey, fingerprint, bookingJSON); err != nil {
		return fmt.Errorf("failed persisting idempotency: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) GetBookingByID(ctx context.Context, id uuid.UUID) (*Booking, error) {
	query := `
		SELECT id, slot_id, citizen_id, stream_type, idempotency_key, status, created_at, updated_at
		FROM bookings
		WHERE id = $1
	`
	var b Booking
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&b.ID, &b.SlotID, &b.CitizenID, &b.StreamType, &b.IdempotencyKey, &b.Status, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrBookingNotFound
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *PostgresRepository) GetBookingByIdempotencyKey(ctx context.Context, key string) (*Booking, error) {
	query := `
		SELECT id, slot_id, citizen_id, stream_type, idempotency_key, status, created_at, updated_at
		FROM bookings
		WHERE idempotency_key = $1
	`
	var b Booking
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&b.ID, &b.SlotID, &b.CitizenID, &b.StreamType, &b.IdempotencyKey, &b.Status, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrBookingNotFound
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *PostgresRepository) GetIdempotencyRecord(ctx context.Context, scope, key string) (*IdempotencyRecord, error) {
	query := `
		SELECT scope, key, request_fingerprint, status, response_code, response_body, created_at
		FROM idempotency_records
		WHERE scope = $1 AND key = $2
	`
	var rec IdempotencyRecord
	err := r.pool.QueryRow(ctx, query, scope, key).Scan(
		&rec.Scope, &rec.Key, &rec.RequestFingerprint, &rec.Status, &rec.ResponseCode, &rec.ResponseBody, &rec.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // Not found
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (r *PostgresRepository) SaveIdempotencyRecord(ctx context.Context, rec *IdempotencyRecord) error {
	query := `
		INSERT INTO idempotency_records (id, scope, key, request_fingerprint, status, response_code, response_body, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (scope, key) DO UPDATE
		SET status = EXCLUDED.status, response_code = EXCLUDED.response_code, response_body = EXCLUDED.response_body, updated_at = NOW()
	`
	_, err := r.pool.Exec(ctx, query,
		uuid.New(), rec.Scope, rec.Key, rec.RequestFingerprint, rec.Status, rec.ResponseCode, rec.ResponseBody, rec.CreatedAt,
	)
	return err
}

// MemoryRepository provides a thread-safe in-memory store for isolated testing
type MemoryRepository struct {
	mu            sync.RWMutex
	slotCapacity  map[uuid.UUID]int // slotID -> totalCapacity
	slotBooked    map[uuid.UUID]int // slotID -> currentBooked
	slotVersion   map[uuid.UUID]int
	bookings      map[uuid.UUID]Booking
	bookingsByKey map[string]uuid.UUID
	idempotency   map[string]IdempotencyRecord // "scope:key" -> record
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		slotCapacity:  make(map[uuid.UUID]int),
		slotBooked:    make(map[uuid.UUID]int),
		slotVersion:   make(map[uuid.UUID]int),
		bookings:      make(map[uuid.UUID]Booking),
		bookingsByKey: make(map[string]uuid.UUID),
		idempotency:   make(map[string]IdempotencyRecord),
	}
}

func (m *MemoryRepository) SetSlot(slotID uuid.UUID, totalCapacity, booked int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slotCapacity[slotID] = totalCapacity
	m.slotBooked[slotID] = booked
	m.slotVersion[slotID] = 1
}

func (m *MemoryRepository) GetSlotCapacity(ctx context.Context, slotID uuid.UUID) (total, booked, version int, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cap, exists := m.slotCapacity[slotID]
	if !exists {
		return 0, 0, 0, ErrSlotNotFound
	}
	return cap, m.slotBooked[slotID], m.slotVersion[slotID], nil
}

func (m *MemoryRepository) CommitBooking(ctx context.Context, b *Booking, fingerprint string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cap, exists := m.slotCapacity[b.SlotID]
	if !exists {
		return ErrSlotNotFound
	}

	booked := m.slotBooked[b.SlotID]
	// Database capacity invariant: booked must not exceed capacity
	if booked >= cap {
		return ErrCapacityExceeded
	}

	m.slotBooked[b.SlotID] = booked + 1
	m.slotVersion[b.SlotID]++
	m.bookings[b.ID] = *b
	m.bookingsByKey[b.IdempotencyKey] = b.ID

	bookingJSON, _ := json.Marshal(b)
	idemKey := "BOOKING:" + b.IdempotencyKey
	m.idempotency[idemKey] = IdempotencyRecord{
		Scope:              "BOOKING",
		Key:                b.IdempotencyKey,
		RequestFingerprint: fingerprint,
		Status:             "COMMITTED",
		ResponseCode:       201,
		ResponseBody:       bookingJSON,
		CreatedAt:          time.Now(),
	}

	return nil
}

func (m *MemoryRepository) GetBookingByID(ctx context.Context, id uuid.UUID) (*Booking, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.bookings[id]
	if !ok {
		return nil, ErrBookingNotFound
	}
	return &b, nil
}

func (m *MemoryRepository) GetBookingByIdempotencyKey(ctx context.Context, key string) (*Booking, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.bookingsByKey[key]
	if !ok {
		return nil, ErrBookingNotFound
	}
	b := m.bookings[id]
	return &b, nil
}

func (m *MemoryRepository) GetIdempotencyRecord(ctx context.Context, scope, key string) (*IdempotencyRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.idempotency[scope+":"+key]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

func (m *MemoryRepository) SaveIdempotencyRecord(ctx context.Context, rec *IdempotencyRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idempotency[rec.Scope+":"+rec.Key] = *rec
	return nil
}
