package hubs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"ecobrio/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository defines storage operations for hubs and slots
type Repository interface {
	CreateHub(ctx context.Context, hub *Hub) error
	GetHubByID(ctx context.Context, id uuid.UUID) (*Hub, error)
	ListHubs(ctx context.Context, lat, lon, radiusKm float64) ([]Hub, error)
	CreateSlot(ctx context.Context, slot *Slot) error
	GetSlotsByHub(ctx context.Context, hubID uuid.UUID) ([]Slot, error)
	GetSlotByID(ctx context.Context, slotID uuid.UUID) (*Slot, error)
}

// PostgresRepository implements Repository using PostgreSQL pgxpool
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) CreateHub(ctx context.Context, hub *Hub) error {
	query := `
		INSERT INTO collection_hubs (id, name, address, latitude, longitude, status, operating_hours, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query,
		hub.ID, hub.Name, hub.Address, hub.Latitude, hub.Longitude,
		hub.Status, hub.OperatingHours, hub.CreatedAt, hub.UpdatedAt,
	)
	return err
}

func (r *PostgresRepository) GetHubByID(ctx context.Context, id uuid.UUID) (*Hub, error) {
	query := `
		SELECT id, name, address, latitude, longitude, status, operating_hours, created_at, updated_at
		FROM collection_hubs
		WHERE id = $1
	`
	var h Hub
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&h.ID, &h.Name, &h.Address, &h.Latitude, &h.Longitude,
		&h.Status, &h.OperatingHours, &h.CreatedAt, &h.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func (r *PostgresRepository) ListHubs(ctx context.Context, lat, lon, radiusKm float64) ([]Hub, error) {
	var rows pgx.Rows
	var err error

	if radiusKm > 0 && lat != 0 && lon != 0 {
		// Haversine formula calculation in SQL
		query := `
			SELECT id, name, address, latitude, longitude, status, operating_hours, created_at, updated_at
			FROM collection_hubs
			WHERE status = 'ACTIVE'
			  AND (6371 * acos(
					cos(radians($1)) * cos(radians(latitude)) * cos(radians(longitude) - radians($2)) +
					sin(radians($1)) * sin(radians(latitude))
			  )) <= $3
			ORDER BY created_at ASC
		`
		rows, err = r.pool.Query(ctx, query, lat, lon, radiusKm)
	} else {
		query := `
			SELECT id, name, address, latitude, longitude, status, operating_hours, created_at, updated_at
			FROM collection_hubs
			ORDER BY created_at ASC
		`
		rows, err = r.pool.Query(ctx, query)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hubs []Hub
	for rows.Next() {
		var h Hub
		if err := rows.Scan(
			&h.ID, &h.Name, &h.Address, &h.Latitude, &h.Longitude,
			&h.Status, &h.OperatingHours, &h.CreatedAt, &h.UpdatedAt,
		); err != nil {
			return nil, err
		}
		hubs = append(hubs, h)
	}
	return hubs, nil
}

func (r *PostgresRepository) CreateSlot(ctx context.Context, slot *Slot) error {
	query := `
		INSERT INTO collection_slots (id, hub_id, stream_type, start_time, end_time, total_capacity, booked_count, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.pool.Exec(ctx, query,
		slot.ID, slot.HubID, slot.StreamType, slot.StartTime, slot.EndTime,
		slot.TotalCapacity, slot.BookedCount, slot.Version, slot.CreatedAt, slot.UpdatedAt,
	)
	return err
}

func (r *PostgresRepository) GetSlotsByHub(ctx context.Context, hubID uuid.UUID) ([]Slot, error) {
	query := `
		SELECT id, hub_id, stream_type, start_time, end_time, total_capacity, booked_count, version, created_at, updated_at
		FROM collection_slots
		WHERE hub_id = $1
		ORDER BY start_time ASC
	`
	rows, err := r.pool.Query(ctx, query, hubID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var slots []Slot
	for rows.Next() {
		var s Slot
		if err := rows.Scan(
			&s.ID, &s.HubID, &s.StreamType, &s.StartTime, &s.EndTime,
			&s.TotalCapacity, &s.BookedCount, &s.Version, &s.CreatedAt, &s.UpdatedAt,
		); err != nil {
			return nil, err
		}
		slots = append(slots, s)
	}
	return slots, nil
}

func (r *PostgresRepository) GetSlotByID(ctx context.Context, slotID uuid.UUID) (*Slot, error) {
	query := `
		SELECT id, hub_id, stream_type, start_time, end_time, total_capacity, booked_count, version, created_at, updated_at
		FROM collection_slots
		WHERE id = $1
	`
	var s Slot
	err := r.pool.QueryRow(ctx, query, slotID).Scan(
		&s.ID, &s.HubID, &s.StreamType, &s.StartTime, &s.EndTime,
		&s.TotalCapacity, &s.BookedCount, &s.Version, &s.CreatedAt, &s.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// MemoryRepository provides an in-memory implementation for unit testing and offline development
type MemoryRepository struct {
	mu    sync.RWMutex
	hubs  map[uuid.UUID]Hub
	slots map[uuid.UUID]Slot
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		hubs:  make(map[uuid.UUID]Hub),
		slots: make(map[uuid.UUID]Slot),
	}
}

func (m *MemoryRepository) CreateHub(ctx context.Context, hub *Hub) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hubs[hub.ID] = *hub
	return nil
}

func (m *MemoryRepository) GetHubByID(ctx context.Context, id uuid.UUID) (*Hub, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.hubs[id]
	if !ok {
		return nil, api.ErrNotFound
	}
	return &h, nil
}

func (m *MemoryRepository) ListHubs(ctx context.Context, lat, lon, radiusKm float64) ([]Hub, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Hub
	for _, h := range m.hubs {
		if radiusKm > 0 && lat != 0 && lon != 0 {
			dist := haversineKm(lat, lon, h.Latitude, h.Longitude)
			if dist <= radiusKm {
				result = append(result, h)
			}
		} else {
			result = append(result, h)
		}
	}
	return result, nil
}

func (m *MemoryRepository) CreateSlot(ctx context.Context, slot *Slot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.hubs[slot.HubID]; !ok {
		return fmt.Errorf("hub not found: %w", api.ErrNotFound)
	}
	m.slots[slot.ID] = *slot
	return nil
}

func (m *MemoryRepository) GetSlotsByHub(ctx context.Context, hubID uuid.UUID) ([]Slot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []Slot
	for _, s := range m.slots {
		if s.HubID == hubID {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MemoryRepository) GetSlotByID(ctx context.Context, slotID uuid.UUID) (*Slot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.slots[slotID]
	if !ok {
		return nil, api.ErrNotFound
	}
	return &s, nil
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	dLat := (lat2 - lat1) * (math.Pi / 180.0)
	dLon := (lon2 - lon1) * (math.Pi / 180.0)

	rLat1 := lat1 * (math.Pi / 180.0)
	rLat2 := lat2 * (math.Pi / 180.0)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(rLat1)*math.Cos(rLat2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}
