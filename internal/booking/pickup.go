package booking

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// PickupRequest represents a citizen request for doorstep waste collection
type PickupRequest struct {
	ID                uuid.UUID `json:"id"`
	CitizenID         uuid.UUID `json:"citizen_id"`
	CitizenName       string    `json:"citizen_name"`
	CitizenPhone      string    `json:"citizen_phone"`
	Address           string    `json:"address"`
	Latitude          float64   `json:"latitude"`
	Longitude         float64   `json:"longitude"`
	StreamType        string    `json:"stream_type"`
	EstimatedWeightKg float64   `json:"estimated_weight_kg"`
	PickupDate        string    `json:"pickup_date"`
	PickupTimeSlot    string    `json:"pickup_time_slot"` // Formatted from the time slider e.g. "14:30 - 15:00"
	Notes             string    `json:"notes,omitempty"`
	Status            string    `json:"status"` // "SCHEDULED", "IN_PROGRESS", "COMPLETED", "CANCELLED"
	TokenBounty       int64     `json:"token_bounty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// MemoryPickupStore provides concurrent-safe in-memory store for doorstep pickup requests
type MemoryPickupStore struct {
	mu      sync.RWMutex
	pickups map[uuid.UUID]*PickupRequest
}

func NewMemoryPickupStore() *MemoryPickupStore {
	store := &MemoryPickupStore{
		pickups: make(map[uuid.UUID]*PickupRequest),
	}
	// Seed initial pickup requests for demonstration
	demoID1 := uuid.MustParse("33333333-0000-0000-0000-000000000001")
	demoCitizen := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	store.pickups[demoID1] = &PickupRequest{
		ID:                demoID1,
		CitizenID:         demoCitizen,
		CitizenName:       "Arun Kumar",
		CitizenPhone:      "+91 98765 43210",
		Address:           "Flat 402, Block B, Preet Vihar, New Delhi 110092",
		Latitude:          28.6360,
		Longitude:         77.2925,
		StreamType:        "EWASTE",
		EstimatedWeightKg: 8.5,
		PickupDate:        "Today",
		PickupTimeSlot:    "14:30 - 15:00",
		Notes:             "Old laptop batteries and power adapters in sealed box",
		Status:            "SCHEDULED",
		TokenBounty:       380,
		CreatedAt:         time.Now().Add(-2 * time.Hour),
		UpdatedAt:         time.Now().Add(-2 * time.Hour),
	}

	demoID2 := uuid.MustParse("33333333-0000-0000-0000-000000000002")
	store.pickups[demoID2] = &PickupRequest{
		ID:                demoID2,
		CitizenID:         demoCitizen,
		CitizenName:       "Arun Kumar",
		CitizenPhone:      "+91 98765 43210",
		Address:           "Plot 14, Sector 62, Noida, Uttar Pradesh 201309",
		Latitude:          28.6270,
		Longitude:         77.3725,
		StreamType:        "PLASTICS",
		EstimatedWeightKg: 12.0,
		PickupDate:        "Tomorrow",
		PickupTimeSlot:    "10:15 - 10:45",
		Notes:             "Clean sorted PET bottles and HDPE containers",
		Status:            "SCHEDULED",
		TokenBounty:       300,
		CreatedAt:         time.Now().Add(-30 * time.Minute),
		UpdatedAt:         time.Now().Add(-30 * time.Minute),
	}

	return store
}

func (s *MemoryPickupStore) CreatePickup(ctx context.Context, req *PickupRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pickups[req.ID] = req
	return nil
}

func (s *MemoryPickupStore) ListPickups(ctx context.Context) ([]*PickupRequest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*PickupRequest, 0, len(s.pickups))
	for _, p := range s.pickups {
		list = append(list, p)
	}
	return list, nil
}

func (s *MemoryPickupStore) GetPickup(ctx context.Context, id uuid.UUID) (*PickupRequest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pickups[id]
	if !ok {
		return nil, ErrBookingNotFound
	}
	return p, nil
}

func (s *MemoryPickupStore) CancelPickup(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pickups[id]
	if !ok {
		return ErrBookingNotFound
	}
	p.Status = "CANCELLED"
	p.UpdatedAt = time.Now()
	return nil
}
