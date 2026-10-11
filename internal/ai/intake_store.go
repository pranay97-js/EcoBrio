package ai

import (
	"context"
	"sync"
	"time"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

// Citizen intake state lifecycle
const (
	IntakeStatusReceived        = "RECEIVED"
	IntakeStatusProcessing      = "PROCESSING"
	IntakeStatusIncidentCreated = "INCIDENT_CREATED"
	IntakeStatusFailed          = "FAILED"
)

// CitizenIntake represents an asynchronous waste report entry
type CitizenIntake struct {
	ID                uuid.UUID  `json:"id"`
	CitizenID         uuid.UUID  `json:"citizen_id"`
	RawText           string     `json:"raw_text"`
	ExtractedStream   string     `json:"extracted_stream"`
	ExtractedSeverity string     `json:"extracted_severity"`
	Latitude          float64    `json:"latitude"`
	Longitude         float64    `json:"longitude"`
	Status            string     `json:"status"`
	IncidentID        *uuid.UUID `json:"incident_id,omitempty"`
	ErrorMessage      string     `json:"error_message,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// IntakeStore defines persistence operations for asynchronous citizen intake
type IntakeStore interface {
	Create(ctx context.Context, intake *CitizenIntake) error
	GetByID(ctx context.Context, id uuid.UUID) (*CitizenIntake, error)
	Update(ctx context.Context, intake *CitizenIntake) error
}

// MemoryIntakeStore is a thread-safe in-memory store
type MemoryIntakeStore struct {
	mu      sync.RWMutex
	intakes map[uuid.UUID]CitizenIntake
}

func NewMemoryIntakeStore() *MemoryIntakeStore {
	return &MemoryIntakeStore{
		intakes: make(map[uuid.UUID]CitizenIntake),
	}
}

func (s *MemoryIntakeStore) Create(ctx context.Context, intake *CitizenIntake) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intakes[intake.ID] = *intake
	return nil
}

func (s *MemoryIntakeStore) GetByID(ctx context.Context, id uuid.UUID) (*CitizenIntake, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.intakes[id]
	if !ok {
		return nil, api.ErrNotFound
	}
	cp := item
	return &cp, nil
}

func (s *MemoryIntakeStore) Update(ctx context.Context, intake *CitizenIntake) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.intakes[intake.ID]; !ok {
		return api.ErrNotFound
	}
	intake.UpdatedAt = time.Now()
	s.intakes[intake.ID] = *intake
	return nil
}
