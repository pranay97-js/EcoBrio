package incidents

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Incident represents a municipal waste report
type Incident struct {
	ID          uuid.UUID  `json:"id"`
	ReporterID  *uuid.UUID `json:"reporter_id,omitempty"`
	StreamType  string     `json:"stream_type"`
	Description string     `json:"description"`
	Latitude    float64    `json:"latitude"`
	Longitude   float64    `json:"longitude"`
	Severity    string     `json:"severity"`
	Status      string     `json:"status"`
	AssigneeID  *uuid.UUID `json:"assignee_id,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Service manages incident state machine
type Service interface {
	ReportIncident(ctx context.Context, inc *Incident) (*Incident, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, newStatus string, actorID uuid.UUID) (*Incident, error)
}
