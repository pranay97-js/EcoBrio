package incidents

import (
	"time"

	"github.com/google/uuid"
)

// Status lifecycle constants
const (
	StatusRaised     = "RAISED"
	StatusInProgress = "IN_PROGRESS"
	StatusResolved   = "RESOLVED"
	StatusDismissed  = "DISMISSED"
)

// Severity constants
const (
	SeverityLow      = "LOW"
	SeverityMedium   = "MEDIUM"
	SeverityHigh     = "HIGH"
	SeverityCritical = "CRITICAL"
)

var ValidStatuses = map[string]bool{
	StatusRaised:     true,
	StatusInProgress: true,
	StatusResolved:   true,
	StatusDismissed:  true,
}

var ValidSeverities = map[string]bool{
	SeverityLow:      true,
	SeverityMedium:   true,
	SeverityHigh:     true,
	SeverityCritical: true,
}

// Incident represents an operational waste or hazardous event
type Incident struct {
	ID                 uuid.UUID  `json:"id"`
	ReporterID         *uuid.UUID `json:"reporter_id,omitempty"`
	StreamType         string     `json:"stream_type"`
	Description        string     `json:"description"`
	Latitude           float64    `json:"latitude"`
	Longitude          float64    `json:"longitude"`
	Severity           string     `json:"severity"`
	Status             string     `json:"status"`
	Place              string     `json:"place,omitempty"`
	Landmark           string     `json:"landmark,omitempty"`
	TimeObserved       string     `json:"time_observed,omitempty"`
	PhotoURL           string     `json:"photo_url,omitempty"`
	ResolutionSLA      string     `json:"resolution_sla,omitempty"`
	ExpectedResolution *time.Time `json:"expected_resolution,omitempty"`
	AssigneeID         *uuid.UUID `json:"assignee_id,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// CreateIncidentRequest holds input for submitting an incident
type CreateIncidentRequest struct {
	ReporterID   *uuid.UUID `json:"reporter_id,omitempty"`
	StreamType   string     `json:"stream_type"`
	Description  string     `json:"description"`
	Latitude     float64    `json:"latitude"`
	Longitude    float64    `json:"longitude"`
	Severity     string     `json:"severity"`
	Place        string     `json:"place,omitempty"`
	Landmark     string     `json:"landmark,omitempty"`
	TimeObserved string     `json:"time_observed,omitempty"`
	PhotoURL     string     `json:"photo_url,omitempty"`
}

// UpdateStatusRequest holds input for progressing an incident status
type UpdateStatusRequest struct {
	Status     string     `json:"status"`
	AssigneeID *uuid.UUID `json:"assignee_id,omitempty"`
}

// Filter holds query criteria for listing incidents
type Filter struct {
	Status     string
	StreamType string
	Limit      int
}
