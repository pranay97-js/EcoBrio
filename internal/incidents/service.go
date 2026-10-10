package incidents

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ecobrio/internal/api"
	"ecobrio/internal/hubs"
	"github.com/google/uuid"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) ReportIncident(ctx context.Context, req CreateIncidentRequest) (*Incident, error) {
	desc := strings.TrimSpace(req.Description)
	if desc == "" {
		return nil, fmt.Errorf("%w: incident description is required", api.ErrInvalidInput)
	}
	if req.Latitude < -90 || req.Latitude > 90 {
		return nil, fmt.Errorf("%w: latitude must be between -90 and 90", api.ErrInvalidInput)
	}
	if req.Longitude < -180 || req.Longitude > 180 {
		return nil, fmt.Errorf("%w: longitude must be between -180 and 180", api.ErrInvalidInput)
	}

	stream := strings.ToUpper(strings.TrimSpace(req.StreamType))
	if !hubs.ValidWasteStreams[stream] {
		return nil, fmt.Errorf("%w: invalid waste stream '%s'", api.ErrInvalidInput, req.StreamType)
	}

	severity := strings.ToUpper(strings.TrimSpace(req.Severity))
	if severity == "" {
		severity = SeverityMedium
	}
	if !ValidSeverities[severity] {
		return nil, fmt.Errorf("%w: invalid severity level '%s'", api.ErrInvalidInput, req.Severity)
	}

	inc := &Incident{
		ID:          uuid.New(),
		ReporterID:  req.ReporterID,
		StreamType:  stream,
		Description: desc,
		Latitude:    req.Latitude,
		Longitude:   req.Longitude,
		Severity:    severity,
		Status:      StatusRaised,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.Create(ctx, inc); err != nil {
		return nil, err
	}
	return inc, nil
}

func (s *Service) GetIncident(ctx context.Context, id uuid.UUID) (*Incident, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *Service) ListIncidents(ctx context.Context, filter Filter) ([]Incident, error) {
	return s.repo.List(ctx, filter)
}

func (s *Service) UpdateStatus(ctx context.Context, id uuid.UUID, targetStatus string, assigneeID *uuid.UUID) (*Incident, error) {
	inc, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	target := strings.ToUpper(strings.TrimSpace(targetStatus))
	if !ValidStatuses[target] {
		return nil, fmt.Errorf("%w: unrecognized status '%s'", api.ErrInvalidInput, targetStatus)
	}

	// Validate deterministic finite-state machine transitions
	if !isValidTransition(inc.Status, target) {
		return nil, fmt.Errorf("%w: illegal transition from '%s' to '%s'", api.ErrInvalidTransition, inc.Status, target)
	}

	inc.Status = target
	if assigneeID != nil {
		inc.AssigneeID = assigneeID
	}
	inc.UpdatedAt = time.Now()

	if err := s.repo.Update(ctx, inc); err != nil {
		return nil, err
	}
	return inc, nil
}

func isValidTransition(current, target string) bool {
	if current == target {
		return true // idempotent re-application
	}
	switch current {
	case StatusRaised:
		return target == StatusInProgress || target == StatusDismissed
	case StatusInProgress:
		return target == StatusResolved || target == StatusDismissed
	case StatusResolved, StatusDismissed:
		return false // terminal state
	default:
		return false
	}
}
