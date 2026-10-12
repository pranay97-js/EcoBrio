package incidents

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ecobrio/internal/api"
	"ecobrio/internal/geospatial"
	"ecobrio/internal/hubs"
	"ecobrio/internal/websocket"
	"github.com/google/uuid"
)

// Broadcaster decouples real-time WebSocket fan-out from the incident service
type Broadcaster interface {
	Broadcast(eventType string, payload any) *websocket.Event
	CurrentSequence() uint64
}

type Service struct {
	repo        Repository
	geoIndexer  geospatial.Indexer
	broadcaster Broadcaster
}

// NewService creates a new incident service with optional spatial index and WebSocket broadcaster
func NewService(repo Repository, optional ...any) *Service {
	svc := &Service{repo: repo}
	for _, opt := range optional {
		switch v := opt.(type) {
		case geospatial.Indexer:
			svc.geoIndexer = v
		case Broadcaster:
			svc.broadcaster = v
		}
	}
	return svc
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

	timeObs := req.TimeObserved
	if timeObs == "" {
		timeObs = time.Now().Format("2006-01-02 15:04")
	}
	expectedRes := time.Now().Add(48 * time.Hour)

	inc := &Incident{
		ID:                 uuid.New(),
		ReporterID:         req.ReporterID,
		StreamType:         stream,
		Description:        desc,
		Latitude:           req.Latitude,
		Longitude:          req.Longitude,
		Severity:           severity,
		Status:             StatusRaised,
		Place:              req.Place,
		Landmark:           req.Landmark,
		TimeObserved:       timeObs,
		PhotoURL:           req.PhotoURL,
		ResolutionSLA:      "1-2 days",
		ExpectedResolution: &expectedRes,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}

	if err := s.repo.Create(ctx, inc); err != nil {
		return nil, err
	}

	// 1. Maintain Redis/Memory Geospatial Projection
	if s.geoIndexer != nil {
		_ = s.geoIndexer.IndexIncident(ctx, geospatial.GeoPoint{
			ID:         inc.ID,
			Latitude:   inc.Latitude,
			Longitude:  inc.Longitude,
			Name:       inc.Description,
			Status:     inc.Status,
			StreamType: inc.StreamType,
		})
	}

	// 2. Broadcast committed transition via WebSocket Hub
	if s.broadcaster != nil {
		s.broadcaster.Broadcast(websocket.EventIncidentCreated, inc)
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

	// 1. Maintain Geospatial Projection
	if s.geoIndexer != nil {
		if inc.Status == StatusResolved || inc.Status == StatusDismissed {
			// Resolved items are removed from active spatial index
			_ = s.geoIndexer.Remove(ctx, inc.ID)
		} else {
			_ = s.geoIndexer.IndexIncident(ctx, geospatial.GeoPoint{
				ID:         inc.ID,
				Latitude:   inc.Latitude,
				Longitude:  inc.Longitude,
				Name:       inc.Description,
				Status:     inc.Status,
				StreamType: inc.StreamType,
			})
		}
	}

	// 2. Broadcast committed transition via WebSocket Hub
	if s.broadcaster != nil {
		s.broadcaster.Broadcast(websocket.EventIncidentUpdated, inc)
	}

	return inc, nil
}

// NearbyIncidents finds active incidents within the given radius in kilometers
func (s *Service) NearbyIncidents(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]Incident, error) {
	if s.geoIndexer != nil {
		points, err := s.geoIndexer.SearchNearby(ctx, lat, lon, radiusKm, limit)
		if err == nil && len(points) > 0 {
			results := make([]Incident, 0, len(points))
			for _, p := range points {
				inc, err := s.repo.GetByID(ctx, p.ID)
				if err == nil && inc != nil {
					results = append(results, *inc)
				}
			}
			return results, nil
		}
	}

	// Fallback to repository filtering with manual distance calculation
	all, err := s.repo.List(ctx, Filter{})
	if err != nil {
		return nil, err
	}

	var matched []Incident
	for _, inc := range all {
		if inc.Status == StatusResolved || inc.Status == StatusDismissed {
			continue
		}
		dist := geospatial.HaversineKm(lat, lon, inc.Latitude, inc.Longitude)
		if dist <= radiusKm {
			matched = append(matched, inc)
			if limit > 0 && len(matched) >= limit {
				break
			}
		}
	}
	return matched, nil
}

// GetActiveSnapshot returns all non-terminal incidents along with the current broadcast sequence
// Used by the WebSocket Hub to provide snapshot synchronization on reconnect
func (s *Service) GetActiveSnapshot(ctx context.Context) (any, uint64, error) {
	all, err := s.repo.List(ctx, Filter{})
	if err != nil {
		return nil, 0, err
	}

	active := make([]Incident, 0, len(all))
	for _, inc := range all {
		if inc.Status != StatusResolved && inc.Status != StatusDismissed {
			active = append(active, inc)
		}
	}

	var seq uint64 = 0
	if s.broadcaster != nil {
		seq = s.broadcaster.CurrentSequence()
	}

	return active, seq, nil
}

// RebuildGeoIndex repopulates the spatial index from the canonical database
func (s *Service) RebuildGeoIndex(ctx context.Context) (int, error) {
	if s.geoIndexer == nil {
		return 0, nil
	}

	all, err := s.repo.List(ctx, Filter{})
	if err != nil {
		return 0, err
	}

	points := make([]geospatial.GeoPoint, 0, len(all))
	for _, inc := range all {
		if inc.Status != StatusResolved && inc.Status != StatusDismissed {
			points = append(points, geospatial.GeoPoint{
				ID:         inc.ID,
				Latitude:   inc.Latitude,
				Longitude:  inc.Longitude,
				Name:       inc.Description,
				Status:     inc.Status,
				StreamType: inc.StreamType,
			})
		}
	}

	if err := s.geoIndexer.RebuildIndex(ctx, points); err != nil {
		return 0, err
	}
	return len(points), nil
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
