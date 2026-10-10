package hubs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateHub(ctx context.Context, req CreateHubRequest) (*Hub, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("%w: hub name is required", api.ErrInvalidInput)
	}
	address := strings.TrimSpace(req.Address)
	if address == "" {
		return nil, fmt.Errorf("%w: hub address is required", api.ErrInvalidInput)
	}
	if req.Latitude < -90 || req.Latitude > 90 {
		return nil, fmt.Errorf("%w: latitude must be between -90 and 90", api.ErrInvalidInput)
	}
	if req.Longitude < -180 || req.Longitude > 180 {
		return nil, fmt.Errorf("%w: longitude must be between -180 and 180", api.ErrInvalidInput)
	}

	operatingHours := req.OperatingHours
	if operatingHours == "" {
		operatingHours = "06:00 - 22:00 Daily"
	}

	hub := &Hub{
		ID:             uuid.New(),
		Name:           name,
		Address:        address,
		Latitude:       req.Latitude,
		Longitude:      req.Longitude,
		Status:         "ACTIVE",
		OperatingHours: operatingHours,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := s.repo.CreateHub(ctx, hub); err != nil {
		return nil, err
	}
	return hub, nil
}

func (s *Service) GetHub(ctx context.Context, id uuid.UUID) (*Hub, error) {
	return s.repo.GetHubByID(ctx, id)
}

func (s *Service) ListHubs(ctx context.Context, lat, lon, radiusKm float64) ([]Hub, error) {
	if (lat != 0 || lon != 0) && (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
		return nil, fmt.Errorf("%w: invalid search coordinates", api.ErrInvalidInput)
	}
	if radiusKm < 0 {
		return nil, fmt.Errorf("%w: search radius must be positive", api.ErrInvalidInput)
	}
	return s.repo.ListHubs(ctx, lat, lon, radiusKm)
}

func (s *Service) CreateSlot(ctx context.Context, req CreateSlotRequest) (*Slot, error) {
	if req.TotalCapacity <= 0 {
		return nil, fmt.Errorf("%w: total capacity must be greater than zero", api.ErrInvalidInput)
	}
	if !req.EndTime.After(req.StartTime) {
		return nil, fmt.Errorf("%w: end_time must be strictly after start_time", api.ErrInvalidInput)
	}
	stream := strings.ToUpper(strings.TrimSpace(req.StreamType))
	if !ValidWasteStreams[stream] {
		return nil, fmt.Errorf("%w: unsupported waste stream '%s'", api.ErrInvalidInput, req.StreamType)
	}

	// Verify hub exists
	if _, err := s.repo.GetHubByID(ctx, req.HubID); err != nil {
		return nil, fmt.Errorf("target hub not found: %w", err)
	}

	slot := &Slot{
		ID:            uuid.New(),
		HubID:         req.HubID,
		StreamType:    stream,
		StartTime:     req.StartTime,
		EndTime:       req.EndTime,
		TotalCapacity: req.TotalCapacity,
		BookedCount:   0,
		Version:       1,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := s.repo.CreateSlot(ctx, slot); err != nil {
		return nil, err
	}
	return slot, nil
}

func (s *Service) GetSlotsByHub(ctx context.Context, hubID uuid.UUID) ([]Slot, error) {
	return s.repo.GetSlotsByHub(ctx, hubID)
}

func (s *Service) GetSlot(ctx context.Context, slotID uuid.UUID) (*Slot, error) {
	return s.repo.GetSlotByID(ctx, slotID)
}
