package hubs

import (
	"context"
	"errors"
	"testing"
	"time"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

func TestCreateHubValidation(t *testing.T) {
	repo := NewMemoryRepository()
	service := NewService(repo)
	ctx := context.Background()

	// 1. Missing name
	_, err := service.CreateHub(ctx, CreateHubRequest{
		Name:      "",
		Address:   "123 Green Way",
		Latitude:  51.5,
		Longitude: -0.1,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for empty name, got %v", err)
	}

	// 2. Invalid Latitude
	_, err = service.CreateHub(ctx, CreateHubRequest{
		Name:      "Station A",
		Address:   "123 Green Way",
		Latitude:  95.0, // > 90
		Longitude: -0.1,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for invalid latitude, got %v", err)
	}

	// 3. Valid creation
	hub, err := service.CreateHub(ctx, CreateHubRequest{
		Name:      "Station Alpha",
		Address:   "123 Green Way",
		Latitude:  51.5074,
		Longitude: -0.1278,
	})
	if err != nil {
		t.Fatalf("unexpected error creating valid hub: %v", err)
	}
	if hub.ID == uuid.Nil {
		t.Error("expected non-nil hub UUID")
	}
	if hub.Status != "ACTIVE" {
		t.Errorf("expected status ACTIVE, got %s", hub.Status)
	}
}

func TestListHubsSpatialRadius(t *testing.T) {
	repo := NewMemoryRepository()
	service := NewService(repo)
	ctx := context.Background()

	// London Hub
	_, _ = service.CreateHub(ctx, CreateHubRequest{
		Name:      "London Hub",
		Address:   "Central London",
		Latitude:  51.5074,
		Longitude: -0.1278,
	})

	// Paris Hub (~340 km away)
	_, _ = service.CreateHub(ctx, CreateHubRequest{
		Name:      "Paris Hub",
		Address:   "Central Paris",
		Latitude:  48.8566,
		Longitude: 2.3522,
	})

	// Search around London with 50 km radius -> only London Hub should match
	results, err := service.ListHubs(ctx, 51.5074, -0.1278, 50.0)
	if err != nil {
		t.Fatalf("unexpected error searching hubs: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 hub within 50km, got %d", len(results))
	}
	if results[0].Name != "London Hub" {
		t.Errorf("expected London Hub, got %s", results[0].Name)
	}

	// Search with 500 km radius -> both London and Paris should match
	resultsWide, err := service.ListHubs(ctx, 51.5074, -0.1278, 500.0)
	if err != nil {
		t.Fatalf("unexpected error searching hubs: %v", err)
	}
	if len(resultsWide) != 2 {
		t.Fatalf("expected 2 hubs within 500km, got %d", len(resultsWide))
	}
}

func TestCreateSlotValidation(t *testing.T) {
	repo := NewMemoryRepository()
	service := NewService(repo)
	ctx := context.Background()

	hub, err := service.CreateHub(ctx, CreateHubRequest{
		Name:      "Test Hub",
		Address:   "10 Tech Blvd",
		Latitude:  51.5,
		Longitude: -0.1,
	})
	if err != nil {
		t.Fatalf("failed to create base hub: %v", err)
	}

	now := time.Now()

	// 1. Negative capacity
	_, err = service.CreateSlot(ctx, CreateSlotRequest{
		HubID:         hub.ID,
		StreamType:    "EWASTE",
		StartTime:     now,
		EndTime:       now.Add(time.Hour),
		TotalCapacity: 0,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for zero/negative capacity, got %v", err)
	}

	// 2. End time before start time
	_, err = service.CreateSlot(ctx, CreateSlotRequest{
		HubID:         hub.ID,
		StreamType:    "EWASTE",
		StartTime:     now.Add(2 * time.Hour),
		EndTime:       now,
		TotalCapacity: 10,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for inverted time window, got %v", err)
	}

	// 3. Unsupported waste stream
	_, err = service.CreateSlot(ctx, CreateSlotRequest{
		HubID:         hub.ID,
		StreamType:    "NUCLEAR_URANIUM",
		StartTime:     now,
		EndTime:       now.Add(time.Hour),
		TotalCapacity: 10,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for invalid waste stream, got %v", err)
	}

	// 4. Valid slot
	slot, err := service.CreateSlot(ctx, CreateSlotRequest{
		HubID:         hub.ID,
		StreamType:    "EWASTE",
		StartTime:     now,
		EndTime:       now.Add(time.Hour),
		TotalCapacity: 15,
	})
	if err != nil {
		t.Fatalf("unexpected error creating valid slot: %v", err)
	}
	if slot.TotalCapacity != 15 {
		t.Errorf("expected capacity 15, got %d", slot.TotalCapacity)
	}
}
