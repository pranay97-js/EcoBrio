package incidents

import (
	"context"
	"errors"
	"testing"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

func TestReportIncidentValidation(t *testing.T) {
	repo := NewMemoryRepository()
	service := NewService(repo)
	ctx := context.Background()

	// 1. Empty description
	_, err := service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "",
		StreamType:  "EWASTE",
		Latitude:    51.5,
		Longitude:   -0.1,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for empty description, got %v", err)
	}

	// 2. Invalid Waste Stream
	_, err = service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "Spill on highway",
		StreamType:  "UNKNOWN_CHEMICAL",
		Latitude:    51.5,
		Longitude:   -0.1,
	})
	if !errors.Is(err, api.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput for invalid stream, got %v", err)
	}

	// 3. Valid Report
	inc, err := service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "Broken CRT monitors leaking lead glass on curb",
		StreamType:  "EWASTE",
		Severity:    SeverityCritical,
		Latitude:    51.5074,
		Longitude:   -0.1278,
	})
	if err != nil {
		t.Fatalf("unexpected error creating valid incident: %v", err)
	}
	if inc.Status != StatusRaised {
		t.Errorf("expected initial status RAISED, got %s", inc.Status)
	}
	if inc.Severity != SeverityCritical {
		t.Errorf("expected CRITICAL, got %s", inc.Severity)
	}
}

func TestIncidentFSMTransitions(t *testing.T) {
	repo := NewMemoryRepository()
	service := NewService(repo)
	ctx := context.Background()

	inc, err := service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "Industrial battery leakage",
		StreamType:  "HAZARDOUS",
		Severity:    SeverityHigh,
		Latitude:    51.5,
		Longitude:   -0.1,
	})
	if err != nil {
		t.Fatalf("failed to report incident: %v", err)
	}

	collectorID := uuid.New()

	// 1. Invalid jump: RAISED -> RESOLVED (cannot resolve without taking In-Progress)
	_, err = service.UpdateStatus(ctx, inc.ID, StatusResolved, &collectorID)
	if !errors.Is(err, api.ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition for RAISED -> RESOLVED jump, got %v", err)
	}

	// 2. Valid transition: RAISED -> IN_PROGRESS
	updated, err := service.UpdateStatus(ctx, inc.ID, StatusInProgress, &collectorID)
	if err != nil {
		t.Fatalf("unexpected error in valid transition RAISED -> IN_PROGRESS: %v", err)
	}
	if updated.Status != StatusInProgress {
		t.Errorf("expected IN_PROGRESS, got %s", updated.Status)
	}
	if *updated.AssigneeID != collectorID {
		t.Errorf("expected assignee %v, got %v", collectorID, updated.AssigneeID)
	}

	// 3. Valid transition: IN_PROGRESS -> RESOLVED
	resolved, err := service.UpdateStatus(ctx, inc.ID, StatusResolved, &collectorID)
	if err != nil {
		t.Fatalf("unexpected error in valid transition IN_PROGRESS -> RESOLVED: %v", err)
	}
	if resolved.Status != StatusResolved {
		t.Errorf("expected RESOLVED, got %s", resolved.Status)
	}

	// 4. Invalid transition: RESOLVED -> IN_PROGRESS (cannot reopen resolved incident)
	_, err = service.UpdateStatus(ctx, inc.ID, StatusInProgress, &collectorID)
	if !errors.Is(err, api.ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition for terminal RESOLVED -> IN_PROGRESS, got %v", err)
	}
}
