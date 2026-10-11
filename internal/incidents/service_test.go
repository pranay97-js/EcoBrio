package incidents

import (
	"context"
	"errors"
	"testing"

	"ecobrio/internal/api"
	"ecobrio/internal/geospatial"
	"ecobrio/internal/websocket"
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

type MockBroadcaster struct {
	events   []*websocket.Event
	sequence uint64
}

func (m *MockBroadcaster) Broadcast(eventType string, payload any) *websocket.Event {
	m.sequence++
	ev := &websocket.Event{
		ID:       uuid.New().String(),
		Sequence: m.sequence,
		Type:     eventType,
		Payload:  payload,
	}
	m.events = append(m.events, ev)
	return ev
}

func (m *MockBroadcaster) CurrentSequence() uint64 {
	return m.sequence
}

func TestIncidentService_GeospatialAndBroadcastLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	geoIdx := geospatial.NewMemoryIndexer()
	broadcaster := &MockBroadcaster{}

	service := NewService(repo, geoIdx, broadcaster)

	// 1. Report an incident
	inc, err := service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "Overflowing battery bins",
		StreamType:  "HAZARDOUS",
		Severity:    SeverityHigh,
		Latitude:    28.6139, // New Delhi
		Longitude:   77.2090,
	})
	if err != nil {
		t.Fatalf("failed to report incident: %v", err)
	}

	// Verify indexed in spatial index
	assert := func(cond bool, msg string) {
		if !cond {
			t.Fatalf("assertion failed: %s", msg)
		}
	}
	assert(geoIdx.Len() == 1, "Expected 1 indexed point in spatial index")

	// Verify broadcast event sent
	assert(len(broadcaster.events) == 1, "Expected 1 broadcast event")
	assert(broadcaster.events[0].Type == websocket.EventIncidentCreated, "Expected INCIDENT_CREATED")

	// 2. Query nearby: should find it within 5km
	nearby, err := service.NearbyIncidents(ctx, 28.6150, 77.2100, 5.0, 10)
	if err != nil || len(nearby) != 1 {
		t.Fatalf("expected 1 nearby incident, got %d (err: %v)", len(nearby), err)
	}
	assert(nearby[0].ID == inc.ID, "Nearby incident ID mismatch")

	// Query far away: within 5km from Mumbai (19.0760, 72.8777) -> 0 results
	farAway, err := service.NearbyIncidents(ctx, 19.0760, 72.8777, 5.0, 10)
	if err != nil || len(farAway) != 0 {
		t.Fatalf("expected 0 nearby incidents in Mumbai, got %d", len(farAway))
	}

	// 3. Update status: RAISED -> IN_PROGRESS
	collectorID := uuid.New()
	_, err = service.UpdateStatus(ctx, inc.ID, StatusInProgress, &collectorID)
	if err != nil {
		t.Fatalf("failed to update status to IN_PROGRESS: %v", err)
	}
	assert(len(broadcaster.events) == 2, "Expected 2nd broadcast event")
	assert(broadcaster.events[1].Type == websocket.EventIncidentUpdated, "Expected INCIDENT_UPDATED")
	assert(geoIdx.Len() == 1, "Active incident should remain in spatial index")

	// 4. Update status: IN_PROGRESS -> RESOLVED (Terminal)
	_, err = service.UpdateStatus(ctx, inc.ID, StatusResolved, &collectorID)
	if err != nil {
		t.Fatalf("failed to update status to RESOLVED: %v", err)
	}
	assert(len(broadcaster.events) == 3, "Expected 3rd broadcast event")
	assert(geoIdx.Len() == 0, "Resolved incident must be pruned from active spatial index")

	// 5. Test snapshot: should return 0 active incidents now
	snapState, snapSeq, err := service.GetActiveSnapshot(ctx)
	if err != nil {
		t.Fatalf("failed to get active snapshot: %v", err)
	}
	activeList := snapState.([]Incident)
	assert(len(activeList) == 0, "Snapshot should have 0 active incidents")
	assert(snapSeq == 3, "Snapshot sequence should match latest broadcast sequence")

	// 6. Report a 2nd incident and test RebuildGeoIndex
	inc2, err := service.ReportIncident(ctx, CreateIncidentRequest{
		Description: "Plastic bottles accumulation",
		StreamType:  "PLASTICS",
		Severity:    SeverityMedium,
		Latitude:    28.7041,
		Longitude:   77.1025,
	})
	if err != nil {
		t.Fatalf("failed reporting 2nd incident: %v", err)
	}
	assert(geoIdx.Len() == 1, "Spatial index should have 1 point")

	// Clear spatial index to simulate cache wipe/drift
	_ = geoIdx.RebuildIndex(ctx, nil)
	assert(geoIdx.Len() == 0, "Spatial index should be cleared")

	// Rebuild from canonical database
	rebuiltCount, err := service.RebuildGeoIndex(ctx)
	if err != nil || rebuiltCount != 1 {
		t.Fatalf("expected 1 rebuilt point, got %d (err: %v)", rebuiltCount, err)
	}
	assert(geoIdx.Len() == 1, "Spatial index should be repopulated with active incident")

	// Verify inc2 is in rebuilt index
	nearby2, err := service.NearbyIncidents(ctx, 28.7041, 77.1025, 1.0, 10)
	if err != nil || len(nearby2) != 1 {
		t.Fatalf("expected 1 nearby after rebuild, got %d", len(nearby2))
	}
	assert(nearby2[0].ID == inc2.ID, "Rebuilt incident ID mismatch")
}
