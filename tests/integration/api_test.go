package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ecobrio/internal/hubs"
	"ecobrio/internal/incidents"
	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"github.com/google/uuid"
)

func setupTestServer() *httptest.Server {
	mux := http.NewServeMux()
	metrics := observability.NewMetrics()

	// Initialize In-Memory Repositories and Services
	hubsRepo := hubs.NewMemoryRepository()
	hubsService := hubs.NewService(hubsRepo)
	hubsHandler := hubs.NewHandler(hubsService)
	hubsHandler.RegisterRoutes(mux)

	incidentsRepo := incidents.NewMemoryRepository()
	incidentsService := incidents.NewService(incidentsRepo)
	incidentsHandler := incidents.NewHandler(incidentsService)
	incidentsHandler.RegisterRoutes(mux)

	handler := middleware.Recovery(middleware.LoggingAndMetrics(metrics, mux))
	return httptest.NewServer(handler)
}

func TestHubsAndSlotsAPIIntegration(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	client := server.Client()

	// 1. Create a Hub via POST /api/v1/hubs
	createHubBody := hubs.CreateHubRequest{
		Name:      "Bristol E-Hub",
		Address:   "10 Harbor Way, Bristol BS1 5AH",
		Latitude:  51.4545,
		Longitude: -2.5879,
	}
	bodyBytes, _ := json.Marshal(createHubBody)

	resp, err := client.Post(server.URL+"/api/v1/hubs", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("POST /api/v1/hubs failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var createdHub hubs.Hub
	if err := json.NewDecoder(resp.Body).Decode(&createdHub); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	_ = resp.Body.Close()

	if createdHub.Name != createHubBody.Name {
		t.Errorf("expected hub name %s, got %s", createHubBody.Name, createdHub.Name)
	}

	// 2. Query Hub via GET /api/v1/hubs/{id}
	resp, err = client.Get(fmt.Sprintf("%s/api/v1/hubs/%s", server.URL, createdHub.ID))
	if err != nil {
		t.Fatalf("GET /api/v1/hubs/{id} failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Create a Slot via POST /api/v1/slots
	now := time.Now()
	createSlotBody := hubs.CreateSlotRequest{
		HubID:         createdHub.ID,
		StreamType:    "EWASTE",
		StartTime:     now,
		EndTime:       now.Add(2 * time.Hour),
		TotalCapacity: 25,
	}
	slotBytes, _ := json.Marshal(createSlotBody)

	resp, err = client.Post(server.URL+"/api/v1/slots", "application/json", bytes.NewReader(slotBytes))
	if err != nil {
		t.Fatalf("POST /api/v1/slots failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var createdSlot hubs.Slot
	if err := json.NewDecoder(resp.Body).Decode(&createdSlot); err != nil {
		t.Fatalf("failed decoding slot response: %v", err)
	}
	_ = resp.Body.Close()

	if createdSlot.TotalCapacity != 25 {
		t.Errorf("expected slot capacity 25, got %d", createdSlot.TotalCapacity)
	}

	// 4. Query Slots for Hub via GET /api/v1/hubs/{id}/slots
	resp, err = client.Get(fmt.Sprintf("%s/api/v1/hubs/%s/slots", server.URL, createdHub.ID))
	if err != nil {
		t.Fatalf("GET /api/v1/hubs/{id}/slots failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var slotsList []hubs.Slot
	if err := json.NewDecoder(resp.Body).Decode(&slotsList); err != nil {
		t.Fatalf("failed decoding slots list: %v", err)
	}
	_ = resp.Body.Close()

	if len(slotsList) != 1 {
		t.Errorf("expected 1 slot, got %d", len(slotsList))
	}
}

func TestIncidentsLifecycleAPIIntegration(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	client := server.Client()

	// 1. Report an Incident via POST /api/v1/incidents
	createIncBody := incidents.CreateIncidentRequest{
		StreamType:  "HAZARDOUS",
		Description: "Chemical solvent containers ruptured near storm drain",
		Latitude:    51.5074,
		Longitude:   -0.1278,
		Severity:    "HIGH",
	}
	incBytes, _ := json.Marshal(createIncBody)

	resp, err := client.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(incBytes))
	if err != nil {
		t.Fatalf("POST /api/v1/incidents failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var createdInc incidents.Incident
	if err := json.NewDecoder(resp.Body).Decode(&createdInc); err != nil {
		t.Fatalf("failed decoding incident response: %v", err)
	}
	_ = resp.Body.Close()

	if createdInc.Status != incidents.StatusRaised {
		t.Errorf("expected initial status RAISED, got %s", createdInc.Status)
	}

	// 2. Progress Incident to IN_PROGRESS via PATCH /api/v1/incidents/{id}/status
	collectorID := uuid.New()
	patchBody := incidents.UpdateStatusRequest{
		Status:     incidents.StatusInProgress,
		AssigneeID: &collectorID,
	}
	patchBytes, _ := json.Marshal(patchBody)

	req, _ := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/api/v1/incidents/%s/status", server.URL, createdInc.ID), bytes.NewReader(patchBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PATCH status failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for transition to IN_PROGRESS, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Negative Test: Illegal transition directly back to an invalid state or jump
	invalidJumpBody := incidents.UpdateStatusRequest{
		Status: "COMPLETELY_INVALID_STATUS",
	}
	invalidBytes, _ := json.Marshal(invalidJumpBody)
	req, _ = http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/api/v1/incidents/%s/status", server.URL, createdInc.ID), bytes.NewReader(invalidBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PATCH invalid status failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for invalid status name, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 4. Resolve Incident via PATCH to RESOLVED
	resolveBody := incidents.UpdateStatusRequest{
		Status: incidents.StatusResolved,
	}
	resolveBytes, _ := json.Marshal(resolveBody)
	req, _ = http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/api/v1/incidents/%s/status", server.URL, createdInc.ID), bytes.NewReader(resolveBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PATCH resolve failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for transition to RESOLVED, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. Negative Test: Cannot transition out of terminal state (409 Conflict)
	reopenBody := incidents.UpdateStatusRequest{
		Status: incidents.StatusInProgress,
	}
	reopenBytes, _ := json.Marshal(reopenBody)
	req, _ = http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/api/v1/incidents/%s/status", server.URL, createdInc.ID), bytes.NewReader(reopenBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PATCH reopen failed: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 Conflict for transition from terminal RESOLVED state, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}
