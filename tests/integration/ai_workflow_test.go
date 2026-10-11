package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ecobrio/internal/ai"
	"ecobrio/internal/credits"
	"ecobrio/internal/geospatial"
	"ecobrio/internal/hubs"
	"ecobrio/internal/incidents"
	"ecobrio/internal/ledger"
	"ecobrio/internal/outbox"
	"ecobrio/internal/queue"
	ws "ecobrio/internal/websocket"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type aiTestServerFixtures struct {
	server       *httptest.Server
	incidentsSvc *incidents.Service
	hubsSvc      *hubs.Service
	creditsSvc   *credits.Service
	intakeSvc    *ai.IntakeService
	wsHub        *ws.Hub
}

func setupAITestServer(t *testing.T) (*aiTestServerFixtures, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. Hubs
	hubsRepo := hubs.NewMemoryRepository()
	hubsSvc := hubs.NewService(hubsRepo)
	_ = hubsRepo.CreateHub(ctx, &hubs.Hub{
		ID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Name:      "Central Delhi Recovery Hub",
		Latitude:  28.6139,
		Longitude: 77.2090,
	})

	// 2. Incidents & WebSocket Hub
	incidentsRepo := incidents.NewMemoryRepository()
	geoIdx := geospatial.NewMemoryIndexer()
	wsHub := ws.NewHub()
	go wsHub.Run(ctx)

	incidentsSvc := incidents.NewService(incidentsRepo, geoIdx, wsHub)
	wsHub.SetSnapshotProvider(incidentsSvc.GetActiveSnapshot)

	// 3. Ledger & Credits
	ledgerRepo := ledger.NewMemoryRepository()
	ledgerSvc := ledger.NewService(ledgerRepo)
	creditsRepo := credits.NewMemoryRepository()
	outboxRepo := outbox.NewMemoryRepository()
	creditsSvc := credits.NewService(creditsRepo, ledgerSvc, outboxRepo)

	// 4. AI Tool Registry & Engine
	tools := ai.NewToolRegistry(incidentsSvc, hubsSvc, creditsSvc)
	engine := ai.NewRuleBasedEngine(tools)

	// 5. Async Intake Pipeline
	msgQueue := queue.NewMemoryQueue()
	intakeStore := ai.NewMemoryIntakeStore()
	intakeSvc := ai.NewIntakeService(intakeStore, tools, msgQueue)
	intakeSvc.StartBackgroundWorker(ctx, msgQueue)

	// 6. Router & Handlers
	mux := http.NewServeMux()
	wsHub.RegisterRoutes(mux)

	hubsHandler := hubs.NewHandler(hubsSvc)
	hubsHandler.RegisterRoutes(mux)

	incidentsHandler := incidents.NewHandler(incidentsSvc)
	incidentsHandler.RegisterRoutes(mux)

	creditsHandler := credits.NewHandler(creditsSvc)
	creditsHandler.RegisterRoutes(mux)

	aiHandler := ai.NewHandler(engine, intakeSvc, tools)
	aiHandler.RegisterRoutes(mux)

	server := httptest.NewServer(mux)

	cleanup := func() {
		cancel()
		server.Close()
	}

	return &aiTestServerFixtures{
		server:       server,
		incidentsSvc: incidentsSvc,
		hubsSvc:      hubsSvc,
		creditsSvc:   creditsSvc,
		intakeSvc:    intakeSvc,
		wsHub:        wsHub,
	}, cleanup
}

func TestAI_AsyncWorkflow_CitizenReportToValidatedIncident(t *testing.T) {
	fixtures, cleanup := setupAITestServer(t)
	defer cleanup()

	// 1. Connect WebSocket client to observe real-time incident fanout
	wsURL := "ws" + strings.TrimPrefix(fixtures.server.URL, "http") + "/ws/incidents?client_id=monitoring-agent"
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer wsConn.Close()

	// Drain initial snapshot sync
	var initEvt ws.Event
	require.NoError(t, wsConn.ReadJSON(&initEvt))
	assert.Equal(t, ws.EventSnapshotSync, initEvt.Type)

	citizenID := uuid.New()
	coords := [2]float64{28.6150, 77.2100}

	// 2. Citizen posts report through asynchronous triage endpoint
	body := map[string]any{
		"text":        "Found large pile of discarded computers, e-waste, motherboards, and wire cables",
		"coordinates": coords,
		"citizen_id":  citizenID,
	}
	payload, _ := json.Marshal(body)

	resp, err := http.Post(fixtures.server.URL+"/api/v1/triage/async", "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode, "Must return HTTP 202 Accepted")

	var asyncRes map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&asyncRes))
	_ = resp.Body.Close()

	intakeIDStr, ok := asyncRes["intake_id"].(string)
	require.True(t, ok)
	intakeID, err := uuid.Parse(intakeIDStr)
	require.NoError(t, err)
	assert.Equal(t, string(ai.IntakeStatusReceived), asyncRes["status"])
	assert.Equal(t, "EWASTE", asyncRes["stream"])

	// 3. Poll intake status until background worker processes message (RECEIVED -> PROCESSING -> INCIDENT_CREATED)
	var finalIntake *ai.CitizenIntake
	require.Eventually(t, func() bool {
		statusResp, sErr := http.Get(fmt.Sprintf("%s/api/v1/triage/intake/%s", fixtures.server.URL, intakeID.String()))
		if sErr != nil || statusResp.StatusCode != http.StatusOK {
			return false
		}
		var item ai.CitizenIntake
		if err := json.NewDecoder(statusResp.Body).Decode(&item); err != nil {
			_ = statusResp.Body.Close()
			return false
		}
		_ = statusResp.Body.Close()

		if item.Status == ai.IntakeStatusIncidentCreated && item.IncidentID != nil {
			finalIntake = &item
			return true
		}
		return false
	}, 3*time.Second, 25*time.Millisecond, "Async intake pipeline must transition to INCIDENT_CREATED")

	require.NotNil(t, finalIntake)
	require.NotNil(t, finalIntake.IncidentID)

	// 4. Verify WebSocket received the real-time INCIDENT_CREATED broadcast
	_ = wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var wsEvt ws.Event
	require.NoError(t, wsConn.ReadJSON(&wsEvt))
	assert.Equal(t, ws.EventIncidentCreated, wsEvt.Type)

	// 5. Verify the validated incident directly via domain API
	incResp, err := http.Get(fmt.Sprintf("%s/api/v1/incidents/%s", fixtures.server.URL, finalIntake.IncidentID.String()))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, incResp.StatusCode)

	var validatedInc incidents.Incident
	require.NoError(t, json.NewDecoder(incResp.Body).Decode(&validatedInc))
	_ = incResp.Body.Close()

	assert.Equal(t, *finalIntake.IncidentID, validatedInc.ID)
	assert.Equal(t, "EWASTE", validatedInc.StreamType)
	assert.Equal(t, &citizenID, validatedInc.ReporterID)
	assert.Equal(t, incidents.StatusRaised, validatedInc.Status)
	assert.InDelta(t, 28.6150, validatedInc.Latitude, 0.001)
	assert.InDelta(t, 77.2100, validatedInc.Longitude, 0.001)
}

func TestAI_SyncTriage_CapabilitiesAndGuardrails(t *testing.T) {
	fixtures, cleanup := setupAITestServer(t)
	defer cleanup()

	ctx := context.Background()
	citizenID := uuid.New()

	// 1. Seed points for citizen (10kg GLASS @ 10 pts/kg = 100 points)
	sub, err := fixtures.creditsSvc.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-AI-INT-100",
		CitizenID:  citizenID,
		HubID:      uuid.New(),
		Material:   "GLASS",
		WeightKg:   10.0,
	})
	require.NoError(t, err)
	_, err = fixtures.creditsSvc.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: uuid.New(),
		Passed:      true,
		Notes:       "Passed inspection",
	})
	require.NoError(t, err)

	// A. Query User Credits via AI Triage
	credBody, _ := json.Marshal(map[string]any{
		"text":       "How many credits and points do I have in my wallet balance?",
		"citizen_id": citizenID,
	})
	resp, err := http.Post(fixtures.server.URL+"/api/v1/triage", "application/json", bytes.NewReader(credBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var credRes ai.TriageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&credRes))
	_ = resp.Body.Close()
	assert.Equal(t, ai.ActionToolExecuted, credRes.ActionTaken)
	assert.Equal(t, ai.ToolGetUserCredits, credRes.ToolName)
	assert.Contains(t, credRes.Message, "100")

	// B. Query Nearest Hub via AI Triage
	hubBody, _ := json.Marshal(map[string]any{
		"text":        "Where is the nearest collection hub to drop off my recycling?",
		"coordinates": [2]float64{28.6139, 77.2090},
	})
	resp, err = http.Post(fixtures.server.URL+"/api/v1/triage", "application/json", bytes.NewReader(hubBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var hubRes ai.TriageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&hubRes))
	_ = resp.Body.Close()
	assert.Equal(t, ai.ActionToolExecuted, hubRes.ActionTaken)
	assert.Equal(t, ai.ToolFindNearestHub, hubRes.ToolName)
	assert.Contains(t, hubRes.Message, "Central Delhi Recovery Hub")

	// C. Ambiguous report triggers clarification guardrail
	ambigBody, _ := json.Marshal(map[string]any{
		"text": "There is a mess of trash dumped on the corner of the road",
	})
	resp, err = http.Post(fixtures.server.URL+"/api/v1/triage", "application/json", bytes.NewReader(ambigBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var ambigRes ai.TriageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ambigRes))
	_ = resp.Body.Close()
	assert.Equal(t, ai.ActionClarificationRequired, ambigRes.ActionTaken)
	assert.Contains(t, ambigRes.Message, "specify what kind of waste materials")
}

func TestAI_DirectToolExecution_Guardrails(t *testing.T) {
	fixtures, cleanup := setupAITestServer(t)
	defer cleanup()

	// 1. Direct tool execution with invalid waste type guardrail
	invalidBody, _ := json.Marshal(ai.ReportIncidentParams{
		Description:        "Chemical uranium leak",
		CategorySuggestion: "URANIUM", // invalid stream
		Severity:           "CRITICAL",
		Latitude:           28.6139,
		Longitude:          77.2090,
	})
	resp, err := http.Post(fixtures.server.URL+"/api/v1/ai/tools/report_waste_incident", "application/json", bytes.NewReader(invalidBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_ = resp.Body.Close()

	// 2. Direct tool execution with coordinates out of range guardrail
	badCoordBody, _ := json.Marshal(ai.FindNearestHubParams{
		Latitude:  120.0, // Invalid latitude > 90
		Longitude: 77.2090,
	})
	resp, err = http.Post(fixtures.server.URL+"/api/v1/ai/tools/find_nearest_hub", "application/json", bytes.NewReader(badCoordBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_ = resp.Body.Close()

	// 3. Unknown tool invocation
	resp, err = http.Post(fixtures.server.URL+"/api/v1/ai/tools/non_existent_tool", "application/json", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	_ = resp.Body.Close()
}
