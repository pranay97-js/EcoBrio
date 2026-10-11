package ai_test

import (
	"context"
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
	"ecobrio/internal/websocket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupAITestFixtures(t *testing.T) (*ai.ToolRegistry, *incidents.Service, *hubs.Service, *credits.Service) {
	ctx := context.Background()

	// Hubs service
	hubsRepo := hubs.NewMemoryRepository()
	hubsSvc := hubs.NewService(hubsRepo)
	_ = hubsRepo.CreateHub(ctx, &hubs.Hub{
		ID:        uuid.New(),
		Name:      "Central Delhi Recovery Hub",
		Latitude:  28.6139,
		Longitude: 77.2090,
	})

	// Incidents service with memory geo index and ws hub
	incidentsRepo := incidents.NewMemoryRepository()
	geoIdx := geospatial.NewMemoryIndexer()
	wsHub := websocket.NewHub()
	go wsHub.Run(ctx)
	incidentsSvc := incidents.NewService(incidentsRepo, geoIdx, wsHub)

	// Credits & Ledger service
	ledgerRepo := ledger.NewMemoryRepository()
	ledgerSvc := ledger.NewService(ledgerRepo)
	creditsRepo := credits.NewMemoryRepository()
	outboxRepo := outbox.NewMemoryRepository()
	creditsSvc := credits.NewService(creditsRepo, ledgerSvc, outboxRepo)

	tools := ai.NewToolRegistry(incidentsSvc, hubsSvc, creditsSvc)
	return tools, incidentsSvc, hubsSvc, creditsSvc
}

func TestTools_ReportWasteIncident_ValidationAndGuardrails(t *testing.T) {
	ctx := context.Background()
	tools, _, _, _ := setupAITestFixtures(t)
	citizenID := uuid.New()
	session := ai.SessionContext{CitizenID: citizenID, CitizenRole: "CITIZEN"}

	// 1. Valid reporting
	inc, err := tools.ExecuteReportWasteIncident(ctx, session, ai.ReportIncidentParams{
		Description:        "Discarded lead acid car battery leaking",
		CategorySuggestion: "HAZARDOUS",
		Severity:           "CRITICAL",
		Latitude:           28.6139,
		Longitude:          77.2090,
	})
	require.NoError(t, err)
	assert.Equal(t, &citizenID, inc.ReporterID, "Caller identity guardrail: Reporter must equal session.CitizenID")
	assert.Equal(t, "HAZARDOUS", inc.StreamType)
	assert.Equal(t, incidents.SeverityCritical, inc.Severity)

	// 2. Invalid category guardrail
	_, err = tools.ExecuteReportWasteIncident(ctx, session, ai.ReportIncidentParams{
		Description:        "Alien matter spill",
		CategorySuggestion: "PLUTONIUM",
		Latitude:           28.6139,
		Longitude:          77.2090,
	})
	assert.Error(t, err, "Invalid waste stream must be rejected")

	// 3. Invalid coordinate guardrail
	_, err = tools.ExecuteReportWasteIncident(ctx, session, ai.ReportIncidentParams{
		Description:        "Plastic bottles",
		CategorySuggestion: "PLASTICS",
		Latitude:           150.0, // Invalid latitude
		Longitude:          77.2090,
	})
	assert.Error(t, err, "Latitude > 90 must be rejected")
}

func TestTools_FindNearestHub_CoordinatesGuardrail(t *testing.T) {
	ctx := context.Background()
	tools, _, _, _ := setupAITestFixtures(t)

	// Valid coordinate search
	hubsList, err := tools.ExecuteFindNearestHub(ctx, ai.FindNearestHubParams{
		Latitude:  28.6139,
		Longitude: 77.2090,
		RadiusKm:  10.0,
	})
	require.NoError(t, err)
	assert.Len(t, hubsList, 1)
	assert.Equal(t, "Central Delhi Recovery Hub", hubsList[0].Name)

	// Invalid coordinates out of range
	_, err = tools.ExecuteFindNearestHub(ctx, ai.FindNearestHubParams{
		Latitude:  95.0,
		Longitude: 200.0,
	})
	assert.Error(t, err, "Out-of-range coordinates must be rejected")
}

func TestTools_GetUserCredits_SessionScopeGuardrail(t *testing.T) {
	ctx := context.Background()
	tools, _, _, creditsSvc := setupAITestFixtures(t)
	citizenID := uuid.New()

	// Seed credit balance (10kg GLASS @ 10 pts/kg = 100 points)
	sub, err := creditsSvc.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-AI-TEST-100",
		CitizenID:  citizenID,
		HubID:      uuid.New(),
		Material:   "GLASS",
		WeightKg:   10.0,
	})
	require.NoError(t, err)
	_, err = creditsSvc.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: uuid.New(),
		Passed:      true,
		Notes:       "Passed AI test inspection",
	})
	require.NoError(t, err)

	// Authorized session lookup
	session := ai.SessionContext{CitizenID: citizenID}
	summary, err := tools.ExecuteGetUserCredits(ctx, session)
	require.NoError(t, err)
	assert.Equal(t, int64(100), summary.AccountBalance)

	// Unauthenticated session guardrail
	unauthSession := ai.SessionContext{CitizenID: uuid.Nil}
	_, err = tools.ExecuteGetUserCredits(ctx, unauthSession)
	assert.Error(t, err, "Unauthenticated session must be rejected")
}

func TestRuleBasedEngine_IntentAndClassification(t *testing.T) {
	ctx := context.Background()
	tools, _, _, _ := setupAITestFixtures(t)
	engine := ai.NewRuleBasedEngine(tools)
	session := ai.SessionContext{CitizenID: uuid.New()}

	// 1. Hazardous Battery Report
	res, err := engine.ProcessPrompt(ctx, session, "Found leaking car battery and toxic acid puddle near metro station")
	require.NoError(t, err)
	assert.Equal(t, ai.ActionToolExecuted, res.ActionTaken)
	assert.Equal(t, ai.ToolReportWasteIncident, res.ToolName)
	assert.Equal(t, "HAZARDOUS", res.ExtractedStream)
	assert.Equal(t, incidents.SeverityCritical, res.ExtractedSeverity)
	assert.NotNil(t, res.IncidentID)

	// 2. Plastic accumulation report with coordinates in text
	res, err = engine.ProcessPrompt(ctx, session, "Large pile of plastic bottles and packaging at 28.6200, 77.2100")
	require.NoError(t, err)
	assert.Equal(t, ai.ActionToolExecuted, res.ActionTaken)
	assert.Equal(t, "PLASTICS", res.ExtractedStream)
	assert.NotNil(t, res.IncidentID)

	// 3. Nearest Hub query
	res, err = engine.ProcessPrompt(ctx, session, "Where is the nearest hub to drop off my recycling?")
	require.NoError(t, err)
	assert.Equal(t, ai.ActionToolExecuted, res.ActionTaken)
	assert.Equal(t, ai.ToolFindNearestHub, res.ToolName)

	// 4. Credits query
	res, err = engine.ProcessPrompt(ctx, session, "How many points and tokens do I have in my wallet balance?")
	require.NoError(t, err)
	assert.Equal(t, ai.ActionToolExecuted, res.ActionTaken)
	assert.Equal(t, ai.ToolGetUserCredits, res.ToolName)

	// 5. Ambiguous prompt triggering clarification guardrail
	res, err = engine.ProcessPrompt(ctx, session, "Hey there, there is a big mess outside")
	require.NoError(t, err)
	assert.Equal(t, ai.ActionClarificationRequired, res.ActionTaken)
	assert.Contains(t, res.Message, "specify what kind of waste materials")
}

func TestIntakeService_AsyncPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tools, incidentsSvc, _, _ := setupAITestFixtures(t)
	store := ai.NewMemoryIntakeStore()
	q := queue.NewMemoryQueue()

	intakeSvc := ai.NewIntakeService(store, tools, q)
	intakeSvc.StartBackgroundWorker(ctx, q)

	citizenID := uuid.New()
	session := ai.SessionContext{CitizenID: citizenID}

	// 1. Submit asynchronous intake
	narrative := "Commercial organic food waste dumping in alley behind hotel"
	intake, err := intakeSvc.SubmitAsyncIntake(ctx, session, narrative, nil)
	require.NoError(t, err)
	assert.Equal(t, ai.IntakeStatusReceived, intake.Status)
	assert.Equal(t, "ORGANIC", intake.ExtractedStream)

	// 2. Poll until async background worker completes processing (RECEIVED -> PROCESSING -> INCIDENT_CREATED)
	require.Eventually(t, func() bool {
		item, err := intakeSvc.GetIntake(ctx, intake.ID)
		if err != nil {
			return false
		}
		return item.Status == ai.IntakeStatusIncidentCreated && item.IncidentID != nil
	}, 2*time.Second, 20*time.Millisecond, "Intake must transition to INCIDENT_CREATED through async flow")

	// 3. Verify created incident in domain service
	finalIntake, err := intakeSvc.GetIntake(ctx, intake.ID)
	require.NoError(t, err)
	require.NotNil(t, finalIntake.IncidentID)

	inc, err := incidentsSvc.GetIncident(ctx, *finalIntake.IncidentID)
	require.NoError(t, err)
	assert.Equal(t, "ORGANIC", inc.StreamType)
	assert.Equal(t, &citizenID, inc.ReporterID)

	// 4. Test duplicate redelivery: processing the same intake again must be a no-op
	firstIncidentID := *finalIntake.IncidentID
	err = intakeSvc.ProcessIntakeItem(ctx, intake.ID)
	require.NoError(t, err)

	dupIntake, err := intakeSvc.GetIntake(ctx, intake.ID)
	require.NoError(t, err)
	assert.Equal(t, firstIncidentID, *dupIntake.IncidentID, "Incident ID must remain unchanged upon duplicate processing")
}
