package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"ecobrio/internal/credits"
	"ecobrio/internal/ledger"
	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"ecobrio/internal/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCreditsTestServer() (*httptest.Server, *credits.Service, *outbox.MemoryRepository) {
	mux := http.NewServeMux()
	metrics := observability.NewMetrics()

	ledgerRepo := ledger.NewMemoryRepository()
	ledgerService := ledger.NewService(ledgerRepo)
	outboxRepo := outbox.NewMemoryRepository()
	creditsRepo := credits.NewMemoryRepository()

	creditsService := credits.NewService(creditsRepo, ledgerService, outboxRepo)
	handler := credits.NewHandler(creditsService)
	handler.RegisterRoutes(mux)

	wrapped := middleware.Recovery(middleware.LoggingAndMetrics(metrics, mux))
	return httptest.NewServer(wrapped), creditsService, outboxRepo
}

func TestCreditsAPI_FullLifecycle(t *testing.T) {
	server, _, outboxRepo := setupCreditsTestServer()
	defer server.Close()

	client := server.Client()
	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	// 1. POST /api/v1/submissions -> Create Submission (SCANNED)
	createReq := credits.CreateSubmissionRequest{
		QRIdentity: "QR-E2E-TEST-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "EWASTE",
		WeightKg:   3.0,
	}
	bodyBytes, _ := json.Marshal(createReq)

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/submissions", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var created credits.RecyclingSubmission
	_ = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()

	assert.Equal(t, credits.StateScanned, created.State)
	assert.Equal(t, "QR-E2E-TEST-001", created.QRIdentity)

	// 2. Duplicate QR rejection -> 409 Conflict
	dupReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/submissions", bytes.NewReader(bodyBytes))
	dupReq.Header.Set("Content-Type", "application/json")
	dupResp, err := client.Do(dupReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, dupResp.StatusCode)
	_ = dupResp.Body.Close()

	// 3. GET /api/v1/submissions/{id} -> Retrieve Submission
	getReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/api/v1/submissions/%s", server.URL, created.ID), nil)
	getResp, err := client.Do(getReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var fetched credits.RecyclingSubmission
	_ = json.NewDecoder(getResp.Body).Decode(&fetched)
	_ = getResp.Body.Close()
	assert.Equal(t, created.ID, fetched.ID)

	// 4. GET /api/v1/credits/me -> Check initial balance (0)
	meReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/credits/me", nil)
	meReq.Header.Set("X-Citizen-ID", citizenID.String())
	meResp, err := client.Do(meReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, meResp.StatusCode)
	var initialProfile credits.CitizenProfileResponse
	_ = json.NewDecoder(meResp.Body).Decode(&initialProfile)
	_ = meResp.Body.Close()
	assert.Equal(t, int64(0), initialProfile.BalancePoints)

	// 5. POST /api/v1/submissions/{id}/inspection -> Pass Inspection & Settle 150 points
	inspectReq := credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
		Notes:       "Certified Grade-A Circuitry",
	}
	inspectBytes, _ := json.Marshal(inspectReq)
	inspHTTPReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/submissions/%s/inspection", server.URL, created.ID), bytes.NewReader(inspectBytes))
	inspHTTPReq.Header.Set("Content-Type", "application/json")
	inspResp, err := client.Do(inspHTTPReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, inspResp.StatusCode)

	var inspected credits.RecyclingSubmission
	_ = json.NewDecoder(inspResp.Body).Decode(&inspected)
	_ = inspResp.Body.Close()
	assert.Equal(t, credits.StateCredited, inspected.State)
	assert.Equal(t, int64(150), inspected.PointsAwarded)

	// 6. GET /api/v1/credits/me -> Balance must now reflect 150 points
	meResp2, err := client.Do(meReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, meResp2.StatusCode)
	var creditedProfile credits.CitizenProfileResponse
	_ = json.NewDecoder(meResp2.Body).Decode(&creditedProfile)
	_ = meResp2.Body.Close()
	assert.Equal(t, int64(150), creditedProfile.BalancePoints)
	assert.NotEmpty(t, creditedProfile.Postings)

	// 7. POST /api/v1/credits/redeem -> Missing Idempotency Key -> 400 Bad Request
	redeemBody, _ := json.Marshal(credits.RedeemPerkRequest{
		CitizenID:  citizenID,
		PerkName:   "Subway Pass",
		PointsCost: 50,
	})
	noKeyReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/credits/redeem", bytes.NewReader(redeemBody))
	noKeyReq.Header.Set("Content-Type", "application/json")
	noKeyResp, err := client.Do(noKeyReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, noKeyResp.StatusCode)
	_ = noKeyResp.Body.Close()

	// 8. POST /api/v1/credits/redeem -> Insufficient Balance -> 422 Unprocessable Entity
	overdrawBody, _ := json.Marshal(credits.RedeemPerkRequest{
		CitizenID:  citizenID,
		PerkName:   "Luxury EV Rental",
		PointsCost: 500, // Available: 150
	})
	overdrawReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/credits/redeem", bytes.NewReader(overdrawBody))
	overdrawReq.Header.Set("Content-Type", "application/json")
	overdrawReq.Header.Set("X-Idempotency-Key", "idem-overdraw-test")
	overdrawResp, err := client.Do(overdrawReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, overdrawResp.StatusCode)
	_ = overdrawResp.Body.Close()

	// 9. POST /api/v1/credits/redeem -> Saga Failure with Compensating Reversal -> 502 Bad Gateway
	failBody, _ := json.Marshal(credits.RedeemPerkRequest{
		CitizenID:    citizenID,
		PerkName:     "Faulty Voucher",
		PointsCost:   50,
		SimulateFail: true,
	})
	failReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/credits/redeem", bytes.NewReader(failBody))
	failReq.Header.Set("Content-Type", "application/json")
	failReq.Header.Set("X-Idempotency-Key", "idem-saga-fail-test")
	failResp, err := client.Do(failReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadGateway, failResp.StatusCode)
	_ = failResp.Body.Close()

	// Invariant check: Balance must STILL BE 150 points after saga reversal
	meResp3, err := client.Do(meReq)
	require.NoError(t, err)
	var postFailProfile credits.CitizenProfileResponse
	_ = json.NewDecoder(meResp3.Body).Decode(&postFailProfile)
	_ = meResp3.Body.Close()
	assert.Equal(t, int64(150), postFailProfile.BalancePoints, "Compensating rollback must guarantee zero point leakage")

	// 10. POST /api/v1/credits/redeem -> Successful Redemption -> 200 OK
	successBody, _ := json.Marshal(credits.RedeemPerkRequest{
		CitizenID:    citizenID,
		PerkName:     "Metro Transit Pass",
		PointsCost:   60,
		SimulateFail: false,
	})
	successReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/credits/redeem", bytes.NewReader(successBody))
	successReq.Header.Set("Content-Type", "application/json")
	successReq.Header.Set("X-Idempotency-Key", "idem-saga-success-test")
	successResp, err := client.Do(successReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, successResp.StatusCode)

	var redemptionResult credits.RedemptionResult
	_ = json.NewDecoder(successResp.Body).Decode(&redemptionResult)
	_ = successResp.Body.Close()
	assert.True(t, redemptionResult.Success)
	assert.NotEmpty(t, redemptionResult.VoucherCode)
	assert.Equal(t, int64(60), redemptionResult.PointsDebited)
	assert.Equal(t, int64(90), redemptionResult.RemainingBalance)

	// Final Profile Check: 90 points balance
	meResp4, err := client.Do(meReq)
	require.NoError(t, err)
	var finalProfile credits.CitizenProfileResponse
	_ = json.NewDecoder(meResp4.Body).Decode(&finalProfile)
	_ = meResp4.Body.Close()
	assert.Equal(t, int64(90), finalProfile.BalancePoints)

	// Verify Outbox Events
	pendingEvents, err := outboxRepo.FetchPending(meReq.Context(), 50)
	require.NoError(t, err)
	assert.NotEmpty(t, pendingEvents)
}
