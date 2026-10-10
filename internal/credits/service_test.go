package credits_test

import (
	"context"
	"errors"
	"testing"

	"ecobrio/internal/credits"
	"ecobrio/internal/ledger"
	"ecobrio/internal/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCreditsTest() (*credits.Service, *credits.MemoryRepository, *ledger.Service, *outbox.MemoryRepository) {
	creditsRepo := credits.NewMemoryRepository()
	ledgerRepo := ledger.NewMemoryRepository()
	ledgerService := ledger.NewService(ledgerRepo)
	outboxRepo := outbox.NewMemoryRepository()

	creditsService := credits.NewService(creditsRepo, ledgerService, outboxRepo)
	return creditsService, creditsRepo, ledgerService, outboxRepo
}

func TestCreateSubmission_Success(t *testing.T) {
	service, _, _, outboxRepo := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()

	req := credits.CreateSubmissionRequest{
		QRIdentity: "QR-TEST-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "EWASTE",
		WeightKg:   2.5,
	}

	sub, err := service.CreateSubmission(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, sub)
	assert.Equal(t, credits.StateScanned, sub.State)
	assert.Equal(t, "EWASTE", sub.Material)
	assert.Equal(t, 2.5, sub.WeightKg)

	// Verify outbox event emitted
	events, err := outboxRepo.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "material.scanned", events[0].EventType)
}

func TestCreateSubmission_DuplicateQR(t *testing.T) {
	service, _, _, _ := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()

	req := credits.CreateSubmissionRequest{
		QRIdentity: "QR-DUPLICATE-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "PLASTICS",
		WeightKg:   3.0,
	}

	_, err := service.CreateSubmission(ctx, req)
	require.NoError(t, err)

	// Second attempt with same QR
	_, err = service.CreateSubmission(ctx, req)
	require.Error(t, err)
	assert.True(t, errors.Is(err, credits.ErrDuplicateQR))
}

func TestInspectSubmission_Passed(t *testing.T) {
	service, _, ledgerService, outboxRepo := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	// 1. Create submission: 2.0 kg EWASTE (50 pts/kg = 100 points)
	sub, err := service.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-INSPECT-PASS-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "EWASTE",
		WeightKg:   2.0,
	})
	require.NoError(t, err)

	// 2. Inspect with Pass
	inspectReq := credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
		Notes:       "High grade circuit boards verified",
	}

	inspected, err := service.InspectSubmission(ctx, sub.ID, inspectReq)
	require.NoError(t, err)
	assert.Equal(t, credits.StateCredited, inspected.State)
	assert.Equal(t, int64(100), inspected.PointsAwarded)
	assert.Equal(t, inspectorID, *inspected.InspectorID)

	// 3. Verify ledger balance credited
	bal, err := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(100), bal)

	// 4. Verify outbox event credit.settled
	events, err := outboxRepo.FetchPending(ctx, 10)
	require.NoError(t, err)
	var foundSettled bool
	for _, e := range events {
		if e.EventType == outbox.EventCreditSettled {
			foundSettled = true
			break
		}
	}
	assert.True(t, foundSettled, "credit.settled event must be published in outbox")
}

func TestInspectSubmission_Rejected(t *testing.T) {
	service, _, ledgerService, outboxRepo := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	sub, err := service.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-INSPECT-FAIL-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "PLASTICS",
		WeightKg:   4.0,
	})
	require.NoError(t, err)

	// Inspect with Fail
	inspectReq := credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      false,
		Notes:       "Contaminated material, cannot process",
	}

	inspected, err := service.InspectSubmission(ctx, sub.ID, inspectReq)
	require.NoError(t, err)
	assert.Equal(t, credits.StateRejected, inspected.State)
	assert.Equal(t, int64(0), inspected.PointsAwarded)

	// Ledger balance remains 0
	bal, err := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), bal)

	// Outbox event submission.rejected
	events, err := outboxRepo.FetchPending(ctx, 10)
	require.NoError(t, err)
	var foundRejected bool
	for _, e := range events {
		if e.EventType == "submission.rejected" {
			foundRejected = true
			break
		}
	}
	assert.True(t, foundRejected)
}

func TestInspectSubmission_IllegalTransition(t *testing.T) {
	service, _, _, _ := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	sub, err := service.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-ILLEGAL-TRANSITION",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "METALS",
		WeightKg:   1.0,
	})
	require.NoError(t, err)

	// Inspect once -> CREDITED
	_, err = service.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
	})
	require.NoError(t, err)

	// Inspecting a second time must be rejected by FSM
	_, err = service.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, credits.ErrInvalidTransition))
}

func TestRedeemPerkSaga_Success(t *testing.T) {
	service, _, ledgerService, outboxRepo := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	// 1. Credit 200 points (4.0 kg EWASTE = 200 pts)
	sub, err := service.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-REDEEM-SAGA-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "EWASTE",
		WeightKg:   4.0,
	})
	require.NoError(t, err)
	_, err = service.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
	})
	require.NoError(t, err)

	bal, _ := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.Equal(t, int64(200), bal)

	// 2. Execute Saga: Redeem 80 points
	res, err := service.RedeemPerkSaga(ctx, credits.RedeemPerkRequest{
		CitizenID:    citizenID,
		PerkName:     "Transit Pass",
		PointsCost:   80,
		SimulateFail: false,
	}, "idemp-perk-saga-001")

	require.NoError(t, err)
	assert.True(t, res.Success)
	assert.NotEmpty(t, res.VoucherCode)
	assert.Equal(t, int64(80), res.PointsDebited)
	assert.Equal(t, int64(120), res.RemainingBalance)

	// Check ledger balance directly
	currentBal, err := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(120), currentBal)

	// Verify outbox events: settlement requested and voucher redeemed
	events, err := outboxRepo.FetchPending(ctx, 20)
	require.NoError(t, err)
	var hasRequested, hasRedeemed bool
	for _, e := range events {
		if e.EventType == outbox.EventCreditSettlementRequested {
			hasRequested = true
		}
		if e.EventType == outbox.EventVoucherRedeemed {
			hasRedeemed = true
		}
	}
	assert.True(t, hasRequested)
	assert.True(t, hasRedeemed)
}

func TestRedeemPerkSaga_CompensatingReversalOnFailure(t *testing.T) {
	service, _, ledgerService, outboxRepo := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()
	hubID := uuid.New()
	inspectorID := uuid.New()

	// 1. Credit 100 points (2.0 kg EWASTE)
	sub, err := service.CreateSubmission(ctx, credits.CreateSubmissionRequest{
		QRIdentity: "QR-SAGA-REVERSAL-001",
		CitizenID:  citizenID,
		HubID:      hubID,
		Material:   "EWASTE",
		WeightKg:   2.0,
	})
	require.NoError(t, err)
	_, err = service.InspectSubmission(ctx, sub.ID, credits.InspectSubmissionRequest{
		InspectorID: inspectorID,
		Passed:      true,
	})
	require.NoError(t, err)

	initialBal, _ := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.Equal(t, int64(100), initialBal)

	// 2. Execute Saga with simulated downstream failure
	_, err = service.RedeemPerkSaga(ctx, credits.RedeemPerkRequest{
		CitizenID:    citizenID,
		PerkName:     "EV Fast Charge",
		PointsCost:   50,
		SimulateFail: true, // Downstream provider failure trigger
	}, "idemp-perk-fail-001")

	require.Error(t, err)
	assert.True(t, errors.Is(err, credits.ErrVoucherFailed))

	// 3. CRUCIAL SAGA INVARIANT: Balance must be 100% restored to citizen wallet!
	restoredBal, err := ledgerService.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(100), restoredBal, "Saga compensating rollback must restore citizen balance")

	// 4. Verify outbox compensation event
	events, err := outboxRepo.FetchPending(ctx, 20)
	require.NoError(t, err)
	var hasCompensated bool
	for _, e := range events {
		if e.EventType == outbox.EventVoucherCompensated {
			hasCompensated = true
		}
	}
	assert.True(t, hasCompensated, "voucher.compensated event must be emitted on saga rollback")
}

func TestRedeemPerkSaga_InsufficientBalance(t *testing.T) {
	service, _, _, _ := setupCreditsTest()
	ctx := context.Background()

	citizenID := uuid.New()

	// Citizen balance is 0, attempting to redeem 50 points
	_, err := service.RedeemPerkSaga(ctx, credits.RedeemPerkRequest{
		CitizenID:    citizenID,
		PerkName:     "Coffee Voucher",
		PointsCost:   50,
		SimulateFail: false,
	}, "idemp-perk-insufficient")

	require.Error(t, err)
	assert.True(t, errors.Is(err, ledger.ErrInsufficientBalance))
}
