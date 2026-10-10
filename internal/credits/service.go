package credits

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ecobrio/internal/ledger"
	"ecobrio/internal/outbox"
	"github.com/google/uuid"
)

type Service struct {
	repo          Repository
	ledgerService *ledger.Service
	outboxRepo    outbox.Repository
}

func NewService(repo Repository, ledgerService *ledger.Service, outboxRepo outbox.Repository) *Service {
	return &Service{
		repo:          repo,
		ledgerService: ledgerService,
		outboxRepo:    outboxRepo,
	}
}

// CreateSubmission handles QR scan and initializes submission in SCANNED state
func (s *Service) CreateSubmission(ctx context.Context, req CreateSubmissionRequest) (*RecyclingSubmission, error) {
	qr := strings.TrimSpace(req.QRIdentity)
	if qr == "" {
		return nil, fmt.Errorf("qr_identity is required")
	}
	if req.CitizenID == uuid.Nil {
		return nil, fmt.Errorf("citizen_id is required")
	}
	if req.HubID == uuid.Nil {
		return nil, fmt.Errorf("hub_id is required")
	}
	if strings.TrimSpace(req.Material) == "" {
		return nil, fmt.Errorf("material is required")
	}
	if req.WeightKg <= 0 {
		return nil, fmt.Errorf("weight_kg must be greater than 0")
	}

	sub := &RecyclingSubmission{
		ID:         uuid.New(),
		QRIdentity: qr,
		CitizenID:  req.CitizenID,
		HubID:      req.HubID,
		Material:   strings.ToUpper(strings.TrimSpace(req.Material)),
		WeightKg:   req.WeightKg,
		State:      StateScanned,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := s.repo.CreateSubmission(ctx, sub); err != nil {
		return nil, err
	}

	// Transactional outbox event
	evt, err := outbox.NewEvent(
		"material.scanned",
		"recycling_submission",
		sub.ID.String(),
		sub.QRIdentity,
		map[string]any{
			"submission_id": sub.ID,
			"citizen_id":    sub.CitizenID,
			"hub_id":        sub.HubID,
			"material":      sub.Material,
			"weight_kg":     sub.WeightKg,
		},
	)
	if err == nil && s.outboxRepo != nil {
		_ = s.outboxRepo.SaveEvent(ctx, evt)
	}

	return sub, nil
}

// InspectSubmission executes the material inspection FSM transition and ledger settlement
func (s *Service) InspectSubmission(ctx context.Context, id uuid.UUID, req InspectSubmissionRequest) (*RecyclingSubmission, error) {
	sub, err := s.repo.GetSubmissionByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Strict state invariant: Inspection only permitted on SCANNED submissions
	if sub.State != StateScanned {
		return nil, fmt.Errorf("%w: cannot inspect submission currently in state %s", ErrInvalidTransition, sub.State)
	}

	if req.InspectorID == uuid.Nil {
		return nil, fmt.Errorf("inspector_id is required")
	}

	// Failed Inspection branch
	if !req.Passed {
		sub.State = StateRejected
		sub.InspectorID = &req.InspectorID
		sub.InspectionNotes = req.Notes
		sub.UpdatedAt = time.Now()

		if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
			return nil, err
		}

		evt, err := outbox.NewEvent(
			"submission.rejected",
			"recycling_submission",
			sub.ID.String(),
			sub.QRIdentity,
			map[string]any{
				"submission_id": sub.ID,
				"inspector_id":  req.InspectorID,
				"notes":         req.Notes,
			},
		)
		if err == nil && s.outboxRepo != nil {
			_ = s.outboxRepo.SaveEvent(ctx, evt)
		}

		return sub, nil
	}

	// Passed Inspection branch: SCANNED -> INSPECTION_PASSED -> ESCROW_LOCKED -> CREDITED
	points := CalculatePoints(sub.Material, sub.WeightKg)
	citizenWalletCode := fmt.Sprintf("CITIZEN_WALLET:%s", sub.CitizenID.String())

	// Settle points via Double-Entry Ledger
	ledgerReq := ledger.PostTransactionRequest{
		IdempotencyKey: fmt.Sprintf("sub-settle-%s", sub.ID.String()),
		Description:    fmt.Sprintf("Reward for recycling %0.2f kg of %s (QR: %s)", sub.WeightKg, sub.Material, sub.QRIdentity),
		Postings: []ledger.PostingDraft{
			{
				AccountCode: "SYSTEM_ISSUANCE_RESERVE",
				Points:      points,
				IsCredit:    false, // Debit system reserve
			},
			{
				AccountCode: citizenWalletCode,
				Points:      points,
				IsCredit:    true, // Credit citizen wallet
			},
		},
	}

	if _, err := s.ledgerService.PostBalancedTransaction(ctx, ledgerReq); err != nil {
		return nil, fmt.Errorf("failed ledger point posting: %w", err)
	}

	// Transactional outbox event
	evt, err := outbox.NewEvent(
		outbox.EventCreditSettled,
		"recycling_submission",
		sub.ID.String(),
		sub.QRIdentity,
		map[string]any{
			"submission_id": sub.ID,
			"citizen_id":    sub.CitizenID,
			"points":        points,
			"material":      sub.Material,
			"weight_kg":     sub.WeightKg,
			"inspector_id":  req.InspectorID,
		},
	)
	if err == nil && s.outboxRepo != nil {
		_ = s.outboxRepo.SaveEvent(ctx, evt)
	}

	sub.State = StateCredited
	sub.PointsAwarded = points
	sub.InspectorID = &req.InspectorID
	sub.InspectionNotes = req.Notes
	sub.UpdatedAt = time.Now()

	if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
		return nil, err
	}

	return sub, nil
}

func (s *Service) GetSubmission(ctx context.Context, id uuid.UUID) (*RecyclingSubmission, error) {
	return s.repo.GetSubmissionByID(ctx, id)
}

func (s *Service) ListSubmissions(ctx context.Context, citizenID uuid.UUID) ([]RecyclingSubmission, error) {
	return s.repo.ListSubmissionsByCitizen(ctx, citizenID)
}

func (s *Service) GetCitizenProfile(ctx context.Context, citizenID uuid.UUID) (*CitizenProfileResponse, error) {
	balance, err := s.ledgerService.GetCitizenBalance(ctx, citizenID)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving citizen balance: %w", err)
	}

	postings, err := s.ledgerService.ListCitizenPostings(ctx, citizenID)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving ledger postings: %w", err)
	}

	return &CitizenProfileResponse{
		CitizenID:     citizenID,
		BalancePoints: balance,
		Postings:      postings,
	}, nil
}

// RedeemPerkSaga coordinates the decoupled redemption saga with compensating reversal
func (s *Service) RedeemPerkSaga(ctx context.Context, req RedeemPerkRequest, idempotencyKey string) (*RedemptionResult, error) {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		return nil, fmt.Errorf("idempotency_key is required")
	}
	if req.CitizenID == uuid.Nil {
		return nil, fmt.Errorf("citizen_id is required")
	}
	if req.PointsCost <= 0 {
		return nil, fmt.Errorf("points_cost must be greater than 0")
	}
	if strings.TrimSpace(req.PerkName) == "" {
		return nil, fmt.Errorf("perk_name is required")
	}

	citizenWallet := fmt.Sprintf("CITIZEN_WALLET:%s", req.CitizenID.String())
	escrowHold := fmt.Sprintf("ESCROW_HOLD:%s", req.CitizenID.String())

	// Step 1: Place Escrow Hold in Ledger
	holdReq := ledger.PostTransactionRequest{
		IdempotencyKey: fmt.Sprintf("perk-hold-%s", key),
		Description:    fmt.Sprintf("Escrow hold for %s perk redemption", req.PerkName),
		Postings: []ledger.PostingDraft{
			{
				AccountCode: citizenWallet,
				Points:      req.PointsCost,
				IsCredit:    false, // Debit citizen wallet (checks balance >= points)
			},
			{
				AccountCode: escrowHold,
				Points:      req.PointsCost,
				IsCredit:    true, // Credit escrow hold account
			},
		},
	}

	if _, err := s.ledgerService.PostBalancedTransaction(ctx, holdReq); err != nil {
		return nil, fmt.Errorf("escrow hold rejected: %w", err)
	}

	// Outbox event: credit.settlement_requested
	evtHold, err := outbox.NewEvent(
		outbox.EventCreditSettlementRequested,
		"perk_redemption",
		key,
		req.CitizenID.String(),
		map[string]any{
			"citizen_id":  req.CitizenID,
			"perk_name":   req.PerkName,
			"points_cost": req.PointsCost,
		},
	)
	if err == nil && s.outboxRepo != nil {
		_ = s.outboxRepo.SaveEvent(ctx, evtHold)
	}

	// Step 2: Downstream Voucher Provider Fulfillment (Simulated)
	if req.SimulateFail {
		// SAGA COMPENSATING ROLLBACK: Reverse Escrow Hold back to Citizen Wallet
		reversalReq := ledger.PostTransactionRequest{
			IdempotencyKey: fmt.Sprintf("perk-reversal-%s", key),
			Description:    fmt.Sprintf("Compensating reversal for failed %s voucher redemption", req.PerkName),
			Postings: []ledger.PostingDraft{
				{
					AccountCode: escrowHold,
					Points:      req.PointsCost,
					IsCredit:    false, // Debit escrow hold
				},
				{
					AccountCode: citizenWallet,
					Points:      req.PointsCost,
					IsCredit:    true, // Credit back citizen wallet
				},
			},
		}

		if _, rErr := s.ledgerService.PostBalancedTransaction(ctx, reversalReq); rErr != nil {
			return nil, fmt.Errorf("critical saga error: failed compensating reversal: %w", rErr)
		}

		// Outbox event: voucher.compensated
		evtComp, err := outbox.NewEvent(
			outbox.EventVoucherCompensated,
			"perk_redemption",
			key,
			req.CitizenID.String(),
			map[string]any{
				"citizen_id":       req.CitizenID,
				"perk_name":        req.PerkName,
				"points_restored":  req.PointsCost,
				"failure_reason":   "downstream voucher provider simulated failure",
			},
		)
		if err == nil && s.outboxRepo != nil {
			_ = s.outboxRepo.SaveEvent(ctx, evtComp)
		}

		return nil, fmt.Errorf("%w: voucher provider unavailable, points held in escrow were fully restored to your citizen wallet", ErrVoucherFailed)
	}

	// Step 3: Success branch -> Settle Escrow Hold into System Sink
	cleanPerkName := strings.ToUpper(strings.ReplaceAll(req.PerkName, " ", ""))
	voucherCode := fmt.Sprintf("ECO-%s-%d", cleanPerkName, time.Now().UnixNano()%1000000)

	settleReq := ledger.PostTransactionRequest{
		IdempotencyKey: fmt.Sprintf("perk-settle-%s", key),
		Description:    fmt.Sprintf("Settle escrow for %s voucher (%s)", req.PerkName, voucherCode),
		Postings: []ledger.PostingDraft{
			{
				AccountCode: escrowHold,
				Points:      req.PointsCost,
				IsCredit:    false, // Debit escrow hold
			},
			{
				AccountCode: "SYSTEM_REDEEMED_SINK",
				Points:      req.PointsCost,
				IsCredit:    true, // Credit system sink
			},
		},
	}

	if _, err := s.ledgerService.PostBalancedTransaction(ctx, settleReq); err != nil {
		return nil, fmt.Errorf("failed settling voucher escrow: %w", err)
	}

	// Outbox event: voucher.redeemed
	evtRedeemed, err := outbox.NewEvent(
		outbox.EventVoucherRedeemed,
		"perk_redemption",
		key,
		req.CitizenID.String(),
		map[string]any{
			"citizen_id":   req.CitizenID,
			"perk_name":    req.PerkName,
			"voucher_code": voucherCode,
			"points_cost":  req.PointsCost,
		},
	)
	if err == nil && s.outboxRepo != nil {
		_ = s.outboxRepo.SaveEvent(ctx, evtRedeemed)
	}

	remainingBal, _ := s.ledgerService.GetCitizenBalance(ctx, req.CitizenID)

	return &RedemptionResult{
		Success:          true,
		VoucherCode:      voucherCode,
		PerkName:         req.PerkName,
		PointsDebited:    req.PointsCost,
		RemainingBalance: remainingBal,
		Message:          "Perk voucher redeemed successfully",
	}, nil
}
