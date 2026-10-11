package credits

import (
	"errors"
	"time"

	"ecobrio/internal/ledger"
	"github.com/google/uuid"
)

var (
	ErrInvalidTransition  = errors.New("illegal submission state transition")
	ErrDuplicateQR        = errors.New("QR code has already been registered")
	ErrSubmissionNotFound = errors.New("recycling submission not found")
	ErrVoucherFailed      = errors.New("downstream voucher provider failed")
)

type SubmissionState string

const (
	StateScanned          SubmissionState = "SCANNED"
	StateInspectionPassed SubmissionState = "INSPECTION_PASSED"
	StateInspectionFailed SubmissionState = "INSPECTION_FAILED"
	StateEscrowLocked     SubmissionState = "ESCROW_LOCKED"
	StateCredited         SubmissionState = "CREDITED"
	StateRejected         SubmissionState = "REJECTED"
)

// MaterialRewardRates per kilogram
var MaterialRewardRates = map[string]int64{
	"EWASTE":    50, // 50 points per kg
	"METALS":    30, // 30 points per kg
	"HAZARDOUS": 25, // 25 points per kg
	"PLASTICS":  15, // 15 points per kg
	"GLASS":     10, // 10 points per kg
	"PAPER":     8,  // 8 points per kg
	"ORGANIC":   5,  // 5 points per kg
}

// RecyclingSubmission represents a material surrender inspection lifecycle
type RecyclingSubmission struct {
	ID              uuid.UUID       `json:"id"`
	QRIdentity      string          `json:"qr_identity"`
	CitizenID       uuid.UUID       `json:"citizen_id"`
	HubID           uuid.UUID       `json:"hub_id"`
	Material        string          `json:"material"`
	WeightKg        float64         `json:"weight_kg"`
	State           SubmissionState `json:"state"`
	InspectorID     *uuid.UUID      `json:"inspector_id,omitempty"`
	InspectionNotes string          `json:"inspection_notes,omitempty"`
	PointsAwarded   int64           `json:"points_awarded"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type CreateSubmissionRequest struct {
	QRIdentity string    `json:"qr_identity"`
	CitizenID  uuid.UUID `json:"citizen_id"`
	HubID      uuid.UUID `json:"hub_id"`
	Material   string    `json:"material"`
	WeightKg   float64   `json:"weight_kg"`
}

type InspectSubmissionRequest struct {
	InspectorID uuid.UUID `json:"inspector_id"`
	Passed      bool      `json:"passed"`
	Notes       string    `json:"notes,omitempty"`
}

type RedeemPerkRequest struct {
	CitizenID    uuid.UUID `json:"citizen_id"`
	PerkName     string    `json:"perk_name"` // e.g. "Transit Pass", "EV Fast Charge"
	PointsCost   int64     `json:"points_cost"`
	SimulateFail bool      `json:"simulate_fail,omitempty"`
}

type RedemptionResult struct {
	Success          bool   `json:"success"`
	VoucherCode      string `json:"voucher_code,omitempty"`
	PerkName         string `json:"perk_name"`
	PointsDebited    int64  `json:"points_debited"`
	RemainingBalance int64  `json:"remaining_balance"`
	Message          string `json:"message"`
}

type CitizenProfileResponse struct {
	CitizenID     uuid.UUID        `json:"citizen_id"`
	BalancePoints int64            `json:"balance_points"`
	Postings      []ledger.Posting `json:"postings"`
}
