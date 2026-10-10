package credits

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// SubmissionState represents the FSM state of material surrender
type SubmissionState string

const (
	StateScanned          SubmissionState = "SCANNED"
	StateInspectionPassed SubmissionState = "INSPECTION_PASSED"
	StateInspectionFailed SubmissionState = "INSPECTION_FAILED"
	StateEscrowLocked     SubmissionState = "ESCROW_LOCKED"
	StateCredited         SubmissionState = "CREDITED"
	StateRejected         SubmissionState = "REJECTED"
)

// RecyclingSubmission represents a material surrender inspection lifecycle
type RecyclingSubmission struct {
	ID          uuid.UUID       `json:"id"`
	QRIdentity  string          `json:"qr_identity"`
	CitizenID   uuid.UUID       `json:"citizen_id"`
	HubID       uuid.UUID       `json:"hub_id"`
	Material    string          `json:"material"`
	WeightKg    float64         `json:"weight_kg"`
	State       SubmissionState `json:"state"`
	InspectorID *uuid.UUID      `json:"inspector_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// Service defines the clearinghouse operations
type Service interface {
	ScanQR(ctx context.Context, qrIdentity string, citizenID, hubID uuid.UUID, material string, weightKg float64) (*RecyclingSubmission, error)
	Inspect(ctx context.Context, submissionID, inspectorID uuid.UUID, passed bool, notes string) (*RecyclingSubmission, error)
}
