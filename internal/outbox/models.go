package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Standard Event Types
const (
	EventCreditSettlementRequested = "credit.settlement_requested"
	EventCreditSettled             = "credit.settled"
	EventVoucherRedeemed           = "voucher.redeemed"
	EventVoucherCompensated        = "voucher.compensated"
	EventBookingConfirmed          = "booking.confirmed"
	EventIncidentAccepted          = "incident.accepted"
)

// Event represents a versioned event envelope committed transactionally with business state
type Event struct {
	ID            uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	Attempts      int             `json:"attempts"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
}

// NewEvent creates an initialized event envelope
func NewEvent(eventType, aggregateType, aggregateID, correlationID string, payload any) (*Event, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Event{
		ID:            uuid.New(),
		EventType:     eventType,
		SchemaVersion: 1,
		OccurredAt:    time.Now(),
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		Payload:       bytes,
		Attempts:      0,
	}, nil
}
