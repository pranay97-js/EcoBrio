package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event represents an outbox message written transactionally with business state
type Event struct {
	ID            uuid.UUID       `json:"id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
}

// Relay handles polling outbox events and pushing to message queues
type Relay interface {
	PublishPending(ctx context.Context, batchSize int) (int, error)
}
