package booking

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// SlotBooking represents an atomic capacity reservation
type SlotBooking struct {
	ID             uuid.UUID `json:"id"`
	SlotID         uuid.UUID `json:"slot_id"`
	CitizenID      uuid.UUID `json:"citizen_id"`
	StreamType     string    `json:"stream_type"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

// Service defines the interface for concurrency-safe kiosk bookings
type Service interface {
	ReserveSlot(ctx context.Context, slotID, citizenID uuid.UUID, streamType, idempotencyKey string) (*SlotBooking, error)
	GetBooking(ctx context.Context, id uuid.UUID) (*SlotBooking, error)
}
