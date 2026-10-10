package booking

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrSlotNotFound        = errors.New("collection slot not found")
	ErrCapacityExceeded    = errors.New("slot capacity exhausted")
	ErrMissingIdempotency  = errors.New("X-Idempotency-Key header is required")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request payload")
	ErrBookingNotFound     = errors.New("booking not found")
)

// Booking represents a confirmed reservation record
type Booking struct {
	ID             uuid.UUID `json:"id"`
	SlotID         uuid.UUID `json:"slot_id"`
	CitizenID      uuid.UUID `json:"citizen_id"`
	StreamType     string    `json:"stream_type"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"` // CONFIRMED, CANCELLED
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Request holds inputs for booking a collection slot
type Request struct {
	SlotID     uuid.UUID `json:"slot_id"`
	CitizenID  uuid.UUID `json:"citizen_id"`
	StreamType string    `json:"stream_type"`
}

// Fingerprint generates a deterministic SHA-256 hash of the request payload
func (r Request) Fingerprint() string {
	h := sha256.New()
	h.Write([]byte(r.SlotID.String() + ":" + r.CitizenID.String() + ":" + r.StreamType))
	return hex.EncodeToString(h.Sum(nil))
}

// ReservationToken represents a Redis-level fast admission lease
type ReservationToken struct {
	TokenID   string    `json:"token_id"`
	SlotID    uuid.UUID `json:"slot_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Confirmed bool      `json:"confirmed"`
}

// IdempotencyRecord tracks in-flight and committed operations
type IdempotencyRecord struct {
	Scope              string    `json:"scope"`
	Key                string    `json:"key"`
	RequestFingerprint string    `json:"request_fingerprint"`
	Status             string    `json:"status"` // PENDING, COMMITTED, FAILED
	ResponseCode       int       `json:"response_code"`
	ResponseBody       []byte    `json:"response_body"`
	CreatedAt          time.Time `json:"created_at"`
}

// Repository defines data access for bookings and idempotency
type Repository interface {
	GetSlotCapacity(ctx context.Context, slotID uuid.UUID) (total, booked int, version int, err error)
	CommitBooking(ctx context.Context, b *Booking, fingerprint string) error
	GetBookingByID(ctx context.Context, id uuid.UUID) (*Booking, error)
	GetBookingByIdempotencyKey(ctx context.Context, key string) (*Booking, error)
	GetIdempotencyRecord(ctx context.Context, scope, key string) (*IdempotencyRecord, error)
	SaveIdempotencyRecord(ctx context.Context, rec *IdempotencyRecord) error
}

// TokenStore defines fast in-memory admission reservation (Redis Lua)
type TokenStore interface {
	AcquireToken(ctx context.Context, slotID uuid.UUID, totalCapacity int, ttl time.Duration) (*ReservationToken, error)
	ReleaseToken(ctx context.Context, tokenID string, slotID uuid.UUID) error
	ConfirmToken(ctx context.Context, tokenID string, slotID uuid.UUID) error
	Reconcile(ctx context.Context, slotID uuid.UUID, actualConfirmedCount int) error
}
