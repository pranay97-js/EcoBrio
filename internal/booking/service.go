package booking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"ecobrio/internal/hubs"
	"github.com/google/uuid"
)

type Service struct {
	repo       Repository
	tokenStore TokenStore
	leaseTTL   time.Duration
}

func NewService(repo Repository, tokenStore TokenStore) *Service {
	return &Service{
		repo:       repo,
		tokenStore: tokenStore,
		leaseTTL:   30 * time.Second,
	}
}

// ReserveSlot executes the two-layer concurrency-safe booking protocol with idempotency
func (s *Service) ReserveSlot(ctx context.Context, req Request, idempotencyKey string) (*Booking, error) {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		return nil, ErrMissingIdempotency
	}
	if req.SlotID == uuid.Nil {
		return nil, errors.New("slot_id is required")
	}
	if req.CitizenID == uuid.Nil {
		return nil, errors.New("citizen_id is required")
	}
	stream := strings.ToUpper(strings.TrimSpace(req.StreamType))
	if !hubs.ValidWasteStreams[stream] {
		return nil, fmt.Errorf("invalid waste stream '%s'", req.StreamType)
	}
	req.StreamType = stream

	fingerprint := req.Fingerprint()

	// 1. Idempotency Gate: Check if this idempotency key was previously processed
	existingRecord, err := s.repo.GetIdempotencyRecord(ctx, "BOOKING", key)
	if err != nil {
		slog.Warn("Idempotency lookup error", "key", key, "error", err)
	}
	if existingRecord != nil {
		if existingRecord.RequestFingerprint != fingerprint {
			// Idempotency violation: same key used for a different request payload
			return nil, ErrIdempotencyConflict
		}
		if existingRecord.Status == "COMMITTED" {
			var cached Booking
			if err := json.Unmarshal(existingRecord.ResponseBody, &cached); err == nil {
				slog.Info("Idempotent booking hit served from store", "idempotency_key", key, "booking_id", cached.ID)
				return &cached, nil
			}
		}
	}

	// 2. Fetch authoritative capacity limits for Layer 1
	totalCapacity, _, _, err := s.repo.GetSlotCapacity(ctx, req.SlotID)
	if err != nil {
		return nil, err
	}

	// 3. Layer 1 (Redis Lua / Token Admission): Fast capacity reservation
	token, err := s.tokenStore.AcquireToken(ctx, req.SlotID, totalCapacity, s.leaseTTL)
	if err != nil {
		return nil, err // ErrCapacityExceeded deflected before DB write
	}

	// 4. Layer 2 (Postgres Relational Commit): Atomic decrement and booking insertion
	booking := &Booking{
		ID:             uuid.New(),
		SlotID:         req.SlotID,
		CitizenID:      req.CitizenID,
		StreamType:     req.StreamType,
		IdempotencyKey: key,
		Status:         "CONFIRMED",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	err = s.repo.CommitBooking(ctx, booking, fingerprint)
	if err != nil {
		// Failure compensation: If relational commit fails, release Redis reservation token idempotently
		if relErr := s.tokenStore.ReleaseToken(ctx, token.TokenID, req.SlotID); relErr != nil {
			slog.Error("Failed to release token after DB rejection", "token_id", token.TokenID, "error", relErr)
		}

		// Uncertain commit resolution: check if commit actually went through before reporting failure
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if existing, checkErr := s.repo.GetBookingByIdempotencyKey(context.Background(), key); checkErr == nil {
				_ = s.tokenStore.ConfirmToken(context.Background(), token.TokenID, req.SlotID)
				return existing, nil
			}
		}
		return nil, err
	}

	// 5. Success confirmation
	if confErr := s.tokenStore.ConfirmToken(ctx, token.TokenID, req.SlotID); confErr != nil {
		slog.Warn("Failed to mark token confirmed in token store", "token_id", token.TokenID, "error", confErr)
	}

	return booking, nil
}

func (s *Service) GetBooking(ctx context.Context, id uuid.UUID) (*Booking, error) {
	return s.repo.GetBookingByID(ctx, id)
}

func (s *Service) GetBookingByRequestKey(ctx context.Context, key string) (*Booking, error) {
	return s.repo.GetBookingByIdempotencyKey(ctx, key)
}
