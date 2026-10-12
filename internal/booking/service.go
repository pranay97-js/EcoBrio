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
	repo        Repository
	tokenStore  TokenStore
	pickupStore *MemoryPickupStore
	leaseTTL    time.Duration
}

func NewService(repo Repository, tokenStore TokenStore) *Service {
	return &Service{
		repo:        repo,
		tokenStore:  tokenStore,
		pickupStore: NewMemoryPickupStore(),
		leaseTTL:    30 * time.Second,
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
		// Uncertain commit resolution: check if commit actually went through before compensating
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if existing, checkErr := s.repo.GetBookingByIdempotencyKey(context.Background(), key); checkErr == nil && existing != nil {
				_ = s.tokenStore.ConfirmToken(context.Background(), token.TokenID, req.SlotID)
				return existing, nil
			}
		}

		// Definite failure: release Redis reservation token idempotently
		if relErr := s.tokenStore.ReleaseToken(ctx, token.TokenID, req.SlotID); relErr != nil {
			slog.Error("Failed to release token after DB rejection", "token_id", token.TokenID, "error", relErr)
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

func (s *Service) CreatePickup(ctx context.Context, p *PickupRequest) (*PickupRequest, error) {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	if p.CitizenID == uuid.Nil {
		p.CitizenID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	}
	if p.Status == "" {
		p.Status = "SCHEDULED"
	}
	if p.TokenBounty == 0 {
		// Calculate token bounty based on stream and weight
		rate := int64(20)
		switch strings.ToUpper(p.StreamType) {
		case "EWASTE":
			rate = 45
		case "PLASTICS":
			rate = 25
		case "ORGANIC":
			rate = 15
		case "METALS":
			rate = 35
		case "GLASS":
			rate = 18
		case "PAPER":
			rate = 20
		}
		p.TokenBounty = int64(p.EstimatedWeightKg * float64(rate))
		if p.TokenBounty < 50 {
			p.TokenBounty = 50
		}
	}
	p.CreatedAt = time.Now()
	p.UpdatedAt = time.Now()

	if err := s.pickupStore.CreatePickup(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Service) ListPickups(ctx context.Context) ([]*PickupRequest, error) {
	return s.pickupStore.ListPickups(ctx)
}

func (s *Service) CancelPickup(ctx context.Context, id uuid.UUID) error {
	return s.pickupStore.CancelPickup(ctx, id)
}
