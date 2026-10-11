package booking

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestBookingConcurrencyRace verifies Blueprint Invariant B1:
// 200 concurrent goroutines racing against 5 slots results in EXACTLY 5 confirmed bookings,
// 0 over-allocations, and 195 deflected capacity conflicts.
func TestBookingConcurrencyRace(t *testing.T) {
	repo := NewMemoryRepository()
	tokenStore := NewMemoryTokenStore()
	service := NewService(repo, tokenStore)

	slotID := uuid.New()
	const totalCapacity = 5
	const concurrentUsers = 200

	repo.SetSlot(slotID, totalCapacity, 0)

	var wg sync.WaitGroup
	startGate := make(chan struct{})

	var successCount int64
	var conflictCount int64
	var errorCount int64

	for i := 0; i < concurrentUsers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startGate // Synchronized simultaneous release

			citizenID := uuid.New()
			idempotencyKey := uuid.New().String()

			req := Request{
				SlotID:     slotID,
				CitizenID:  citizenID,
				StreamType: "EWASTE",
			}

			_, err := service.ReserveSlot(context.Background(), req, idempotencyKey)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else if errors.Is(err, ErrCapacityExceeded) {
				atomic.AddInt64(&conflictCount, 1)
			} else {
				atomic.AddInt64(&errorCount, 1)
			}
		}(i)
	}

	// Trigger the race
	close(startGate)
	wg.Wait()

	// Assert Invariants
	if successCount != totalCapacity {
		t.Fatalf("VIOLATION OF CAPACITY INVARIANT: expected exactly %d successful bookings, got %d", totalCapacity, successCount)
	}
	if conflictCount != (concurrentUsers - totalCapacity) {
		t.Errorf("expected %d deflected conflicts, got %d", concurrentUsers-totalCapacity, conflictCount)
	}
	if errorCount != 0 {
		t.Errorf("expected 0 unexpected errors, got %d", errorCount)
	}

	// Check final database booked count
	_, finalBooked, _, err := repo.GetSlotCapacity(context.Background(), slotID)
	if err != nil {
		t.Fatalf("failed checking final slot capacity: %v", err)
	}
	if finalBooked != totalCapacity {
		t.Fatalf("OVERSELLING DETECTED: slot booked_count is %d, max capacity is %d", finalBooked, totalCapacity)
	}
}

// TestBookingIdempotencyReplay verifies Blueprint Invariant B2:
// Replaying identical request with the same idempotency key returns the original result
// without double-booking capacity.
func TestBookingIdempotencyReplay(t *testing.T) {
	repo := NewMemoryRepository()
	tokenStore := NewMemoryTokenStore()
	service := NewService(repo, tokenStore)

	slotID := uuid.New()
	repo.SetSlot(slotID, 10, 0)

	citizenID := uuid.New()
	idemKey := "stable-citizen-request-key-99"

	req := Request{
		SlotID:     slotID,
		CitizenID:  citizenID,
		StreamType: "EWASTE",
	}

	// First execution
	b1, err := service.ReserveSlot(context.Background(), req, idemKey)
	if err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}

	// Second execution with identical idempotency key
	b2, err := service.ReserveSlot(context.Background(), req, idemKey)
	if err != nil {
		t.Fatalf("second reservation replay failed: %v", err)
	}

	if b1.ID != b2.ID {
		t.Errorf("expected identical booking ID on replay, got %s vs %s", b1.ID, b2.ID)
	}

	// Check that capacity was only decremented once
	_, booked, _, _ := repo.GetSlotCapacity(context.Background(), slotID)
	if booked != 1 {
		t.Errorf("expected booked_count to remain 1 after idempotency replay, got %d", booked)
	}
}

// TestBookingIdempotencyConflict verifies Blueprint Invariant:
// Using the same idempotency key with a materially different payload must be rejected.
func TestBookingIdempotencyConflict(t *testing.T) {
	repo := NewMemoryRepository()
	tokenStore := NewMemoryTokenStore()
	service := NewService(repo, tokenStore)

	slotID := uuid.New()
	repo.SetSlot(slotID, 10, 0)

	citizenA := uuid.New()
	citizenB := uuid.New()
	idemKey := "shared-key-123"

	reqA := Request{SlotID: slotID, CitizenID: citizenA, StreamType: "EWASTE"}
	reqB := Request{SlotID: slotID, CitizenID: citizenB, StreamType: "EWASTE"}

	// First call succeeds
	_, err := service.ReserveSlot(context.Background(), reqA, idemKey)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Second call with different payload must fail with ErrIdempotencyConflict
	_, err = service.ReserveSlot(context.Background(), reqB, idemKey)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("expected ErrIdempotencyConflict when reusing key with different payload, got %v", err)
	}
}

// TestReconciliationWorker verifies Blueprint Invariant B5:
// Reconciler repairs token drift without modifying authoritative database truth.
func TestReconciliationWorker(t *testing.T) {
	repo := NewMemoryRepository()
	tokenStore := NewMemoryTokenStore()
	service := NewService(repo, tokenStore)
	reconciler := NewReconciler(repo, tokenStore)

	slotID := uuid.New()
	repo.SetSlot(slotID, 5, 0)

	// Create 2 legitimate bookings
	for i := 0; i < 2; i++ {
		_, err := service.ReserveSlot(context.Background(), Request{
			SlotID:     slotID,
			CitizenID:  uuid.New(),
			StreamType: "EWASTE",
		}, uuid.New().String())
		if err != nil {
			t.Fatalf("booking %d failed: %v", i, err)
		}
	}

	// Simulate an unconfirmed orphaned token injected into token store
	orphanToken, _ := tokenStore.AcquireToken(context.Background(), slotID, 5, 10*time.Minute)
	if orphanToken == nil {
		t.Fatal("failed creating orphan token")
	}

	// Run ReconcileSlot
	err := reconciler.ReconcileSlot(context.Background(), slotID)
	if err != nil {
		t.Fatalf("reconcile slot failed: %v", err)
	}

	// Database truth must remain untouched (2 booked)
	_, booked, _, _ := repo.GetSlotCapacity(context.Background(), slotID)
	if booked != 2 {
		t.Errorf("database truth altered during reconciliation, expected 2 got %d", booked)
	}

	// Verify token store active tokens were purged of orphan:
	// Total capacity is 5, 2 confirmed. Exactly 3 additional tokens should be acquirable.
	for i := 0; i < 3; i++ {
		tkn, aErr := tokenStore.AcquireToken(context.Background(), slotID, 5, 30*time.Second)
		if aErr != nil {
			t.Fatalf("expected to acquire remaining capacity %d/3, got error: %v", i+1, aErr)
		}
		if tkn == nil {
			t.Fatal("expected non-nil token")
		}
	}
	// 6th token must be rejected
	_, overflowErr := tokenStore.AcquireToken(context.Background(), slotID, 5, 30*time.Second)
	if !errors.Is(overflowErr, ErrCapacityExceeded) {
		t.Errorf("expected ErrCapacityExceeded after capacity full, got %v", overflowErr)
	}
}
