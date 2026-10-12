package booking

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestPickupLifecycle(t *testing.T) {
	repo := NewMemoryRepository()
	tokenStore := NewMemoryTokenStore()
	svc := NewService(repo, tokenStore)
	ctx := context.Background()

	// Initial pre-seeded pickups should be returned
	initialList, err := svc.ListPickups(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing pickups: %v", err)
	}
	if len(initialList) < 2 {
		t.Fatalf("expected at least 2 seeded pickups, got %d", len(initialList))
	}

	// Create a new pickup request
	req := PickupRequest{
		CitizenID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		CitizenName:       "Test Citizen",
		CitizenPhone:      "+1 555 123456",
		Address:           "123 Green Eco Way",
		Latitude:          51.5074,
		Longitude:         -0.1278,
		StreamType:        "EWASTE",
		EstimatedWeightKg: 10.0,
		PickupDate:        "Today",
		PickupTimeSlot:    "14:00 - 14:30",
		Notes:             "Packed safely in box",
	}

	created, err := svc.CreatePickup(ctx, &req)
	if err != nil {
		t.Fatalf("unexpected error creating pickup: %v", err)
	}
	if created.ID == uuid.Nil {
		t.Fatal("expected non-nil pickup ID")
	}
	if created.Status != "SCHEDULED" {
		t.Fatalf("expected status SCHEDULED, got %s", created.Status)
	}
	// EWASTE rate is 45 ET/kg -> 10 * 45 = 450
	if created.TokenBounty != 450 {
		t.Fatalf("expected 450 token bounty for 10kg EWASTE, got %d", created.TokenBounty)
	}

	// Verify listing includes new pickup
	updatedList, err := svc.ListPickups(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing updated pickups: %v", err)
	}
	if len(updatedList) != len(initialList)+1 {
		t.Fatalf("expected %d pickups, got %d", len(initialList)+1, len(updatedList))
	}

	// Cancel the created pickup
	if err := svc.CancelPickup(ctx, created.ID); err != nil {
		t.Fatalf("unexpected error cancelling pickup: %v", err)
	}

	// Verify status is CANCELLED
	cancelledList, err := svc.ListPickups(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing pickups after cancellation: %v", err)
	}
	found := false
	for _, p := range cancelledList {
		if p.ID == created.ID {
			found = true
			if p.Status != "CANCELLED" {
				t.Fatalf("expected status CANCELLED, got %s", p.Status)
			}
		}
	}
	if !found {
		t.Fatal("expected to find cancelled pickup in list")
	}
}
