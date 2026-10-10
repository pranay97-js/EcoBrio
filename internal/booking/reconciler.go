package booking

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Reconciler repairs drift between fast in-memory tokens and authoritative database bookings
type Reconciler struct {
	repo       Repository
	tokenStore TokenStore
}

func NewReconciler(repo Repository, tokenStore TokenStore) *Reconciler {
	return &Reconciler{
		repo:       repo,
		tokenStore: tokenStore,
	}
}

// ReconcileSlot aligns the token store state for a given slot to match PostgreSQL truth
func (r *Reconciler) ReconcileSlot(ctx context.Context, slotID uuid.UUID) error {
	_, booked, _, err := r.repo.GetSlotCapacity(ctx, slotID)
	if err != nil {
		return err
	}

	if err := r.tokenStore.Reconcile(ctx, slotID, booked); err != nil {
		slog.Error("Reconciliation error in token store", "slot_id", slotID, "error", err)
		return err
	}

	slog.Debug("Slot capacity reconciled successfully", "slot_id", slotID, "actual_booked", booked)
	return nil
}

// StartBackgroundLoop launches periodic reconciliation in the background
func (r *Reconciler) StartBackgroundLoop(ctx context.Context, interval time.Duration, slotsProvider func() []uuid.UUID) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				slog.Info("Booking reconciler loop stopped")
				return
			case <-ticker.C:
				for _, slotID := range slotsProvider() {
					_ = r.ReconcileSlot(ctx, slotID)
				}
			}
		}
	}()
}
