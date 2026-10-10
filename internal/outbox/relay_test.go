package outbox_test

import (
	"context"
	"testing"

	"ecobrio/internal/outbox"
	"ecobrio/internal/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelay_PublishPending(t *testing.T) {
	ctx := context.Background()
	repo := outbox.NewMemoryRepository()
	q := queue.NewMemoryQueue()
	relay := outbox.NewRelay(repo, q)

	// 1. Create and save 3 outbox events
	for i := 0; i < 3; i++ {
		evt, err := outbox.NewEvent(
			outbox.EventCreditSettled,
			"test_aggregate",
			uuid.New().String(),
			"corr-001",
			map[string]any{"index": i},
		)
		require.NoError(t, err)
		err = repo.SaveEvent(ctx, evt)
		require.NoError(t, err)
	}

	// Verify 3 pending events
	pending, err := repo.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 3)

	// 2. Relay publishes pending events
	published, err := relay.PublishPending(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 3, published)

	// 3. Verify no pending events remain
	pendingAfter, err := repo.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pendingAfter, 0)

	// Second publish call publishes 0 events
	publishedSecond, err := relay.PublishPending(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, publishedSecond)
}

func TestConsumer_Idempotency(t *testing.T) {
	ctx := context.Background()
	repo := outbox.NewMemoryRepository()

	eventID := uuid.New()
	consumer := "loyalty-sync-consumer"

	// Initially not processed
	processed, err := repo.IsProcessed(ctx, consumer, eventID)
	require.NoError(t, err)
	assert.False(t, processed)

	// Mark as processed
	err = repo.MarkProcessed(ctx, consumer, eventID)
	require.NoError(t, err)

	// Check again
	processed, err = repo.IsProcessed(ctx, consumer, eventID)
	require.NoError(t, err)
	assert.True(t, processed)
}
