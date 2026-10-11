package queue_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ecobrio/internal/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryQueue_PublishAndConsume(t *testing.T) {
	q := queue.NewMemoryQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "test-topic"
	var receivedCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		_ = q.Consume(ctx, topic, func(cCtx context.Context, msg []byte) error {
			receivedCount.Add(1)
			wg.Done()
			return nil
		})
	}()

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		err := q.Publish(ctx, topic, []byte("msg"))
		require.NoError(t, err)
	}

	// Wait for consumer to receive all 5 messages
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, int32(5), receivedCount.Load())
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for queue consumer messages")
	}
}

func TestMemoryQueue_ContextCancellation(t *testing.T) {
	q := queue.NewMemoryQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	topic := "cancel-topic"
	done := make(chan struct{})

	go func() {
		_ = q.Consume(ctx, topic, func(cCtx context.Context, msg []byte) error {
			return nil
		})
		close(done)
	}()

	select {
	case <-done:
		// Succeeded in exiting upon context timeout
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Consume did not exit upon context cancellation")
	}
}
