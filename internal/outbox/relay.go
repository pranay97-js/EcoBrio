package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"ecobrio/internal/queue"
)

type Relay struct {
	repo     Repository
	producer queue.Producer
}

func NewRelay(repo Repository, producer queue.Producer) *Relay {
	return &Relay{
		repo:     repo,
		producer: producer,
	}
}

// PublishPending pulls pending outbox events and pushes them to the queue
func (r *Relay) PublishPending(ctx context.Context, batchSize int) (int, error) {
	events, err := r.repo.FetchPending(ctx, batchSize)
	if err != nil {
		return 0, err
	}

	publishedCount := 0
	for _, event := range events {
		bytes, err := json.Marshal(event)
		if err != nil {
			slog.Error("Failed to serialize outbox event", "event_id", event.ID, "error", err)
			continue
		}

		if err := r.producer.Publish(ctx, event.EventType, bytes); err != nil {
			slog.Warn("Failed publishing outbox event to queue", "event_id", event.ID, "error", err)
			continue
		}

		if err := r.repo.MarkPublished(ctx, event.ID); err != nil {
			slog.Error("Failed marking outbox event published", "event_id", event.ID, "error", err)
			continue
		}
		publishedCount++
	}

	return publishedCount, nil
}

// StartBackgroundRelay runs a background ticker pulling pending events
func (r *Relay) StartBackgroundRelay(ctx context.Context, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				slog.Info("Outbox relay background loop stopped")
				return
			case <-ticker.C:
				_, _ = r.PublishPending(ctx, batchSize)
			}
		}
	}()
}
