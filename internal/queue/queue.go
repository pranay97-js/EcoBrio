package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Producer sends messages to a topic/queue
type Producer interface {
	Publish(ctx context.Context, topic string, msg []byte) error
}

// Consumer processes messages from a topic/queue
type Consumer interface {
	Consume(ctx context.Context, topic string, handler func(ctx context.Context, msg []byte) error) error
}

// MemoryQueue provides a concurrent in-memory message channel queue
type MemoryQueue struct {
	mu     sync.RWMutex
	topics map[string]chan []byte
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		topics: make(map[string]chan []byte),
	}
}

func (q *MemoryQueue) getOrCreateChannel(topic string) chan []byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	ch, exists := q.topics[topic]
	if !exists {
		ch = make(chan []byte, 1024)
		q.topics[topic] = ch
	}
	return ch
}

func (q *MemoryQueue) Publish(ctx context.Context, topic string, msg []byte) error {
	ch := q.getOrCreateChannel(topic)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- msg:
		return nil
	default:
		return fmt.Errorf("queue channel buffer full for topic %s", topic)
	}
}

func (q *MemoryQueue) Consume(ctx context.Context, topic string, handler func(ctx context.Context, msg []byte) error) error {
	ch := q.getOrCreateChannel(topic)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := handler(ctx, msg); err != nil {
				slog.Error("Queue consumer message handler failed", "topic", topic, "error", err)
			}
		}
	}
}
