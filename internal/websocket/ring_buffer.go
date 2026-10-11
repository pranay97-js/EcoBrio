package websocket

import (
	"sync"
)

// RingBuffer stores a bounded window of recent sequenced events for gap detection and reconnect resync
type RingBuffer struct {
	mu       sync.RWMutex
	capacity int
	events   []*Event
}

// NewRingBuffer initializes an event ring buffer with the given capacity
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 500
	}
	return &RingBuffer{
		capacity: capacity,
		events:   make([]*Event, 0, capacity),
	}
}

// Add appends an event to the buffer, evicting the oldest event if capacity is reached
func (r *RingBuffer) Add(e *Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.events) >= r.capacity {
		// Drop oldest event (FIFO eviction)
		copy(r.events, r.events[1:])
		r.events[len(r.events)-1] = e
	} else {
		r.events = append(r.events, e)
	}
}

// EventsSince retrieves all events with Sequence > lastSeq
// Returns (events, true) if all missing events are present in the buffer
// Returns (nil, false) if lastSeq is too old (a gap occurred requiring full snapshot resync)
func (r *RingBuffer) EventsSince(lastSeq uint64) ([]*Event, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.events) == 0 {
		return []*Event{}, true
	}

	oldestSeq := r.events[0].Sequence
	latestSeq := r.events[len(r.events)-1].Sequence

	if lastSeq >= latestSeq {
		return []*Event{}, true // Up to date
	}

	// If client's sequence is older than our oldest preserved event, gap cannot be filled
	if lastSeq < oldestSeq {
		return nil, false
	}

	// Slice matching events
	var result []*Event
	for _, e := range r.events {
		if e.Sequence > lastSeq {
			result = append(result, e)
		}
	}

	return result, true
}

// OldestSequence returns the lowest sequence preserved in the buffer, or 0 if empty
func (r *RingBuffer) OldestSequence() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.events) == 0 {
		return 0
	}
	return r.events[0].Sequence
}

// LatestSequence returns the highest sequence preserved in the buffer, or 0 if empty
func (r *RingBuffer) LatestSequence() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.events) == 0 {
		return 0
	}
	return r.events[len(r.events)-1].Sequence
}

// Size returns the count of buffered events
func (r *RingBuffer) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.events)
}
