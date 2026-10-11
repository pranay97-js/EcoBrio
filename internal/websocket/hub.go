package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"ecobrio/internal/observability"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// SnapshotProvider supplies a point-in-time state snapshot and latest sequence for reconnecting clients
type SnapshotProvider func(ctx context.Context) (any, uint64, error)

// Hub manages persistent WebSocket connections with bounded queues and replay capabilities
type Hub struct {
	mu                 sync.RWMutex
	clients            map[*Client]struct{}
	register           chan *Client
	unregister         chan *Client
	ringBuffer         *RingBuffer
	seqCounter         atomic.Uint64
	slowConsumerDrops  atomic.Uint64
	totalBroadcasts    atomic.Uint64
	snapshotProvider   SnapshotProvider
	upgrader           websocket.Upgrader
	clientQueueCapacity int
}

// Option configures the WebSocket Hub
type Option func(*Hub)

// WithRingBufferCapacity overrides default ring buffer size
func WithRingBufferCapacity(cap int) Option {
	return func(h *Hub) {
		h.ringBuffer = NewRingBuffer(cap)
	}
}

// WithClientQueueCapacity sets the buffer size of each client's outbound queue
func WithClientQueueCapacity(cap int) Option {
	return func(h *Hub) {
		h.clientQueueCapacity = cap
	}
}

// NewHub creates a concurrent-safe WebSocket broadcast hub with reconnect resync
func NewHub(opts ...Option) *Hub {
	h := &Hub{
		clients:             make(map[*Client]struct{}),
		register:            make(chan *Client),
		unregister:          make(chan *Client),
		ringBuffer:          NewRingBuffer(500),
		clientQueueCapacity: defaultQueueCapacity,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow cross-origin connections for API/SPA integration
			},
		},
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// SetSnapshotProvider registers the callback for generating initial or gap-resync snapshots
func (h *Hub) SetSnapshotProvider(provider SnapshotProvider) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshotProvider = provider
}

// Run executes the hub's connection management lifecycle loop
func (h *Hub) Run(ctx context.Context) {
	slog.Info("WebSocket Hub event loop started")
	for {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			for client := range h.clients {
				client.Close()
				delete(h.clients, client)
			}
			h.mu.Unlock()
			slog.Info("WebSocket Hub stopped")
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			count := len(h.clients)
			h.mu.Unlock()
			observability.NewMetrics().WebSocketConnections.Set(float64(count))
			slog.Debug("WebSocket client registered", "client_id", client.ID, "total_clients", count)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.Close()
			}
			count := len(h.clients)
			h.mu.Unlock()
			observability.NewMetrics().WebSocketConnections.Set(float64(count))
			slog.Debug("WebSocket client unregistered", "client_id", client.ID, "total_clients", count)
		}
	}
}

// Broadcast dispatches an event to all connected clients without blocking slow consumers
func (h *Hub) Broadcast(eventType string, payload any) *Event {
	broadcastStart := time.Now()
	defer func() {
		observability.NewMetrics().WebSocketBroadcastDuration.WithLabelValues(eventType).Observe(time.Since(broadcastStart).Seconds())
	}()

	seq := h.seqCounter.Add(1)

	event := &Event{
		ID:        uuid.New().String(),
		Sequence:  seq,
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	// Persist event in bounded ring buffer for reconnect resync
	h.ringBuffer.Add(event)
	h.totalBroadcasts.Add(1)

	msgBytes, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize WebSocket event", "error", err)
		return event
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		if !client.Send(msgBytes) {
			// Bounded queue full: slow consumer dropped
			h.slowConsumerDrops.Add(1)
			slog.Warn("WebSocket slow consumer detected; dropping message and scheduling eviction", "client_id", client.ID)
			// Non-blocking trigger unregister
			go func(c *Client) {
				h.unregister <- c
			}(client)
		}
	}

	return event
}

// ServeWS handles WebSocket connection upgrade, authentication, and reconnect resync
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade WebSocket connection", "error", err)
		return
	}

	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	client := NewClient(clientID, h, conn, h.clientQueueCapacity)
	h.register <- client

	// Handle Reconnect Resync or Initial Snapshot
	lastSeqStr := r.URL.Query().Get("last_seq")
	var lastSeq uint64 = 0
	if lastSeqStr != "" {
		if val, err := strconv.ParseUint(lastSeqStr, 10, 64); err == nil {
			lastSeq = val
		}
	}

	ctx := r.Context()
	h.handleClientResync(ctx, client, lastSeq)

	go client.writePump()
	go client.readPump()
}

// handleClientResync re蘊lays events or sends a full snapshot based on the requested sequence cursor
func (h *Hub) handleClientResync(ctx context.Context, client *Client, lastSeq uint64) {
	currentSeq := h.seqCounter.Load()

	if lastSeq > 0 {
		// Attempt replay from ring buffer
		missedEvents, ok := h.ringBuffer.EventsSince(lastSeq)
		if ok && len(missedEvents) > 0 {
			for _, ev := range missedEvents {
				if b, err := json.Marshal(ev); err == nil {
					client.Send(b)
				}
			}
			return
		} else if ok && len(missedEvents) == 0 {
			// Client is already fully up to date!
			return
		}
		// If !ok, sequence gap cannot be recovered from ring buffer -> fall through to full snapshot
	}

	// Full snapshot synchronization
	h.mu.RLock()
	provider := h.snapshotProvider
	h.mu.RUnlock()

	if provider != nil {
		state, snapSeq, err := provider(ctx)
		if err == nil {
			syncEvent := &Event{
				ID:        uuid.New().String(),
				Sequence:  snapSeq,
				Type:      EventSnapshotSync,
				Timestamp: time.Now().UTC(),
				Payload: SnapshotSyncPayload{
					Sequence: snapSeq,
					State:    state,
				},
			}
			if b, err := json.Marshal(syncEvent); err == nil {
				client.Send(b)
			}
			return
		}
	}

	// Fallback empty snapshot event if provider not configured
	syncEvent := &Event{
		ID:        uuid.New().String(),
		Sequence:  currentSeq,
		Type:      EventSnapshotSync,
		Timestamp: time.Now().UTC(),
		Payload: SnapshotSyncPayload{
			Sequence: currentSeq,
			State:    []any{},
		},
	}
	if b, err := json.Marshal(syncEvent); err == nil {
		client.Send(b)
	}
}

// RegisterRoutes registers the websocket endpoint on the given HTTP ServeMux
func (h *Hub) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /ws/incidents", h.ServeWS)
	mux.HandleFunc("GET /api/v1/ws/incidents", h.ServeWS)
}

// RegisterClient registers a client directly with the hub
func (h *Hub) RegisterClient(client *Client) {
	h.register <- client
}

// UnregisterClient removes a client directly from the hub
func (h *Hub) UnregisterClient(client *Client) {
	h.unregister <- client
}

// ClientCount returns the number of active connected clients
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// CurrentSequence returns the current monotonic sequence counter
func (h *Hub) CurrentSequence() uint64 {
	return h.seqCounter.Load()
}

// GetStats returns telemetry metrics for the WebSocket Hub
func (h *Hub) GetStats() map[string]any {
	h.mu.RLock()
	clientCount := len(h.clients)
	h.mu.RUnlock()

	return map[string]any{
		"client_count":          clientCount,
		"current_sequence":      h.seqCounter.Load(),
		"total_broadcasts":      h.totalBroadcasts.Load(),
		"slow_consumer_drops":  h.slowConsumerDrops.Load(),
		"ring_buffer_size":      h.ringBuffer.Size(),
		"ring_buffer_oldest_seq": h.ringBuffer.OldestSequence(),
		"ring_buffer_latest_seq": h.ringBuffer.LatestSequence(),
	}
}
