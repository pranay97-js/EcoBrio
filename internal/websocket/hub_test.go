package websocket_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ws "ecobrio/internal/websocket"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBuffer_AddAndReplay(t *testing.T) {
	rb := ws.NewRingBuffer(5)

	// Empty buffer check
	events, ok := rb.EventsSince(0)
	assert.True(t, ok)
	assert.Empty(t, events)

	// Add 3 events
	for i := uint64(1); i <= 3; i++ {
		rb.Add(&ws.Event{
			ID:       fmt.Sprintf("id-%d", i),
			Sequence: i,
			Type:     ws.EventIncidentCreated,
		})
	}

	assert.Equal(t, 3, rb.Size())
	assert.Equal(t, uint64(1), rb.OldestSequence())
	assert.Equal(t, uint64(3), rb.LatestSequence())

	// Replay from sequence 1: should return events 2 and 3
	replayed, ok := rb.EventsSince(1)
	require.True(t, ok)
	require.Len(t, replayed, 2)
	assert.Equal(t, uint64(2), replayed[0].Sequence)
	assert.Equal(t, uint64(3), replayed[1].Sequence)

	// Already up to date check
	upToDate, ok := rb.EventsSince(3)
	require.True(t, ok)
	assert.Empty(t, upToDate)

	// Now add 5 more events to trigger FIFO evictions (capacity = 5)
	for i := uint64(4); i <= 8; i++ {
		rb.Add(&ws.Event{
			ID:       fmt.Sprintf("id-%d", i),
			Sequence: i,
			Type:     ws.EventIncidentUpdated,
		})
	}

	assert.Equal(t, 5, rb.Size())
	assert.Equal(t, uint64(4), rb.OldestSequence()) // 1, 2, 3 were evicted
	assert.Equal(t, uint64(8), rb.LatestSequence())

	// Requesting seq 1 should detect gap (1 < oldestSeq 4)
	gapEvents, ok := rb.EventsSince(1)
	assert.False(t, ok, "Expected gap detection when requesting sequence older than ring buffer window")
	assert.Nil(t, gapEvents)

	// Requesting seq 5 should return 6, 7, 8
	replayed, ok = rb.EventsSince(5)
	require.True(t, ok)
	require.Len(t, replayed, 3)
	assert.Equal(t, uint64(6), replayed[0].Sequence)
	assert.Equal(t, uint64(7), replayed[1].Sequence)
	assert.Equal(t, uint64(8), replayed[2].Sequence)
}

func TestHub_BroadcastFanout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := ws.NewHub()
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect 3 clients
	numClients := 3
	conns := make([]*websocket.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL+"?last_seq=0", nil)
		require.NoError(t, err)
		defer conn.Close()
		conns[i] = conn

		// Read initial SNAPSHOT_SYNC
		var syncEvent ws.Event
		err = conn.ReadJSON(&syncEvent)
		require.NoError(t, err)
		assert.Equal(t, ws.EventSnapshotSync, syncEvent.Type)
	}

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, numClients, hub.ClientCount())

	// Broadcast an incident event
	testPayload := map[string]string{"incident_id": "test-123", "status": "RAISED"}
	broadcastEvent := hub.Broadcast(ws.EventIncidentCreated, testPayload)
	assert.Equal(t, uint64(1), broadcastEvent.Sequence)

	// Verify all clients receive the broadcast concurrently
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *websocket.Conn) {
			defer wg.Done()
			var ev ws.Event
			err := c.ReadJSON(&ev)
			assert.NoError(t, err)
			assert.Equal(t, ws.EventIncidentCreated, ev.Type)
			assert.Equal(t, uint64(1), ev.Sequence)
		}(conns[i])
	}
	wg.Wait()
}

func TestHub_SlowConsumer_Protection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure hub with tiny queue capacity (2) to easily trigger slow consumer detection
	hub := ws.NewHub(ws.WithClientQueueCapacity(2))
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 1. Fast Client that actively reads concurrently
	fastConn, _, err := websocket.DefaultDialer.Dial(wsURL+"?last_seq=0", nil)
	require.NoError(t, err)
	defer fastConn.Close()
	var fastSync ws.Event
	_ = fastConn.ReadJSON(&fastSync)

	fastReceived := make(chan ws.Event, 20)
	go func() {
		for {
			var ev ws.Event
			if err := fastConn.ReadJSON(&ev); err != nil {
				return
			}
			fastReceived <- ev
		}
	}()

	// 2. Slow / Stalled Client that never drains its bounded outbound queue
	slowClient := ws.NewClient("mock-slow", hub, nil, 2)
	hub.RegisterClient(slowClient)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, hub.ClientCount())

	// Broadcast 15 messages with small pacing: fast client reads in real-time, slow client buffer quickly overflows
	for i := 1; i <= 15; i++ {
		hub.Broadcast(ws.EventIncidentUpdated, map[string]int{"iteration": i})
		time.Sleep(5 * time.Millisecond)
	}

	// Verify fast client received messages without deadlock or blocking
	receivedCount := 0
	timeout := time.After(1 * time.Second)
OuterLoop:
	for receivedCount < 15 {
		select {
		case <-fastReceived:
			receivedCount++
		case <-timeout:
			break OuterLoop
		}
	}
	assert.True(t, receivedCount >= 10, "Fast client should receive messages without being blocked")

	// Allow unregister goroutine to complete
	time.Sleep(100 * time.Millisecond)

	stats := hub.GetStats()
	assert.True(t, stats["slow_consumer_drops"].(uint64) > 0, "Slow consumer drops should be recorded")
}

func TestClient_BoundedQueueDirect(t *testing.T) {
	client := ws.NewClient("test-client", nil, nil, 2)
	assert.True(t, client.Send([]byte("msg-1")))
	assert.True(t, client.Send([]byte("msg-2")))
	assert.False(t, client.Send([]byte("msg-3")), "Queue capacity 2 must reject 3rd unconsumed message")
}

func TestHub_ReconnectReplayAndSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := ws.NewHub()
	go hub.Run(ctx)

	// Provide a snapshot provider
	hub.SetSnapshotProvider(func(ctx context.Context) (any, uint64, error) {
		return []map[string]string{{"id": "inc-1", "status": "IN_PROGRESS"}}, 5, nil
	})

	// Broadcast 3 events (sequences 1, 2, 3)
	for i := 1; i <= 3; i++ {
		hub.Broadcast(ws.EventIncidentCreated, map[string]int{"num": i})
	}

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Client reconnects with last_seq=1: should immediately receive events 2 and 3 replayed!
	connReplay, _, err := websocket.DefaultDialer.Dial(wsURL+"?last_seq=1", nil)
	require.NoError(t, err)
	defer connReplay.Close()

	connReplay.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var ev2 ws.Event
	require.NoError(t, connReplay.ReadJSON(&ev2))
	assert.Equal(t, uint64(2), ev2.Sequence)

	var ev3 ws.Event
	require.NoError(t, connReplay.ReadJSON(&ev3))
	assert.Equal(t, uint64(3), ev3.Sequence)

	// Client reconnects with last_seq=0: should receive SNAPSHOT_SYNC
	connSnap, _, err := websocket.DefaultDialer.Dial(wsURL+"?last_seq=0", nil)
	require.NoError(t, err)
	defer connSnap.Close()

	connSnap.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var snapEv ws.Event
	require.NoError(t, connSnap.ReadJSON(&snapEv))
	assert.Equal(t, ws.EventSnapshotSync, snapEv.Type)
}

func BenchmarkHub_BroadcastFanout(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := ws.NewHub()
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect 50 subscribers
	numClients := 50
	conns := make([]*websocket.Conn, numClients)
	for i := 0; i < numClients; i++ {
		c, _, err := websocket.DefaultDialer.Dial(wsURL+"?last_seq=0", nil)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		defer c.Close()
		conns[i] = c
		var discard ws.Event
		_ = c.ReadJSON(&discard)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Broadcast(ws.EventIncidentUpdated, map[string]int{"idx": i})
	}
}
