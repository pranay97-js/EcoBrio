package websocket

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 4096

	// Client outbound queue buffer capacity
	defaultQueueCapacity = 256
)

// Client represents a connected WebSocket subscriber with bounded queues
type Client struct {
	ID        string
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	isClosed  atomic.Bool
	lastSeq   uint64
	createdAt time.Time
}

// NewClient initializes a Client instance
func NewClient(id string, hub *Hub, conn *websocket.Conn, queueCap ...int) *Client {
	capacity := defaultQueueCapacity
	if len(queueCap) > 0 && queueCap[0] > 0 {
		capacity = queueCap[0]
	}
	return &Client{
		ID:        id,
		hub:       hub,
		conn:      conn,
		send:      make(chan []byte, capacity),
		createdAt: time.Now(),
	}
}

// Send attempts to enqueue a payload into the client's bounded buffer
// Returns false immediately if buffer is full (slow consumer protection)
func (c *Client) Send(message []byte) bool {
	if c.isClosed.Load() {
		return false
	}

	select {
	case c.send <- message:
		return true
	default:
		// Bounded queue overflow: slow consumer detected!
		return false
	}
}

// readPump pumps messages from the websocket connection to the hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Debug("WebSocket client disconnected", "client_id", c.ID, "error", err)
			}
			break
		}
	}
}

// writePump pumps messages from the client queue to the websocket connection
// Dedicated writer goroutine guarantees sequential writes per connection
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Close gracefully shuts down the client connection
func (c *Client) Close() {
	if c.isClosed.CompareAndSwap(false, true) {
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}
}
