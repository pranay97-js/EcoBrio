package websocket

import (
	"sync"
)

// Hub manages persistent WebSocket connections with bounded queues
type Hub struct {
	mu         sync.RWMutex
	clients    map[string]chan []byte
	broadcast  chan []byte
	register   chan string
	unregister chan string
}

// NewHub creates a concurrent safe WebSocket broadcast hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]chan []byte),
		broadcast:  make(chan []byte, 1024),
		register:   make(chan string),
		unregister: make(chan string),
	}
}

// Broadcast sends a message to all connected clients without blocking slow consumers
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, clientChan := range h.clients {
		select {
		case clientChan <- msg:
		default:
			// Drop message for slow client to protect hub
		}
	}
}
