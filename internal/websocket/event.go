package websocket

import (
	"time"
)

// Standard WebSocket event type constants
const (
	EventIncidentCreated = "INCIDENT_CREATED"
	EventIncidentUpdated = "INCIDENT_UPDATED"
	EventSnapshotSync    = "SNAPSHOT_SYNC"
	EventHeartbeat       = "HEARTBEAT"
)

// Event represents a sequenced broadcast event payload
type Event struct {
	ID        string    `json:"id"`
	Sequence  uint64    `json:"sequence"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload"`
}

// SnapshotSyncPayload represents the data sent during an initial connect or gap resync
type SnapshotSyncPayload struct {
	Sequence uint64 `json:"sequence"`
	State    any    `json:"state"`
}
