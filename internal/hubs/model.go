package hubs

import (
	"time"

	"github.com/google/uuid"
)

// ValidWasteStreams defines acceptable streams across ECOBrio
var ValidWasteStreams = map[string]bool{
	"ORGANIC":   true,
	"PLASTICS":  true,
	"GLASS":     true,
	"METALS":    true,
	"PAPER":     true,
	"EWASTE":    true,
	"HAZARDOUS": true,
}

// Hub represents a physical municipal collection terminal
type Hub struct {
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Address        string    `json:"address"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	Status         string    `json:"status"`
	OperatingHours string    `json:"operating_hours"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// CreateHubRequest holds input for registering a collection hub
type CreateHubRequest struct {
	Name           string  `json:"name"`
	Address        string  `json:"address"`
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	OperatingHours string  `json:"operating_hours,omitempty"`
}

// Slot represents a discrete capacity booking window
type Slot struct {
	ID            uuid.UUID `json:"id"`
	HubID         uuid.UUID `json:"hub_id"`
	StreamType    string    `json:"stream_type"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	TotalCapacity int       `json:"total_capacity"`
	BookedCount   int       `json:"booked_count"`
	Version       int       `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// CreateSlotRequest holds input for adding capacity slots
type CreateSlotRequest struct {
	HubID         uuid.UUID `json:"hub_id"`
	StreamType    string    `json:"stream_type"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	TotalCapacity int       `json:"total_capacity"`
}
