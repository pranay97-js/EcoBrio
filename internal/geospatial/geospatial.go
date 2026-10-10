package geospatial

import (
	"context"

	"github.com/google/uuid"
)

// GeoPoint represents a localized entity
type GeoPoint struct {
	ID        uuid.UUID `json:"id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Name      string    `json:"name"`
}

// Indexer manages fast Redis GEO spatial indexing
type Indexer interface {
	IndexIncident(ctx context.Context, id uuid.UUID, lat, lon float64) error
	SearchNearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]GeoPoint, error)
	Remove(ctx context.Context, id uuid.UUID) error
}
