package geospatial

import (
	"context"
	"math"

	"github.com/google/uuid"
)

// GeoPoint represents a localized spatial entity
type GeoPoint struct {
	ID         uuid.UUID         `json:"id"`
	Latitude   float64           `json:"latitude"`
	Longitude  float64           `json:"longitude"`
	DistanceKm float64           `json:"distance_km,omitempty"`
	Name       string            `json:"name,omitempty"`
	Status     string            `json:"status,omitempty"`
	StreamType string            `json:"stream_type,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Indexer manages spatial indexing for incidents and hubs
type Indexer interface {
	IndexIncident(ctx context.Context, point GeoPoint) error
	SearchNearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]GeoPoint, error)
	Remove(ctx context.Context, id uuid.UUID) error
	RebuildIndex(ctx context.Context, points []GeoPoint) error
}

// HaversineKm calculates the great-circle distance between two geographical coordinates in kilometers
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	dLat := (lat2 - lat1) * (math.Pi / 180.0)
	dLon := (lon2 - lon1) * (math.Pi / 180.0)

	rLat1 := lat1 * (math.Pi / 180.0)
	rLat2 := lat2 * (math.Pi / 180.0)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(rLat1)*math.Cos(rLat2)
	if a > 1.0 {
		a = 1.0
	} else if a < 0.0 {
		a = 0.0
	}
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}
