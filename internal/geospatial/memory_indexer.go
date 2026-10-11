package geospatial

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// MemoryIndexer provides a thread-safe in-memory spatial index implementation
type MemoryIndexer struct {
	mu     sync.RWMutex
	points map[uuid.UUID]GeoPoint
}

// NewMemoryIndexer creates a new in-memory spatial index
func NewMemoryIndexer() *MemoryIndexer {
	return &MemoryIndexer{
		points: make(map[uuid.UUID]GeoPoint),
	}
}

func (m *MemoryIndexer) IndexIncident(ctx context.Context, point GeoPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.points[point.ID] = point
	return nil
}

func (m *MemoryIndexer) SearchNearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]GeoPoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []GeoPoint
	for _, p := range m.points {
		dist := HaversineKm(lat, lon, p.Latitude, p.Longitude)
		if dist <= radiusKm {
			pCopy := p
			pCopy.DistanceKm = dist
			results = append(results, pCopy)
		}
	}

	// Sort results ascending by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].DistanceKm < results[j].DistanceKm
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

func (m *MemoryIndexer) Remove(ctx context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.points, id)
	return nil
}

func (m *MemoryIndexer) RebuildIndex(ctx context.Context, points []GeoPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.points = make(map[uuid.UUID]GeoPoint, len(points))
	for _, p := range points {
		m.points[p.ID] = p
	}
	return nil
}

// Len returns the number of currently indexed points
func (m *MemoryIndexer) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.points)
}
