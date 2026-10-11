package geospatial_test

import (
	"context"
	"math"
	"testing"

	"ecobrio/internal/geospatial"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHaversineKm_KnownDistances(t *testing.T) {
	// London (51.5074, -0.1278) to Paris (48.8566, 2.3522) is approx 343.5 km
	londonLat, londonLon := 51.5074, -0.1278
	parisLat, parisLon := 48.8566, 2.3522

	dist := geospatial.HaversineKm(londonLat, londonLon, parisLat, parisLon)
	assert.True(t, math.Abs(dist-343.5) < 3.0, "Expected approx 343.5 km, got %f", dist)

	// Distance from point to itself is 0
	selfDist := geospatial.HaversineKm(londonLat, londonLon, londonLat, londonLon)
	assert.Equal(t, 0.0, selfDist)

	// Antipodal points (opposite sides of Earth) should not produce NaN
	antiDist := geospatial.HaversineKm(90.0, 0.0, -90.0, 0.0)
	assert.False(t, math.IsNaN(antiDist), "Antipodal distance should not be NaN")
	assert.True(t, math.Abs(antiDist-20015.0) < 50.0, "Expected approx 20015 km, got %f", antiDist)
}

func TestMemoryIndexer_EmptyResultsNonNil(t *testing.T) {
	ctx := context.Background()
	idx := geospatial.NewMemoryIndexer()

	results, err := idx.SearchNearby(ctx, 0, 0, 10.0, 0)
	require.NoError(t, err)
	assert.NotNil(t, results, "Empty search should return initialized non-nil slice")
	assert.Empty(t, results)
}

func TestMemoryIndexer_Lifecycle(t *testing.T) {
	ctx := context.Background()
	idx := geospatial.NewMemoryIndexer()

	centerLat, centerLon := 28.6139, 77.2090 // New Delhi

	p1 := geospatial.GeoPoint{
		ID:         uuid.New(),
		Latitude:   28.6200, // ~1 km away
		Longitude:  77.2100,
		Name:       "Near Point",
		Status:     "RAISED",
		StreamType: "PLASTICS",
	}

	p2 := geospatial.GeoPoint{
		ID:         uuid.New(),
		Latitude:   28.7041, // ~12 km away
		Longitude:  77.1025,
		Name:       "Far Point",
		Status:     "RAISED",
		StreamType: "ORGANIC",
	}

	p3 := geospatial.GeoPoint{
		ID:         uuid.New(),
		Latitude:   19.0760, // Mumbai (~1,150 km away)
		Longitude:  72.8777,
		Name:       "Very Far Point",
		Status:     "IN_PROGRESS",
		StreamType: "METALS",
	}

	require.NoError(t, idx.IndexIncident(ctx, p1))
	require.NoError(t, idx.IndexIncident(ctx, p2))
	require.NoError(t, idx.IndexIncident(ctx, p3))
	assert.Equal(t, 3, idx.Len())

	// Search within 5 km: should only return p1
	results5km, err := idx.SearchNearby(ctx, centerLat, centerLon, 5.0, 10)
	require.NoError(t, err)
	require.Len(t, results5km, 1)
	assert.Equal(t, p1.ID, results5km[0].ID)
	assert.True(t, results5km[0].DistanceKm < 2.0)

	// Search within 20 km: should return p1 and p2, sorted by distance
	results20km, err := idx.SearchNearby(ctx, centerLat, centerLon, 20.0, 10)
	require.NoError(t, err)
	require.Len(t, results20km, 2)
	assert.Equal(t, p1.ID, results20km[0].ID)
	assert.Equal(t, p2.ID, results20km[1].ID)
	assert.True(t, results20km[0].DistanceKm < results20km[1].DistanceKm)

	// Remove p1
	require.NoError(t, idx.Remove(ctx, p1.ID))
	assert.Equal(t, 2, idx.Len())
	resultsAfterRemoval, err := idx.SearchNearby(ctx, centerLat, centerLon, 5.0, 10)
	require.NoError(t, err)
	assert.Empty(t, resultsAfterRemoval)

	// Rebuild index
	rebuildList := []geospatial.GeoPoint{p1, p3}
	require.NoError(t, idx.RebuildIndex(ctx, rebuildList))
	assert.Equal(t, 2, idx.Len())
}

func TestRedisIndexer_Integration(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not available locally; skipping live Redis GEO integration test")
		return
	}
	defer rdb.Close()

	prefix := "test:" + uuid.New().String()
	idx := geospatial.NewRedisIndexer(rdb, prefix)

	centerLat, centerLon := 28.6139, 77.2090

	p1 := geospatial.GeoPoint{
		ID:         uuid.New(),
		Latitude:   28.6200,
		Longitude:  77.2100,
		Name:       "Redis Point 1",
		Status:     "RAISED",
		StreamType: "GLASS",
	}

	p2 := geospatial.GeoPoint{
		ID:         uuid.New(),
		Latitude:   28.7041,
		Longitude:  77.1025,
		Name:       "Redis Point 2",
		Status:     "IN_PROGRESS",
		StreamType: "EWASTE",
	}

	require.NoError(t, idx.IndexIncident(ctx, p1))
	require.NoError(t, idx.IndexIncident(ctx, p2))

	// Search within 5km
	res5km, err := idx.SearchNearby(ctx, centerLat, centerLon, 5.0, 10)
	require.NoError(t, err)
	require.Len(t, res5km, 1)
	assert.Equal(t, p1.ID, res5km[0].ID)
	assert.Equal(t, "GLASS", res5km[0].StreamType)

	// Remove p1
	require.NoError(t, idx.Remove(ctx, p1.ID))
	resAfter, err := idx.SearchNearby(ctx, centerLat, centerLon, 5.0, 10)
	require.NoError(t, err)
	assert.Empty(t, resAfter)

	// Clean up test keys
	_ = rdb.Del(ctx, prefix+":geo:incidents", prefix+":geo:incidents:meta")
}
