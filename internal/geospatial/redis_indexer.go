package geospatial

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultGeoKey      = "ecobrio:geo:incidents"
	DefaultGeoMetaHash = "ecobrio:geo:incidents:meta"
)

// RedisIndexer manages Redis GEO spatial indexing with coordinate and metadata caching
type RedisIndexer struct {
	client   redis.UniversalClient
	geoKey   string
	metaHash string
}

// NewRedisIndexer initializes a spatial indexer backed by Redis GEO
func NewRedisIndexer(client redis.UniversalClient, prefix ...string) *RedisIndexer {
	key := DefaultGeoKey
	meta := DefaultGeoMetaHash
	if len(prefix) > 0 && prefix[0] != "" {
		key = fmt.Sprintf("%s:geo:incidents", strings.TrimSuffix(prefix[0], ":"))
		meta = fmt.Sprintf("%s:geo:incidents:meta", strings.TrimSuffix(prefix[0], ":"))
	}
	return &RedisIndexer{
		client:   client,
		geoKey:   key,
		metaHash: meta,
	}
}

// IndexIncident adds or updates a point in the Redis GEO set and caches its metadata
func (r *RedisIndexer) IndexIncident(ctx context.Context, point GeoPoint) error {
	idStr := point.ID.String()

	pipe := r.client.Pipeline()
	// Redis GEOADD expects longitude first, then latitude
	pipe.GeoAdd(ctx, r.geoKey, &redis.GeoLocation{
		Name:      idStr,
		Longitude: point.Longitude,
		Latitude:  point.Latitude,
	})

	metaBytes, err := json.Marshal(point)
	if err == nil {
		pipe.HSet(ctx, r.metaHash, idStr, metaBytes)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed indexing point in Redis GEO: %w", err)
	}
	return nil
}

// SearchNearby queries Redis GEO for members within the given radius in kilometers
func (r *RedisIndexer) SearchNearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]GeoPoint, error) {
	if limit <= 0 {
		limit = 50
	}

	// 1. Try modern GEOSEARCH command
	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  lon,
			Latitude:   lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      limit,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := r.client.GeoSearchLocation(ctx, r.geoKey, searchQuery).Result()
	if err != nil {
		// Fallback to GEORADIUS for backward compatibility
		radiusQuery := &redis.GeoRadiusQuery{
			Radius:    radiusKm,
			Unit:      "km",
			WithCoord: true,
			WithDist:  true,
			Count:     limit,
			Sort:      "ASC",
		}
		locations, err = r.client.GeoRadius(ctx, r.geoKey, lon, lat, radiusQuery).Result()
		if err != nil {
			return nil, fmt.Errorf("failed executing Redis GEO radius search: %w", err)
		}
	}

	if len(locations) == 0 {
		return []GeoPoint{}, nil
	}

	// Retrieve metadata for returned members
	memberIDs := make([]string, len(locations))
	for i, loc := range locations {
		memberIDs[i] = loc.Name
	}

	metaList, _ := r.client.HMGet(ctx, r.metaHash, memberIDs...).Result()

	results := make([]GeoPoint, 0, len(locations))
	for i, loc := range locations {
		uid, parseErr := uuid.Parse(loc.Name)
		if parseErr != nil {
			continue
		}

		point := GeoPoint{
			ID:         uid,
			Latitude:   loc.Latitude,
			Longitude:  loc.Longitude,
			DistanceKm: loc.Dist,
		}

		if i < len(metaList) && metaList[i] != nil {
			if strVal, ok := metaList[i].(string); ok {
				var cached GeoPoint
				if err := json.Unmarshal([]byte(strVal), &cached); err == nil {
					point.Name = cached.Name
					point.Status = cached.Status
					point.StreamType = cached.StreamType
					point.Metadata = cached.Metadata
				}
			}
		}

		results = append(results, point)
	}

	return results, nil
}

// Remove deletes an incident from both the GEO set and the metadata hash
func (r *RedisIndexer) Remove(ctx context.Context, id uuid.UUID) error {
	idStr := id.String()
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, r.geoKey, idStr)
	pipe.HDel(ctx, r.metaHash, idStr)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed removing point from Redis GEO: %w", err)
	}
	return nil
}

// RebuildIndex replaces the entire Redis GEO projection with canonical data
func (r *RedisIndexer) RebuildIndex(ctx context.Context, points []GeoPoint) error {
	pipe := r.client.Pipeline()
	pipe.Del(ctx, r.geoKey)
	pipe.Del(ctx, r.metaHash)

	for _, p := range points {
		idStr := p.ID.String()
		pipe.GeoAdd(ctx, r.geoKey, &redis.GeoLocation{
			Name:      idStr,
			Longitude: p.Longitude,
			Latitude:  p.Latitude,
		})
		if metaBytes, err := json.Marshal(p); err == nil {
			pipe.HSet(ctx, r.metaHash, idStr, metaBytes)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed rebuilding Redis GEO index: %w", err)
	}
	return nil
}
