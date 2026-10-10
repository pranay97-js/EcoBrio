package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ecobrio/internal/config"
	"github.com/redis/go-redis/v9"
)

// RedisClient wraps the go-redis client
type RedisClient struct {
	Client *redis.Client
}

// NewRedisClient establishes connection to Redis
func NewRedisClient(ctx context.Context, cfg *config.RedisConfig) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping redis at %s: %w", cfg.Addr, err)
	}

	slog.Info("Redis connection established successfully", "addr", cfg.Addr, "db", cfg.DB)
	return &RedisClient{Client: client}, nil
}

// Close gracefully terminates the Redis client
func (r *RedisClient) Close() {
	if r.Client != nil {
		_ = r.Client.Close()
		slog.Info("Redis connection closed")
	}
}

// Ping checks whether Redis is currently responsive
func (r *RedisClient) Ping(ctx context.Context) error {
	if r.Client == nil {
		return fmt.Errorf("redis client not initialized")
	}
	return r.Client.Ping(ctx).Err()
}
