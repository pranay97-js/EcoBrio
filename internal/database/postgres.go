package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ecobrio/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresClient wraps the pgxpool connection
type PostgresClient struct {
	Pool *pgxpool.Pool
}

// NewPostgresPool establishes connection to PostgreSQL with configured pool parameters
func NewPostgresPool(ctx context.Context, cfg *config.DatabaseConfig) (*PostgresClient, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.MaxIdleConns)
	poolConfig.MaxConnIdleTime = cfg.MaxIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	slog.Info("PostgreSQL connection pool established successfully",
		"max_conns", cfg.MaxOpenConns,
		"min_conns", cfg.MaxIdleConns,
	)

	return &PostgresClient{Pool: pool}, nil
}

// Close gracefully terminates all connections in the pool
func (p *PostgresClient) Close() {
	if p.Pool != nil {
		p.Pool.Close()
		slog.Info("PostgreSQL connection pool closed")
	}
}

// Ping checks whether PostgreSQL is currently responsive
func (p *PostgresClient) Ping(ctx context.Context) error {
	if p.Pool == nil {
		return fmt.Errorf("postgres pool not initialized")
	}
	return p.Pool.Ping(ctx)
}
