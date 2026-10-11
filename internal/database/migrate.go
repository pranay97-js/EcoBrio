package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrator manages applying SQL migrations to PostgreSQL
type Migrator struct {
	pool *pgxpool.Pool
}

// NewMigrator returns an initialized Migrator instance
func NewMigrator(pool *pgxpool.Pool) *Migrator {
	return &Migrator{pool: pool}
}

// RunMigrations applies all pending .up.sql files in alphabetical order
func (m *Migrator) RunMigrations(ctx context.Context, migrationsDir string) error {
	if m.pool == nil {
		return errors.New("database pool is nil")
	}

	// Ensure migrations tracking table exists
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(255) PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);`
	if _, err := m.pool.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// Fetch already applied migrations
	rows, err := m.pool.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}

	// Read migration directory
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var upFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".up.sql") {
			upFiles = append(upFiles, entry.Name())
		}
	}
	sort.Strings(upFiles)

	for _, file := range upFiles {
		version := strings.TrimSuffix(file, ".up.sql")
		if applied[version] {
			slog.Debug("Migration already applied", "version", version)
			continue
		}

		filePath := filepath.Join(migrationsDir, file)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", filePath, err)
		}

		slog.Info("Applying migration", "version", version, "file", file)
		tx, err := m.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start migration transaction for %s: %w", version, err)
		}

		if _, err := tx.Exec(ctx, string(content)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed executing migration %s: %w", version, err)
		}

		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)", version, time.Now()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed recording migration %s: %w", version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed committing migration %s: %w", version, err)
		}
		slog.Info("Successfully applied migration", "version", version)
	}

	return nil
}
