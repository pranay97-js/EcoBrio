package database_test

import (
	"context"
	"testing"

	"ecobrio/internal/database"
	"github.com/stretchr/testify/assert"
)

func TestMigrator_NonExistentDirectory(t *testing.T) {
	migrator := database.NewMigrator(nil)
	assert.NotNil(t, migrator)

	// Attempting to run with non-existent directory and nil pool should fail gracefully
	// Note: since pool is nil, it will fail on pool exec first if invoked, or if directory doesn't exist
	// In our implementation, it creates table first, which fails on nil pool
	err := migrator.RunMigrations(context.Background(), "non_existent_dir")
	assert.Error(t, err)
}
