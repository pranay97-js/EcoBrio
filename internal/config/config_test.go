package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigLoadDefaults(t *testing.T) {
	// Clear any overrides
	os.Unsetenv("PORT")
	os.Unsetenv("ENV")
	os.Unsetenv("WAL_BATCH_DELAY_MS")

	cfg := Load()

	if cfg.Server.Port != "8080" {
		t.Errorf("expected default Port 8080, got %s", cfg.Server.Port)
	}
	if cfg.Server.Env != "development" {
		t.Errorf("expected default Env development, got %s", cfg.Server.Env)
	}
	if cfg.WAL.BatchDelay != 10*time.Millisecond {
		t.Errorf("expected default BatchDelay 10ms, got %v", cfg.WAL.BatchDelay)
	}
	if cfg.WAL.SegmentMaxBytes != 10*1024*1024 {
		t.Errorf("expected default SegmentMaxBytes 10MB, got %d", cfg.WAL.SegmentMaxBytes)
	}
}

func TestConfigLoadOverrides(t *testing.T) {
	os.Setenv("PORT", "9095")
	os.Setenv("ENV", "production")
	os.Setenv("WAL_BATCH_DELAY_MS", "25")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("ENV")
		os.Unsetenv("WAL_BATCH_DELAY_MS")
	}()

	cfg := Load()

	if cfg.Server.Port != "9095" {
		t.Errorf("expected overridden Port 9095, got %s", cfg.Server.Port)
	}
	if cfg.Server.Env != "production" {
		t.Errorf("expected overridden Env production, got %s", cfg.Server.Env)
	}
	if cfg.WAL.BatchDelay != 25*time.Millisecond {
		t.Errorf("expected overridden BatchDelay 25ms, got %v", cfg.WAL.BatchDelay)
	}
}
