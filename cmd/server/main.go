package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ecobrio/internal/config"
	"ecobrio/internal/database"
	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadinessResponse struct {
	Status      string            `json:"status"`
	Timestamp   time.Time         `json:"timestamp"`
	Components  map[string]string `json:"components"`
}

func main() {
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	slog.Info("Initializing ECOBrio Distributed Platform", "version", "1.0.0-dev")

	// Load Configuration
	cfg := config.Load()
	slog.Info("Configuration loaded successfully", "env", cfg.Server.Env, "port", cfg.Server.Port)

	// Context for graceful startup and shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize Observability
	metrics := observability.NewMetrics()

	// Initialize PostgreSQL Connection Pool (Optional soft-start if DB is initializing)
	pgClient, err := database.NewPostgresPool(ctx, &cfg.Database)
	if err != nil {
		slog.Warn("PostgreSQL pool initialization failed (will retry on readiness)", "error", err)
	} else {
		defer pgClient.Close()
		// Run pending migrations
		migrator := database.NewMigrator(pgClient.Pool)
		if err := migrator.RunMigrations(ctx, "./migrations"); err != nil {
			slog.Warn("Automated migrations encountered an issue", "error", err)
		}
	}

	// Initialize Redis Client
	redisClient, err := database.NewRedisClient(ctx, &cfg.Redis)
	if err != nil {
		slog.Warn("Redis client initialization failed (will retry on readiness)", "error", err)
	} else {
		defer redisClient.Close()
	}

	// Configure HTTP Router
	mux := http.NewServeMux()

	// Health & Readiness Endpoints
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"ecobrio"}`))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		readiness := ReadinessResponse{
			Status:     "UP",
			Timestamp:  time.Now(),
			Components: make(map[string]string),
		}
		statusCode := http.StatusOK

		// Check PostgreSQL
		if pgClient == nil || pgClient.Ping(r.Context()) != nil {
			readiness.Components["postgres"] = "DOWN"
			readiness.Status = "DEGRADED"
		} else {
			readiness.Components["postgres"] = "UP"
		}

		// Check Redis
		if redisClient == nil || redisClient.Ping(r.Context()) != nil {
			readiness.Components["redis"] = "DOWN"
			readiness.Status = "DEGRADED"
		} else {
			readiness.Components["redis"] = "UP"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(readiness)
	})

	// Prometheus Metrics Endpoint
	mux.Handle("GET /metrics", promhttp.Handler())

	// Static Web Experience Platform
	webDir := "./web"
	if _, err := os.Stat(webDir); err == nil {
		fileServer := http.FileServer(http.Dir(webDir))
		mux.Handle("/", fileServer)
		slog.Info("Serving static UI assets from", "directory", webDir)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("ECOBrio Engine Running. Web assets not found at ./web"))
		})
	}

	// Wrap Router in Middleware Pipeline
	handler := middleware.Recovery(middleware.LoggingAndMetrics(metrics, mux))

	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start HTTP Server in background goroutine
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("Starting ECOBrio HTTP server", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for termination signal or server error
	select {
	case err := <-serverErr:
		slog.Error("Fatal server startup error", "error", err)
	case <-ctx.Done():
		slog.Info("Shutdown signal received, initiating graceful teardown...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("Server forced shutdown", "error", err)
		}
		slog.Info("ECOBrio server gracefully stopped")
	}
}
