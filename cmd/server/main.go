package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ecobrio/internal/ai"
	"ecobrio/internal/api"
	"ecobrio/internal/booking"
	"ecobrio/internal/config"
	"ecobrio/internal/credits"
	"ecobrio/internal/database"
	"ecobrio/internal/geospatial"
	"ecobrio/internal/hubs"
	"ecobrio/internal/incidents"
	"ecobrio/internal/ledger"
	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"ecobrio/internal/outbox"
	"ecobrio/internal/queue"
	"ecobrio/internal/telemetry"
	"ecobrio/internal/telemetry/snapshots"
	"ecobrio/internal/telemetry/wal"
	"ecobrio/internal/websocket"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadinessResponse struct {
	Status     string            `json:"status"`
	Timestamp  time.Time         `json:"timestamp"`
	Components map[string]string `json:"components"`
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

	isMigrateOnly := false
	for _, arg := range os.Args[1:] {
		if arg == "--migrate-only" {
			isMigrateOnly = true
			break
		}
	}

	// Initialize PostgreSQL Connection Pool (Optional soft-start if DB is initializing)
	pgClient, err := database.NewPostgresPool(ctx, &cfg.Database)
	if err != nil {
		if isMigrateOnly {
			slog.Error("Migration failed: unable to connect to PostgreSQL", "error", err)
			os.Exit(1)
		}
		slog.Warn("PostgreSQL pool initialization failed (will retry on readiness)", "error", err)
	} else {
		defer pgClient.Close()
		// Run pending migrations
		migrator := database.NewMigrator(pgClient.Pool)
		if err := migrator.RunMigrations(ctx, "./migrations"); err != nil {
			if isMigrateOnly {
				slog.Error("Migration execution failed", "error", err)
				os.Exit(1)
			}
			slog.Warn("Automated migrations encountered an issue", "error", err)
		}
		if isMigrateOnly {
			slog.Info("Schema migrations executed successfully. Exiting (--migrate-only requested).")
			return
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

	// Health & Readiness Endpoints (with /health and /ready aliases)
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"ecobrio"}`))
	}
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /health", healthHandler)

	readyHandler := func(w http.ResponseWriter, r *http.Request) {
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
	}
	mux.HandleFunc("GET /readyz", readyHandler)
	mux.HandleFunc("GET /ready", readyHandler)

	// Prometheus Metrics Endpoint
	mux.Handle("GET /metrics", promhttp.Handler())

	// Initialize Domain Repositories & Services
	var hubsRepo hubs.Repository
	var incidentsRepo incidents.Repository
	var bookingRepo booking.Repository
	var tokenStore booking.TokenStore
	var ledgerRepo ledger.Repository
	var outboxRepo outbox.Repository
	var queueProducer queue.Producer
	var queueConsumer queue.Consumer
	var creditsRepo credits.Repository
	var telemetryRepo telemetry.Repository
	var checkpointStore wal.CheckpointStore

	var memLedger *ledger.MemoryRepository

	if pgClient != nil && redisClient != nil {
		slog.Info("Using PostgreSQL & Redis storage for hubs, incidents, bookings, and ledger")
		hubsRepo = hubs.NewPostgresRepository(pgClient.Pool)
		incidentsRepo = incidents.NewPostgresRepository(pgClient.Pool)
		bookingRepo = booking.NewPostgresRepository(pgClient.Pool)
		tokenStore = booking.NewRedisTokenStore(redisClient.Client)
		ledgerRepo = ledger.NewPostgresRepository(pgClient.Pool)
		outboxRepo = outbox.NewPostgresRepository(pgClient.Pool)
		memQueue := queue.NewMemoryQueue()
		queueProducer = memQueue
		queueConsumer = memQueue
		creditsRepo = credits.NewPostgresRepository(pgClient.Pool)
		telemetryRepo = telemetry.NewPostgresRepository(pgClient.Pool)
		checkpointStore = wal.NewPostgresCheckpointStore(pgClient.Pool)
	} else {
		slog.Info("Operating with in-memory persistence fallback")
		memHubs := hubs.NewMemoryRepository()
		seedMemoryHubs(memHubs)
		memIncidents := incidents.NewMemoryRepository()
		seedMemoryIncidents(memIncidents)
		incidentsRepo = memIncidents

		memBooking := booking.NewMemoryRepository()
		seedMemoryBookingSlots(memBooking)
		bookingRepo = memBooking
		tokenStore = booking.NewMemoryTokenStore()

		memLedger = ledger.NewMemoryRepository()
		ledgerRepo = memLedger
		outboxRepo = outbox.NewMemoryRepository()
		memQueue := queue.NewMemoryQueue()
		queueProducer = memQueue
		queueConsumer = memQueue
		creditsRepo = credits.NewMemoryRepository()
		telemetryRepo = telemetry.NewMemoryRepository()
		checkpointStore = wal.NewMemoryCheckpointStore()
	}

	// Geospatial Indexer & Native Go WebSocket Hub
	var geoIndexer geospatial.Indexer
	if redisClient != nil {
		geoIndexer = geospatial.NewRedisIndexer(redisClient.Client)
	} else {
		geoIndexer = geospatial.NewMemoryIndexer()
	}

	wsHub := websocket.NewHub()
	go wsHub.Run(ctx)
	wsHub.RegisterRoutes(mux)

	hubsService := hubs.NewService(hubsRepo)
	hubsHandler := hubs.NewHandler(hubsService)
	hubsHandler.RegisterRoutes(mux)

	incidentsService := incidents.NewService(incidentsRepo, geoIndexer, wsHub)
	wsHub.SetSnapshotProvider(incidentsService.GetActiveSnapshot)

	// Rebuild / Reconcile Redis GEO projection from canonical DB on boot
	if reindexed, err := incidentsService.RebuildGeoIndex(ctx); err == nil && reindexed > 0 {
		slog.Info("Geospatial incident projection synchronized on boot", "indexed_count", reindexed)
	}

	incidentsHandler := incidents.NewHandler(incidentsService)
	incidentsHandler.RegisterRoutes(mux)

	bookingService := booking.NewService(bookingRepo, tokenStore)
	bookingHandler := booking.NewHandler(bookingService)
	bookingHandler.RegisterRoutes(mux)

	ledgerService := ledger.NewService(ledgerRepo)
	creditsService := credits.NewService(creditsRepo, ledgerService, outboxRepo)
	creditsHandler := credits.NewHandler(creditsService)
	creditsHandler.RegisterRoutes(mux)

	if memLedger != nil {
		seedMemoryLedger(memLedger, ledgerService)
	}

	// Transactional Outbox Relay
	outboxRelay := outbox.NewRelay(outboxRepo, queueProducer)
	outboxRelay.StartBackgroundRelay(ctx, 2*time.Second, 50)

	// KachraSeth AI Triage Agent & Citizen Asynchronous Workflow
	aiToolRegistry := ai.NewToolRegistry(incidentsService, hubsService, creditsService)
	ruleBasedEngine := ai.NewRuleBasedEngine(aiToolRegistry)
	aiEngine := ai.NewOpenRouterEngine(cfg.AI.OpenRouterAPIKey, cfg.AI.Model, aiToolRegistry, ruleBasedEngine)
	aiIntakeStore := ai.NewMemoryIntakeStore()
	aiIntakeService := ai.NewIntakeService(aiIntakeStore, aiToolRegistry, queueProducer)
	aiIntakeService.StartBackgroundWorker(ctx, queueConsumer)
	aiHandler := ai.NewHandler(aiEngine, aiIntakeService, aiToolRegistry)
	aiHandler.RegisterRoutes(mux)

	// Public Google Maps Configuration Endpoint
	mux.HandleFunc("GET /api/v1/config/maps", func(w http.ResponseWriter, r *http.Request) {
		api.WriteJSON(w, http.StatusOK, map[string]string{
			"google_maps_api_key": cfg.Maps.GoogleMapsAPIKey,
		})
	})

	// Segmented Write-Ahead Log (WAL) & Telemetry Ingestion Engine
	walDir := "./data/wal"
	snapDir := "./data/snapshots"
	walEngine, err := wal.NewSegmentedEngine(wal.DefaultConfig(walDir))
	if err != nil {
		slog.Error("Failed to initialize segmented WAL engine", "error", err)
	} else {
		defer func() { _ = walEngine.Close() }()

		snapMgr, sErr := snapshots.NewManager(snapDir)
		if sErr != nil {
			slog.Warn("Snapshot manager initialization notice", "error", sErr)
		}

		telemetryService := telemetry.NewService(telemetryRepo, walEngine, checkpointStore, snapMgr)
		telemetryHandler := telemetry.NewHandler(telemetryService)
		telemetryHandler.RegisterRoutes(mux)

		// On-boot Crash Recovery Replay Daemon with Snapshot support
		restoredSnap, replayedWAL, err := telemetryService.RunCrashRecoveryWithSnapshot(ctx)
		if err != nil {
			slog.Warn("WAL crash recovery notice on boot", "error", err)
		} else if restoredSnap > 0 || replayedWAL > 0 {
			slog.Info("WAL crash recovery daemon completed",
				"restored_from_snapshot", restoredSnap,
				"replayed_from_wal", replayedWAL,
			)
		}

		// Launch background snapshot compaction worker
		telemetryService.StartBackgroundCompactor(ctx, 30*time.Second)
	}

	reconciler := booking.NewReconciler(bookingRepo, tokenStore)
	reconciler.StartBackgroundLoop(ctx, 30*time.Second, func() []uuid.UUID {
		return []uuid.UUID{
			uuid.MustParse("11111111-0000-0000-0000-000000000001"),
			uuid.MustParse("11111111-0000-0000-0000-000000000002"),
		}
	})

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

	// Start auxiliary HTTP server on port 3000 for dual-port accessibility
	var auxServer *http.Server
	if cfg.Server.Port != "3000" {
		auxServer = &http.Server{
			Addr:         ":3000",
			Handler:      handler,
			ReadTimeout:  cfg.Server.ReadTimeout,
			WriteTimeout: cfg.Server.WriteTimeout,
		}
		go func() {
			slog.Info("Starting auxiliary ECOBrio HTTP server for port 3000 access", "address", auxServer.Addr)
			if err := auxServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Warn("Auxiliary port 3000 server stopped or port already bound", "error", err)
			}
		}()
	}

	// Start primary HTTP Server in background goroutine
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

		if auxServer != nil {
			_ = auxServer.Shutdown(shutdownCtx)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("Server forced shutdown", "error", err)
		}
		slog.Info("ECOBrio server gracefully stopped")
	}
}

func seedMemoryHubs(repo *hubs.MemoryRepository) {
	ctx := context.Background()
	hubAlphaID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	_ = repo.CreateHub(ctx, &hubs.Hub{
		ID:             hubAlphaID,
		Name:           "Connaught Place Green Hub Alpha",
		Address:        "Near Rajiv Chowk Metro Gate 7, Connaught Place, New Delhi, 110001",
		Latitude:       28.6315,
		Longitude:      77.2167,
		Status:         "ACTIVE",
		OperatingHours: "06:00 - 22:00 Daily",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	})

	hubBetaID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	_ = repo.CreateHub(ctx, &hubs.Hub{
		ID:             hubBetaID,
		Name:           "Koramangala Eco Hub Beta",
		Address:        "80 Feet Road, 4th Block, Koramangala, Bengaluru, 560034",
		Latitude:       12.9352,
		Longitude:      77.6245,
		Status:         "ACTIVE",
		OperatingHours: "07:00 - 21:00 Daily",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	})

	_ = repo.CreateSlot(ctx, &hubs.Slot{
		ID:            uuid.MustParse("11111111-0000-0000-0000-000000000001"),
		HubID:         hubAlphaID,
		StreamType:    "EWASTE",
		StartTime:     time.Now(),
		EndTime:       time.Now().Add(2 * time.Hour),
		TotalCapacity: 10,
		BookedCount:   0,
		Version:       1,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})

	_ = repo.CreateSlot(ctx, &hubs.Slot{
		ID:            uuid.MustParse("11111111-0000-0000-0000-000000000002"),
		HubID:         hubAlphaID,
		StreamType:    "PLASTICS",
		StartTime:     time.Now(),
		EndTime:       time.Now().Add(2 * time.Hour),
		TotalCapacity: 20,
		BookedCount:   5,
		Version:       1,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})
}

func seedMemoryBookingSlots(repo *booking.MemoryRepository) {
	slot1 := uuid.MustParse("11111111-0000-0000-0000-000000000001")
	slot2 := uuid.MustParse("11111111-0000-0000-0000-000000000002")
	repo.SetSlot(slot1, 10, 0)
	repo.SetSlot(slot2, 20, 5)
}

func seedMemoryLedger(repo *ledger.MemoryRepository, svc *ledger.Service) {
	ctx := context.Background()
	_, _ = repo.GetOrCreateAccount(ctx, "SYSTEM_ISSUANCE_RESERVE", ledger.AccountEquity, nil)
	citizenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	code := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())
	_, _ = repo.GetOrCreateAccount(ctx, code, ledger.AccountLiability, &citizenID)
	// Seed demo balance of 500 points for Citizen Wallet
	_, _ = svc.PostBalancedTransaction(ctx, ledger.PostTransactionRequest{
		IdempotencyKey: "seed-demo-citizen-grant",
		Description:    "Welcome Eco-Credit Grant for Citizen Wallet",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 500, IsCredit: false},
			{AccountCode: code, Points: 500, IsCredit: true},
		},
	})
}

func seedMemoryIncidents(repo *incidents.MemoryRepository) {
	ctx := context.Background()
	exp1 := time.Now().Add(36 * time.Hour)
	_ = repo.Create(ctx, &incidents.Incident{
		ID:                 uuid.MustParse("22222222-0000-0000-0000-000000000001"),
		Description:        "High-density discarded lithium battery bank and circuit boards near transit terminal",
		StreamType:         "EWASTE",
		Severity:           incidents.SeverityCritical,
		Latitude:           28.6139,
		Longitude:          77.2090,
		Status:             incidents.StatusRaised,
		Place:              "New Delhi Central Transit",
		Landmark:           "Opposite Metro Gate 3",
		TimeObserved:       time.Now().Add(-2 * time.Hour).Format("2006-01-02 15:04"),
		ResolutionSLA:      "1-2 days",
		ExpectedResolution: &exp1,
		CreatedAt:          time.Now().Add(-15 * time.Minute),
		UpdatedAt:          time.Now().Add(-15 * time.Minute),
	})

	exp2 := time.Now().Add(24 * time.Hour)
	_ = repo.Create(ctx, &incidents.Incident{
		ID:                 uuid.MustParse("22222222-0000-0000-0000-000000000002"),
		Description:        "Industrial polymer packaging bales and unsegregated plastic sacks overflowing canal culvert",
		StreamType:         "PLASTICS",
		Severity:           incidents.SeverityHigh,
		Latitude:           28.6250,
		Longitude:          77.2180,
		Status:             incidents.StatusInProgress,
		Place:              "Mayur Vihar Phase 1",
		Landmark:           "Near Culvert 4, Behind DND Expressway",
		TimeObserved:       time.Now().Add(-4 * time.Hour).Format("2006-01-02 15:04"),
		ResolutionSLA:      "1-2 days",
		ExpectedResolution: &exp2,
		CreatedAt:          time.Now().Add(-45 * time.Minute),
		UpdatedAt:          time.Now().Add(-20 * time.Minute),
	})

	exp3 := time.Now().Add(40 * time.Hour)
	_ = repo.Create(ctx, &incidents.Incident{
		ID:                 uuid.MustParse("22222222-0000-0000-0000-000000000003"),
		Description:        "Commercial food market organic wet-waste mound requiring immediate municipal collection dispatch",
		StreamType:         "ORGANIC",
		Severity:           incidents.SeverityMedium,
		Latitude:           28.6080,
		Longitude:          77.1950,
		Status:             incidents.StatusRaised,
		Place:              "Lajpat Nagar Central Market",
		Landmark:           "Adjacent to Block 4 Community Centre",
		TimeObserved:       time.Now().Add(-1 * time.Hour).Format("2006-01-02 15:04"),
		ResolutionSLA:      "1-2 days",
		ExpectedResolution: &exp3,
		CreatedAt:          time.Now().Add(-5 * time.Minute),
		UpdatedAt:          time.Now().Add(-5 * time.Minute),
	})
}
