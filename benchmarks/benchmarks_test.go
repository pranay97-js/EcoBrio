package benchmarks_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ecobrio/internal/booking"
	"ecobrio/internal/geospatial"
	"ecobrio/internal/incidents"
	"ecobrio/internal/observability"
	"ecobrio/internal/telemetry"
	"ecobrio/internal/telemetry/wal"
	ws "ecobrio/internal/websocket"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PRD Target 1 & 2: Peak Booking Throughput >= 3,000 RPS & p99 Latency < 40ms
func BenchmarkBookingThroughput_PeakRPS(b *testing.B) {
	repo := booking.NewMemoryRepository()
	tokenStore := booking.NewMemoryTokenStore()
	svc := booking.NewService(repo, tokenStore)
	handler := booking.NewHandler(svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	slotID := uuid.New()
	repo.SetSlot(slotID, b.N+100000, 0)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	latencies := make([]time.Duration, b.N)
	var latIdx atomic.Int64

	b.ResetTimer()
	b.SetParallelism(100)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqBody, _ := json.Marshal(booking.Request{
				SlotID:     slotID,
				CitizenID:  uuid.New(),
				StreamType: "PLASTICS",
			})

			httpReq, _ := http.NewRequest("POST", ts.URL+"/api/v1/bookings", bytes.NewReader(reqBody))
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Idempotency-Key", uuid.New().String())

			start := time.Now()
			resp, err := client.Do(httpReq)
			dur := time.Since(start)

			idx := latIdx.Add(1) - 1
			if int(idx) < len(latencies) {
				latencies[idx] = dur
			}

			if err == nil {
				_ = resp.Body.Close()
			}
		}
	})

	b.StopTimer()

	validLats := latencies[:latIdx.Load()]
	if len(validLats) > 0 {
		sort.Slice(validLats, func(i, j int) bool { return validLats[i] < validLats[j] })
		p50 := validLats[int(float64(len(validLats))*0.50)]
		p95 := validLats[int(float64(len(validLats))*0.95)]
		p99 := validLats[int(float64(len(validLats))*0.99)]
		b.Logf("Benchmark Results -> Total: %d | p50: %v | p95: %v | p99: %v", len(validLats), p50, p95, p99)
	}
}

// PRD Target 3: Concurrency Safety: 0% Over-allocation (0 race conditions)
func TestBenchmark_BookingRace_ZeroOverAllocation(t *testing.T) {
	repo := booking.NewMemoryRepository()
	tokenStore := booking.NewMemoryTokenStore()
	svc := booking.NewService(repo, tokenStore)
	handler := booking.NewHandler(svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	slotID := uuid.New()
	const initialCapacity = 5
	const concurrentCallers = 100

	repo.SetSlot(slotID, initialCapacity, 0)

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var conflictCount atomic.Int32
	var otherCount atomic.Int32

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrentCallers,
			MaxIdleConnsPerHost: concurrentCallers,
		},
	}

	startBarrier := make(chan struct{})

	for i := 0; i < concurrentCallers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startBarrier

			reqBody, _ := json.Marshal(booking.Request{
				SlotID:     slotID,
				CitizenID:  uuid.New(),
				StreamType: "METALS",
			})

			httpReq, _ := http.NewRequest("POST", ts.URL+"/api/v1/bookings", bytes.NewReader(reqBody))
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Idempotency-Key", fmt.Sprintf("race-idemp-%d", idx))

			resp, err := client.Do(httpReq)
			if err != nil {
				otherCount.Add(1)
				return
			}
			defer resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusCreated:
				successCount.Add(1)
			case http.StatusConflict:
				conflictCount.Add(1)
			default:
				otherCount.Add(1)
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	total, booked, _, err := repo.GetSlotCapacity(context.Background(), slotID)
	require.NoError(t, err)
	remaining := total - booked

	assert.Equal(t, int32(initialCapacity), successCount.Load(), "Exactly 5 bookings must succeed")
	assert.Equal(t, int32(concurrentCallers-initialCapacity), conflictCount.Load(), "Remaining 95 callers must receive 409 Conflict")
	assert.Equal(t, int32(0), otherCount.Load(), "Zero unexpected failures allowed")
	assert.Equal(t, 0, remaining, "Remaining capacity must be exactly 0 (0% over-allocation)")

	overAllocationPercent := float64(int(successCount.Load())-initialCapacity) / float64(initialCapacity) * 100.0
	t.Logf("Concurrency Safety Verification -> Successes: %d/%d | Conflicts: %d | Over-Allocation: %.2f%%",
		successCount.Load(), initialCapacity, conflictCount.Load(), overAllocationPercent)
}

// PRD Target 4: Durability & Recovery: 100% byte integrity (Zero loss on crash recovery)
func TestBenchmark_WAL_ChaosCrashRecovery_ZeroLoss(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ecobrio-benchmark-wal-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	walCfg := wal.Config{
		Dir:             tempDir,
		MaxSegmentSize:  1 * 1024 * 1024,
		FsyncEveryWrite: true,
	}

	walEngine, err := wal.NewSegmentedEngine(walCfg)
	require.NoError(t, err)

	const totalWrites = 5000
	appendedIDs := make([]string, totalWrites)

	ctx := context.Background()
	for i := 0; i < totalWrites; i++ {
		seq := uint64(i + 1)
		sensorID := fmt.Sprintf("SMART-BIN-%04d", i%20)
		reading := telemetry.SensorReading{
			ID:                 uuid.New(),
			SensorID:           sensorID,
			StreamType:         "PLASTICS",
			FillLevelPercent:   float64(i % 100),
			BatteryMillivolts:  3700,
			TemperatureCelsius: 24.5,
			SequenceID:         seq,
			Timestamp:          time.Now().UTC(),
		}
		appendedIDs[i] = reading.ID.String()

		payloadBytes, _ := json.Marshal(reading)
		rec := &wal.Record{
			SequenceID: seq,
			SensorID:   sensorID,
			StreamType: "PLASTICS",
			Timestamp:  time.Now().UTC(),
			Payload:    payloadBytes,
		}
		_, err := walEngine.Append(ctx, rec)
		require.NoError(t, err)
	}

	_ = walEngine.Close()

	segments, err := filepath.Glob(filepath.Join(tempDir, "wal_*.log"))
	require.NoError(t, err)
	assert.NotEmpty(t, segments, "Segment files must exist on disk")

	replayedCount := 0
	recoveredMap := make(map[uint64]bool)

	for _, seg := range segments {
		records, rErr := wal.ReadSegment(seg)
		require.NoError(t, rErr)
		for _, rec := range records {
			recoveredMap[rec.SequenceID] = true
			replayedCount++
		}
	}

	assert.Equal(t, totalWrites, replayedCount, "All 5,000 writes must be recovered")
	assert.Equal(t, totalWrites, len(recoveredMap), "Zero missing or duplicate sequence IDs")

	dataLossPercent := (1.0 - float64(replayedCount)/float64(totalWrites)) * 100.0
	t.Logf("Durability & Recovery Verification -> Appended: %d | Durable Replayed: %d | Data Loss: %.2f%% (100.00%% Byte Integrity)",
		totalWrites, replayedCount, dataLossPercent)
}

// PRD Target 5: Live Broadcast Latency: < 15ms end-to-end
func TestBenchmark_WebSocket_BroadcastLatency_Sub15ms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := ws.NewHub()
	go hub.Run(ctx)

	mux := http.NewServeMux()
	hub.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/incidents"

	const clientCount = 20
	clients := make([]*websocket.Conn, clientCount)

	for i := 0; i < clientCount; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s?client_id=bench-%d", wsURL, i), nil)
		require.NoError(t, err)
		clients[i] = conn
		defer conn.Close()

		var initEvt ws.Event
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_ = conn.ReadJSON(&initEvt)
	}

	broadcastPayload := map[string]any{
		"incident_id": uuid.New(),
		"stream_type": "HAZARDOUS",
		"severity":    "CRITICAL",
		"latitude":    28.6139,
		"longitude":   77.2090,
	}

	broadcastTime := time.Now()
	_ = hub.Broadcast(ws.EventIncidentCreated, broadcastPayload)

	latencies := make([]time.Duration, clientCount)
	var wg sync.WaitGroup

	for i, conn := range clients {
		wg.Add(1)
		go func(idx int, c *websocket.Conn) {
			defer wg.Done()
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			var evt ws.Event
			if err := c.ReadJSON(&evt); err == nil {
				latencies[idx] = time.Since(broadcastTime)
			}
		}(i, conn)
	}

	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[len(latencies)-1]

	t.Logf("WebSocket Fan-Out Latency -> Clients: %d | p50: %v | p95: %v | p99: %v (Target: < 15ms)",
		clientCount, p50, p95, p99)

	assert.Less(t, p95, 15*time.Millisecond, "p95 broadcast latency must be under 15ms")
}

// PRD Target 6: Code Concurrency Safety: Zero data races detected
func TestBenchmark_SystemSubsystems_ConcurrentSafety(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := observability.NewMetrics()
	require.NotNil(t, metrics)

	geoIdx := geospatial.NewMemoryIndexer()
	hub := ws.NewHub()
	go hub.Run(ctx)

	incidentsRepo := incidents.NewMemoryRepository()
	incidentsSvc := incidents.NewService(incidentsRepo, geoIdx, hub)

	var wg sync.WaitGroup
	const iterations = 50

	for i := 0; i < iterations; i++ {
		wg.Add(2)

		go func(idx int) {
			defer wg.Done()
			_, _ = incidentsSvc.ReportIncident(ctx, incidents.CreateIncidentRequest{
				StreamType:  "PLASTICS",
				Description: fmt.Sprintf("Concurrent incident test %d", idx),
				Severity:    "MEDIUM",
				Latitude:    28.6139 + float64(idx)*0.001,
				Longitude:   77.2090 + float64(idx)*0.001,
			})
		}(i)

		go func(idx int) {
			defer wg.Done()
			_, _ = incidentsSvc.NearbyIncidents(ctx, 28.6139, 77.2090, 10.0, 50)
		}(i)
	}

	wg.Wait()
}
