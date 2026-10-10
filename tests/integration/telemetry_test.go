package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"ecobrio/internal/telemetry"
	"ecobrio/internal/telemetry/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTelemetryTestServer(t *testing.T) (*httptest.Server, *telemetry.Service, *telemetry.MemoryRepository, string) {
	tempDir, err := os.MkdirTemp("", "telemetry_integ_*")
	require.NoError(t, err)

	cfg := wal.Config{
		Dir:             tempDir,
		MaxSegmentSize:  1024 * 1024,
		FsyncEveryWrite: true,
	}

	walEngine, err := wal.NewSegmentedEngine(cfg)
	require.NoError(t, err)

	cpStore := wal.NewMemoryCheckpointStore()
	memRepo := telemetry.NewMemoryRepository()

	svc := telemetry.NewService(memRepo, walEngine, cpStore)
	handler := telemetry.NewHandler(svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	metrics := observability.NewMetrics()
	wrapped := middleware.Recovery(middleware.LoggingAndMetrics(metrics, mux))

	server := httptest.NewServer(wrapped)
	return server, svc, memRepo, tempDir
}

func TestTelemetry_JSONIngestion_FullFlow(t *testing.T) {
	server, _, _, tempDir := setupTelemetryTestServer(t)
	defer server.Close()
	defer os.RemoveAll(tempDir)

	client := server.Client()
	sensorID := "SMART-BIN-CENTRAL-01"

	// 1. Ingest normal telemetry reading (Fill: 42.5%)
	ingestReq := telemetry.IngestRequest{
		SensorID:           sensorID,
		SequenceID:         1,
		StreamType:         "EWASTE",
		FillLevelPercent:   42.5,
		BatteryMillivolts:  3350,
		TemperatureCelsius: 22.0,
		TimestampUnixMs:    time.Now().UnixMilli(),
	}
	bodyBytes, _ := json.Marshal(ingestReq)

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var created telemetry.SensorReading
	_ = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()
	assert.Equal(t, sensorID, created.SensorID)
	assert.Equal(t, uint64(1), created.SequenceID)

	// 2. GET /api/v1/telemetry/sensors/{sensor_id}/latest -> Not alerting (< 80%)
	latestReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/api/v1/telemetry/sensors/%s/latest", server.URL, sensorID), nil)
	latestResp, err := client.Do(latestReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, latestResp.StatusCode)

	var latest telemetry.LatestReadingResponse
	_ = json.NewDecoder(latestResp.Body).Decode(&latest)
	_ = latestResp.Body.Close()
	assert.Equal(t, sensorID, latest.SensorID)
	assert.False(t, latest.IsAlerting)

	// 3. Ingest high-fill telemetry reading (Fill: 85.0% -> High Capacity Alert)
	highFillReq := telemetry.IngestRequest{
		SensorID:           sensorID,
		SequenceID:         2,
		StreamType:         "EWASTE",
		FillLevelPercent:   85.0,
		BatteryMillivolts:  3340,
		TemperatureCelsius: 23.5,
		TimestampUnixMs:    time.Now().UnixMilli(),
	}
	highBytes, _ := json.Marshal(highFillReq)
	req2, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest", bytes.NewReader(highBytes))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := client.Do(req2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp2.StatusCode)
	_ = resp2.Body.Close()

	// 4. GET latest -> IsAlerting must be TRUE (>= 80%)
	latestResp2, err := client.Do(latestReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, latestResp2.StatusCode)

	var latestAlert telemetry.LatestReadingResponse
	_ = json.NewDecoder(latestResp2.Body).Decode(&latestAlert)
	_ = latestResp2.Body.Close()
	assert.True(t, latestAlert.IsAlerting, "Fill level >= 80% must trigger alert state")

	// 5. Ingest invalid fill level (> 100%) -> 400 Bad Request
	badReq := telemetry.IngestRequest{
		SensorID:         sensorID,
		SequenceID:       3,
		StreamType:       "EWASTE",
		FillLevelPercent: 120.0,
	}
	badBytes, _ := json.Marshal(badReq)
	reqBad, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest", bytes.NewReader(badBytes))
	reqBad.Header.Set("Content-Type", "application/json")
	badResp, err := client.Do(reqBad)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, badResp.StatusCode)
	_ = badResp.Body.Close()
}

func TestTelemetry_BinaryPacketIngestion_CRC32(t *testing.T) {
	server, _, _, tempDir := setupTelemetryTestServer(t)
	defer server.Close()
	defer os.RemoveAll(tempDir)

	client := server.Client()

	reading := &telemetry.SensorReading{
		SensorID:           "HARDWARE-BEACON-99",
		SequenceID:         505,
		StreamType:         "METALS",
		FillLevelPercent:   62.4,
		BatteryMillivolts:  3280,
		TemperatureCelsius: 19.8,
		Timestamp:          time.Now(),
	}

	validBytes, err := telemetry.EncodePacket(reading)
	require.NoError(t, err)

	// 1. Post valid binary packet
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest-binary", bytes.NewReader(validBytes))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var ingested telemetry.SensorReading
	_ = json.NewDecoder(resp.Body).Decode(&ingested)
	_ = resp.Body.Close()
	assert.Equal(t, "HARDWARE-BEACON-99", ingested.SensorID)
	assert.Equal(t, uint64(505), ingested.SequenceID)

	// 2. Post corrupt binary packet (bit flipped)
	corruptBytes := make([]byte, len(validBytes))
	copy(corruptBytes, validBytes)
	corruptBytes[10] ^= 0xEE // Flip bits to fail CRC-32

	reqCorrupt, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest-binary", bytes.NewReader(corruptBytes))
	reqCorrupt.Header.Set("Content-Type", "application/octet-stream")
	respCorrupt, err := client.Do(reqCorrupt)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, respCorrupt.StatusCode)
	_ = respCorrupt.Body.Close()
}

func TestTelemetry_CrashRecoveryReplay_HTTP(t *testing.T) {
	server, svc, _, tempDir := setupTelemetryTestServer(t)
	defer server.Close()
	defer os.RemoveAll(tempDir)

	client := server.Client()
	sensorID := "DISASTER-RECOVERY-BIN"

	// 1. Ingest 5 readings
	for i := uint64(1); i <= 5; i++ {
		ingestReq := telemetry.IngestRequest{
			SensorID:           sensorID,
			SequenceID:         i,
			StreamType:         "PLASTICS",
			FillLevelPercent:   float64(i * 15),
			BatteryMillivolts:  3300,
			TemperatureCelsius: 20.0,
			TimestampUnixMs:    time.Now().UnixMilli(),
		}
		bodyBytes, _ := json.Marshal(ingestReq)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/telemetry/ingest", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		_ = resp.Body.Close()
	}

	// 2. Simulate server memory crash: wipe repository in-memory map
	// The WAL on disk in tempDir still holds all 5 frames
	newMemRepo := telemetry.NewMemoryRepository()
	ctx := context.Background()
	// Create a new service bound to the fresh repo and same WAL
	reopenedService := telemetry.NewService(newMemRepo, svc.WALEngine(), svc.CheckpointStore())
	replayed, err := reopenedService.RunCrashRecovery(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, replayed, "Crash recovery must replay all 5 uncommitted WAL records into memory repo")

	// 3. Verify reading is restored in the new repository
	latest, err := newMemRepo.GetLatestReading(ctx, sensorID)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), latest.SequenceID)
	assert.Equal(t, 75.0, latest.FillLevelPercent)
}
