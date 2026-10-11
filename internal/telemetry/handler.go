package telemetry

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"ecobrio/internal/api"
	"ecobrio/internal/telemetry/snapshots"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes binds telemetry endpoints to the ServeMux
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/telemetry/ingest", h.handleIngestJSON)
	mux.HandleFunc("POST /api/v1/telemetry/ingest-binary", h.handleIngestBinary)
	mux.HandleFunc("GET /api/v1/telemetry/sensors/{sensor_id}/latest", h.handleGetLatest)
	mux.HandleFunc("POST /api/v1/telemetry/recovery", h.handleTriggerRecovery)
	mux.HandleFunc("POST /api/v1/telemetry/snapshot", h.handleTriggerSnapshot)
	mux.HandleFunc("GET /api/v1/telemetry/snapshots/latest", h.handleGetLatestSnapshot)
}

func (h *Handler) handleIngestJSON(w http.ResponseWriter, r *http.Request) {
	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse telemetry JSON body")
		return
	}

	reading, err := h.service.IngestReading(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrInvalidFillLevel) || errors.Is(err, ErrInvalidStreamType) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INGESTION_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusCreated, reading)
}

func (h *Handler) handleIngestBinary(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "READ_ERROR", "Failed to read binary packet body")
		return
	}

	reading, err := h.service.IngestBinaryPacket(r.Context(), body)
	if err != nil {
		if errors.Is(err, ErrCorruptPacket) {
			api.WriteError(w, http.StatusBadRequest, "CORRUPT_PACKET", err.Error())
			return
		}
		api.WriteError(w, http.StatusBadRequest, "INVALID_PACKET", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusCreated, reading)
}

func (h *Handler) handleGetLatest(w http.ResponseWriter, r *http.Request) {
	sensorID := r.PathValue("sensor_id")
	if sensorID == "" {
		api.WriteError(w, http.StatusBadRequest, "MISSING_SENSOR_ID", "sensor_id is required")
		return
	}

	resp, err := h.service.GetLatestReading(r.Context(), sensorID)
	if err != nil {
		if errors.Is(err, ErrSensorNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "No telemetry found for sensor")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch telemetry")
		return
	}

	api.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleTriggerRecovery(w http.ResponseWriter, r *http.Request) {
	restoredSnap, replayedWAL, err := h.service.RunCrashRecoveryWithSnapshot(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "RECOVERY_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, map[string]any{
		"status":                 "RECOVERY_COMPLETE",
		"restored_from_snapshot": restoredSnap,
		"replayed_from_wal":      replayedWAL,
		"total_recovered":        restoredSnap + replayedWAL,
	})
}

func (h *Handler) handleTriggerSnapshot(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.CompactAndSnapshot(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "COMPACTION_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, result)
}

func (h *Handler) handleGetLatestSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := h.service.GetLatestSnapshot(r.Context())
	if err != nil {
		if errors.Is(err, snapshots.ErrNoSnapshotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "No snapshot available yet")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load latest snapshot")
		return
	}

	api.WriteJSON(w, http.StatusOK, snap)
}
