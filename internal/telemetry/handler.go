package telemetry

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"ecobrio/internal/api"
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
	replayed, err := h.service.RunCrashRecovery(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "RECOVERY_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, map[string]any{
		"status":         "RECOVERY_COMPLETE",
		"replayed_count": replayed,
	})
}
