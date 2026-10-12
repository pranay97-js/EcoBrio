package incidents

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes attaches incident endpoints to the ServeMux
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/incidents", h.handleCreateIncident)
	mux.HandleFunc("GET /api/v1/incidents", h.handleListIncidents)
	mux.HandleFunc("GET /api/v1/incidents/nearby", h.handleNearbyIncidents)
	mux.HandleFunc("GET /api/v1/incidents/snapshot", h.handleGetSnapshot)
	mux.HandleFunc("POST /api/v1/incidents/reindex", h.handleRebuildIndex)
	mux.HandleFunc("GET /api/v1/incidents/{id}", h.handleGetIncident)
	mux.HandleFunc("PATCH /api/v1/incidents/{id}/status", h.handleUpdateStatus)
}

func (h *Handler) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	var req CreateIncidentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	inc, err := h.service.ReportIncident(r.Context(), req)
	if err != nil {
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create incident")
		return
	}

	api.WriteJSON(w, http.StatusCreated, inc)
}

func (h *Handler) handleListIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))

	filter := Filter{
		Status:     q.Get("status"),
		StreamType: q.Get("stream_type"),
		Limit:      limit,
	}

	incidentsList, err := h.service.ListIncidents(r.Context(), filter)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve incidents")
		return
	}

	if incidentsList == nil {
		incidentsList = []Incident{}
	}
	api.WriteJSON(w, http.StatusOK, incidentsList)
}

func (h *Handler) handleNearbyIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	lat, err := strconv.ParseFloat(q.Get("lat"), 64)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_PARAM", "Query param 'lat' is required and must be numeric")
		return
	}
	lon, err := strconv.ParseFloat(q.Get("lon"), 64)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_PARAM", "Query param 'lon' is required and must be numeric")
		return
	}

	radiusKm, _ := strconv.ParseFloat(q.Get("radius"), 64)
	if radiusKm <= 0 {
		radiusKm = 10.0 // Default 10km radius
	}

	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}

	nearby, err := h.service.NearbyIncidents(r.Context(), lat, lon, radiusKm, limit)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query nearby incidents")
		return
	}

	if nearby == nil {
		nearby = []Incident{}
	}
	api.WriteJSON(w, http.StatusOK, nearby)
}

func (h *Handler) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	state, seq, err := h.service.GetActiveSnapshot(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to capture active incident snapshot")
		return
	}

	api.WriteJSON(w, http.StatusOK, map[string]any{
		"sequence": seq,
		"snapshot": state,
	})
}

func (h *Handler) handleRebuildIndex(w http.ResponseWriter, r *http.Request) {
	reindexed, err := h.service.RebuildGeoIndex(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to rebuild geospatial index")
		return
	}

	api.WriteJSON(w, http.StatusOK, map[string]any{
		"status":          "INDEX_REBUILT",
		"reindexed_count": reindexed,
	})
}

func (h *Handler) handleGetIncident(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided incident ID is not a valid UUID")
		return
	}

	inc, err := h.service.GetIncident(r.Context(), id)
	if err != nil {
		if errors.Is(err, api.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Incident not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve incident")
		return
	}

	api.WriteJSON(w, http.StatusOK, inc)
}

func (h *Handler) handleUpdateStatus(w http.ResponseWriter, r *http.Request) {
	// RBAC Enforcement: Only municipal administrators have permission to update or resolve incidents
	userRole := strings.ToUpper(strings.TrimSpace(r.Header.Get("X-User-Role")))
	if userRole != "" && userRole != "ADMIN" {
		api.WriteError(w, http.StatusForbidden, "FORBIDDEN", "Only Municipal Administrators have permission to modify incident state")
		return
	}

	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided incident ID is not a valid UUID")
		return
	}

	var req UpdateStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	updated, err := h.service.UpdateStatus(r.Context(), id, req.Status, req.AssigneeID)
	if err != nil {
		if errors.Is(err, api.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Incident not found")
			return
		}
		if errors.Is(err, api.ErrInvalidTransition) {
			api.WriteError(w, http.StatusConflict, "ILLEGAL_TRANSITION", err.Error())
			return
		}
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update incident status")
		return
	}

	api.WriteJSON(w, http.StatusOK, updated)
}
