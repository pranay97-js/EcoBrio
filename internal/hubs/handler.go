package hubs

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes hooks hub and slot endpoints into the ServeMux
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/hubs", h.handleCreateHub)
	mux.HandleFunc("GET /api/v1/hubs", h.handleListHubs)
	mux.HandleFunc("GET /api/v1/hubs/{id}", h.handleGetHub)
	mux.HandleFunc("GET /api/v1/hubs/{id}/slots", h.handleGetSlots)
	mux.HandleFunc("POST /api/v1/slots", h.handleCreateSlot)
}

func (h *Handler) handleCreateHub(w http.ResponseWriter, r *http.Request) {
	var req CreateHubRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	hub, err := h.service.CreateHub(r.Context(), req)
	if err != nil {
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create hub")
		return
	}

	api.WriteJSON(w, http.StatusCreated, hub)
}

func (h *Handler) handleListHubs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	lat, _ := strconv.ParseFloat(q.Get("lat"), 64)
	lon, _ := strconv.ParseFloat(q.Get("lon"), 64)
	radius, _ := strconv.ParseFloat(q.Get("radius"), 64)

	hubsList, err := h.service.ListHubs(r.Context(), lat, lon, radius)
	if err != nil {
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve hubs")
		return
	}

	if hubsList == nil {
		hubsList = []Hub{}
	}
	api.WriteJSON(w, http.StatusOK, hubsList)
}

func (h *Handler) handleGetHub(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided hub ID is not a valid UUID")
		return
	}

	hub, err := h.service.GetHub(r.Context(), id)
	if err != nil {
		if errors.Is(err, api.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Hub not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve hub")
		return
	}

	api.WriteJSON(w, http.StatusOK, hub)
}

func (h *Handler) handleGetSlots(w http.ResponseWriter, r *http.Request) {
	hubIDStr := r.PathValue("id")
	hubID, err := uuid.Parse(hubIDStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided hub ID is not a valid UUID")
		return
	}

	slotsList, err := h.service.GetSlotsByHub(r.Context(), hubID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve slots")
		return
	}

	if slotsList == nil {
		slotsList = []Slot{}
	}
	api.WriteJSON(w, http.StatusOK, slotsList)
}

func (h *Handler) handleCreateSlot(w http.ResponseWriter, r *http.Request) {
	var req CreateSlotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	slot, err := h.service.CreateSlot(r.Context(), req)
	if err != nil {
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		if errors.Is(err, api.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Referenced hub not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create slot")
		return
	}

	api.WriteJSON(w, http.StatusCreated, slot)
}
