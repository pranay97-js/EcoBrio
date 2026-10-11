package booking

import (
	"encoding/json"
	"errors"
	"net/http"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes connects booking endpoints to the ServeMux
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/bookings", h.handleCreateBooking)
	mux.HandleFunc("GET /api/v1/bookings/{id}", h.handleGetBooking)
	mux.HandleFunc("GET /api/v1/bookings/by-request/{key}", h.handleGetBookingByRequestKey)
}

func (h *Handler) handleCreateBooking(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("X-Idempotency-Key")
	if idempotencyKey == "" {
		api.WriteError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "X-Idempotency-Key header is required")
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	booking, err := h.service.ReserveSlot(r.Context(), req, idempotencyKey)
	if err != nil {
		if errors.Is(err, ErrCapacityExceeded) {
			api.WriteError(w, http.StatusConflict, "CAPACITY_EXHAUSTED", "Slot capacity has been completely booked")
			return
		}
		if errors.Is(err, ErrIdempotencyConflict) {
			api.WriteError(w, http.StatusUnprocessableEntity, "IDEMPOTENCY_KEY_REUSED", "Idempotency key has already been used for a different request")
			return
		}
		if errors.Is(err, ErrSlotNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Collection slot not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
		return
	}

	api.WriteJSON(w, http.StatusCreated, booking)
}

func (h *Handler) handleGetBooking(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided booking ID is not a valid UUID")
		return
	}

	booking, err := h.service.GetBooking(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrBookingNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Booking not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve booking")
		return
	}

	api.WriteJSON(w, http.StatusOK, booking)
}

func (h *Handler) handleGetBookingByRequestKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		api.WriteError(w, http.StatusBadRequest, "INVALID_KEY", "Key is required")
		return
	}

	booking, err := h.service.GetBookingByRequestKey(r.Context(), key)
	if err != nil {
		if errors.Is(err, ErrBookingNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Booking not found for idempotency key")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve booking")
		return
	}

	api.WriteJSON(w, http.StatusOK, booking)
}
