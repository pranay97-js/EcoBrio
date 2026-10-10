package credits

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"ecobrio/internal/api"
	"ecobrio/internal/ledger"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes registers HTTP routes with the ServeMux
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/submissions", h.handleCreateSubmission)
	mux.HandleFunc("GET /api/v1/submissions/{id}", h.handleGetSubmission)
	mux.HandleFunc("POST /api/v1/submissions/{id}/inspection", h.handleInspectSubmission)
	mux.HandleFunc("GET /api/v1/credits/me", h.handleGetCitizenProfile)
	mux.HandleFunc("POST /api/v1/credits/redeem", h.handleRedeemPerk)
}

func (h *Handler) handleCreateSubmission(w http.ResponseWriter, r *http.Request) {
	var req CreateSubmissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	// Fallback citizen ID from header if missing in body
	if req.CitizenID == uuid.Nil {
		if headerID := r.Header.Get("X-Citizen-ID"); headerID != "" {
			if parsed, err := uuid.Parse(headerID); err == nil {
				req.CitizenID = parsed
			}
		}
	}

	sub, err := h.service.CreateSubmission(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrDuplicateQR) {
			api.WriteError(w, http.StatusConflict, "DUPLICATE_QR", err.Error())
			return
		}
		api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusCreated, sub)
}

func (h *Handler) handleGetSubmission(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided submission ID is not a valid UUID")
		return
	}

	sub, err := h.service.GetSubmission(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrSubmissionNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Submission not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve submission")
		return
	}

	api.WriteJSON(w, http.StatusOK, sub)
}

func (h *Handler) handleInspectSubmission(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided submission ID is not a valid UUID")
		return
	}

	var req InspectSubmissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	sub, err := h.service.InspectSubmission(r.Context(), id, req)
	if err != nil {
		if errors.Is(err, ErrSubmissionNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Submission not found")
			return
		}
		if errors.Is(err, ErrInvalidTransition) {
			api.WriteError(w, http.StatusConflict, "INVALID_TRANSITION", err.Error())
			return
		}
		api.WriteError(w, http.StatusBadRequest, "INSPECTION_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, sub)
}

func (h *Handler) handleGetCitizenProfile(w http.ResponseWriter, r *http.Request) {
	var citizenID uuid.UUID
	var err error

	headerID := r.Header.Get("X-Citizen-ID")
	if headerID != "" {
		citizenID, err = uuid.Parse(headerID)
	} else if qID := r.URL.Query().Get("citizen_id"); qID != "" {
		citizenID, err = uuid.Parse(qID)
	} else {
		api.WriteError(w, http.StatusBadRequest, "MISSING_CITIZEN_ID", "X-Citizen-ID header or citizen_id parameter is required")
		return
	}

	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Provided citizen ID is not a valid UUID")
		return
	}

	profile, err := h.service.GetCitizenProfile(r.Context(), citizenID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve credit profile")
		return
	}

	api.WriteJSON(w, http.StatusOK, profile)
}

func (h *Handler) handleRedeemPerk(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	if idempotencyKey == "" {
		api.WriteError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "X-Idempotency-Key header is required for perk redemptions")
		return
	}

	var req RedeemPerkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to parse JSON body")
		return
	}

	// Fallback citizen ID from header if missing in body
	if req.CitizenID == uuid.Nil {
		if headerID := r.Header.Get("X-Citizen-ID"); headerID != "" {
			if parsed, err := uuid.Parse(headerID); err == nil {
				req.CitizenID = parsed
			}
		}
	}

	res, err := h.service.RedeemPerkSaga(r.Context(), req, idempotencyKey)
	if err != nil {
		if errors.Is(err, ledger.ErrInsufficientBalance) {
			api.WriteError(w, http.StatusUnprocessableEntity, "INSUFFICIENT_BALANCE", err.Error())
			return
		}
		if errors.Is(err, ErrVoucherFailed) {
			api.WriteError(w, http.StatusBadGateway, "VOUCHER_PROVIDER_FAILED", err.Error())
			return
		}
		api.WriteError(w, http.StatusBadRequest, "REDEMPTION_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, res)
}
