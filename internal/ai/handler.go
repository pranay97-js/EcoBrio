package ai

import (
	"encoding/json"
	"errors"
	"net/http"

	"ecobrio/internal/api"
	"github.com/google/uuid"
)

type Handler struct {
	engine        Engine
	intakeService *IntakeService
	tools         *ToolRegistry
}

func NewHandler(engine Engine, intakeService *IntakeService, tools *ToolRegistry) *Handler {
	return &Handler{
		engine:        engine,
		intakeService: intakeService,
		tools:         tools,
	}
}

// RegisterRoutes mounts AI triage, chat, and tool endpoints
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/triage", h.handleSyncTriage)
	mux.HandleFunc("POST /api/v1/triage/async", h.handleAsyncIntake)
	mux.HandleFunc("GET /api/v1/triage/intake/{id}", h.handleGetIntakeStatus)
	mux.HandleFunc("POST /api/v1/ai/tools/{toolName}", h.handleDirectToolExecution)
	mux.HandleFunc("POST /api/v1/ai/chat", h.handleChat)
}

type TriageAPIRequest struct {
	Text        string      `json:"text"`
	Coordinates *[2]float64 `json:"coordinates,omitempty"`
	CitizenID   *uuid.UUID  `json:"citizen_id,omitempty"`
}

func (h *Handler) handleSyncTriage(w http.ResponseWriter, r *http.Request) {
	var req TriageAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to decode triage request")
		return
	}

	session := h.resolveSession(req)
	res, err := h.engine.ProcessPrompt(r.Context(), session, req.Text)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "TRIAGE_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) handleAsyncIntake(w http.ResponseWriter, r *http.Request) {
	var req TriageAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to decode triage request")
		return
	}

	session := h.resolveSession(req)
	intake, err := h.intakeService.SubmitAsyncIntake(r.Context(), session, req.Text, req.Coordinates)
	if err != nil {
		if errors.Is(err, api.ErrInvalidInput) {
			api.WriteError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTAKE_FAILED", err.Error())
		return
	}

	api.WriteJSON(w, http.StatusAccepted, map[string]any{
		"intake_id": intake.ID,
		"status":    intake.Status,
		"stream":    intake.ExtractedStream,
		"severity":  intake.ExtractedSeverity,
		"message":   "Citizen report accepted and queued for asynchronous verification.",
	})
}

func (h *Handler) handleGetIntakeStatus(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_UUID", "Intake ID must be a valid UUID")
		return
	}

	intake, err := h.intakeService.GetIntake(r.Context(), id)
	if err != nil {
		if errors.Is(err, api.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Intake record not found")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve intake status")
		return
	}

	api.WriteJSON(w, http.StatusOK, intake)
}

func (h *Handler) handleDirectToolExecution(w http.ResponseWriter, r *http.Request) {
	toolName := r.PathValue("toolName")
	session := h.resolveSession(TriageAPIRequest{})

	switch toolName {
	case ToolReportWasteIncident:
		var params ReportIncidentParams
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", err.Error())
			return
		}
		inc, err := h.tools.ExecuteReportWasteIncident(r.Context(), session, params)
		if err != nil {
			api.WriteError(w, http.StatusBadRequest, "TOOL_ERROR", err.Error())
			return
		}
		api.WriteJSON(w, http.StatusCreated, inc)

	case ToolFindNearestHub:
		var params FindNearestHubParams
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", err.Error())
			return
		}
		hubsList, err := h.tools.ExecuteFindNearestHub(r.Context(), params)
		if err != nil {
			api.WriteError(w, http.StatusBadRequest, "TOOL_ERROR", err.Error())
			return
		}
		api.WriteJSON(w, http.StatusOK, hubsList)

	case ToolGetUserCredits:
		summary, err := h.tools.ExecuteGetUserCredits(r.Context(), session)
		if err != nil {
			api.WriteError(w, http.StatusBadRequest, "TOOL_ERROR", err.Error())
			return
		}
		api.WriteJSON(w, http.StatusOK, summary)

	default:
		api.WriteError(w, http.StatusNotFound, "TOOL_NOT_FOUND", "Unrecognized or unauthorized tool")
	}
}

func (h *Handler) resolveSession(req TriageAPIRequest) SessionContext {
	citizenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	if req.CitizenID != nil && *req.CitizenID != uuid.Nil {
		citizenID = *req.CitizenID
	}

	return SessionContext{
		CitizenID:          citizenID,
		CitizenRole:        "CITIZEN",
		DefaultCoordinates: req.Coordinates,
	}
}

func (h *Handler) handleChat(w http.ResponseWriter, r *http.Request) {
	var req TriageAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "MALFORMED_JSON", "Failed to decode chat request")
		return
	}

	session := h.resolveSession(req)
	if orEngine, ok := h.engine.(*OpenRouterEngine); ok {
		chatRes, err := orEngine.Chat(r.Context(), session, req.Text)
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "CHAT_FAILED", err.Error())
			return
		}
		api.WriteJSON(w, http.StatusOK, chatRes)
		return
	}

	res, err := h.engine.ProcessPrompt(r.Context(), session, req.Text)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "CHAT_FAILED", err.Error())
		return
	}
	api.WriteJSON(w, http.StatusOK, ChatResponse{
		Message:           res.Message,
		ActionTaken:       res.ActionTaken,
		ExtractedStream:   res.ExtractedStream,
		ExtractedSeverity: res.ExtractedSeverity,
		ToolName:          res.ToolName,
		IncidentID:        res.IncidentID,
	})
}
