package ai

import (
	"context"
	"fmt"
	"strings"

	"ecobrio/internal/api"
	"ecobrio/internal/credits"
	"ecobrio/internal/hubs"
	"ecobrio/internal/incidents"
	"github.com/google/uuid"
)

// Standard Go tool names
const (
	ToolReportWasteIncident = "report_waste_incident"
	ToolFindNearestHub      = "find_nearest_hub"
	ToolGetUserCredits      = "get_user_credits"
)

// SessionContext represents the authenticated caller's security boundary
type SessionContext struct {
	CitizenID          uuid.UUID
	CitizenRole        string
	DefaultCoordinates *[2]float64 // [lat, lon]
}

// ToolRegistry manages execution of domain tools with strict security guardrails
type ToolRegistry struct {
	incidentsService *incidents.Service
	hubsService      *hubs.Service
	creditsService   *credits.Service
}

// NewToolRegistry initializes the deterministic Go tool registry
func NewToolRegistry(incidentsSvc *incidents.Service, hubsSvc *hubs.Service, creditsSvc *credits.Service) *ToolRegistry {
	return &ToolRegistry{
		incidentsService: incidentsSvc,
		hubsService:      hubsSvc,
		creditsService:   creditsSvc,
	}
}

// ReportIncidentParams defines the typed parameters for report_waste_incident
type ReportIncidentParams struct {
	Description        string   `json:"description"`
	ReportedLocation   string   `json:"reported_location,omitempty"`
	CategorySuggestion string   `json:"category_suggestion"`
	Severity           string   `json:"severity"`
	Latitude           float64  `json:"latitude"`
	Longitude          float64  `json:"longitude"`
}

// ExecuteReportWasteIncident enforces backend caller authorization and validates inputs
func (r *ToolRegistry) ExecuteReportWasteIncident(ctx context.Context, session SessionContext, params ReportIncidentParams) (*incidents.Incident, error) {
	if r.incidentsService == nil {
		return nil, fmt.Errorf("incidents service not available")
	}

	desc := strings.TrimSpace(params.Description)
	if desc == "" {
		return nil, fmt.Errorf("%w: incident description is required", api.ErrInvalidInput)
	}

	// Validate geographic coordinates
	if params.Latitude < -90 || params.Latitude > 90 {
		return nil, fmt.Errorf("%w: latitude must be between -90 and 90", api.ErrInvalidInput)
	}
	if params.Longitude < -180 || params.Longitude > 180 {
		return nil, fmt.Errorf("%w: longitude must be between -180 and 180", api.ErrInvalidInput)
	}

	stream := strings.ToUpper(strings.TrimSpace(params.CategorySuggestion))
	if !hubs.ValidWasteStreams[stream] {
		return nil, fmt.Errorf("%w: invalid waste stream '%s'", api.ErrInvalidInput, params.CategorySuggestion)
	}

	severity := strings.ToUpper(strings.TrimSpace(params.Severity))
	if severity == "" {
		severity = incidents.SeverityMedium
	}
	if !incidents.ValidSeverities[severity] {
		return nil, fmt.Errorf("%w: invalid severity '%s'", api.ErrInvalidInput, params.Severity)
	}

	// Security guardrail: Caller identity is strictly bound to authenticated session
	return r.incidentsService.ReportIncident(ctx, incidents.CreateIncidentRequest{
		ReporterID:  &session.CitizenID,
		StreamType:  stream,
		Description: desc,
		Latitude:    params.Latitude,
		Longitude:   params.Longitude,
		Severity:    severity,
	})
}

// FindNearestHubParams defines parameters for find_nearest_hub
type FindNearestHubParams struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	RadiusKm  float64 `json:"radius_km,omitempty"`
}

// ExecuteFindNearestHub queries eligible collection hubs within a spatial radius
func (r *ToolRegistry) ExecuteFindNearestHub(ctx context.Context, params FindNearestHubParams) ([]hubs.Hub, error) {
	if r.hubsService == nil {
		return nil, fmt.Errorf("hubs service not available")
	}

	// Validate coordinates guardrail
	if params.Latitude < -90 || params.Latitude > 90 || params.Longitude < -180 || params.Longitude > 180 {
		return nil, fmt.Errorf("%w: coordinates out of valid range [-90..90, -180..180]", api.ErrInvalidInput)
	}

	radius := params.RadiusKm
	if radius <= 0 {
		radius = 15.0 // Default 15km
	}

	return r.hubsService.ListHubs(ctx, params.Latitude, params.Longitude, radius)
}

// CitizenCreditSummary represents the caller's authorized account balance
type CitizenCreditSummary struct {
	CitizenID      uuid.UUID `json:"citizen_id"`
	AccountBalance int64     `json:"account_balance"`
	PointsActive   int64     `json:"points_active"`
}

// ExecuteGetUserCredits reads caller balance strictly scoped to authenticated session
func (r *ToolRegistry) ExecuteGetUserCredits(ctx context.Context, session SessionContext) (*CitizenCreditSummary, error) {
	if r.creditsService == nil {
		return nil, fmt.Errorf("credits service not available")
	}

	// Security guardrail: Caller ID comes strictly from session, rejecting arbitrary citizen IDs
	if session.CitizenID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing authenticated citizen identity", api.ErrUnauthorized)
	}

	profile, err := r.creditsService.GetCitizenProfile(ctx, session.CitizenID)
	if err != nil {
		return nil, err
	}

	return &CitizenCreditSummary{
		CitizenID:      session.CitizenID,
		AccountBalance: profile.BalancePoints,
		PointsActive:   profile.BalancePoints,
	}, nil
}
