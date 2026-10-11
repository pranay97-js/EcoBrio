package ai

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"ecobrio/internal/incidents"
	"github.com/google/uuid"
)

// Actions taken by the AI triage engine
const (
	ActionToolExecuted          = "TOOL_EXECUTED"
	ActionClarificationRequired = "CLARIFICATION_REQUIRED"
	ActionIntakeQueued          = "INTAKE_QUEUED"
)

// TriageResponse models the structured output returned to caller and UI
type TriageResponse struct {
	ActionTaken     string     `json:"action_taken"`
	ToolName        string     `json:"tool_name,omitempty"`
	ToolResult      any        `json:"tool_result,omitempty"`
	Message         string     `json:"message"`
	IntakeID        *uuid.UUID `json:"intake_id,omitempty"`
	Status          string     `json:"status,omitempty"`
	IncidentID      *uuid.UUID `json:"incident_id,omitempty"`
	ConfidenceScore float64    `json:"confidence_score"`
	ExtractedStream string     `json:"extracted_stream,omitempty"`
	ExtractedSeverity string   `json:"extracted_severity,omitempty"`
}

// Engine defines the interface for KachraSeth AI processing
type Engine interface {
	ProcessPrompt(ctx context.Context, session SessionContext, text string) (*TriageResponse, error)
}

// RuleBasedEngine provides deterministic NLP, tool selection, and guardrail enforcement
type RuleBasedEngine struct {
	tools *ToolRegistry
}

// NewRuleBasedEngine initializes an offline deterministic AI triage engine
func NewRuleBasedEngine(tools *ToolRegistry) *RuleBasedEngine {
	return &RuleBasedEngine{tools: tools}
}

// ProcessPrompt classifies user input, checks ambiguity, and triggers appropriate domain tool
func (e *RuleBasedEngine) ProcessPrompt(ctx context.Context, session SessionContext, text string) (*TriageResponse, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return &TriageResponse{
			ActionTaken: ActionClarificationRequired,
			Message:     "Please describe the waste situation or ask a question regarding hubs or credits.",
		}, nil
	}

	lower := strings.ToLower(trimmed)

	// 1. Intent: User Credits / Rewards Query
	if isCreditIntent(lower) {
		res, err := e.tools.ExecuteGetUserCredits(ctx, session)
		if err != nil {
			return nil, err
		}
		return &TriageResponse{
			ActionTaken:     ActionToolExecuted,
			ToolName:        ToolGetUserCredits,
			ToolResult:      res,
			Message:         fmt.Sprintf("You currently have %d EcoTokens active in your authorized account ledger.", res.AccountBalance),
			ConfidenceScore: 0.99,
		}, nil
	}

	// 2. Intent: Nearest Collection Hub Query
	if isHubSearchIntent(lower) {
		lat, lon := extractCoordinates(trimmed, session)
		res, err := e.tools.ExecuteFindNearestHub(ctx, FindNearestHubParams{
			Latitude:  lat,
			Longitude: lon,
			RadiusKm:  15.0,
		})
		if err != nil {
			return nil, err
		}
		hubCount := len(res)
		msg := fmt.Sprintf("Found %d collection hubs within 15 km of your location.", hubCount)
		if hubCount > 0 {
			msg = fmt.Sprintf("Found %d collection hubs. Nearest is %s located at (%.4f, %.4f).", hubCount, res[0].Name, res[0].Latitude, res[0].Longitude)
		}
		return &TriageResponse{
			ActionTaken:     ActionToolExecuted,
			ToolName:        ToolFindNearestHub,
			ToolResult:      res,
			Message:         msg,
			ConfidenceScore: 0.98,
		}, nil
	}

	// 3. Intent: Waste Incident Report
	stream, streamConf := classifyStream(lower)
	if stream == "" {
		// Ambiguous waste category -> Ask clarification guardrail
		return &TriageResponse{
			ActionTaken:     ActionClarificationRequired,
			Message:         "Could you specify what kind of waste materials were found (e.g., plastics, organic, electronic, metal, glass, paper, or hazardous chemical)?",
			ConfidenceScore: 0.40,
		}, nil
	}

	severity := classifySeverity(lower)
	lat, lon := extractCoordinates(trimmed, session)

	// Execute incident reporting tool
	inc, err := e.tools.ExecuteReportWasteIncident(ctx, session, ReportIncidentParams{
		Description:        trimmed,
		CategorySuggestion: stream,
		Severity:           severity,
		Latitude:           lat,
		Longitude:          lon,
	})
	if err != nil {
		return nil, err
	}

	return &TriageResponse{
		ActionTaken:       ActionToolExecuted,
		ToolName:          ToolReportWasteIncident,
		ToolResult:        inc,
		Message:           fmt.Sprintf("Incident #%s has been successfully validated and logged as %s stream (%s severity).", inc.ID.String()[:8], stream, severity),
		IncidentID:        &inc.ID,
		Status:            inc.Status,
		ConfidenceScore:   streamConf,
		ExtractedStream:   stream,
		ExtractedSeverity: severity,
	}, nil
}

func isCreditIntent(s string) bool {
	keywords := []string{"credit", "balance", "token", "points", "reward", "my points", "wallet"}
	for _, kw := range keywords {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

func isHubSearchIntent(s string) bool {
	if strings.Contains(s, "hub") {
		if matchesAny(s, "near", "close", "find", "locate", "where", "drop", "search", "center", "facility") {
			return true
		}
	}
	keywords := []string{"collection point", "where to drop", "where can i drop", "recycling center", "recycling bin near", "drop off my recycling"}
	for _, kw := range keywords {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

func classifyStream(s string) (string, float64) {
	if matchesAny(s, "battery", "batteries", "acid", "chemical", "toxic", "flammable", "pesticide", "paint", "asbestos", "hazard") {
		return "HAZARDOUS", 0.98
	}
	if matchesAny(s, "laptop", "computer", "phone", "circuit", "pcb", "electronic", "wire", "cable", "cpu", "screen", "e-waste", "ewaste") {
		return "EWASTE", 0.97
	}
	if matchesAny(s, "food", "compost", "kitchen", "leaves", "garden", "trimmings", "organic", "rotting", "fruit", "vegetable") {
		return "ORGANIC", 0.96
	}
	if matchesAny(s, "plastic", "polymer", "pvc", "pet", "bottle", "bag", "packaging", "polystyrene", "straw") {
		return "PLASTICS", 0.95
	}
	if matchesAny(s, "copper", "aluminum", "steel", "iron", "metal", "can", "alloy", "scrap metal", "tin") {
		return "METALS", 0.96
	}
	if matchesAny(s, "glass", "broken bottle", "jar", "cullet", "silica", "mirror") {
		return "GLASS", 0.95
	}
	if matchesAny(s, "cardboard", "box", "paper", "newspaper", "magazine", "carton") {
		return "PAPER", 0.94
	}
	return "", 0.0
}

func classifySeverity(s string) string {
	if matchesAny(s, "fire", "explosion", "urgent", "danger", "toxic", "acid", "battery", "emergency", "burning", "critical", "poison") {
		return incidents.SeverityCritical
	}
	if matchesAny(s, "heavy", "overflowing", "blocked road", "large pile", "spill", "major", "leaking", "high") {
		return incidents.SeverityHigh
	}
	if matchesAny(s, "minor", "small", "few", "tiny", "low") {
		return incidents.SeverityLow
	}
	return incidents.SeverityMedium
}

type boolMatches = string

func matchesAny(text string, targets ...string) bool {
	for _, t := range targets {
		if strings.Contains(text, t) {
			return true
		}
	}
	return false
}

var coordRegex = regexp.MustCompile(`(-?\d{1,2}\.\d+)[,\s]+(-?\d{1,3}\.\d+)`)

func extractCoordinates(text string, session SessionContext) (float64, float64) {
	matches := coordRegex.FindStringSubmatch(text)
	if len(matches) == 3 {
		lat, err1 := strconv.ParseFloat(matches[1], 64)
		lon, err2 := strconv.ParseFloat(matches[2], 64)
		if err1 == nil && err2 == nil && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 {
			return lat, lon
		}
	}

	if session.DefaultCoordinates != nil {
		return session.DefaultCoordinates[0], session.DefaultCoordinates[1]
	}

	// Default municipal center coordinates (e.g. New Delhi 28.6139, 77.2090)
	return 28.6139, 77.2090
}
