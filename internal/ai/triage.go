package ai

import (
	"context"
)

// TriageRequest represents raw citizen narrative
type TriageRequest struct {
	CitizenText string   `json:"citizen_text"`
	Coordinates *[2]float64 `json:"coordinates,omitempty"`
}

// TriageResult represents structured verified classification
type TriageResult struct {
	StreamType      string  `json:"stream_type"`
	Severity        string  `json:"severity"`
	ConfidenceScore float64 `json:"confidence_score"`
	SuggestedTool   string  `json:"suggested_tool"`
	NormalizedNotes string  `json:"normalized_notes"`
}

// Engine defines AI tool calling interface
type Engine interface {
	Triage(ctx context.Context, req *TriageRequest) (*TriageResult, error)
}
