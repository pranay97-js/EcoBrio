package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"ecobrio/internal/hubs"
	"github.com/google/uuid"
)

// OpenRouterEngine provides live LLM integration with deterministic tool fallbacks
type OpenRouterEngine struct {
	apiKey     string
	model      string
	baseURL    string
	httpClient *http.Client
	fallback   *RuleBasedEngine
	tools      *ToolRegistry
}

func NewOpenRouterEngine(apiKey, model string, tools *ToolRegistry, fallback *RuleBasedEngine) *OpenRouterEngine {
	if model == "" {
		model = "openai/gpt-4o-mini"
	}
	return &OpenRouterEngine{
		apiKey:  apiKey,
		model:   model,
		baseURL: "https://openrouter.ai/api/v1/chat/completions",
		httpClient: &http.Client{
			Timeout: 12 * time.Second,
		},
		fallback: fallback,
		tools:    tools,
	}
}

type openRouterRequest struct {
	Model       string              `json:"model"`
	Messages    []openRouterMessage `json:"messages"`
	Temperature float64             `json:"temperature"`
}

type openRouterMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openRouterResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// ChatResponse represents rich conversational AI feedback with action hooks
type ChatResponse struct {
	Message           string     `json:"message"`
	ActionTaken       string     `json:"action_taken,omitempty"`
	ExtractedStream   string     `json:"extracted_stream,omitempty"`
	ExtractedSeverity string     `json:"extracted_severity,omitempty"`
	ToolName          string     `json:"tool_name,omitempty"`
	IncidentID        *uuid.UUID `json:"incident_id,omitempty"`
	BountyAwarded     int64      `json:"bounty_awarded,omitempty"`
	RecommendedHub    *hubs.Hub  `json:"recommended_hub,omitempty"`
}

const systemPrompt = `You are KachraSeth AI, the official municipal circular intelligence assistant for the EcoBrio platform.
Your core mission:
1. Help citizens segregate waste into 7 circular streams:
   - ORGANIC (food scraps, yard trimmings, compostable waste) -> Green Hopper
   - PLASTICS (PET bottles, HDPE rigid containers, clean film) -> Cyan Hopper
   - EWASTE (laptops, phones, lithium batteries, cables, PCBs) -> Purple Hopper
   - METALS (aluminum cans, copper wires, brass, steel scrap) -> Lime Hopper
   - GLASS (bottles, jars, beverage cullet) -> Sky Blue Hopper
   - PAPER (cardboard cartons, packaging pulp, newsprint) -> Amber Hopper
   - HAZARDOUS (chemical solvents, motor oils, toxic waste) -> Red Emergency Drop
2. Regional smart collection hubs:
   - Connaught Place Central E-Hub (Block B, Inner Circle, New Delhi 110001) - All circular streams
   - Noida Sector 62 Circular Depot (Plot 8, Electronic City, Noida 201309) - Organics & Polymers
   - Gurugram Cyber Hub Eco-Center (DLF Cyber City Phase II, Gurugram 122002) - E-waste & Critical Alloys
   - Lajpat Nagar Material Recovery Facility (Central Market, Ring Road, New Delhi 110024) - Paper & Glass
3. Citizen rewards: EcoTokens (+50 ET for drop passes, +150 ET for verified incident reports, +100 ET for issue resolution bounties).
4. Formatting: Respond with warm, helpful, concise advice using bullet points and emojis. Answer citizen questions naturally and directly.`

func (e *OpenRouterEngine) ProcessPrompt(ctx context.Context, session SessionContext, text string) (*TriageResponse, error) {
	// First execute domain tools through the deterministic engine
	res, err := e.fallback.ProcessPrompt(ctx, session, text)
	if err != nil {
		return nil, err
	}

	// If we have an active OpenRouter key and user requested clarification or conversational context, enrich with LLM
	if e.apiKey != "" && res.ActionTaken == ActionClarificationRequired {
		llmReply, llmErr := e.callLLM(ctx, text)
		if llmErr == nil && llmReply != "" {
			res.Message = llmReply
		}
	}

	return res, nil
}

// Chat handles interactive free-form conversational queries with live LLM and automatic domain triggers
func (e *OpenRouterEngine) Chat(ctx context.Context, session SessionContext, text string) (*ChatResponse, error) {
	lower := strings.ToLower(text)

	// Check if this is an incident report
	isIncident := matchesAny(lower, "battery", "acid", "leak", "spill", "dump", "overflow", "hazard", "toxic", "chemical", "fire", "smoke", "abandoned", "illegal")
	if isIncident {
		stream, _ := classifyStream(lower)
		if stream == "" {
			stream = "HAZARDOUS"
		}
		severity := classifySeverity(lower)
		lat, lon := extractCoordinates(text, session)

		inc, err := e.tools.ExecuteReportWasteIncident(ctx, session, ReportIncidentParams{
			Description:        text,
			CategorySuggestion: stream,
			Severity:           severity,
			Latitude:           lat,
			Longitude:          lon,
		})
		if err == nil {
			return &ChatResponse{
				Message:           fmt.Sprintf("🚨 **Incident Reported & Fleet Dispatched!**\n\nI have registered this **%s** incident (%s priority) at (%.4f, %.4f) into the municipal grid. A circular response vehicle has been routed to the location.\n\n🎉 **+150 EcoTokens** have been credited to your verified citizen wallet (#%s).", stream, severity, lat, lon, inc.ID.String()[:8]),
				ActionTaken:       ActionToolExecuted,
				ToolName:          ToolReportWasteIncident,
				IncidentID:        &inc.ID,
				ExtractedStream:   stream,
				ExtractedSeverity: severity,
				BountyAwarded:     150,
			}, nil
		}
	}

	// Check if user is looking for a hub
	if isHubSearchIntent(lower) {
		lat, lon := extractCoordinates(text, session)
		hubsList, err := e.tools.ExecuteFindNearestHub(ctx, FindNearestHubParams{
			Latitude:  lat,
			Longitude: lon,
			RadiusKm:  15.0,
		})
		if err == nil && len(hubsList) > 0 {
			nearest := hubsList[0]
			return &ChatResponse{
				Message:        fmt.Sprintf("📍 **Recommended Collection Hub:**\n\nI found **%s** located at **%s** (%.4f, %.4f).\n\nOperating hours: %s.\nYou can reserve a guaranteed contactless drop pass with zero waiting queue.", nearest.Name, nearest.Address, nearest.Latitude, nearest.Longitude, nearest.OperatingHours),
				ActionTaken:    ActionToolExecuted,
				ToolName:       ToolFindNearestHub,
				RecommendedHub: &nearest,
			}, nil
		}
	}

	// Check if user is checking credits
	if isCreditIntent(lower) {
		summary, err := e.tools.ExecuteGetUserCredits(ctx, session)
		if err == nil {
			return &ChatResponse{
				Message:     fmt.Sprintf("💰 **Citizen Wallet Ledger:**\n\nYou currently have **%d EcoTokens (ET)** in your verified balance (Universal Citizen Tier).\n\nYou can redeem your tokens for free metropolitan transit vouchers (300 ET) or 25kg cured municipal compost bags (150 ET)!", summary.AccountBalance),
				ActionTaken: ActionToolExecuted,
				ToolName:    ToolGetUserCredits,
			}, nil
		}
	}

	// Otherwise, call OpenRouter LLM for intelligent conversation
	if e.apiKey != "" {
		reply, err := e.callLLM(ctx, text)
		if err == nil && strings.TrimSpace(reply) != "" {
			return &ChatResponse{
				Message:     reply,
				ActionTaken: "LLM_RESPONSE",
			}, nil
		}
		slog.Warn("OpenRouter call failed, falling back to rule-based response", "error", err)
	}

	// Fallback response
	fallbackRes, err := e.fallback.ProcessPrompt(ctx, session, text)
	if err != nil {
		return nil, err
	}
	return &ChatResponse{
		Message:           fallbackRes.Message,
		ActionTaken:       fallbackRes.ActionTaken,
		ExtractedStream:   fallbackRes.ExtractedStream,
		ExtractedSeverity: fallbackRes.ExtractedSeverity,
	}, nil
}

func (e *OpenRouterEngine) callLLM(ctx context.Context, userPrompt string) (string, error) {
	reqBody := openRouterRequest{
		Model: e.model,
		Messages: []openRouterMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Temperature: 0.3,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}

	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("HTTP-Referer", "http://localhost:8080")
	httpReq.Header.Set("X-Title", "EcoBrio Universal Circular Engine")

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("openrouter returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var orResp openRouterResponse
	if err := json.Unmarshal(respBytes, &orResp); err != nil {
		return "", err
	}

	if len(orResp.Choices) > 0 && orResp.Choices[0].Message.Content != "" {
		return orResp.Choices[0].Message.Content, nil
	}

	return "", fmt.Errorf("empty choice returned from openrouter")
}
