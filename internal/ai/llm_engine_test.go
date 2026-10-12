package ai

import (
	"context"
	"testing"

	"ecobrio/internal/hubs"
	"github.com/google/uuid"
)

func TestOpenRouterEngineFallback(t *testing.T) {
	ctx := context.Background()
	hubsRepo := hubs.NewMemoryRepository()
	hubsSvc := hubs.NewService(hubsRepo)
	_ = hubsRepo.CreateHub(ctx, &hubs.Hub{
		ID:        uuid.New(),
		Name:      "Test London Central Hub",
		Latitude:  51.5074,
		Longitude: -0.1278,
	})

	registry := NewToolRegistry(nil, hubsSvc, nil)
	fallback := NewRuleBasedEngine(registry)

	// Engine with empty API key should gracefully fall back to RuleBasedEngine
	engine := NewOpenRouterEngine("", "openai/gpt-4o-mini", registry, fallback)
	session := SessionContext{
		CitizenID:   uuid.New(),
		CitizenRole: "CITIZEN",
	}

	// Test chat fallback
	res, err := engine.Chat(ctx, session, "Where is the nearest hub for plastic recycling?")
	if err != nil {
		t.Fatalf("unexpected error during chat fallback: %v", err)
	}
	if res.Message == "" {
		t.Fatal("expected non-empty message in chat response")
	}

	// Test process prompt fallback
	pRes, err := engine.ProcessPrompt(ctx, session, "Where is the nearest collection hub?")
	if err != nil {
		t.Fatalf("unexpected error during process prompt fallback: %v", err)
	}
	if pRes.Message == "" {
		t.Fatal("expected non-empty message in prompt response")
	}
}
