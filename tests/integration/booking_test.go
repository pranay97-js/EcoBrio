package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"ecobrio/internal/booking"
	"ecobrio/internal/middleware"
	"ecobrio/internal/observability"
	"github.com/google/uuid"
)

func setupBookingTestServer(slotID uuid.UUID, capacity int) (*httptest.Server, *booking.MemoryRepository) {
	mux := http.NewServeMux()
	metrics := observability.NewMetrics()

	memRepo := booking.NewMemoryRepository()
	memRepo.SetSlot(slotID, capacity, 0)
	tokenStore := booking.NewMemoryTokenStore()

	service := booking.NewService(memRepo, tokenStore)
	handler := booking.NewHandler(service)
	handler.RegisterRoutes(mux)

	wrapped := middleware.Recovery(middleware.LoggingAndMetrics(metrics, mux))
	return httptest.NewServer(wrapped), memRepo
}

func TestBookingAPIHTTPFlows(t *testing.T) {
	slotID := uuid.New()
	server, _ := setupBookingTestServer(slotID, 10)
	defer server.Close()

	client := server.Client()
	citizenID := uuid.New()
	idemKey := "test-http-idem-key-01"

	reqBody := booking.Request{
		SlotID:     slotID,
		CitizenID:  citizenID,
		StreamType: "EWASTE",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	// 1. Missing Idempotency Key -> 400 Bad Request
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/bookings", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing idempotency key, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. Successful Booking -> 201 Created
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/bookings", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", idemKey)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var created booking.Booking
	_ = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()

	if created.SlotID != slotID {
		t.Errorf("expected slot %s, got %s", slotID, created.SlotID)
	}

	// 3. Idempotent Replay -> 201 Created with same Booking ID
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/bookings", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", idemKey)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 on replay, got %d", resp.StatusCode)
	}
	var replayed booking.Booking
	_ = json.NewDecoder(resp.Body).Decode(&replayed)
	_ = resp.Body.Close()

	if replayed.ID != created.ID {
		t.Errorf("expected identical booking ID %s, got %s", created.ID, replayed.ID)
	}

	// 4. Same key with different payload -> 422 Unprocessable Entity
	differentReqBody := booking.Request{
		SlotID:     slotID,
		CitizenID:  uuid.New(), // Different citizen
		StreamType: "EWASTE",
	}
	diffBytes, _ := json.Marshal(differentReqBody)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/bookings", bytes.NewReader(diffBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", idemKey)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("expected 422 Unprocessable Entity for conflicting key reuse, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. Query by ID via GET /api/v1/bookings/{id}
	resp, err = client.Get(fmt.Sprintf("%s/api/v1/bookings/%s", server.URL, created.ID))
	if err != nil {
		t.Fatalf("GET by ID failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 6. Query by Request Key via GET /api/v1/bookings/by-request/{key}
	resp, err = client.Get(fmt.Sprintf("%s/api/v1/bookings/by-request/%s", server.URL, idemKey))
	if err != nil {
		t.Fatalf("GET by key failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestBookingHTTPConcurrencyRace(t *testing.T) {
	slotID := uuid.New()
	const availableCapacity = 3
	server, repo := setupBookingTestServer(slotID, availableCapacity)
	defer server.Close()

	client := server.Client()
	const concurrentClients = 25

	var successCount int64
	var conflictCount int64

	var wg sync.WaitGroup
	startGate := make(chan struct{})

	for i := 0; i < concurrentClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startGate

			reqBody := booking.Request{
				SlotID:     slotID,
				CitizenID:  uuid.New(),
				StreamType: "PLASTICS",
			}
			b, _ := json.Marshal(reqBody)

			req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/bookings", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Idempotency-Key", fmt.Sprintf("race-client-%d", idx))

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusCreated {
				atomic.AddInt64(&successCount, 1)
			} else if resp.StatusCode == http.StatusConflict {
				atomic.AddInt64(&conflictCount, 1)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	if successCount != availableCapacity {
		t.Fatalf("HTTP CONCURRENCY RACE FAILED: expected exactly %d bookings, got %d", availableCapacity, successCount)
	}
	if conflictCount != (concurrentClients - availableCapacity) {
		t.Errorf("expected %d conflicts (409), got %d", concurrentClients-availableCapacity, conflictCount)
	}

	_, booked, _, _ := repo.GetSlotCapacity(nil, slotID)
	if booked != availableCapacity {
		t.Fatalf("Database oversold: booked is %d, max is %d", booked, availableCapacity)
	}

	// Invariant check: Exactly availableCapacity distinct booking entities exist in the store
	var foundBookings int
	for i := 0; i < concurrentClients; i++ {
		key := fmt.Sprintf("race-client-%d", i)
		b, err := repo.GetBookingByIdempotencyKey(context.Background(), key)
		if err == nil && b != nil {
			foundBookings++
			if b.SlotID != slotID {
				t.Errorf("booking slot_id mismatch: expected %s, got %s", slotID, b.SlotID)
			}
			if b.Status != "CONFIRMED" {
				t.Errorf("booking status mismatch: expected CONFIRMED, got %s", b.Status)
			}
		}
	}
	if foundBookings != availableCapacity {
		t.Fatalf("Database entity count mismatch: expected %d confirmed bookings in store, found %d", availableCapacity, foundBookings)
	}
}
