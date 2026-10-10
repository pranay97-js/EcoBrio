package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"ecobrio/internal/observability"
)

func TestLoggingAndMetricsMiddleware(t *testing.T) {
	metrics := observability.NewMetrics()

	// Handler that returns 200 OK
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Context().Value(RequestIDKey)
		if reqID == nil || reqID == "" {
			t.Error("expected request_id in context, got nil")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	wrapped := LoggingAndMetrics(metrics, dummyHandler)

	req := httptest.NewRequest("GET", "/healthz", nil)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}

	headerReqID := rr.Header().Get("X-Request-ID")
	if headerReqID == "" {
		t.Error("expected X-Request-ID response header to be set")
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("simulated fatal crash")
	})

	wrapped := Recovery(panicHandler)

	req := httptest.NewRequest("GET", "/panic", nil)
	rr := httptest.NewRecorder()

	// Should not crash the process
	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 Internal Server Error, got %d", rr.Code)
	}
}
