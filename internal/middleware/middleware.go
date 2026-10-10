package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"ecobrio/internal/observability"
	"github.com/google/uuid"
)

type contextKey string

const RequestIDKey contextKey = "request_id"

// statusRecorder captures HTTP response status code for metrics and logging
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// LoggingAndMetrics wraps HTTP handlers with structured logging, Request-ID injection, and Prometheus tracking
func LoggingAndMetrics(metrics *observability.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}
		w.Header().Set("X-Request-ID", reqID)

		ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
		r = r.WithContext(ctx)

		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		defer func() {
			duration := time.Since(start)
			statusStr := strconv.Itoa(rec.statusCode)

			// Record Prometheus metrics
			metrics.RequestDuration.WithLabelValues(r.Method, r.URL.Path, statusStr).Observe(duration.Seconds())
			metrics.RequestsTotal.WithLabelValues(r.Method, r.URL.Path, statusStr).Inc()

			slog.Info("HTTP Request Completed",
				"request_id", reqID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.statusCode,
				"duration_ms", duration.Milliseconds(),
				"remote_addr", r.RemoteAddr,
			)
		}()

		next.ServeHTTP(rec, r)
	})
}

// Recovery catches panics and returns an internal server error response
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				err, ok := rec.(error)
				if !ok {
					err = fmt.Errorf("%v", rec)
				}
				slog.Error("Unhandled Panic Recovered", "error", err, "path", r.URL.Path)
				http.Error(w, `{"error":"internal_server_error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
