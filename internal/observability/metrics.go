package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the registered Prometheus metrics for ECOBrio
type Metrics struct {
	RequestDuration       *prometheus.HistogramVec
	RequestsTotal         *prometheus.CounterVec
	BookingConflictsTotal *prometheus.CounterVec
	IdempotencyHitsTotal  *prometheus.CounterVec
	WALRecordsAppended    prometheus.Counter
	WALRecordsDurable     prometheus.Counter
	WebSocketConnections  prometheus.Gauge
}

// NewMetrics initializes and registers all Prometheus golden signals
func NewMetrics() *Metrics {
	return &Metrics{
		RequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ecobrio_request_duration_seconds",
				Help:    "HTTP request latency distributions across routes and status codes",
				Buckets: []float64{0.005, 0.01, 0.025, 0.04, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
			},
			[]string{"method", "route", "status"},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_requests_total",
				Help: "Total HTTP requests processed by status code class and route",
			},
			[]string{"method", "route", "status"},
		),
		BookingConflictsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_booking_conflicts_total",
				Help: "Total booking contention conflicts intercepted before or during DB transactions",
			},
			[]string{"reason"},
		),
		IdempotencyHitsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_idempotency_hits_total",
				Help: "Total duplicate requests intercepted and served from idempotency store",
			},
			[]string{"scope"},
		),
		WALRecordsAppended: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_records_appended_total",
				Help: "Total telemetry events written to the in-memory write buffer",
			},
		),
		WALRecordsDurable: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_records_durable_total",
				Help: "Total telemetry events fsynced and acknowledged as durable",
			},
		),
		WebSocketConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "ecobrio_websocket_connections",
				Help: "Number of active persistent WebSocket client connections",
			},
		),
	}
}
