package observability

import (
	"sync"

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

var (
	defaultMetrics *Metrics
	once           sync.Once
)

// NewMetrics returns the singleton metrics instance using the default registerer
func NewMetrics() *Metrics {
	once.Do(func() {
		defaultMetrics = NewMetricsWithRegistry(prometheus.DefaultRegisterer)
	})
	return defaultMetrics
}

// NewMetricsWithRegistry initializes metrics on a specified Prometheus registry
func NewMetricsWithRegistry(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		RequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ecobrio_request_duration_seconds",
				Help:    "HTTP request latency distributions across routes and status codes",
				Buckets: []float64{0.005, 0.01, 0.025, 0.04, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
			},
			[]string{"method", "route", "status"},
		),
		RequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_requests_total",
				Help: "Total HTTP requests processed by status code class and route",
			},
			[]string{"method", "route", "status"},
		),
		BookingConflictsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_booking_conflicts_total",
				Help: "Total booking contention conflicts intercepted before or during DB transactions",
			},
			[]string{"reason"},
		),
		IdempotencyHitsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_idempotency_hits_total",
				Help: "Total duplicate requests intercepted and served from idempotency store",
			},
			[]string{"scope"},
		),
		WALRecordsAppended: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_records_appended_total",
				Help: "Total telemetry events written to the in-memory write buffer",
			},
		),
		WALRecordsDurable: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_records_durable_total",
				Help: "Total telemetry events fsynced and acknowledged as durable",
			},
		),
		WebSocketConnections: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "ecobrio_websocket_connections",
				Help: "Number of active persistent WebSocket client connections",
			},
		),
	}
}
