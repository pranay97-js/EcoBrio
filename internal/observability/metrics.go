package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the registered Prometheus metrics for ECOBrio
type Metrics struct {
	RequestDuration            *prometheus.HistogramVec
	RequestsTotal              *prometheus.CounterVec
	BookingConflictsTotal      *prometheus.CounterVec
	BookingCompensationTotal   *prometheus.CounterVec
	IdempotencyHitsTotal       *prometheus.CounterVec
	WALRecordsAppended         prometheus.Counter
	WALRecordsDurable          prometheus.Counter
	WALRecordsReplayed         *prometheus.CounterVec
	WALCorruptionTotal         *prometheus.CounterVec
	WebSocketConnections       prometheus.Gauge
	WebSocketBroadcastDuration *prometheus.HistogramVec
	QueueMessagesPending       *prometheus.GaugeVec
	OutboxOldestPendingSeconds prometheus.Gauge
	GeoQueryDuration           *prometheus.HistogramVec
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
		BookingCompensationTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_booking_compensation_total",
				Help: "Total booking compensation and release operations tracked by outcome class",
			},
			[]string{"outcome"},
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
		WALRecordsReplayed: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_records_replayed_total",
				Help: "Total telemetry records replayed during crash recovery",
			},
			[]string{"outcome"},
		),
		WALCorruptionTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ecobrio_wal_corruption_total",
				Help: "Integrity failures surfaced during WAL decode and validation",
			},
			[]string{"corruption_class"},
		),
		WebSocketConnections: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "ecobrio_websocket_connections",
				Help: "Number of active persistent WebSocket client connections",
			},
		),
		WebSocketBroadcastDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ecobrio_websocket_broadcast_duration_seconds",
				Help:    "Publisher-to-receiver fan-out latency duration across event types",
				Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.015, 0.025, 0.05, 0.1},
			},
			[]string{"event_type"},
		),
		QueueMessagesPending: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ecobrio_queue_messages_pending",
				Help: "Pending backlog messages in async processing queues",
			},
			[]string{"queue_name"},
		),
		OutboxOldestPendingSeconds: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "ecobrio_outbox_oldest_pending_seconds",
				Help: "Age of oldest pending transactional outbox event in seconds",
			},
		),
		GeoQueryDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ecobrio_geo_query_duration_seconds",
				Help:    "Latency distributions of geospatial index queries by query type",
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
			},
			[]string{"query_type"},
		),
	}
}
