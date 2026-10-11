package observability_test

import (
	"testing"

	"ecobrio/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetricsWithRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := observability.NewMetricsWithRegistry(reg)
	require.NotNil(t, m)

	assert.NotNil(t, m.RequestDuration)
	assert.NotNil(t, m.RequestsTotal)
	assert.NotNil(t, m.BookingConflictsTotal)
	assert.NotNil(t, m.BookingCompensationTotal)
	assert.NotNil(t, m.IdempotencyHitsTotal)
	assert.NotNil(t, m.WALRecordsAppended)
	assert.NotNil(t, m.WALRecordsDurable)
	assert.NotNil(t, m.WALRecordsReplayed)
	assert.NotNil(t, m.WALCorruptionTotal)
	assert.NotNil(t, m.WebSocketConnections)
	assert.NotNil(t, m.WebSocketBroadcastDuration)
	assert.NotNil(t, m.QueueMessagesPending)
	assert.NotNil(t, m.OutboxOldestPendingSeconds)
	assert.NotNil(t, m.GeoQueryDuration)

	// Test metric operations
	m.WALRecordsAppended.Inc()
	m.WebSocketConnections.Set(42)
	m.BookingConflictsTotal.WithLabelValues("capacity_exhausted").Inc()
}
