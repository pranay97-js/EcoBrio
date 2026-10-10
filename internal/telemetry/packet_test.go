package telemetry_test

import (
	"errors"
	"testing"
	"time"

	"ecobrio/internal/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode_Valid(t *testing.T) {
	orig := &telemetry.SensorReading{
		ID:                 uuid.New(),
		SensorID:           "BIN-ALPHA-001",
		SequenceID:         1042,
		StreamType:         "EWASTE",
		FillLevelPercent:   87.50,
		BatteryMillivolts:  3300,
		TemperatureCelsius: 24.5,
		Timestamp:          time.UnixMilli(time.Now().UnixMilli()), // Truncate to ms resolution
		AppliedAt:          time.Now(),
	}

	data, err := telemetry.EncodePacket(orig)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := telemetry.DecodePacket(data)
	require.NoError(t, err)
	assert.NotNil(t, decoded)

	assert.Equal(t, orig.SequenceID, decoded.SequenceID)
	assert.Equal(t, orig.SensorID, decoded.SensorID)
	assert.Equal(t, orig.StreamType, decoded.StreamType)
	assert.InDelta(t, orig.FillLevelPercent, decoded.FillLevelPercent, 0.01)
	assert.Equal(t, orig.BatteryMillivolts, decoded.BatteryMillivolts)
	assert.InDelta(t, orig.TemperatureCelsius, decoded.TemperatureCelsius, 0.1)
	assert.Equal(t, orig.Timestamp.UnixMilli(), decoded.Timestamp.UnixMilli())
}

func TestDecode_CRC32_CorruptionDetection(t *testing.T) {
	orig := &telemetry.SensorReading{
		SensorID:           "BIN-BETA-002",
		SequenceID:         2001,
		StreamType:         "PLASTICS",
		FillLevelPercent:   45.20,
		BatteryMillivolts:  3250,
		TemperatureCelsius: 21.0,
		Timestamp:          time.Now(),
	}

	data, err := telemetry.EncodePacket(orig)
	require.NoError(t, err)

	// Flip a single bit in the payload (byte 12)
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[12] ^= 0xFF

	_, err = telemetry.DecodePacket(corrupted)
	require.Error(t, err)
	assert.True(t, errors.Is(err, telemetry.ErrCorruptPacket), "Expected CRC-32 checksum mismatch error, got: %v", err)
}

func TestEncode_InvalidBounds(t *testing.T) {
	reading := &telemetry.SensorReading{
		SensorID:         "BIN-ERR-001",
		SequenceID:       1,
		StreamType:       "METALS",
		FillLevelPercent: 105.0, // Invalid: > 100%
		Timestamp:        time.Now(),
	}

	_, err := telemetry.EncodePacket(reading)
	require.Error(t, err)
	assert.True(t, errors.Is(err, telemetry.ErrInvalidFillLevel))

	reading.FillLevelPercent = -5.0 // Invalid: < 0%
	_, err = telemetry.EncodePacket(reading)
	require.Error(t, err)
	assert.True(t, errors.Is(err, telemetry.ErrInvalidFillLevel))

	reading.FillLevelPercent = 50.0
	reading.StreamType = "UNKNOWN_STREAM"
	_, err = telemetry.EncodePacket(reading)
	require.Error(t, err)
	assert.True(t, errors.Is(err, telemetry.ErrInvalidStreamType))
}
