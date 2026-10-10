package telemetry

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"

	"github.com/google/uuid"
)

const (
	PacketMagic = uint16(0xEB01)
	MinPacketSize = 2 + 8 + 1 + 1 + 2 + 2 + 2 + 8 + 4 // 30 bytes minimum (with 1-byte sensor ID)
)

var StreamTypeToID = map[string]uint8{
	"EWASTE":    1,
	"PLASTICS":  2,
	"METALS":    3,
	"ORGANIC":   4,
	"HAZARDOUS": 5,
	"PAPER":     6,
	"GLASS":     7,
}

var IDToStreamType = map[uint8]string{
	1: "EWASTE",
	2: "PLASTICS",
	3: "METALS",
	4: "ORGANIC",
	5: "HAZARDOUS",
	6: "PAPER",
	7: "GLASS",
}

// BinaryPacket represents a hardware-encoded sensor frame
type BinaryPacket struct {
	Magic             uint16
	SequenceID        uint64
	SensorID          string
	StreamType        string
	FillLevelPercent  float64
	BatteryMillivolts int
	TemperatureCelsius float64
	Timestamp         time.Time
	CRC32             uint32
}

// EncodePacket serializes a sensor reading into a binary frame with CRC-32 checksum
func EncodePacket(r *SensorReading) ([]byte, error) {
	if r.FillLevelPercent < 0 || r.FillLevelPercent > 100 {
		return nil, ErrInvalidFillLevel
	}
	streamID, ok := StreamTypeToID[r.StreamType]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidStreamType, r.StreamType)
	}

	sensorBytes := []byte(r.SensorID)
	if len(sensorBytes) > 255 {
		return nil, fmt.Errorf("sensor_id length exceeds 255 bytes")
	}

	packetLen := 2 + 8 + 1 + len(sensorBytes) + 1 + 2 + 2 + 2 + 8 + 4
	buf := make([]byte, packetLen)

	idx := 0
	binary.BigEndian.PutUint16(buf[idx:], PacketMagic)
	idx += 2

	binary.BigEndian.PutUint64(buf[idx:], r.SequenceID)
	idx += 8

	buf[idx] = uint8(len(sensorBytes))
	idx++

	copy(buf[idx:], sensorBytes)
	idx += len(sensorBytes)

	buf[idx] = streamID
	idx++

	// Fill level in hundredths of a percent (0.00% -> 0, 100.00% -> 10000)
	centiles := uint16(math.Round(r.FillLevelPercent * 100))
	binary.BigEndian.PutUint16(buf[idx:], centiles)
	idx += 2

	binary.BigEndian.PutUint16(buf[idx:], uint16(r.BatteryMillivolts))
	idx += 2

	// Temperature in deci-Celsius (e.g. 25.4 C -> 254)
	deciC := int16(math.Round(r.TemperatureCelsius * 10))
	binary.BigEndian.PutUint16(buf[idx:], uint16(deciC))
	idx += 2

	tsMs := r.Timestamp.UnixMilli()
	binary.BigEndian.PutUint64(buf[idx:], uint64(tsMs))
	idx += 8

	// Compute IEEE 802.3 CRC-32 over everything preceding the CRC field
	checksum := crc32.ChecksumIEEE(buf[:idx])
	binary.BigEndian.PutUint32(buf[idx:], checksum)

	return buf, nil
}

// DecodePacket deserializes a binary frame and validates its CRC-32 checksum
func DecodePacket(data []byte) (*BinaryPacket, error) {
	if len(data) < MinPacketSize {
		return nil, fmt.Errorf("packet too short: got %d bytes, minimum %d", len(data), MinPacketSize)
	}

	magic := binary.BigEndian.Uint16(data[:2])
	if magic != PacketMagic {
		return nil, fmt.Errorf("invalid packet magic: expected 0x%04X, got 0x%04X", PacketMagic, magic)
	}

	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	computedCRC := crc32.ChecksumIEEE(data[:len(data)-4])

	if storedCRC != computedCRC {
		return nil, fmt.Errorf("%w: stored 0x%08X != computed 0x%08X", ErrCorruptPacket, storedCRC, computedCRC)
	}

	idx := 2
	seqID := binary.BigEndian.Uint64(data[idx:])
	idx += 8

	sensorIDLen := int(data[idx])
	idx++

	if idx+sensorIDLen > len(data)-19 {
		return nil, fmt.Errorf("corrupt packet: invalid sensor_id length boundary")
	}

	sensorID := string(data[idx : idx+sensorIDLen])
	idx += sensorIDLen

	streamID := data[idx]
	idx++

	streamType, ok := IDToStreamType[streamID]
	if !ok {
		return nil, fmt.Errorf("%w: id %d", ErrInvalidStreamType, streamID)
	}

	centiles := binary.BigEndian.Uint16(data[idx:])
	idx += 2
	fillLevel := float64(centiles) / 100.0

	battMv := int(binary.BigEndian.Uint16(data[idx:]))
	idx += 2

	deciC := int16(binary.BigEndian.Uint16(data[idx:]))
	idx += 2
	tempC := float64(deciC) / 10.0

	tsMs := int64(binary.BigEndian.Uint64(data[idx:]))
	timestamp := time.UnixMilli(tsMs)

	return &BinaryPacket{
		Magic:              magic,
		SequenceID:         seqID,
		SensorID:           sensorID,
		StreamType:         streamType,
		FillLevelPercent:   fillLevel,
		BatteryMillivolts:  battMv,
		TemperatureCelsius: tempC,
		Timestamp:          timestamp,
		CRC32:              storedCRC,
	}, nil
}

// ToReading converts a decoded BinaryPacket into a SensorReading
func (p *BinaryPacket) ToReading() *SensorReading {
	return &SensorReading{
		ID:                 uuid.New(),
		SensorID:           p.SensorID,
		SequenceID:         p.SequenceID,
		StreamType:         p.StreamType,
		FillLevelPercent:   p.FillLevelPercent,
		BatteryMillivolts:  p.BatteryMillivolts,
		TemperatureCelsius: p.TemperatureCelsius,
		Timestamp:          p.Timestamp,
		AppliedAt:          time.Now(),
	}
}
