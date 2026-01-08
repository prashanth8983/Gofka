package message

import (
	"encoding/binary"
	"hash/crc32"
)

// CRC32 table for Kafka compatibility
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ComputeCRC computes the CRC32 checksum for a message
func ComputeCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// ValidateCRC validates the CRC32 checksum of a message
func ValidateCRC(data []byte, expected uint32) bool {
	actual := ComputeCRC(data)
	return actual == expected
}

// MessageWithCRC represents a message with CRC validation
type MessageWithCRC struct {
	CRC       uint32
	Magic     byte    // Protocol version
	Attributes byte   // Compression codec, timestamp type, etc.
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp int64
}

// Header represents a message header
type Header struct {
	Key   string
	Value []byte
}

// Encode encodes the message with CRC
func (m *MessageWithCRC) Encode() []byte {
	// Calculate size
	size := 1 + 1 + 8 // magic + attributes + timestamp
	size += 4 + len(m.Key)    // key length + key
	size += 4 + len(m.Value)  // value length + value
	size += 4 // headers count
	for _, h := range m.Headers {
		size += 2 + len(h.Key) + 4 + len(h.Value)
	}

	// Allocate buffer
	buf := make([]byte, 4+size) // CRC + data
	offset := 4 // Skip CRC field initially

	// Write magic byte
	buf[offset] = m.Magic
	offset++

	// Write attributes
	buf[offset] = m.Attributes
	offset++

	// Write timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(m.Timestamp))
	offset += 8

	// Write key
	if m.Key == nil {
		binary.BigEndian.PutUint32(buf[offset:], 0xFFFFFFFF) // null marker
		offset += 4
	} else {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Key)))
		offset += 4
		copy(buf[offset:], m.Key)
		offset += len(m.Key)
	}

	// Write value
	if m.Value == nil {
		binary.BigEndian.PutUint32(buf[offset:], 0xFFFFFFFF) // null marker
		offset += 4
	} else {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Value)))
		offset += 4
		copy(buf[offset:], m.Value)
		offset += len(m.Value)
	}

	// Write headers
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Headers)))
	offset += 4
	for _, h := range m.Headers {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(h.Key)))
		offset += 2
		copy(buf[offset:], h.Key)
		offset += len(h.Key)
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(h.Value)))
		offset += 4
		copy(buf[offset:], h.Value)
		offset += len(h.Value)
	}

	// Calculate and write CRC (exclude the CRC field itself)
	crc := ComputeCRC(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)
	m.CRC = crc

	return buf
}

// Decode decodes a message with CRC validation
func (m *MessageWithCRC) Decode(data []byte) error {
	if len(data) < 4 {
		return ErrInvalidMessage
	}

	// Read and validate CRC
	m.CRC = binary.BigEndian.Uint32(data[0:4])
	if !ValidateCRC(data[4:], m.CRC) {
		return ErrCRCMismatch
	}

	offset := 4

	// Read magic byte
	if offset >= len(data) {
		return ErrInvalidMessage
	}
	m.Magic = data[offset]
	offset++

	// Read attributes
	if offset >= len(data) {
		return ErrInvalidMessage
	}
	m.Attributes = data[offset]
	offset++

	// Read timestamp
	if offset+8 > len(data) {
		return ErrInvalidMessage
	}
	m.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Read key
	if offset+4 > len(data) {
		return ErrInvalidMessage
	}
	keyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if keyLen == 0xFFFFFFFF {
		m.Key = nil
	} else {
		if offset+int(keyLen) > len(data) {
			return ErrInvalidMessage
		}
		m.Key = make([]byte, keyLen)
		copy(m.Key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)
	}

	// Read value
	if offset+4 > len(data) {
		return ErrInvalidMessage
	}
	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if valueLen == 0xFFFFFFFF {
		m.Value = nil
	} else {
		if offset+int(valueLen) > len(data) {
			return ErrInvalidMessage
		}
		m.Value = make([]byte, valueLen)
		copy(m.Value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)
	}

	// Read headers
	if offset+4 > len(data) {
		return ErrInvalidMessage
	}
	headerCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	m.Headers = make([]Header, headerCount)
	for i := uint32(0); i < headerCount; i++ {
		if offset+2 > len(data) {
			return ErrInvalidMessage
		}
		keyLen := binary.BigEndian.Uint16(data[offset:])
		offset += 2
		if offset+int(keyLen) > len(data) {
			return ErrInvalidMessage
		}
		m.Headers[i].Key = string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		if offset+4 > len(data) {
			return ErrInvalidMessage
		}
		valueLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		if offset+int(valueLen) > len(data) {
			return ErrInvalidMessage
		}
		m.Headers[i].Value = make([]byte, valueLen)
		copy(m.Headers[i].Value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)
	}

	return nil
}