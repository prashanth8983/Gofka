package message

import (
	"encoding/binary"
	"time"
)

// MessageBatch represents a batch of messages
type MessageBatch struct {
	FirstOffset      int64
	LastOffset       int64
	FirstTimestamp   time.Time
	MaxTimestamp     time.Time
	ProducerID       int64
	ProducerEpoch    int16
	BaseSequence     int32
	Messages         []MessageWithCRC
	CompressionType  byte
	CompressedData   []byte
}

// AddMessage adds a message to the batch
func (b *MessageBatch) AddMessage(msg MessageWithCRC) {
	if len(b.Messages) == 0 {
		b.FirstTimestamp = time.Unix(0, msg.Timestamp)
		b.FirstOffset = int64(len(b.Messages))
	}

	b.Messages = append(b.Messages, msg)
	b.LastOffset = int64(len(b.Messages) - 1)

	msgTime := time.Unix(0, msg.Timestamp)
	if msgTime.After(b.MaxTimestamp) {
		b.MaxTimestamp = msgTime
	}
}

// Encode encodes the message batch
func (b *MessageBatch) Encode() []byte {
	// Calculate batch size
	size := 8 + 8 + 8 + 8 + 8 + 2 + 4 + 1 // Fixed fields

	// Add size for each message
	for _, msg := range b.Messages {
		msgData := msg.Encode()
		size += 4 + len(msgData) // Length prefix + message data
	}

	buf := make([]byte, size)
	offset := 0

	// First offset
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.FirstOffset))
	offset += 8

	// Last offset
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.LastOffset))
	offset += 8

	// First timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.FirstTimestamp.UnixNano()))
	offset += 8

	// Max timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.MaxTimestamp.UnixNano()))
	offset += 8

	// Producer ID
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.ProducerID))
	offset += 8

	// Producer epoch
	binary.BigEndian.PutUint16(buf[offset:], uint16(b.ProducerEpoch))
	offset += 2

	// Base sequence
	binary.BigEndian.PutUint32(buf[offset:], uint32(b.BaseSequence))
	offset += 4

	// Compression type
	buf[offset] = b.CompressionType
	offset++

	// Messages count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(b.Messages)))
	offset += 4

	// Write each message
	for _, msg := range b.Messages {
		msgData := msg.Encode()
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(msgData)))
		offset += 4
		copy(buf[offset:], msgData)
		offset += len(msgData)
	}

	return buf[:offset]
}

// Decode decodes a message batch
func (b *MessageBatch) Decode(data []byte) error {
	if len(data) < 37 { // Minimum batch size
		return ErrInvalidMessage
	}

	offset := 0

	// First offset
	b.FirstOffset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Last offset
	b.LastOffset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// First timestamp
	b.FirstTimestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[offset:])))
	offset += 8

	// Max timestamp
	b.MaxTimestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[offset:])))
	offset += 8

	// Producer ID
	b.ProducerID = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Producer epoch
	b.ProducerEpoch = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Base sequence
	b.BaseSequence = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Compression type
	b.CompressionType = data[offset]
	offset++

	// Messages count
	if offset+4 > len(data) {
		return ErrInvalidMessage
	}
	msgCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Read each message
	b.Messages = make([]MessageWithCRC, 0, msgCount)
	for i := uint32(0); i < msgCount; i++ {
		if offset+4 > len(data) {
			return ErrInvalidMessage
		}
		msgLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(msgLen) > len(data) {
			return ErrInvalidMessage
		}

		var msg MessageWithCRC
		if err := msg.Decode(data[offset : offset+int(msgLen)]); err != nil {
			return err
		}
		b.Messages = append(b.Messages, msg)
		offset += int(msgLen)
	}

	return nil
}