package crc

import (
	"hash/crc32"
	"sort"
)

// CRC32 represents a CRC32 checksum calculator
type CRC32 struct {
	table *crc32.Table
}

// New creates a new CRC32 calculator using the Castagnoli polynomial
// which is more efficient on modern CPUs with hardware support
func New() *CRC32 {
	return &CRC32{
		table: crc32.MakeTable(crc32.Castagnoli),
	}
}

// Calculate computes the CRC32 checksum of the given data
func (c *CRC32) Calculate(data []byte) uint32 {
	return crc32.Checksum(data, c.table)
}

// Verify checks if the given checksum matches the data
func (c *CRC32) Verify(data []byte, checksum uint32) bool {
	return c.Calculate(data) == checksum
}

// CalculateWithSeed computes the CRC32 checksum with an initial seed value
func (c *CRC32) CalculateWithSeed(data []byte, seed uint32) uint32 {
	h := crc32.New(c.table)
	// Write the seed as initial value
	seedBytes := make([]byte, 4)
	seedBytes[0] = byte(seed >> 24)
	seedBytes[1] = byte(seed >> 16)
	seedBytes[2] = byte(seed >> 8)
	seedBytes[3] = byte(seed)
	h.Write(seedBytes)
	h.Write(data)
	return h.Sum32()
}

// MessageCRC represents a message with CRC validation
type MessageCRC struct {
	crc *CRC32
}

// NewMessageCRC creates a new message CRC validator
func NewMessageCRC() *MessageCRC {
	return &MessageCRC{
		crc: New(),
	}
}

// ComputeMessageCRC calculates CRC for a message including key, value, and headers
func (m *MessageCRC) ComputeMessageCRC(key, value []byte, headers map[string][]byte) uint32 {
	h := crc32.New(m.crc.table)

	// Write key length and key
	keyLen := uint32(len(key))
	h.Write([]byte{byte(keyLen >> 24), byte(keyLen >> 16), byte(keyLen >> 8), byte(keyLen)})
	if len(key) > 0 {
		h.Write(key)
	}

	// Write value length and value
	valueLen := uint32(len(value))
	h.Write([]byte{byte(valueLen >> 24), byte(valueLen >> 16), byte(valueLen >> 8), byte(valueLen)})
	if len(value) > 0 {
		h.Write(value)
	}

	// Write headers count
	headerCount := uint32(len(headers))
	h.Write([]byte{byte(headerCount >> 24), byte(headerCount >> 16), byte(headerCount >> 8), byte(headerCount)})

	// Sort header keys for deterministic CRC
	var headerKeys []string
	for k := range headers {
		headerKeys = append(headerKeys, k)
	}
	sort.Strings(headerKeys)

	// Write each header (key-value pairs) in sorted order
	for _, hKey := range headerKeys {
		hValue := headers[hKey]
		// Header key length and key
		hKeyLen := uint32(len(hKey))
		h.Write([]byte{byte(hKeyLen >> 24), byte(hKeyLen >> 16), byte(hKeyLen >> 8), byte(hKeyLen)})
		h.Write([]byte(hKey))

		// Header value length and value
		hValueLen := uint32(len(hValue))
		h.Write([]byte{byte(hValueLen >> 24), byte(hValueLen >> 16), byte(hValueLen >> 8), byte(hValueLen)})
		h.Write(hValue)
	}

	return h.Sum32()
}

// ValidateMessageCRC validates a message's CRC
func (m *MessageCRC) ValidateMessageCRC(key, value []byte, headers map[string][]byte, expectedCRC uint32) bool {
	return m.ComputeMessageCRC(key, value, headers) == expectedCRC
}

// BatchCRC represents CRC validation for message batches
type BatchCRC struct {
	crc *CRC32
}

// NewBatchCRC creates a new batch CRC validator
func NewBatchCRC() *BatchCRC {
	return &BatchCRC{
		crc: New(),
	}
}

// ComputeBatchCRC calculates CRC for an entire batch of messages
func (b *BatchCRC) ComputeBatchCRC(messages [][]byte) uint32 {
	h := crc32.New(b.crc.table)

	// Write message count
	count := uint32(len(messages))
	h.Write([]byte{byte(count >> 24), byte(count >> 16), byte(count >> 8), byte(count)})

	// Write each message
	for _, msg := range messages {
		msgLen := uint32(len(msg))
		h.Write([]byte{byte(msgLen >> 24), byte(msgLen >> 16), byte(msgLen >> 8), byte(msgLen)})
		h.Write(msg)
	}

	return h.Sum32()
}

// ValidateBatchCRC validates a batch's CRC
func (b *BatchCRC) ValidateBatchCRC(messages [][]byte, expectedCRC uint32) bool {
	return b.ComputeBatchCRC(messages) == expectedCRC
}