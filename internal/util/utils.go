package util

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// uuidCounter provides a monotonically increasing counter for UUID uniqueness
var uuidCounter uint64

// GenerateUUID generates a new UUID (version 4, random-based).
// Returns a UUID in the format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
// where x is any hexadecimal digit and y is one of 8, 9, a, or b.
func GenerateUUID() string {
	uuid := make([]byte, 16)

	// Read random bytes
	_, err := rand.Read(uuid)
	if err != nil {
		// Fallback to time-based generation if crypto/rand fails
		return generateFallbackUUID()
	}

	// Set version 4 (random UUID) in the 7th byte
	uuid[6] = (uuid[6] & 0x0f) | 0x40

	// Set variant (RFC 4122) in the 9th byte
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	// Format as standard UUID string
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(uuid[0:4]),
		hex.EncodeToString(uuid[4:6]),
		hex.EncodeToString(uuid[6:8]),
		hex.EncodeToString(uuid[8:10]),
		hex.EncodeToString(uuid[10:16]),
	)
}

// generateFallbackUUID generates a UUID using time and counter when crypto/rand is unavailable
func generateFallbackUUID() string {
	now := time.Now().UnixNano()
	counter := atomic.AddUint64(&uuidCounter, 1)

	// Create a deterministic but unique UUID
	uuid := make([]byte, 16)

	// Use timestamp for first 8 bytes
	uuid[0] = byte(now >> 56)
	uuid[1] = byte(now >> 48)
	uuid[2] = byte(now >> 40)
	uuid[3] = byte(now >> 32)
	uuid[4] = byte(now >> 24)
	uuid[5] = byte(now >> 16)
	uuid[6] = byte(now >> 8)
	uuid[7] = byte(now)

	// Use counter for last 8 bytes
	uuid[8] = byte(counter >> 56)
	uuid[9] = byte(counter >> 48)
	uuid[10] = byte(counter >> 40)
	uuid[11] = byte(counter >> 32)
	uuid[12] = byte(counter >> 24)
	uuid[13] = byte(counter >> 16)
	uuid[14] = byte(counter >> 8)
	uuid[15] = byte(counter)

	// Set version 4 and variant bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(uuid[0:4]),
		hex.EncodeToString(uuid[4:6]),
		hex.EncodeToString(uuid[6:8]),
		hex.EncodeToString(uuid[8:10]),
		hex.EncodeToString(uuid[10:16]),
	)
}

// GenerateShortID generates a shorter unique identifier (16 characters)
// Useful for member IDs, client IDs, etc.
func GenerateShortID() string {
	bytes := make([]byte, 8)
	_, err := rand.Read(bytes)
	if err != nil {
		// Fallback
		now := time.Now().UnixNano()
		counter := atomic.AddUint64(&uuidCounter, 1)
		bytes[0] = byte(now >> 24)
		bytes[1] = byte(now >> 16)
		bytes[2] = byte(now >> 8)
		bytes[3] = byte(now)
		bytes[4] = byte(counter >> 24)
		bytes[5] = byte(counter >> 16)
		bytes[6] = byte(counter >> 8)
		bytes[7] = byte(counter)
	}
	return hex.EncodeToString(bytes)
}
