package crc

import (
	"bytes"
	"testing"
)

func TestCRC32_Calculate(t *testing.T) {
	crc := New()

	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{
			name:     "empty data",
			data:     []byte{},
			expected: 0,
		},
		{
			name: "simple string",
			data: []byte("hello world"),
		},
		{
			name: "binary data",
			data: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checksum := crc.Calculate(tt.data)
			// Verify that the same data produces the same checksum
			checksum2 := crc.Calculate(tt.data)
			if checksum != checksum2 {
				t.Errorf("CRC not consistent: %d != %d", checksum, checksum2)
			}
		})
	}
}

func TestCRC32_Verify(t *testing.T) {
	crc := New()

	data := []byte("test data for verification")
	checksum := crc.Calculate(data)

	// Test valid checksum
	if !crc.Verify(data, checksum) {
		t.Error("Failed to verify valid checksum")
	}

	// Test invalid checksum
	if crc.Verify(data, checksum+1) {
		t.Error("Incorrectly verified invalid checksum")
	}

	// Test modified data
	modifiedData := append([]byte{}, data...)
	modifiedData[0] = 'T'
	if crc.Verify(modifiedData, checksum) {
		t.Error("Incorrectly verified checksum for modified data")
	}
}

func TestMessageCRC_ComputeMessageCRC(t *testing.T) {
	msgCRC := NewMessageCRC()

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		headers map[string][]byte
	}{
		{
			name:    "message without headers",
			key:     []byte("key1"),
			value:   []byte("value1"),
			headers: nil,
		},
		{
			name:  "message with headers",
			key:   []byte("key2"),
			value: []byte("value2"),
			headers: map[string][]byte{
				"header1": []byte("hvalue1"),
				"header2": []byte("hvalue2"),
			},
		},
		{
			name:    "message with empty key",
			key:     []byte{},
			value:   []byte("value3"),
			headers: nil,
		},
		{
			name:    "message with empty value",
			key:     []byte("key3"),
			value:   []byte{},
			headers: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crc1 := msgCRC.ComputeMessageCRC(tt.key, tt.value, tt.headers)
			crc2 := msgCRC.ComputeMessageCRC(tt.key, tt.value, tt.headers)

			// CRC should be consistent
			if crc1 != crc2 {
				t.Errorf("CRC not consistent: %d != %d", crc1, crc2)
			}

			// Validate CRC
			if !msgCRC.ValidateMessageCRC(tt.key, tt.value, tt.headers, crc1) {
				t.Error("Failed to validate computed CRC")
			}

			// Modified value should produce different CRC
			if len(tt.value) > 0 {
				modifiedValue := append([]byte{}, tt.value...)
				modifiedValue[0] = modifiedValue[0] + 1
				crc3 := msgCRC.ComputeMessageCRC(tt.key, modifiedValue, tt.headers)
				if crc1 == crc3 {
					t.Error("Modified message produced same CRC")
				}
			}
		})
	}
}

func TestBatchCRC_ComputeBatchCRC(t *testing.T) {
	batchCRC := NewBatchCRC()

	tests := []struct {
		name     string
		messages [][]byte
	}{
		{
			name:     "empty batch",
			messages: [][]byte{},
		},
		{
			name: "single message batch",
			messages: [][]byte{
				[]byte("message1"),
			},
		},
		{
			name: "multiple messages batch",
			messages: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
			},
		},
		{
			name: "batch with empty messages",
			messages: [][]byte{
				[]byte("message1"),
				[]byte{},
				[]byte("message3"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crc1 := batchCRC.ComputeBatchCRC(tt.messages)
			crc2 := batchCRC.ComputeBatchCRC(tt.messages)

			// CRC should be consistent
			if crc1 != crc2 {
				t.Errorf("CRC not consistent: %d != %d", crc1, crc2)
			}

			// Validate CRC
			if !batchCRC.ValidateBatchCRC(tt.messages, crc1) {
				t.Error("Failed to validate computed CRC")
			}

			// Modified batch should produce different CRC
			if len(tt.messages) > 0 {
				modifiedMessages := make([][]byte, len(tt.messages))
				copy(modifiedMessages, tt.messages)
				modifiedMessages[0] = append([]byte{}, tt.messages[0]...)
				if len(modifiedMessages[0]) > 0 {
					modifiedMessages[0][0] = modifiedMessages[0][0] + 1
					crc3 := batchCRC.ComputeBatchCRC(modifiedMessages)
					if crc1 == crc3 {
						t.Error("Modified batch produced same CRC")
					}
				}
			}
		})
	}
}

func TestCRC32_CalculateWithSeed(t *testing.T) {
	crc := New()

	data := []byte("test data")
	seed1 := uint32(12345)
	seed2 := uint32(67890)

	checksum1 := crc.CalculateWithSeed(data, seed1)
	checksum2 := crc.CalculateWithSeed(data, seed2)

	// Different seeds should produce different checksums
	if checksum1 == checksum2 {
		t.Error("Different seeds produced same checksum")
	}

	// Same seed should produce same checksum
	checksum3 := crc.CalculateWithSeed(data, seed1)
	if checksum1 != checksum3 {
		t.Errorf("Same seed produced different checksums: %d != %d", checksum1, checksum3)
	}
}

func BenchmarkCRC32_Calculate(b *testing.B) {
	crc := New()
	data := bytes.Repeat([]byte("a"), 1024) // 1KB of data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = crc.Calculate(data)
	}
}

func BenchmarkMessageCRC_ComputeMessageCRC(b *testing.B) {
	msgCRC := NewMessageCRC()
	key := []byte("benchmark-key")
	value := bytes.Repeat([]byte("value"), 200) // ~1KB value
	headers := map[string][]byte{
		"header1": []byte("value1"),
		"header2": []byte("value2"),
		"header3": []byte("value3"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msgCRC.ComputeMessageCRC(key, value, headers)
	}
}

func BenchmarkBatchCRC_ComputeBatchCRC(b *testing.B) {
	batchCRC := NewBatchCRC()
	messages := make([][]byte, 100)
	for i := range messages {
		messages[i] = bytes.Repeat([]byte("m"), 100) // 100 bytes per message
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batchCRC.ComputeBatchCRC(messages)
	}
}