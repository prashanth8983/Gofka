package compression

import (
	"bytes"
	"testing"
)

func TestCompressionTypes(t *testing.T) {
	testData := []byte("Hello, World! This is a test message for compression.")

	compressionTypes := []CompressionType{
		None,
		Gzip,
		Snappy,
		LZ4,
	}

	for _, ct := range compressionTypes {
		t.Run(ct.String(), func(t *testing.T) {
			compressor, err := NewCompressor(ct)
			if err != nil {
				t.Fatalf("Failed to create compressor: %v", err)
			}

			// Test compress
			compressed, err := compressor.Compress(testData)
			if err != nil {
				t.Fatalf("Failed to compress: %v", err)
			}

			// For None compression, data should be unchanged
			if ct == None {
				if !bytes.Equal(compressed, testData) {
					t.Errorf("None compression changed the data")
				}
			} else {
				// For other types, compressed should be different (usually smaller)
				if bytes.Equal(compressed, testData) {
					t.Logf("Warning: Compression didn't change data for %v", ct)
				}
			}

			// Test decompress
			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Failed to decompress: %v", err)
			}

			// Verify round-trip
			if !bytes.Equal(decompressed, testData) {
				t.Errorf("Decompressed data doesn't match original for %v", ct)
			}
		})
	}
}

func TestBatchCompression(t *testing.T) {
	messages := [][]byte{
		[]byte("First message"),
		[]byte("Second message with more content"),
		[]byte("Third message"),
		[]byte(""),
		[]byte("Fifth message after empty"),
	}

	compressionTypes := []CompressionType{
		None,
		Gzip,
		Snappy,
		LZ4,
	}

	for _, ct := range compressionTypes {
		t.Run(ct.String()+"_batch", func(t *testing.T) {
			// Compress batch
			compressed, err := CompressBatch(messages, ct)
			if err != nil {
				t.Fatalf("Failed to compress batch: %v", err)
			}

			// Decompress batch
			decompressed, err := DecompressBatch(compressed, ct)
			if err != nil {
				t.Fatalf("Failed to decompress batch: %v", err)
			}

			// Verify messages
			if len(decompressed) != len(messages) {
				t.Fatalf("Message count mismatch: expected %d, got %d",
					len(messages), len(decompressed))
			}

			for i, msg := range messages {
				if !bytes.Equal(decompressed[i], msg) {
					t.Errorf("Message %d mismatch: expected %q, got %q",
						i, msg, decompressed[i])
				}
			}
		})
	}
}

func TestEmptyData(t *testing.T) {
	compressionTypes := []CompressionType{
		None,
		Gzip,
		Snappy,
		LZ4,
	}

	for _, ct := range compressionTypes {
		t.Run(ct.String()+"_empty", func(t *testing.T) {
			compressor, err := NewCompressor(ct)
			if err != nil {
				t.Fatalf("Failed to create compressor: %v", err)
			}

			// Test empty data
			compressed, err := compressor.Compress([]byte{})
			if err != nil {
				t.Fatalf("Failed to compress empty data: %v", err)
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Failed to decompress: %v", err)
			}

			if len(decompressed) != 0 {
				t.Errorf("Expected empty result, got %d bytes", len(decompressed))
			}
		})
	}
}

func BenchmarkCompression(b *testing.B) {
	// Create test data of various sizes
	small := bytes.Repeat([]byte("a"), 100)
	medium := bytes.Repeat([]byte("abcdefghij"), 1000)
	large := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 10000)

	testCases := []struct {
		name string
		data []byte
	}{
		{"small", small},
		{"medium", medium},
		{"large", large},
	}

	compressionTypes := []CompressionType{
		None,
		Gzip,
		Snappy,
		LZ4,
	}

	for _, tc := range testCases {
		for _, ct := range compressionTypes {
			b.Run(tc.name+"_"+ct.String(), func(b *testing.B) {
				compressor, err := NewCompressor(ct)
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					compressed, err := compressor.Compress(tc.data)
					if err != nil {
						b.Fatal(err)
					}
					_, err = compressor.Decompress(compressed)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}