package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// CompressionType represents the type of compression used
type CompressionType int32

const (
	// None means no compression
	None CompressionType = 0
	// Gzip compression
	Gzip CompressionType = 1
	// Snappy compression
	Snappy CompressionType = 2
	// LZ4 compression
	LZ4 CompressionType = 3
)

// String returns the string representation of the compression type
func (c CompressionType) String() string {
	switch c {
	case None:
		return "none"
	case Gzip:
		return "gzip"
	case Snappy:
		return "snappy"
	case LZ4:
		return "lz4"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

// Compressor interface for different compression algorithms
type Compressor interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

// NewCompressor creates a new compressor for the given compression type
func NewCompressor(compressionType CompressionType) (Compressor, error) {
	switch compressionType {
	case None:
		return &noneCompressor{}, nil
	case Gzip:
		return &gzipCompressor{}, nil
	case Snappy:
		return &snappyCompressor{}, nil
	case LZ4:
		return &lz4Compressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
	}
}

// noneCompressor implements no compression
type noneCompressor struct{}

func (n *noneCompressor) Compress(src []byte) ([]byte, error) {
	return src, nil
}

func (n *noneCompressor) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

// gzipCompressor implements gzip compression
type gzipCompressor struct{}

func (g *gzipCompressor) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(src)
	if err != nil {
		return nil, fmt.Errorf("gzip compress write failed: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("gzip compress close failed: %w", err)
	}

	return buf.Bytes(), nil
}

func (g *gzipCompressor) Decompress(src []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress reader failed: %w", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("gzip decompress read failed: %w", err)
	}

	return decompressed, nil
}

// snappyCompressor implements snappy compression
type snappyCompressor struct{}

func (s *snappyCompressor) Compress(src []byte) ([]byte, error) {
	return snappy.Encode(nil, src), nil
}

func (s *snappyCompressor) Decompress(src []byte) ([]byte, error) {
	decoded, err := snappy.Decode(nil, src)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress failed: %w", err)
	}
	return decoded, nil
}

// lz4Compressor implements LZ4 compression
type lz4Compressor struct{}

func (l *lz4Compressor) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)

	_, err := writer.Write(src)
	if err != nil {
		return nil, fmt.Errorf("lz4 compress write failed: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("lz4 compress close failed: %w", err)
	}

	return buf.Bytes(), nil
}

func (l *lz4Compressor) Decompress(src []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(src))

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress read failed: %w", err)
	}

	return decompressed, nil
}

// CompressBatch compresses a batch of messages
func CompressBatch(messages [][]byte, compressionType CompressionType) ([]byte, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// Combine all messages into a single byte slice with length prefixes
	var totalSize int
	for _, msg := range messages {
		totalSize += 4 + len(msg) // 4 bytes for length + message
	}

	combined := make([]byte, 0, totalSize)
	for _, msg := range messages {
		// Add length prefix (4 bytes, big-endian)
		lenBytes := make([]byte, 4)
		lenBytes[0] = byte(len(msg) >> 24)
		lenBytes[1] = byte(len(msg) >> 16)
		lenBytes[2] = byte(len(msg) >> 8)
		lenBytes[3] = byte(len(msg))
		combined = append(combined, lenBytes...)
		combined = append(combined, msg...)
	}

	// Compress the combined data
	compressor, err := NewCompressor(compressionType)
	if err != nil {
		return nil, err
	}

	return compressor.Compress(combined)
}

// DecompressBatch decompresses a batch of messages
func DecompressBatch(compressed []byte, compressionType CompressionType) ([][]byte, error) {
	if len(compressed) == 0 {
		return nil, nil
	}

	// Decompress the data
	compressor, err := NewCompressor(compressionType)
	if err != nil {
		return nil, err
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		return nil, err
	}

	// Extract individual messages
	var messages [][]byte
	offset := 0
	for offset < len(decompressed) {
		if offset+4 > len(decompressed) {
			return nil, fmt.Errorf("invalid batch format: insufficient length bytes")
		}

		// Read length prefix (4 bytes, big-endian)
		msgLen := int(decompressed[offset])<<24 |
			int(decompressed[offset+1])<<16 |
			int(decompressed[offset+2])<<8 |
			int(decompressed[offset+3])
		offset += 4

		if offset+msgLen > len(decompressed) {
			return nil, fmt.Errorf("invalid batch format: message length exceeds data")
		}

		// Extract message
		msg := decompressed[offset : offset+msgLen]
		messages = append(messages, msg)
		offset += msgLen
	}

	return messages, nil
}