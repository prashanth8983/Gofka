package log

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/user/gofka/pkg/storage"
)

// Segment represents a single log segment file.
type Segment struct {
	mu         sync.RWMutex
	storage    *storage.DiskStorage
	path       string
	baseOffset int64
	nextOffset int64
	size       int64
	maxSize    int64
}

// SegmentConfig holds configuration for a segment.
type SegmentConfig struct {
	BaseOffset int64
	MaxSize    int64
}

// NewSegment creates a new Segment instance.
func NewSegment(path string, config *SegmentConfig) (*Segment, error) {
	if config == nil {
		config = &SegmentConfig{
			BaseOffset: 0,
			MaxSize:    1024 * 1024 * 10, // 10MB default - more generous for testing
		}
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	storage, err := storage.NewDiskStorage(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk storage for segment: %w", err)
	}

	// For new segments, start with size 0
	// The file will be created when we first write to it
	return &Segment{
		storage:    storage,
		path:       path,
		baseOffset: config.BaseOffset,
		nextOffset: config.BaseOffset,
		size:       0, // Start with 0 size for new segments
		maxSize:    config.MaxSize,
	}, nil
}

// Close closes the segment file.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.storage.Close()
}

// Append appends a message to the segment.
func (s *Segment) Append(message []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.size >= s.maxSize {
		return 0, fmt.Errorf("segment is full (size: %d, maxSize: %d)", s.size, s.maxSize)
	}

	offset := s.nextOffset

	// Simple message format: 4-byte length prefix + message
	msgBytes := make([]byte, 4+len(message))
	copy(msgBytes[4:], message)
	binary.BigEndian.PutUint32(msgBytes[:4], uint32(len(message)))

	n, err := s.storage.Write(msgBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write to segment: %w", err)
	}

	s.size += int64(n)
	s.nextOffset++

	return offset, nil
}

// Read reads a message from the segment at a given offset.
func (s *Segment) Read(offset int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	// Find the file offset for the given message offset
	fileOffset, err := s.findFileOffset(offset)
	if err != nil {
		return nil, err
	}

	// Read the message length
	lenBuf, err := s.storage.Read(fileOffset, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	// Read the message
	msgBuf, err := s.storage.Read(fileOffset+4, int(msgLen))
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	return msgBuf, nil
}

// IsFull returns true if the segment is full.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxSize
}

// BaseOffset returns the base offset of the segment.
func (s *Segment) BaseOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseOffset
}

// NextOffset returns the next available offset.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

func (s *Segment) findFileOffset(offset int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return 0, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	fileOffset := int64(0)
	currentOffset := s.baseOffset

	for currentOffset < offset {
		// Read the message length
		lenBuf, err := s.storage.Read(fileOffset, 4)
		if err != nil {
			return 0, fmt.Errorf("failed to read message length: %w", err)
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		fileOffset += 4 + int64(msgLen)
		currentOffset++
	}

	return fileOffset, nil
}
