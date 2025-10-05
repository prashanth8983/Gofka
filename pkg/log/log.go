package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log manages the segments of a topic partition.
type Log struct {
	mu            sync.RWMutex
	dir           string
	segments      []*Segment
	activeSegment *Segment
}

// NewLog creates a new Log instance.
func NewLog(dir string) (*Log, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	l := &Log{
		dir: dir,
	}

	return l, l.loadSegments()
}

func (l *Log) loadSegments() error {
	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	var segmentPaths []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			segmentPaths = append(segmentPaths, filepath.Join(l.dir, file.Name()))
		}
	}

	sort.Strings(segmentPaths)

	for _, path := range segmentPaths {
		baseOffset, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), ".log"), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse base offset from segment file name: %w", err)
		}

		segment, err := NewSegment(path, &SegmentConfig{
			BaseOffset: baseOffset,
			MaxSize:    1024 * 1024 * 10, // 10MB
		})
		if err != nil {
			return fmt.Errorf("failed to create segment: %w", err)
		}

		l.segments = append(l.segments, segment)
	}

	if len(l.segments) > 0 {
		l.activeSegment = l.segments[len(l.segments)-1]
	} else {
		// Create a new segment if none exist
		segment, err := NewSegment(filepath.Join(l.dir, "00000000000000000000.log"), &SegmentConfig{
			BaseOffset: 0,
			MaxSize:    1024 * 1024 * 10, // 10MB
		})
		if err != nil {
			return fmt.Errorf("failed to create initial segment: %w", err)
		}
		l.segments = append(l.segments, segment)
		l.activeSegment = segment
	}

	return nil
}

// Append appends a message to the log.
func (l *Log) Append(message []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.IsFull() {
		newBaseOffset := l.activeSegment.NextOffset()
		path := filepath.Join(l.dir, fmt.Sprintf("%020d.log", newBaseOffset))
		segment, err := NewSegment(path, &SegmentConfig{
			BaseOffset: newBaseOffset,
			MaxSize:    1024 * 1024 * 10, // 10MB
		})
		if err != nil {
			return 0, fmt.Errorf("failed to create new segment: %w", err)
		}
		l.segments = append(l.segments, segment)
		l.activeSegment = segment
	}

	return l.activeSegment.Append(message)
}

// Read reads a message from the log at a given offset.
func (l *Log) Read(offset int64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment that contains the offset
	for i := len(l.segments) - 1; i >= 0; i-- {
		segment := l.segments[i]
		if offset >= segment.BaseOffset() {
			return segment.Read(offset)
		}
	}

	return nil, fmt.Errorf("offset %d not found in any segment", offset)
}
