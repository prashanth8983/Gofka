package log

import (
	"os"
	"testing"
)

func TestNewLog(t *testing.T) {
	logDir := "/tmp/gofka/test-log"
	defer os.RemoveAll(logDir)

	log, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	if log == nil {
		t.Fatal("Log should not be nil")
	}

	if log.dir != logDir {
		t.Errorf("Expected dir %s, got %s", logDir, log.dir)
	}
}

func TestLogAppendRead(t *testing.T) {
	logDir := "/tmp/gofka/test-log-append"
	defer os.RemoveAll(logDir)

	log, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	// Test message append
	message := []byte("Hello, Gofka!")
	offset, err := log.Append(message)
	if err != nil {
		t.Fatalf("Failed to append message: %v", err)
	}

	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}

	// Test message read
	readMessage, err := log.Read(offset)
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	if string(readMessage) != string(message) {
		t.Errorf("Expected message %s, got %s", string(message), string(readMessage))
	}
}

func TestLogBatchRead(t *testing.T) {
	logDir := "/tmp/gofka/test-log-batch"
	defer os.RemoveAll(logDir)

	log, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	// Append multiple messages
	messages := [][]byte{
		[]byte("Message 1"),
		[]byte("Message 2"),
		[]byte("Message 3"),
	}

	var offsets []int64
	for _, msg := range messages {
		offset, err := log.Append(msg)
		if err != nil {
			t.Fatalf("Failed to append message: %v", err)
		}
		offsets = append(offsets, offset)
	}

	// Test batch read
	readMessages, nextOffset, err := log.ReadBatch(offsets[0], 1000)
	if err != nil {
		t.Fatalf("Failed to read batch: %v", err)
	}

	if len(readMessages) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(readMessages))
	}

	for i, msg := range readMessages {
		if string(msg) != string(messages[i]) {
			t.Errorf("Expected message %s, got %s", string(messages[i]), string(msg))
		}
	}

	if nextOffset != offsets[len(offsets)-1]+1 {
		t.Errorf("Expected next offset %d, got %d", offsets[len(offsets)-1]+1, nextOffset)
	}
}

func TestLogOffsets(t *testing.T) {
	logDir := "/tmp/gofka/test-log-offsets"
	defer os.RemoveAll(logDir)

	log, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	// Test initial offsets
	baseOffset := log.GetBaseOffset()
	endOffset := log.GetLogEndOffset()

	if baseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", baseOffset)
	}

	if endOffset != 0 {
		t.Errorf("Expected end offset 0, got %d", endOffset)
	}

	// Append a message
	message := []byte("Test message")
	offset, err := log.Append(message)
	if err != nil {
		t.Fatalf("Failed to append message: %v", err)
	}

	// Test offsets after append
	newEndOffset := log.GetLogEndOffset()
	if newEndOffset != offset+1 {
		t.Errorf("Expected end offset %d, got %d", offset+1, newEndOffset)
	}
}

func TestLogInvalidRead(t *testing.T) {
	logDir := "/tmp/gofka/test-log-invalid"
	defer os.RemoveAll(logDir)

	log, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	// Try to read from non-existent offset
	_, err = log.Read(100)
	if err == nil {
		t.Error("Expected error for reading non-existent offset")
	}
}

func TestLogWithExistingSegments(t *testing.T) {
	logDir := "/tmp/gofka/test-log-existing"
	defer os.RemoveAll(logDir)

	// Create initial log and add some messages
	log1, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create initial log: %v", err)
	}

	message1 := []byte("First message")
	offset1, err := log1.Append(message1)
	if err != nil {
		t.Fatalf("Failed to append first message: %v", err)
	}

	// Close the log
	// Note: Log doesn't have a Close method, but segments will be closed when broker stops

	// Create new log instance (should load existing segments)
	log2, err := NewLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create second log: %v", err)
	}

	// Verify we can read the existing message
	readMessage, err := log2.Read(offset1)
	if err != nil {
		t.Fatalf("Failed to read existing message: %v", err)
	}

	if string(readMessage) != string(message1) {
		t.Errorf("Expected message %s, got %s", string(message1), string(readMessage))
	}

	// Verify offsets are correct
	baseOffset := log2.GetBaseOffset()
	endOffset := log2.GetLogEndOffset()

	if baseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", baseOffset)
	}

	if endOffset != offset1+1 {
		t.Errorf("Expected end offset %d, got %d", offset1+1, endOffset)
	}
}
