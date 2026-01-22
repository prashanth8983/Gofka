package kraft

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// LogEntry represents a single entry in the KRaft log
type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Log represents the KRaft consensus log.
// This is an in-memory implementation suitable for development and testing.
// For production, this should be backed by persistent storage.
type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
	// Index of the first entry in entries slice (for log compaction)
	baseIndex uint64
	// Term of the entry at baseIndex
	baseTerm uint64
	// Current term
	currentTerm uint64
}

// NewLog creates a new Log instance.
func NewLog() *Log {
	return &Log{
		entries:     make([]LogEntry, 0),
		baseIndex:   0,
		baseTerm:    0,
		currentTerm: 0,
	}
}

// Append appends an entry to the log and returns the index of the new entry.
func (l *Log) Append(entry []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	nextIndex := l.lastIndexLocked() + 1

	logEntry := LogEntry{
		Index: nextIndex,
		Term:  l.currentTerm,
		Data:  make([]byte, len(entry)),
	}
	copy(logEntry.Data, entry)

	l.entries = append(l.entries, logEntry)
	return nil
}

// AppendWithTerm appends an entry with a specific term
func (l *Log) AppendWithTerm(entry []byte, term uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	nextIndex := l.lastIndexLocked() + 1

	logEntry := LogEntry{
		Index: nextIndex,
		Term:  term,
		Data:  make([]byte, len(entry)),
	}
	copy(logEntry.Data, entry)

	l.entries = append(l.entries, logEntry)
	return nil
}

// AppendEntries appends multiple entries to the log
func (l *Log) AppendEntries(entries []LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, entry := range entries {
		entryCopy := LogEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Data:  make([]byte, len(entry.Data)),
		}
		copy(entryCopy.Data, entry.Data)
		l.entries = append(l.entries, entryCopy)
	}
	return nil
}

// Read reads an entry from the log at the given index.
func (l *Log) Read(index uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entry, err := l.getEntryLocked(index)
	if err != nil {
		return nil, err
	}

	// Return a copy to prevent external modification
	data := make([]byte, len(entry.Data))
	copy(data, entry.Data)
	return data, nil
}

// GetEntry returns the full log entry at the given index
func (l *Log) GetEntry(index uint64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entry, err := l.getEntryLocked(index)
	if err != nil {
		return nil, err
	}

	// Return a copy
	entryCopy := &LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Data:  make([]byte, len(entry.Data)),
	}
	copy(entryCopy.Data, entry.Data)
	return entryCopy, nil
}

// getEntryLocked returns an entry by index (caller must hold lock)
func (l *Log) getEntryLocked(index uint64) (*LogEntry, error) {
	if index < l.baseIndex {
		return nil, fmt.Errorf("index %d is before base index %d (log compacted)", index, l.baseIndex)
	}

	if index == 0 && l.baseIndex == 0 && len(l.entries) == 0 {
		return nil, fmt.Errorf("log is empty")
	}

	sliceIndex := int(index - l.baseIndex)
	if sliceIndex < 0 || sliceIndex >= len(l.entries) {
		return nil, fmt.Errorf("index %d out of range [%d, %d]", index, l.baseIndex, l.lastIndexLocked())
	}

	return &l.entries[sliceIndex], nil
}

// GetEntriesFrom returns all entries starting from the given index
func (l *Log) GetEntriesFrom(startIndex uint64) ([]LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if startIndex < l.baseIndex {
		return nil, fmt.Errorf("start index %d is before base index %d", startIndex, l.baseIndex)
	}

	sliceIndex := int(startIndex - l.baseIndex)
	if sliceIndex > len(l.entries) {
		return []LogEntry{}, nil
	}

	// Return copies of the entries
	result := make([]LogEntry, len(l.entries)-sliceIndex)
	for i, entry := range l.entries[sliceIndex:] {
		result[i] = LogEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Data:  make([]byte, len(entry.Data)),
		}
		copy(result[i].Data, entry.Data)
	}
	return result, nil
}

// LastIndex returns the index of the last entry in the log
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

// lastIndexLocked returns the last index (caller must hold lock)
func (l *Log) lastIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return l.baseIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log
func (l *Log) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return l.baseTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at the given index
func (l *Log) TermAt(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == l.baseIndex && len(l.entries) == 0 {
		return l.baseTerm, nil
	}

	entry, err := l.getEntryLocked(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// SetCurrentTerm sets the current term
func (l *Log) SetCurrentTerm(term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.currentTerm = term
}

// GetCurrentTerm returns the current term
func (l *Log) GetCurrentTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.currentTerm
}

// TruncateAfter removes all entries after the given index
func (l *Log) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < l.baseIndex {
		return fmt.Errorf("cannot truncate before base index %d", l.baseIndex)
	}

	sliceIndex := int(index - l.baseIndex)
	if sliceIndex < 0 {
		sliceIndex = 0
	}
	if sliceIndex < len(l.entries) {
		l.entries = l.entries[:sliceIndex+1]
	}
	return nil
}

// Compact removes all entries before the given index (for log compaction)
func (l *Log) Compact(upToIndex uint64, snapshotTerm uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if upToIndex <= l.baseIndex {
		return nil // Already compacted
	}

	sliceIndex := int(upToIndex - l.baseIndex)
	if sliceIndex >= len(l.entries) {
		// Compact entire log
		l.entries = make([]LogEntry, 0)
		l.baseIndex = upToIndex
		l.baseTerm = snapshotTerm
		return nil
	}

	// Keep entries after upToIndex
	l.entries = l.entries[sliceIndex:]
	l.baseIndex = upToIndex
	l.baseTerm = snapshotTerm
	return nil
}

// Len returns the number of entries in the log
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// IsEmpty returns true if the log is empty
func (l *Log) IsEmpty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries) == 0
}

// Encode serializes a log entry for network transmission or storage
func (e *LogEntry) Encode() []byte {
	// Format: index(8) + term(8) + dataLen(4) + data
	buf := make([]byte, 20+len(e.Data))
	binary.BigEndian.PutUint64(buf[0:8], e.Index)
	binary.BigEndian.PutUint64(buf[8:16], e.Term)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(e.Data)))
	copy(buf[20:], e.Data)
	return buf
}

// DecodeLogEntry deserializes a log entry from bytes
func DecodeLogEntry(data []byte) (*LogEntry, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("data too short for log entry: %d bytes", len(data))
	}

	index := binary.BigEndian.Uint64(data[0:8])
	term := binary.BigEndian.Uint64(data[8:16])
	dataLen := binary.BigEndian.Uint32(data[16:20])

	if len(data) < int(20+dataLen) {
		return nil, fmt.Errorf("data too short for log entry data: expected %d, got %d", 20+dataLen, len(data))
	}

	entryData := make([]byte, dataLen)
	copy(entryData, data[20:20+dataLen])

	return &LogEntry{
		Index: index,
		Term:  term,
		Data:  entryData,
	}, nil
}
