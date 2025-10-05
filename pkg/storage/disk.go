package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DiskStorage manages persistent storage on disk.
type DiskStorage struct {
	mu      sync.RWMutex
	dataDir string
	file    *os.File
}

// NewDiskStorage creates a new DiskStorage instance.
func NewDiskStorage(dataDir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join(dataDir, "data.log")
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return &DiskStorage{
		dataDir: dataDir,
		file:    file,
	}, nil
}

// Write writes data to disk.
func (ds *DiskStorage) Write(data []byte) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	n, err := ds.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write data to disk: %w", err)
	}
	return n, nil
}

// Read reads data from disk.
func (ds *DiskStorage) Read(offset int64, size int) ([]byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	buffer := make([]byte, size)
	n, err := ds.file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read data from disk: %w", err)
	}
	return buffer[:n], nil
}

// Close closes the disk storage.
func (ds *DiskStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.file.Close()
}

// File returns the underlying os.File.
func (ds *DiskStorage) File() *os.File {
	return ds.file
}
