package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OffsetManager manages consumer offsets for topics and partitions
type OffsetManager struct {
	mu        sync.RWMutex
	broker    *Broker
	offsetDir string

	// In-memory offset cache
	offsets map[string]map[string]map[int32]int64 // groupID -> topic -> partition -> offset

	// Configuration
	commitInterval time.Duration
	autoCommit     bool
}

// OffsetCommit represents an offset commit request
type OffsetCommit struct {
	GroupID     string    `json:"group_id"`
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Timestamp   time.Time `json:"timestamp"`
	Metadata    string    `json:"metadata"`
	LeaderEpoch int32     `json:"leader_epoch"`
}

// OffsetFetch represents an offset fetch request
type OffsetFetch struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// NewOffsetManager creates a new OffsetManager
func NewOffsetManager(broker *Broker, offsetDir string) *OffsetManager {
	return &OffsetManager{
		broker:         broker,
		offsetDir:      offsetDir,
		offsets:        make(map[string]map[string]map[int32]int64),
		commitInterval: 5 * time.Second,
		autoCommit:     true,
	}
}

// Start starts the offset manager
func (c *OffsetManager) Start() error {
	// Create offset directory if it doesn't exist
	if err := os.MkdirAll(c.offsetDir, 0755); err != nil {
		return fmt.Errorf("failed to create offset directory: %w", err)
	}

	// Load existing offsets from disk
	if err := c.loadOffsets(); err != nil {
		return fmt.Errorf("failed to load offsets: %w", err)
	}

	// Start periodic offset commits if auto-commit is enabled
	if c.autoCommit {
		go c.periodicCommit()
	}

	return nil
}

// Stop stops the offset manager
func (c *OffsetManager) Stop() error {
	// Perform final commit of all offsets
	return c.commitAllOffsets()
}

// CommitOffset commits an offset for a consumer group
func (c *OffsetManager) CommitOffset(groupID, topic string, partition int32, offset int64, metadata string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Initialize nested maps if they don't exist
	if c.offsets[groupID] == nil {
		c.offsets[groupID] = make(map[string]map[int32]int64)
	}
	if c.offsets[groupID][topic] == nil {
		c.offsets[groupID][topic] = make(map[int32]int64)
	}

	// Update the offset
	c.offsets[groupID][topic][partition] = offset

	// Create offset commit record
	commit := &OffsetCommit{
		GroupID:     groupID,
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		Timestamp:   time.Now(),
		Metadata:    metadata,
		LeaderEpoch: 0, // TODO: Implement leader epoch tracking
	}

	// Write to disk immediately for durability
	return c.writeOffsetCommit(commit)
}

// FetchOffset fetches the committed offset for a consumer group
func (c *OffsetManager) FetchOffset(groupID, topic string, partition int32) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.offsets[groupID] == nil {
		return -1, fmt.Errorf("no offsets found for group %s", groupID)
	}

	if c.offsets[groupID][topic] == nil {
		return -1, fmt.Errorf("no offsets found for group %s, topic %s", groupID, topic)
	}

	offset, exists := c.offsets[groupID][topic][partition]
	if !exists {
		return -1, fmt.Errorf("no offset found for group %s, topic %s, partition %d", groupID, topic, partition)
	}

	return offset, nil
}

// GetGroupOffsets returns all offsets for a consumer group
func (c *OffsetManager) GetGroupOffsets(groupID string) (map[string]map[int32]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groupOffsets, exists := c.offsets[groupID]
	if !exists {
		return nil, fmt.Errorf("no offsets found for group %s", groupID)
	}

	// Create a copy to avoid race conditions
	result := make(map[string]map[int32]int64)
	for topic, partitions := range groupOffsets {
		result[topic] = make(map[int32]int64)
		for partition, offset := range partitions {
			result[topic][partition] = offset
		}
	}

	return result, nil
}

// DeleteGroupOffsets deletes all offsets for a consumer group
func (c *OffsetManager) DeleteGroupOffsets(groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove from memory
	delete(c.offsets, groupID)

	// Remove from disk
	groupFile := filepath.Join(c.offsetDir, fmt.Sprintf("%s.json", groupID))
	return os.Remove(groupFile)
}

// DeleteTopicOffsets deletes all offsets for a specific topic in a group
func (c *OffsetManager) DeleteTopicOffsets(groupID, topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.offsets[groupID] != nil {
		delete(c.offsets[groupID], topic)
	}

	// Reload from disk to ensure consistency
	return c.loadOffsets()
}

// GetEarliestOffset returns the earliest available offset for a topic/partition
func (c *OffsetManager) GetEarliestOffset(topic string, partition int32) (int64, error) {
	// This would typically query the log segments
	// For now, return 0 as the earliest offset
	return 0, nil
}

// GetLatestOffset returns the latest available offset for a topic/partition
func (c *OffsetManager) GetLatestOffset(topic string, partition int32) (int64, error) {
	// This would typically query the log segments
	// For now, return a placeholder value
	return 1000, nil
}

// ResetOffset resets the offset for a consumer group to the earliest available offset
func (c *OffsetManager) ResetOffset(groupID, topic string, partition int32) error {
	earliestOffset, err := c.GetEarliestOffset(topic, partition)
	if err != nil {
		return fmt.Errorf("failed to get earliest offset: %w", err)
	}

	return c.CommitOffset(groupID, topic, partition, earliestOffset, "reset")
}

// writeOffsetCommit writes an offset commit to disk
func (c *OffsetManager) writeOffsetCommit(commit *OffsetCommit) error {
	// Create group directory if it doesn't exist
	groupDir := filepath.Join(c.offsetDir, commit.GroupID)
	if err := os.MkdirAll(groupDir, 0755); err != nil {
		return fmt.Errorf("failed to create group directory: %w", err)
	}

	// Create topic directory if it doesn't exist
	topicDir := filepath.Join(groupDir, commit.Topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Write offset file
	offsetFile := filepath.Join(topicDir, fmt.Sprintf("partition_%d.json", commit.Partition))

	data, err := json.Marshal(commit)
	if err != nil {
		return fmt.Errorf("failed to marshal offset commit: %w", err)
	}

	if err := os.WriteFile(offsetFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write offset file: %w", err)
	}

	return nil
}

// loadOffsets loads all offsets from disk into memory
func (c *OffsetManager) loadOffsets() error {
	// Clear existing offsets
	c.offsets = make(map[string]map[string]map[int32]int64)

	// Read all group directories
	entries, err := os.ReadDir(c.offsetDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet, which is fine
		}
		return fmt.Errorf("failed to read offset directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		groupID := entry.Name()
		groupDir := filepath.Join(c.offsetDir, groupID)

		// Read all topic directories
		topicEntries, err := os.ReadDir(groupDir)
		if err != nil {
			continue // Skip problematic groups
		}

		c.offsets[groupID] = make(map[string]map[int32]int64)

		for _, topicEntry := range topicEntries {
			if !topicEntry.IsDir() {
				continue
			}

			topic := topicEntry.Name()
			topicDir := filepath.Join(groupDir, topic)

			// Read all partition files
			partitionEntries, err := os.ReadDir(topicDir)
			if err != nil {
				continue // Skip problematic topics
			}

			c.offsets[groupID][topic] = make(map[int32]int64)

			for _, partitionEntry := range partitionEntries {
				if partitionEntry.IsDir() || filepath.Ext(partitionEntry.Name()) != ".json" {
					continue
				}

				partitionFile := filepath.Join(topicDir, partitionEntry.Name())
				data, err := os.ReadFile(partitionFile)
				if err != nil {
					continue // Skip problematic partitions
				}

				var commit OffsetCommit
				if err := json.Unmarshal(data, &commit); err != nil {
					continue // Skip corrupted files
				}

				c.offsets[groupID][topic][commit.Partition] = commit.Offset
			}
		}
	}

	return nil
}

// commitAllOffsets commits all in-memory offsets to disk
func (c *OffsetManager) commitAllOffsets() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for groupID, topics := range c.offsets {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				commit := &OffsetCommit{
					GroupID:     groupID,
					Topic:       topic,
					Partition:   partition,
					Offset:      offset,
					Timestamp:   time.Now(),
					Metadata:    "",
					LeaderEpoch: 0,
				}

				if err := c.writeOffsetCommit(commit); err != nil {
					return fmt.Errorf("failed to commit offset for group %s, topic %s, partition %d: %w", groupID, topic, partition, err)
				}
			}
		}
	}

	return nil
}

// periodicCommit performs periodic commits of all offsets
func (c *OffsetManager) periodicCommit() {
	ticker := time.NewTicker(c.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.commitAllOffsets(); err != nil {
				// Log error but continue
				fmt.Printf("Failed to commit offsets: %v\n", err)
			}
		}
	}
}

// GetOffsetStats returns statistics about offset management
func (c *OffsetManager) GetOffsetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make(map[string]interface{})

	totalGroups := len(c.offsets)
	totalTopics := 0
	totalPartitions := 0

	for _, topics := range c.offsets {
		totalTopics += len(topics)
		for _, partitions := range topics {
			totalPartitions += len(partitions)
		}
	}

	stats["total_groups"] = totalGroups
	stats["total_topics"] = totalTopics
	stats["total_partitions"] = totalPartitions
	stats["auto_commit"] = c.autoCommit
	stats["commit_interval"] = c.commitInterval.String()

	return stats
}
