package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/prashanth8983/gofka/pkg/logger"
)

// OffsetManager manages consumer offsets for topics and partitions
type OffsetManager struct {
	mu        sync.RWMutex
	broker    *Broker
	offsetDir string

	// In-memory offset cache
	offsets map[string]map[string]map[int32]*OffsetMetadata // groupID -> topic -> partition -> offset metadata

	// Leader epoch tracking
	leaderEpochs map[string]map[int32]int32 // topic -> partition -> leader epoch

	// Offset retention
	retentionTime   time.Duration
	compactInterval time.Duration
	lastCompaction  time.Time

	// Configuration
	commitInterval time.Duration
	autoCommit     bool
}

// OffsetMetadata stores detailed offset information
type OffsetMetadata struct {
	Offset        int64     `json:"offset"`
	LeaderEpoch   int32     `json:"leader_epoch"`
	Timestamp     time.Time `json:"timestamp"`
	Metadata      string    `json:"metadata,omitempty"`
	CommitTime    time.Time `json:"commit_time"`
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
		broker:          broker,
		offsetDir:       offsetDir,
		offsets:         make(map[string]map[string]map[int32]*OffsetMetadata),
		leaderEpochs:    make(map[string]map[int32]int32),
		commitInterval:  5 * time.Second,
		autoCommit:      true,
		retentionTime:   7 * 24 * time.Hour, // 7 days default retention
		compactInterval: 1 * time.Hour,       // Compact every hour
		lastCompaction:  time.Now(),
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

	// Start offset compaction routine
	go c.periodicCompaction()

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
		c.offsets[groupID] = make(map[string]map[int32]*OffsetMetadata)
	}
	if c.offsets[groupID][topic] == nil {
		c.offsets[groupID][topic] = make(map[int32]*OffsetMetadata)
	}

	// Get current leader epoch
	leaderEpoch := c.getLeaderEpoch(topic, partition)

	// Update the offset
	now := time.Now()
	c.offsets[groupID][topic][partition] = &OffsetMetadata{
		Offset:      offset,
		LeaderEpoch: leaderEpoch,
		Timestamp:   now,
		Metadata:    metadata,
		CommitTime:  now,
	}

	// Create offset commit record
	commit := &OffsetCommit{
		GroupID:     groupID,
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		Timestamp:   now,
		Metadata:    metadata,
		LeaderEpoch: leaderEpoch,
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

	offsetMeta, exists := c.offsets[groupID][topic][partition]
	if !exists || offsetMeta == nil {
		return -1, fmt.Errorf("no offset found for group %s, topic %s, partition %d", groupID, topic, partition)
	}

	return offsetMeta.Offset, nil
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
		for partition, offsetMeta := range partitions {
			if offsetMeta != nil {
				result[topic][partition] = offsetMeta.Offset
			}
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
	// Get the log for the topic/partition
	log := c.broker.getLog(topic, partition)
	if log == nil {
		return 0, fmt.Errorf("log not found for topic %s, partition %d", topic, partition)
	}

	return log.GetBaseOffset(), nil
}

// GetLatestOffset returns the latest available offset for a topic/partition
func (c *OffsetManager) GetLatestOffset(topic string, partition int32) (int64, error) {
	// Get the log for the topic/partition
	log := c.broker.getLog(topic, partition)
	if log == nil {
		return 0, fmt.Errorf("log not found for topic %s, partition %d", topic, partition)
	}

	return log.GetLogEndOffset(), nil
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
	c.offsets = make(map[string]map[string]map[int32]*OffsetMetadata)

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

		c.offsets[groupID] = make(map[string]map[int32]*OffsetMetadata)

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

			c.offsets[groupID][topic] = make(map[int32]*OffsetMetadata)

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

				// Convert to OffsetMetadata
				c.offsets[groupID][topic][commit.Partition] = &OffsetMetadata{
					Offset:      commit.Offset,
					LeaderEpoch: commit.LeaderEpoch,
					Timestamp:   commit.Timestamp,
					Metadata:    commit.Metadata,
					CommitTime:  commit.Timestamp,
				}

				// Track leader epochs
				if c.leaderEpochs[topic] == nil {
					c.leaderEpochs[topic] = make(map[int32]int32)
				}
				if commit.LeaderEpoch > c.leaderEpochs[topic][commit.Partition] {
					c.leaderEpochs[topic][commit.Partition] = commit.LeaderEpoch
				}
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
			for partition, offsetMeta := range partitions {
				if offsetMeta == nil {
					continue
				}

				commit := &OffsetCommit{
					GroupID:     groupID,
					Topic:       topic,
					Partition:   partition,
					Offset:      offsetMeta.Offset,
					Timestamp:   offsetMeta.Timestamp,
					Metadata:    offsetMeta.Metadata,
					LeaderEpoch: offsetMeta.LeaderEpoch,
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
				logger.Warn("Failed to commit offsets", zap.Error(err))
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
	stats["retention_time"] = c.retentionTime.String()
	stats["compaction_interval"] = c.compactInterval.String()
	stats["last_compaction"] = c.lastCompaction.Format(time.RFC3339)

	return stats
}

// getLeaderEpoch returns the current leader epoch for a topic/partition
func (c *OffsetManager) getLeaderEpoch(topic string, partition int32) int32 {
	if c.leaderEpochs[topic] == nil {
		c.leaderEpochs[topic] = make(map[int32]int32)
	}
	return c.leaderEpochs[topic][partition]
}

// UpdateLeaderEpoch updates the leader epoch for a topic/partition
func (c *OffsetManager) UpdateLeaderEpoch(topic string, partition int32, epoch int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leaderEpochs[topic] == nil {
		c.leaderEpochs[topic] = make(map[int32]int32)
	}
	c.leaderEpochs[topic][partition] = epoch
}

// periodicCompaction performs periodic compaction of old offsets
func (c *OffsetManager) periodicCompaction() {
	ticker := time.NewTicker(c.compactInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := c.compactOffsets(); err != nil {
			logger.Warn("Failed to compact offsets", zap.Error(err))
		}
	}
}

// compactOffsets removes expired offsets based on retention policy
func (c *OffsetManager) compactOffsets() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	// Iterate through all offsets and remove expired ones
	for groupID, topics := range c.offsets {
		for topic, partitions := range topics {
			for partition, offsetMeta := range partitions {
				if offsetMeta == nil {
					continue
				}

				// Check if offset has expired
				if now.Sub(offsetMeta.CommitTime) > c.retentionTime {
					delete(partitions, partition)
					expiredCount++

					// Remove from disk
					groupDir := filepath.Join(c.offsetDir, groupID)
					topicDir := filepath.Join(groupDir, topic)
					offsetFile := filepath.Join(topicDir, fmt.Sprintf("partition_%d.json", partition))
					os.Remove(offsetFile)
				}
			}

			// Remove topic if no partitions left
			if len(partitions) == 0 {
				delete(topics, topic)
			}
		}

		// Remove group if no topics left
		if len(topics) == 0 {
			delete(c.offsets, groupID)
		}
	}

	c.lastCompaction = now
	if expiredCount > 0 {
		logger.Debug("Compacted expired offsets", zap.Int("count", expiredCount))
	}

	return nil
}

// FetchOffsetWithMetadata fetches the full offset metadata for a consumer group
func (c *OffsetManager) FetchOffsetWithMetadata(groupID, topic string, partition int32) (*OffsetMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.offsets[groupID] == nil {
		return nil, fmt.Errorf("no offsets found for group %s", groupID)
	}

	if c.offsets[groupID][topic] == nil {
		return nil, fmt.Errorf("no offsets found for group %s, topic %s", groupID, topic)
	}

	offsetMeta, exists := c.offsets[groupID][topic][partition]
	if !exists || offsetMeta == nil {
		return nil, fmt.Errorf("no offset found for group %s, topic %s, partition %d", groupID, topic, partition)
	}

	// Return a copy to avoid race conditions
	copy := *offsetMeta
	return &copy, nil
}

// SetRetentionTime updates the offset retention time
func (c *OffsetManager) SetRetentionTime(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retentionTime = duration
}

// SetCompactInterval updates the compaction interval
func (c *OffsetManager) SetCompactInterval(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compactInterval = duration
}
