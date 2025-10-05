package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReplicaManager manages partition replication across brokers
type ReplicaManager struct {
	mu     sync.RWMutex
	broker *Broker
	ctx    context.Context
	cancel context.CancelFunc

	// Replication state
	replicas      map[string]*Replica // partition -> replica info
	leaderState   map[string]*LeaderState
	followerState map[string]*FollowerState

	// Configuration
	replicationTimeout time.Duration
	heartbeatInterval  time.Duration
}

// Replica represents a replica of a partition
type Replica struct {
	TopicName    string
	PartitionID  int32
	BrokerID     string
	BrokerAddr   string
	IsLeader     bool
	IsInSync     bool
	LastCaughtUp time.Time
	ReplicaID    int32
}

// LeaderState tracks leader-specific replication state
type LeaderState struct {
	mu         sync.RWMutex
	replicas   []*Replica
	isr        []*Replica // In-Sync Replicas
	hwMark     int64      // High Water Mark
	leo        int64      // Log End Offset
	lastUpdate time.Time
}

// FollowerState tracks follower-specific replication state
type FollowerState struct {
	mu           sync.RWMutex
	leaderID     string
	leaderAddr   string
	hwMark       int64
	leo          int64
	lastFetch    time.Time
	lastCaughtUp time.Time
}

// NewReplicaManager creates a new ReplicaManager
func NewReplicaManager(broker *Broker) *ReplicaManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaManager{
		broker:             broker,
		ctx:                ctx,
		cancel:             cancel,
		replicas:           make(map[string]*Replica),
		leaderState:        make(map[string]*LeaderState),
		followerState:      make(map[string]*FollowerState),
		replicationTimeout: 30 * time.Second,
		heartbeatInterval:  1 * time.Second,
	}
}

// Start starts the replica manager
func (rm *ReplicaManager) Start() error {
	// Start replication heartbeat
	go rm.replicationHeartbeat()

	// Start ISR monitoring
	go rm.monitorISR()

	return nil
}

// Stop stops the replica manager
func (rm *ReplicaManager) Stop() error {
	rm.cancel()
	return nil
}

// AddReplica adds a replica for a partition
func (rm *ReplicaManager) AddReplica(topicName string, partitionID int32, brokerID, brokerAddr string, isLeader bool) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	replica := &Replica{
		TopicName:    topicName,
		PartitionID:  partitionID,
		BrokerID:     brokerID,
		BrokerAddr:   brokerAddr,
		IsLeader:     isLeader,
		IsInSync:     true,
		LastCaughtUp: time.Now(),
	}

	rm.replicas[key] = replica

	if isLeader {
		rm.leaderState[key] = &LeaderState{
			replicas:   []*Replica{replica},
			isr:        []*Replica{replica},
			hwMark:     0,
			leo:        0,
			lastUpdate: time.Now(),
		}
	} else {
		rm.followerState[key] = &FollowerState{
			leaderID:     brokerID,
			leaderAddr:   brokerAddr,
			hwMark:       0,
			leo:          0,
			lastFetch:    time.Now(),
			lastCaughtUp: time.Now(),
		}
	}

	return nil
}

// RemoveReplica removes a replica for a partition
func (rm *ReplicaManager) RemoveReplica(topicName string, partitionID int32) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	delete(rm.replicas, key)
	delete(rm.leaderState, key)
	delete(rm.followerState, key)

	return nil
}

// PromoteToLeader promotes a replica to leader
func (rm *ReplicaManager) PromoteToLeader(topicName string, partitionID int32) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	replica, exists := rm.replicas[key]
	if !exists {
		return fmt.Errorf("replica not found for %s", key)
	}

	replica.IsLeader = true
	replica.IsInSync = true

	// Initialize leader state
	rm.leaderState[key] = &LeaderState{
		replicas:   []*Replica{replica},
		isr:        []*Replica{replica},
		hwMark:     0,
		leo:        0,
		lastUpdate: time.Now(),
	}

	// Remove follower state if it exists
	delete(rm.followerState, key)

	return nil
}

// DemoteFromLeader demotes a replica from leader
func (rm *ReplicaManager) DemoteFromLeader(topicName string, partitionID int32) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	replica, exists := rm.replicas[key]
	if !exists {
		return fmt.Errorf("replica not found for %s", key)
	}

	replica.IsLeader = false

	// Remove leader state
	delete(rm.leaderState, key)

	return nil
}

// UpdateHighWaterMark updates the high water mark for a partition
func (rm *ReplicaManager) UpdateHighWaterMark(topicName string, partitionID int32, hwMark int64) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)

	// Update leader state if this broker is the leader
	if leaderState, exists := rm.leaderState[key]; exists {
		leaderState.mu.Lock()
		leaderState.hwMark = hwMark
		leaderState.lastUpdate = time.Now()
		leaderState.mu.Unlock()
	}

	// Update follower state if this broker is a follower
	if followerState, exists := rm.followerState[key]; exists {
		followerState.mu.Lock()
		followerState.hwMark = hwMark
		followerState.mu.Unlock()
	}

	return nil
}

// GetISR returns the In-Sync Replicas for a partition
func (rm *ReplicaManager) GetISR(topicName string, partitionID int32) []*Replica {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	if leaderState, exists := rm.leaderState[key]; exists {
		leaderState.mu.RLock()
		defer leaderState.mu.RUnlock()
		return leaderState.isr
	}
	return nil
}

// replicationHeartbeat sends periodic heartbeats to replicas
func (rm *ReplicaManager) replicationHeartbeat() {
	ticker := time.NewTicker(rm.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends heartbeats to all replicas
func (rm *ReplicaManager) sendHeartbeats() {
	rm.mu.RLock()
	replicas := make([]*Replica, 0, len(rm.replicas))
	for _, replica := range rm.replicas {
		replicas = append(replicas, replica)
	}
	rm.mu.RUnlock()

	for _, replica := range replicas {
		if replica.IsLeader {
			rm.sendLeaderHeartbeat(replica)
		} else {
			rm.sendFollowerHeartbeat(replica)
		}
	}
}

// sendLeaderHeartbeat sends heartbeat from leader to followers
func (rm *ReplicaManager) sendLeaderHeartbeat(replica *Replica) {
	// TODO: Implement actual network communication with followers
	// For now, just log the heartbeat
	key := fmt.Sprintf("%s-%d", replica.TopicName, replica.PartitionID)
	if leaderState, exists := rm.leaderState[key]; exists {
		leaderState.mu.Lock()
		leaderState.lastUpdate = time.Now()
		leaderState.mu.Unlock()
	}
}

// sendFollowerHeartbeat sends heartbeat from follower to leader
func (rm *ReplicaManager) sendFollowerHeartbeat(replica *Replica) {
	// TODO: Implement actual network communication with leader
	// For now, just log the heartbeat
	key := fmt.Sprintf("%s-%d", replica.TopicName, replica.PartitionID)
	if followerState, exists := rm.followerState[key]; exists {
		followerState.mu.Lock()
		followerState.lastFetch = time.Now()
		followerState.mu.Unlock()
	}
}

// monitorISR monitors and updates the In-Sync Replicas
func (rm *ReplicaManager) monitorISR() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.updateISR()
		}
	}
}

// updateISR updates the ISR based on replica health
func (rm *ReplicaManager) updateISR() {
	rm.mu.RLock()
	leaderStates := make(map[string]*LeaderState)
	for key, state := range rm.leaderState {
		leaderStates[key] = state
	}
	rm.mu.RUnlock()

	for _, leaderState := range leaderStates {
		leaderState.mu.Lock()

		// Check which replicas are still in sync
		var newISR []*Replica
		for _, replica := range leaderState.replicas {
			if time.Since(replica.LastCaughtUp) < rm.replicationTimeout {
				replica.IsInSync = true
				newISR = append(newISR, replica)
			} else {
				replica.IsInSync = false
			}
		}

		leaderState.isr = newISR
		leaderState.lastUpdate = time.Now()

		leaderState.mu.Unlock()
	}
}

// GetReplicaInfo returns information about a specific replica
func (rm *ReplicaManager) GetReplicaInfo(topicName string, partitionID int32) (*Replica, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	replica, exists := rm.replicas[key]
	if !exists {
		return nil, fmt.Errorf("replica not found for %s", key)
	}

	return replica, nil
}

// IsLeader checks if this broker is the leader for a partition
func (rm *ReplicaManager) IsLeader(topicName string, partitionID int32) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	replica, exists := rm.replicas[key]
	if !exists {
		return false
	}

	return replica.IsLeader
}

// GetReplicationFactor returns the replication factor for a topic
func (rm *ReplicaManager) GetReplicationFactor(topicName string) int32 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	count := int32(0)
	for _, replica := range rm.replicas {
		if replica.TopicName == topicName {
			count++
		}
	}

	return count
}
