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

	// Network client for inter-broker communication
	brokerClient *BrokerClient

	// Configuration
	replicationTimeout time.Duration
	heartbeatInterval  time.Duration
	fetchMaxBytes      int64
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
		brokerClient:       NewBrokerClient(),
		replicationTimeout: 30 * time.Second,
		heartbeatInterval:  1 * time.Second,
		fetchMaxBytes:      1024 * 1024, // 1MB
	}
}

// Start starts the replica manager
func (rm *ReplicaManager) Start() error {
	// Start replication heartbeat
	go rm.replicationHeartbeat()

	// Start ISR monitoring
	go rm.monitorISR()

	// Start follower fetch loop
	go rm.followerFetchLoop()

	return nil
}

// Stop stops the replica manager
func (rm *ReplicaManager) Stop() error {
	rm.cancel()
	if rm.brokerClient != nil {
		rm.brokerClient.Close()
	}
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
	// Leader just updates local state - followers will fetch
	key := fmt.Sprintf("%s-%d", replica.TopicName, replica.PartitionID)
	if leaderState, exists := rm.leaderState[key]; exists {
		leaderState.mu.Lock()
		leaderState.lastUpdate = time.Now()
		leaderState.mu.Unlock()
	}
}

// sendFollowerHeartbeat sends heartbeat from follower to leader
func (rm *ReplicaManager) sendFollowerHeartbeat(replica *Replica) {
	key := fmt.Sprintf("%s-%d", replica.TopicName, replica.PartitionID)
	followerState, exists := rm.followerState[key]
	if !exists {
		return
	}

	followerState.mu.RLock()
	leaderAddr := followerState.leaderAddr
	leo := followerState.leo
	followerState.mu.RUnlock()

	// Send heartbeat to leader
	_, err := rm.brokerClient.SendHeartbeatToLeader(
		leaderAddr,
		rm.broker.nodeID,
		replica.TopicName,
		replica.PartitionID,
		leo,
	)

	if err != nil {
		fmt.Printf("Failed to send heartbeat to leader %s: %v\n", leaderAddr, err)
		return
	}

	followerState.mu.Lock()
	followerState.lastFetch = time.Now()
	followerState.mu.Unlock()
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

// followerFetchLoop continuously fetches data from leaders for follower partitions
func (rm *ReplicaManager) followerFetchLoop() {
	ticker := time.NewTicker(100 * time.Millisecond) // Fetch every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.fetchFromLeaders()
		}
	}
}

// fetchFromLeaders fetches data from all leaders this broker is following
func (rm *ReplicaManager) fetchFromLeaders() {
	rm.mu.RLock()
	followerStates := make(map[string]*FollowerState)
	replicas := make(map[string]*Replica)
	for key, state := range rm.followerState {
		followerStates[key] = state
		replicas[key] = rm.replicas[key]
	}
	rm.mu.RUnlock()

	for key, followerState := range followerStates {
		replica := replicas[key]
		if replica == nil {
			continue
		}

		rm.fetchFromLeader(replica, followerState)
	}
}

// fetchFromLeader fetches data from a leader for a specific partition
func (rm *ReplicaManager) fetchFromLeader(replica *Replica, followerState *FollowerState) {
	followerState.mu.RLock()
	leaderAddr := followerState.leaderAddr
	fetchOffset := followerState.leo
	followerState.mu.RUnlock()

	// Fetch from leader
	resp, err := rm.brokerClient.FetchFromLeader(
		leaderAddr,
		replica.TopicName,
		replica.PartitionID,
		fetchOffset,
		rm.fetchMaxBytes,
		rm.broker.nodeID,
	)

	if err != nil {
		fmt.Printf("Failed to fetch from leader %s for %s-%d: %v\n",
			leaderAddr, replica.TopicName, replica.PartitionID, err)
		return
	}

	if resp.Error != "" {
		fmt.Printf("Leader returned error for %s-%d: %s\n",
			replica.TopicName, replica.PartitionID, resp.Error)
		return
	}

	// Append records to local log
	if len(resp.Records) > 0 {
		log := rm.broker.GetLog(replica.TopicName, replica.PartitionID)
		if log == nil {
			fmt.Printf("Log not found for %s-%d\n", replica.TopicName, replica.PartitionID)
			return
		}

		for _, record := range resp.Records {
			_, err := log.Append(record.Value)
			if err != nil {
				fmt.Printf("Failed to append record to log %s-%d: %v\n",
					replica.TopicName, replica.PartitionID, err)
				return
			}
		}

		// Update follower state
		followerState.mu.Lock()
		followerState.leo = resp.LogEndOffset
		followerState.hwMark = resp.HighWatermark
		followerState.lastFetch = time.Now()
		followerState.lastCaughtUp = time.Now()
		followerState.mu.Unlock()

		// Update replica state
		replica.LastCaughtUp = time.Now()
		replica.IsInSync = true
	}
}
