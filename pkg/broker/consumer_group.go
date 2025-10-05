package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/user/gofka/pkg/network/protocol"
)

// ConsumerGroupCoordinator manages consumer groups and their partition assignments
type ConsumerGroupCoordinator struct {
	mu     sync.RWMutex
	broker *Broker
	ctx    context.Context
	cancel context.CancelFunc

	// Group state
	groups      map[string]*ConsumerGroup
	members     map[string]*ConsumerMember      // memberID -> member
	assignments map[string]*PartitionAssignment // groupID -> assignment

	// Configuration
	rebalanceTimeout  time.Duration
	sessionTimeout    time.Duration
	heartbeatInterval time.Duration
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	mu            sync.RWMutex
	GroupID       string
	Protocol      string // "range", "roundrobin", "sticky"
	Members       map[string]*ConsumerMember
	Assignments   map[string][]int32 // memberID -> partitionIDs
	Generation    int32
	LeaderID      string
	State         string // "stable", "preparing_rebalance", "completing_rebalance"
	LastRebalance time.Time
}

// ConsumerMember represents a consumer in a group
type ConsumerMember struct {
	mu             sync.RWMutex
	MemberID       string
	GroupID        string
	ClientID       string
	ClientHost     string
	SessionTimeout int32
	Protocol       string
	Subscriptions  []string // topics
	LastHeartbeat  time.Time
	IsActive       bool
}

// PartitionAssignment represents partition assignments for a group
type PartitionAssignment struct {
	mu          sync.RWMutex
	GroupID     string
	Generation  int32
	Assignments map[string][]int32 // memberID -> partitionIDs
	LastUpdate  time.Time
}

// NewConsumerGroupCoordinator creates a new ConsumerGroupCoordinator
func NewConsumerGroupCoordinator(broker *Broker) *ConsumerGroupCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerGroupCoordinator{
		broker:            broker,
		ctx:               ctx,
		cancel:            cancel,
		groups:            make(map[string]*ConsumerGroup),
		members:           make(map[string]*ConsumerMember),
		assignments:       make(map[string]*PartitionAssignment),
		rebalanceTimeout:  60 * time.Second,
		sessionTimeout:    30 * time.Second,
		heartbeatInterval: 3 * time.Second,
	}
}

// Start starts the consumer group coordinator
func (c *ConsumerGroupCoordinator) Start() error {
	// Start heartbeat monitoring
	go c.monitorHeartbeats()

	// Start rebalancing monitoring
	go c.monitorRebalancing()

	return nil
}

// Stop stops the consumer group coordinator
func (c *ConsumerGroupCoordinator) Stop() error {
	c.cancel()
	return nil
}

// JoinGroup handles a consumer joining a group
func (c *ConsumerGroupCoordinator) JoinGroup(groupID, memberID, clientID, clientHost string, sessionTimeout int32, subscriptions []string) (protocol.ConsumerGroupInterface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create or get the group
	group, exists := c.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			GroupID:       groupID,
			Protocol:      "range", // Default protocol
			Members:       make(map[string]*ConsumerMember),
			Assignments:   make(map[string][]int32),
			Generation:    0,
			State:         "stable",
			LastRebalance: time.Now(),
		}
		c.groups[groupID] = group
	}

	// Create or update the member
	member := &ConsumerMember{
		MemberID:       memberID,
		GroupID:        groupID,
		ClientID:       clientID,
		ClientHost:     clientHost,
		SessionTimeout: sessionTimeout,
		Protocol:       group.Protocol,
		Subscriptions:  subscriptions,
		LastHeartbeat:  time.Now(),
		IsActive:       true,
	}

	c.members[memberID] = member
	group.Members[memberID] = member

	// If this is the first member, make them the leader
	if len(group.Members) == 1 {
		group.LeaderID = memberID
	}

	// Trigger rebalancing if needed
	if group.State == "stable" {
		group.State = "preparing_rebalance"
		group.Generation++
	}

	return group, nil
}

// LeaveGroup handles a consumer leaving a group
func (c *ConsumerGroupCoordinator) LeaveGroup(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	// Remove member from group
	delete(group.Members, memberID)
	delete(c.members, memberID)

	// If group is empty, remove it
	if len(group.Members) == 0 {
		delete(c.groups, groupID)
		delete(c.assignments, groupID)
		return nil
	}

	// If the leader left, assign a new leader
	if group.LeaderID == memberID {
		for newLeaderID := range group.Members {
			group.LeaderID = newLeaderID
			break
		}
	}

	// Trigger rebalancing
	group.State = "preparing_rebalance"
	group.Generation++

	return nil
}

// SyncGroup handles partition assignment synchronization
func (c *ConsumerGroupCoordinator) SyncGroup(groupID, memberID string, generation int32) (protocol.ConsumerGroupInterface, error) {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	group.mu.RLock()
	if group.Generation != generation {
		group.mu.RUnlock()
		return nil, fmt.Errorf("generation mismatch: expected %d, got %d", group.Generation, generation)
	}
	group.mu.RUnlock()

	// If rebalancing is needed, perform it
	if group.State != "stable" {
		if err := c.performRebalancing(group); err != nil {
			return nil, fmt.Errorf("failed to perform rebalancing: %w", err)
		}
	}

	// Return the group which contains the assignments
	return group, nil
}

// Heartbeat handles consumer heartbeats
func (c *ConsumerGroupCoordinator) Heartbeat(groupID, memberID string, generation int32) error {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.RLock()
	if group.Generation != generation {
		group.mu.RUnlock()
		return fmt.Errorf("generation mismatch: expected %d, got %d", group.Generation, generation)
	}

	member, exists := group.Members[memberID]
	group.mu.RUnlock()

	if !exists {
		return fmt.Errorf("member %s not found in group %s", memberID, groupID)
	}

	member.mu.Lock()
	member.LastHeartbeat = time.Now()
	member.IsActive = true
	member.mu.Unlock()

	return nil
}

// performRebalancing performs partition assignment for a group
func (c *ConsumerGroupCoordinator) performRebalancing(group *ConsumerGroup) error {
	group.mu.Lock()
	defer group.mu.Unlock()

	// Get all topics that members are subscribed to
	topicSet := make(map[string]bool)
	for _, member := range group.Members {
		for _, topic := range member.Subscriptions {
			topicSet[topic] = true
		}
	}

	// Get partitions for each topic
	topicPartitions := make(map[string][]int32)
	for topic := range topicSet {
		partitions := c.broker.GetPartitions(topic)
		if partitions != nil {
			partitionIDs := make([]int32, 0, len(partitions))
			for _, partition := range partitions {
				partitionIDs = append(partitionIDs, partition.ID)
			}
			topicPartitions[topic] = partitionIDs
		}
	}

	// Perform partition assignment based on protocol
	var assignments map[string][]int32
	switch group.Protocol {
	case "range":
		assignments = c.assignPartitionsRange(group.Members, topicPartitions)
	case "roundrobin":
		assignments = c.assignPartitionsRoundRobin(group.Members, topicPartitions)
	case "sticky":
		assignments = c.assignPartitionsSticky(group.Members, topicPartitions)
	default:
		assignments = c.assignPartitionsRange(group.Members, topicPartitions)
	}

	// Update group assignments
	group.Assignments = assignments
	group.State = "stable"

	// Update coordinator assignments
	c.mu.Lock()
	c.assignments[group.GroupID] = &PartitionAssignment{
		GroupID:     group.GroupID,
		Generation:  group.Generation,
		Assignments: assignments,
		LastUpdate:  time.Now(),
	}
	c.mu.Unlock()

	return nil
}

// assignPartitionsRange assigns partitions using range-based strategy
func (c *ConsumerGroupCoordinator) assignPartitionsRange(members map[string]*ConsumerMember, topicPartitions map[string][]int32) map[string][]int32 {
	assignments := make(map[string][]int32)
	memberIDs := make([]string, 0, len(members))
	for memberID := range members {
		memberIDs = append(memberIDs, memberID)
		assignments[memberID] = make([]int32, 0)
	}

	if len(memberIDs) == 0 {
		return assignments
	}

	// Sort member IDs for consistent assignment
	// In a real implementation, you'd want a more sophisticated sorting

	for _, partitions := range topicPartitions {
		partitionsPerMember := len(partitions) / len(memberIDs)
		extraPartitions := len(partitions) % len(memberIDs)

		partitionIndex := 0
		for i, memberID := range memberIDs {
			start := partitionIndex
			end := start + partitionsPerMember
			if i < extraPartitions {
				end++
			}

			if start < len(partitions) {
				assignments[memberID] = append(assignments[memberID], partitions[start:end]...)
			}
			partitionIndex = end
		}
	}

	return assignments
}

// assignPartitionsRoundRobin assigns partitions using round-robin strategy
func (c *ConsumerGroupCoordinator) assignPartitionsRoundRobin(members map[string]*ConsumerMember, topicPartitions map[string][]int32) map[string][]int32 {
	assignments := make(map[string][]int32)
	memberIDs := make([]string, 0, len(members))
	for memberID := range members {
		memberIDs = append(memberIDs, memberID)
		assignments[memberID] = make([]int32, 0)
	}

	if len(memberIDs) == 0 {
		return assignments
	}

	memberIndex := 0
	for _, partitions := range topicPartitions {
		for _, partitionID := range partitions {
			memberID := memberIDs[memberIndex%len(memberIDs)]
			assignments[memberID] = append(assignments[memberID], partitionID)
			memberIndex++
		}
	}

	return assignments
}

// assignPartitionsSticky assigns partitions using sticky strategy (maintains previous assignments when possible)
func (c *ConsumerGroupCoordinator) assignPartitionsSticky(members map[string]*ConsumerMember, topicPartitions map[string][]int32) map[string][]int32 {
	// For now, fall back to round-robin
	// TODO: Implement sticky assignment logic
	return c.assignPartitionsRoundRobin(members, topicPartitions)
}

// monitorHeartbeats monitors member heartbeats and removes inactive members
func (c *ConsumerGroupCoordinator) monitorHeartbeats() {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkMemberHealth()
		}
	}
}

// checkMemberHealth checks member health and removes inactive ones
func (c *ConsumerGroupCoordinator) checkMemberHealth() {
	c.mu.RLock()
	members := make([]*ConsumerMember, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, member)
	}
	c.mu.RUnlock()

	now := time.Now()
	for _, member := range members {
		member.mu.RLock()
		lastHeartbeat := member.LastHeartbeat
		sessionTimeout := time.Duration(member.SessionTimeout) * time.Millisecond
		member.mu.RUnlock()

		if now.Sub(lastHeartbeat) > sessionTimeout {
			// Member is inactive, remove it
			c.LeaveGroup(member.GroupID, member.MemberID)
		}
	}
}

// monitorRebalancing monitors rebalancing state and completes it when ready
func (c *ConsumerGroupCoordinator) monitorRebalancing() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkRebalancingState()
		}
	}
}

// checkRebalancingState checks if rebalancing can be completed
func (c *ConsumerGroupCoordinator) checkRebalancingState() {
	c.mu.RLock()
	groups := make([]*ConsumerGroup, 0, len(c.groups))
	for _, group := range c.groups {
		groups = append(groups, group)
	}
	c.mu.RUnlock()

	for _, group := range groups {
		group.mu.RLock()
		state := group.State
		lastRebalance := group.LastRebalance
		group.mu.RUnlock()

		if state == "preparing_rebalance" && time.Since(lastRebalance) > c.rebalanceTimeout {
			// Rebalancing timeout, complete it
			group.mu.Lock()
			group.State = "stable"
			group.mu.Unlock()
		}
	}
}

// GetGroup returns a consumer group by ID
func (c *ConsumerGroupCoordinator) GetGroup(groupID string) (*ConsumerGroup, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, exists := c.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	return group, nil
}

// GetMember returns a consumer member by ID
func (c *ConsumerGroupCoordinator) GetMember(memberID string) (*ConsumerMember, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	member, exists := c.members[memberID]
	if !exists {
		return nil, fmt.Errorf("member %s not found", memberID)
	}

	return member, nil
}

// GetAssignment returns partition assignments for a group
func (c *ConsumerGroupCoordinator) GetAssignment(groupID string) (*PartitionAssignment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	assignment, exists := c.assignments[groupID]
	if !exists {
		return nil, fmt.Errorf("no assignment found for group %s", groupID)
	}

	return assignment, nil
}

// Protocol interface implementation for ConsumerGroup

// GetGroupID returns the group ID
func (g *ConsumerGroup) GetGroupID() string {
	return g.GroupID
}

// GetGeneration returns the generation number
func (g *ConsumerGroup) GetGeneration() int32 {
	return g.Generation
}

// GetLeaderID returns the leader member ID
func (g *ConsumerGroup) GetLeaderID() string {
	return g.LeaderID
}

// GetMembers returns the group members
func (g *ConsumerGroup) GetMembers() map[string]protocol.ConsumerMemberInterface {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make(map[string]protocol.ConsumerMemberInterface, len(g.Members))
	for id, member := range g.Members {
		result[id] = member
	}
	return result
}

// GetAssignments returns partition assignments
func (g *ConsumerGroup) GetAssignments() map[string][]int32 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Make a copy to avoid race conditions
	result := make(map[string][]int32, len(g.Assignments))
	for memberID, partitions := range g.Assignments {
		result[memberID] = append([]int32{}, partitions...)
	}
	return result
}

// Protocol interface implementation for ConsumerMember

// GetMemberID returns the member ID
func (m *ConsumerMember) GetMemberID() string {
	return m.MemberID
}

// GetSubscriptions returns the subscribed topics
func (m *ConsumerMember) GetSubscriptions() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string{}, m.Subscriptions...)
}
