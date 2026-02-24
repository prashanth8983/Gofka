package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/prashanth8983/gofka/pkg/logger"
	"github.com/prashanth8983/gofka/pkg/network/protocol"
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

	// Sticky assignment cache for maintaining previous assignments
	previousAssignments map[string]map[string][]int32 // groupID -> memberID -> partitions

	// Persistence
	stateDir        string
	persistInterval time.Duration
	lastPersist     time.Time

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

	// Static membership support
	StaticMemberID string    // If set, member maintains ID across restarts
	JoinTime       time.Time // When member first joined
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

	// Use broker's log directory for state storage
	stateDir := filepath.Join(broker.logDir, "consumer-groups")

	return &ConsumerGroupCoordinator{
		broker:              broker,
		ctx:                 ctx,
		cancel:              cancel,
		groups:              make(map[string]*ConsumerGroup),
		members:             make(map[string]*ConsumerMember),
		assignments:         make(map[string]*PartitionAssignment),
		previousAssignments: make(map[string]map[string][]int32),
		stateDir:            stateDir,
		persistInterval:     30 * time.Second, // Persist every 30 seconds
		lastPersist:         time.Now(),
		rebalanceTimeout:    60 * time.Second,
		sessionTimeout:      30 * time.Second,
		heartbeatInterval:   3 * time.Second,
	}
}

// Start starts the consumer group coordinator
func (c *ConsumerGroupCoordinator) Start() error {
	// Create state directory if it doesn't exist
	if err := os.MkdirAll(c.stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	// Load existing state from disk
	if err := c.loadState(); err != nil {
		// Log error but don't fail - state will be rebuilt from scratch
		logger.Warn("Failed to load consumer group state", zap.Error(err))
	}

	// Start heartbeat monitoring
	go c.monitorHeartbeats()

	// Start rebalancing monitoring
	go c.monitorRebalancing()

	// Start periodic state persistence
	go c.periodicPersist()

	return nil
}

// Stop stops the consumer group coordinator
func (c *ConsumerGroupCoordinator) Stop() error {
	c.cancel()

	// Persist state one final time before stopping
	if err := c.persistState(); err != nil {
		logger.Warn("Failed to persist consumer group state on shutdown", zap.Error(err))
	}

	return nil
}

// JoinGroup handles a consumer joining a group
func (c *ConsumerGroupCoordinator) JoinGroup(groupID, memberID, clientID, clientHost string, sessionTimeout int32, subscriptions []string) (protocol.ConsumerGroupInterface, error) {
	return c.joinGroupInternal(groupID, memberID, clientID, clientHost, "", sessionTimeout, subscriptions)
}

// JoinGroupWithStatic handles a consumer joining a group with static membership
func (c *ConsumerGroupCoordinator) JoinGroupWithStatic(groupID, memberID, clientID, clientHost, staticMemberID string, sessionTimeout int32, subscriptions []string) (protocol.ConsumerGroupInterface, error) {
	return c.joinGroupInternal(groupID, memberID, clientID, clientHost, staticMemberID, sessionTimeout, subscriptions)
}

// joinGroupInternal is the internal implementation for joining a group
func (c *ConsumerGroupCoordinator) joinGroupInternal(groupID, memberID, clientID, clientHost, staticMemberID string, sessionTimeout int32, subscriptions []string) (protocol.ConsumerGroupInterface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create or get the group
	group, exists := c.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			GroupID:       groupID,
			Protocol:      "sticky", // Default to sticky for better performance
			Members:       make(map[string]*ConsumerMember),
			Assignments:   make(map[string][]int32),
			Generation:    0,
			State:         "stable",
			LastRebalance: time.Now(),
		}
		c.groups[groupID] = group
	}

	// Handle static membership
	if staticMemberID != "" {
		// Check if this static member already exists
		for existingMemberID, existingMember := range c.members {
			if existingMember.StaticMemberID == staticMemberID && existingMember.GroupID == groupID {
				// Found existing static member, update it
				if existingMemberID != memberID {
					// Remove old member ID
					delete(c.members, existingMemberID)
					delete(group.Members, existingMemberID)
				}

				// Update member info but keep the join time
				existingMember.MemberID = memberID
				existingMember.ClientID = clientID
				existingMember.ClientHost = clientHost
				existingMember.SessionTimeout = sessionTimeout
				existingMember.Subscriptions = subscriptions
				existingMember.LastHeartbeat = time.Now()
				existingMember.IsActive = true

				// Add with new member ID
				c.members[memberID] = existingMember
				group.Members[memberID] = existingMember

				// If this is the only member, make them the leader
				if len(group.Members) == 1 {
					group.LeaderID = memberID
				}

				// No rebalancing needed for static member rejoin
				return group, nil
			}
		}
	}

	// Create new member
	now := time.Now()
	member := &ConsumerMember{
		MemberID:       memberID,
		GroupID:        groupID,
		ClientID:       clientID,
		ClientHost:     clientHost,
		SessionTimeout: sessionTimeout,
		Protocol:       group.Protocol,
		Subscriptions:  subscriptions,
		LastHeartbeat:  now,
		IsActive:       true,
		StaticMemberID: staticMemberID,
		JoinTime:       now,
	}

	c.members[memberID] = member
	group.Members[memberID] = member

	// If this is the first member, make them the leader
	if len(group.Members) == 1 {
		group.LeaderID = memberID
	}

	// Trigger rebalancing if needed (not for static member rejoin)
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
	assignments := make(map[string][]int32)

	// Initialize empty assignments for all members
	for memberID := range members {
		assignments[memberID] = make([]int32, 0)
	}

	// Collect all partitions
	allPartitions := make([]int32, 0)
	for _, partitions := range topicPartitions {
		allPartitions = append(allPartitions, partitions...)
	}

	if len(allPartitions) == 0 || len(members) == 0 {
		return assignments
	}

	// Get the group ID from any member (they should all be in the same group)
	var groupID string
	for _, member := range members {
		groupID = member.GroupID
		break
	}

	// Get previous assignments for this group
	previousGroupAssignments, hasPrevious := c.previousAssignments[groupID]
	if !hasPrevious {
		previousGroupAssignments = make(map[string][]int32)
	}

	// Track which partitions have been assigned
	assignedPartitions := make(map[int32]bool)
	unassignedPartitions := make([]int32, 0)

	// First pass: Keep previous assignments if the member is still active
	for memberID, prevPartitions := range previousGroupAssignments {
		if _, stillActive := members[memberID]; stillActive {
			// Member is still active, try to keep their partitions
			keptPartitions := make([]int32, 0)
			for _, partition := range prevPartitions {
				// Check if partition still exists
				partitionExists := false
				for _, p := range allPartitions {
					if p == partition {
						partitionExists = true
						break
					}
				}

				if partitionExists && !assignedPartitions[partition] {
					keptPartitions = append(keptPartitions, partition)
					assignedPartitions[partition] = true
				}
			}
			assignments[memberID] = keptPartitions
		}
	}

	// Collect unassigned partitions
	for _, partition := range allPartitions {
		if !assignedPartitions[partition] {
			unassignedPartitions = append(unassignedPartitions, partition)
		}
	}

	// Second pass: Distribute unassigned partitions evenly
	if len(unassignedPartitions) > 0 {
		// Calculate target partition count per member
		targetPerMember := len(allPartitions) / len(members)
		extraPartitions := len(allPartitions) % len(members)

		// Sort members by current assignment size for fair distribution
		membersBySize := make([]string, 0, len(members))
		for memberID := range members {
			membersBySize = append(membersBySize, memberID)
		}

		// Simple sort by current assignment size (ascending)
		for i := 0; i < len(membersBySize)-1; i++ {
			for j := i + 1; j < len(membersBySize); j++ {
				if len(assignments[membersBySize[i]]) > len(assignments[membersBySize[j]]) {
					membersBySize[i], membersBySize[j] = membersBySize[j], membersBySize[i]
				}
			}
		}

		// Assign unassigned partitions to members with fewer partitions
		partitionIdx := 0
		for _, memberID := range membersBySize {
			target := targetPerMember
			if extraPartitions > 0 {
				target++
				extraPartitions--
			}

			// Assign partitions until member reaches target
			for len(assignments[memberID]) < target && partitionIdx < len(unassignedPartitions) {
				assignments[memberID] = append(assignments[memberID], unassignedPartitions[partitionIdx])
				partitionIdx++
			}
		}
	}

	// Update previous assignments cache for next rebalance
	c.previousAssignments[groupID] = assignments

	return assignments
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

// GetGroup returns a consumer group by ID (implements protocol.ConsumerGroupCoordinatorInterface)
func (c *ConsumerGroupCoordinator) GetGroup(groupID string) (protocol.ConsumerGroupInterface, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, exists := c.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	return group, nil
}

// GetGroupInternal returns a consumer group by ID (internal method returning concrete type)
func (c *ConsumerGroupCoordinator) GetGroupInternal(groupID string) (*ConsumerGroup, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, exists := c.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	return group, nil
}

// ListGroups returns all consumer group IDs (implements protocol.ConsumerGroupCoordinatorInterface)
func (c *ConsumerGroupCoordinator) ListGroups() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groupIDs := make([]string, 0, len(c.groups))
	for groupID := range c.groups {
		groupIDs = append(groupIDs, groupID)
	}
	return groupIDs
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

// GetProtocol returns the group protocol (e.g., "range", "roundrobin", "sticky")
func (g *ConsumerGroup) GetProtocol() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Protocol
}

// GetState returns the group state (e.g., "stable", "preparing_rebalance", "completing_rebalance")
func (g *ConsumerGroup) GetState() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.State
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

// IsStaticMember returns whether this is a static member
func (m *ConsumerMember) IsStaticMember() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.StaticMemberID != ""
}

// GetStaticMemberID returns the static member ID if set
func (m *ConsumerMember) GetStaticMemberID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.StaticMemberID
}

// GetClientID returns the client ID
func (m *ConsumerMember) GetClientID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ClientID
}

// GetClientHost returns the client host
func (m *ConsumerMember) GetClientHost() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ClientHost
}

// GroupState represents the persistent state of a consumer group
type GroupState struct {
	GroupID       string                 `json:"group_id"`
	Protocol      string                 `json:"protocol"`
	Generation    int32                  `json:"generation"`
	LeaderID      string                 `json:"leader_id"`
	State         string                 `json:"state"`
	Members       []MemberState          `json:"members"`
	Assignments   map[string][]int32     `json:"assignments"`
	LastRebalance time.Time              `json:"last_rebalance"`
}

// MemberState represents the persistent state of a group member
type MemberState struct {
	MemberID       string    `json:"member_id"`
	ClientID       string    `json:"client_id"`
	ClientHost     string    `json:"client_host"`
	SessionTimeout int32     `json:"session_timeout"`
	Subscriptions  []string  `json:"subscriptions"`
	StaticMemberID string    `json:"static_member_id,omitempty"`
	JoinTime       time.Time `json:"join_time,omitempty"`
}

// persistState saves the current state of all consumer groups to disk
func (c *ConsumerGroupCoordinator) persistState() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for groupID, group := range c.groups {
		if err := c.persistGroup(group); err != nil {
			return fmt.Errorf("failed to persist group %s: %w", groupID, err)
		}
	}

	// Also persist the previous assignments cache
	if err := c.persistPreviousAssignments(); err != nil {
		return fmt.Errorf("failed to persist previous assignments: %w", err)
	}

	c.lastPersist = time.Now()
	return nil
}

// persistGroup saves a single consumer group to disk
func (c *ConsumerGroupCoordinator) persistGroup(group *ConsumerGroup) error {
	group.mu.RLock()
	defer group.mu.RUnlock()

	// Convert to persistent state
	state := GroupState{
		GroupID:       group.GroupID,
		Protocol:      group.Protocol,
		Generation:    group.Generation,
		LeaderID:      group.LeaderID,
		State:         group.State,
		Assignments:   group.Assignments,
		LastRebalance: group.LastRebalance,
		Members:       make([]MemberState, 0, len(group.Members)),
	}

	// Convert members
	for _, member := range group.Members {
		member.mu.RLock()
		state.Members = append(state.Members, MemberState{
			MemberID:       member.MemberID,
			ClientID:       member.ClientID,
			ClientHost:     member.ClientHost,
			SessionTimeout: member.SessionTimeout,
			Subscriptions:  member.Subscriptions,
			StaticMemberID: member.StaticMemberID,
			JoinTime:       member.JoinTime,
		})
		member.mu.RUnlock()
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal group state: %w", err)
	}

	// Write to file
	groupFile := filepath.Join(c.stateDir, fmt.Sprintf("%s.json", group.GroupID))
	if err := os.WriteFile(groupFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write group state file: %w", err)
	}

	return nil
}

// persistPreviousAssignments saves the previous assignments cache to disk
func (c *ConsumerGroupCoordinator) persistPreviousAssignments() error {
	data, err := json.MarshalIndent(c.previousAssignments, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal previous assignments: %w", err)
	}

	assignmentsFile := filepath.Join(c.stateDir, "previous_assignments.json")
	if err := os.WriteFile(assignmentsFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write previous assignments file: %w", err)
	}

	return nil
}

// loadState loads consumer group state from disk
func (c *ConsumerGroupCoordinator) loadState() error {
	// Read all JSON files in state directory
	entries, err := os.ReadDir(c.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No state to load yet
		}
		return fmt.Errorf("failed to read state directory: %w", err)
	}

	// Load previous assignments first
	assignmentsFile := filepath.Join(c.stateDir, "previous_assignments.json")
	if data, err := os.ReadFile(assignmentsFile); err == nil {
		if err := json.Unmarshal(data, &c.previousAssignments); err != nil {
			logger.Debug("Failed to unmarshal previous assignments", zap.Error(err))
		}
	}

	// Load each group
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		if entry.Name() == "previous_assignments.json" {
			continue // Already loaded
		}

		groupFile := filepath.Join(c.stateDir, entry.Name())
		data, err := os.ReadFile(groupFile)
		if err != nil {
			logger.Warn("Failed to read group file", zap.String("file", groupFile), zap.Error(err))
			continue
		}

		var state GroupState
		if err := json.Unmarshal(data, &state); err != nil {
			logger.Warn("Failed to unmarshal group file", zap.String("file", groupFile), zap.Error(err))
			continue
		}

		// Recreate the group
		group := &ConsumerGroup{
			GroupID:       state.GroupID,
			Protocol:      state.Protocol,
			Generation:    state.Generation,
			LeaderID:      state.LeaderID,
			State:         "stable", // Always start in stable state after restart
			Assignments:   state.Assignments,
			LastRebalance: state.LastRebalance,
			Members:       make(map[string]*ConsumerMember),
		}

		// Note: Members are not restored as they need to rejoin after restart
		// Only assignments are preserved for sticky behavior

		c.groups[state.GroupID] = group

		// Restore assignments
		if _, exists := c.assignments[state.GroupID]; !exists {
			c.assignments[state.GroupID] = &PartitionAssignment{
				GroupID:     state.GroupID,
				Generation:  state.Generation,
				Assignments: state.Assignments,
				LastUpdate:  time.Now(),
			}
		}
	}

	logger.Info("Loaded consumer groups from disk", zap.Int("count", len(c.groups)))
	return nil
}

// periodicPersist periodically saves state to disk
func (c *ConsumerGroupCoordinator) periodicPersist() {
	ticker := time.NewTicker(c.persistInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.persistState(); err != nil {
				logger.Warn("Failed to persist consumer group state", zap.Error(err))
			}
		}
	}
}
