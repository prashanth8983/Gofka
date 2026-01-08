package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	gofkav1 "github.com/prashanth8983/gofka/api/v1"
)

// grpcBrokerWrapper wraps the broker to implement gRPC service interface
type grpcBrokerWrapper struct {
	gofkav1.UnimplementedGofkaServiceServer
	broker *Broker
}

// ReplicaFetch handles replica fetch requests from followers
func (w *grpcBrokerWrapper) ReplicaFetch(ctx context.Context, req *gofkav1.ReplicaFetchRequest) (*gofkav1.ReplicaFetchResponse, error) {
	// Get the log for the topic/partition
	log := w.broker.GetLog(req.Topic, req.Partition)
	if log == nil {
		return &gofkav1.ReplicaFetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Error:     fmt.Sprintf("log not found for %s-%d", req.Topic, req.Partition),
		}, nil
	}

	// Get high watermark and LEO
	leo := log.GetLogEndOffset()
	hwMark := leo // For simplicity, HW == LEO for now

	// If fetch offset is at LEO, return empty (caught up)
	if req.FetchOffset >= leo {
		return &gofkav1.ReplicaFetchResponse{
			Topic:          req.Topic,
			Partition:      req.Partition,
			HighWatermark:  hwMark,
			LogEndOffset:   leo,
			Records:        []*gofkav1.LogRecord{},
		}, nil
	}

	// Read batch of records
	messages, _, err := log.ReadBatch(req.FetchOffset, req.MaxBytes)
	if err != nil {
		return &gofkav1.ReplicaFetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Error:     fmt.Sprintf("failed to read batch: %v", err),
		}, nil
	}

	// Convert to log records
	records := make([]*gofkav1.LogRecord, len(messages))
	for i, msg := range messages {
		records[i] = &gofkav1.LogRecord{
			Offset:    req.FetchOffset + int64(i),
			Value:     msg,
			Timestamp: time.Now().UnixNano(),
		}
	}

	return &gofkav1.ReplicaFetchResponse{
		Topic:          req.Topic,
		Partition:      req.Partition,
		HighWatermark:  hwMark,
		LogEndOffset:   leo,
		Records:        records,
	}, nil
}

// ReplicaHeartbeat handles replica heartbeat requests from followers
func (w *grpcBrokerWrapper) ReplicaHeartbeat(ctx context.Context, req *gofkav1.ReplicaHeartbeatRequest) (*gofkav1.ReplicaHeartbeatResponse, error) {
	// Get the log for the topic/partition
	log := w.broker.GetLog(req.Topic, req.Partition)
	if log == nil {
		return &gofkav1.ReplicaHeartbeatResponse{
			Success: false,
			Error:   fmt.Sprintf("log not found for %s-%d", req.Topic, req.Partition),
		}, nil
	}

	// Get high watermark
	leo := log.GetLogEndOffset()
	hwMark := leo // For simplicity, HW == LEO for now

	// Update replica state in replication manager
	rm := w.broker.GetReplicationManager()
	key := fmt.Sprintf("%s-%d", req.Topic, req.Partition)

	rm.mu.RLock()
	leaderState, exists := rm.leaderState[key]
	rm.mu.RUnlock()

	if exists {
		// Update follower's last caught up time
		leaderState.mu.Lock()
		for _, replica := range leaderState.replicas {
			if replica.BrokerID == req.ReplicaId {
				replica.LastCaughtUp = time.Now()
				// Check if follower is caught up
				if req.CurrentOffset >= hwMark-10 { // Within 10 messages
					replica.IsInSync = true
				}
				break
			}
		}
		leaderState.mu.Unlock()
	}

	return &gofkav1.ReplicaHeartbeatResponse{
		Success:       true,
		HighWatermark: hwMark,
		LeaderId:      w.broker.nodeID,
	}, nil
}

// JoinCluster handles cluster join requests from new nodes
func (w *grpcBrokerWrapper) JoinCluster(ctx context.Context, req *gofkav1.JoinClusterRequest) (*gofkav1.JoinClusterResponse, error) {
	fmt.Printf("Received join cluster request from node %s (raft: %s, broker: %s)\n",
		req.NodeId, req.RaftAddress, req.BrokerAddress)

	// Check if we're the leader
	isLeader := w.broker.consensus.IsLeader()
	leaderAddr := string(w.broker.consensus.GetLeader())

	if !isLeader {
		// We're not the leader, return the leader address
		return &gofkav1.JoinClusterResponse{
			Success:       false,
			Error:         "not the leader",
			LeaderAddress: leaderAddr,
			IsLeader:      false,
		}, nil
	}

	// We're the leader, add the new node to the cluster
	err := w.broker.consensus.AddVoter(
		raft.ServerID(req.NodeId),
		raft.ServerAddress(req.RaftAddress),
	)

	if err != nil {
		return &gofkav1.JoinClusterResponse{
			Success:       false,
			Error:         fmt.Sprintf("failed to add voter: %v", err),
			LeaderAddress: leaderAddr,
			IsLeader:      true,
		}, nil
	}

	fmt.Printf("Successfully added node %s to cluster\n", req.NodeId)

	return &gofkav1.JoinClusterResponse{
		Success:       true,
		LeaderAddress: leaderAddr,
		IsLeader:      true,
	}, nil
}

// Produce implements the gRPC Produce method
func (w *grpcBrokerWrapper) Produce(ctx context.Context, req *gofkav1.ProduceRequest) (*gofkav1.ProduceResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.ProduceResponse{}, fmt.Errorf("use binary protocol for produce requests")
}

// Consume implements the gRPC Consume method
func (w *grpcBrokerWrapper) Consume(ctx context.Context, req *gofkav1.ConsumeRequest) (*gofkav1.ConsumeResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.ConsumeResponse{}, fmt.Errorf("use binary protocol for consume requests")
}

// GetMetadata implements the gRPC GetMetadata method
func (w *grpcBrokerWrapper) GetMetadata(ctx context.Context, req *gofkav1.MetadataRequest) (*gofkav1.MetadataResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.MetadataResponse{}, fmt.Errorf("use binary protocol for metadata requests")
}

// CreateTopic implements the gRPC CreateTopic method (renamed from CreateTopicGRPC)
func (w *grpcBrokerWrapper) CreateTopic(ctx context.Context, req *gofkav1.CreateTopicRequest) (*gofkav1.CreateTopicResponse, error) {
	// Delegate to existing CreateTopic method
	err := w.broker.CreateTopic(req.TopicName, req.NumPartitions)
	if err != nil {
		return &gofkav1.CreateTopicResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}
	return &gofkav1.CreateTopicResponse{
		Success: true,
		Message: "Topic created successfully",
	}, nil
}

// OffsetCommit implements the gRPC OffsetCommit method
func (w *grpcBrokerWrapper) OffsetCommit(ctx context.Context, req *gofkav1.OffsetCommitRequest) (*gofkav1.OffsetCommitResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.OffsetCommitResponse{}, fmt.Errorf("use binary protocol for offset commit requests")
}

// OffsetFetch implements the gRPC OffsetFetch method
func (w *grpcBrokerWrapper) OffsetFetch(ctx context.Context, req *gofkav1.OffsetFetchRequest) (*gofkav1.OffsetFetchResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.OffsetFetchResponse{}, fmt.Errorf("use binary protocol for offset fetch requests")
}

// JoinGroup implements the gRPC JoinGroup method
func (w *grpcBrokerWrapper) JoinGroup(ctx context.Context, req *gofkav1.JoinGroupRequest) (*gofkav1.JoinGroupResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.JoinGroupResponse{}, fmt.Errorf("use binary protocol for join group requests")
}

// Heartbeat implements the gRPC Heartbeat method
func (w *grpcBrokerWrapper) Heartbeat(ctx context.Context, req *gofkav1.HeartbeatRequest) (*gofkav1.HeartbeatResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.HeartbeatResponse{}, fmt.Errorf("use binary protocol for heartbeat requests")
}

// SyncGroup implements the gRPC SyncGroup method
func (w *grpcBrokerWrapper) SyncGroup(ctx context.Context, req *gofkav1.SyncGroupRequest) (*gofkav1.SyncGroupResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.SyncGroupResponse{}, fmt.Errorf("use binary protocol for sync group requests")
}

// LeaveGroup implements the gRPC LeaveGroup method
func (w *grpcBrokerWrapper) LeaveGroup(ctx context.Context, req *gofkav1.LeaveGroupRequest) (*gofkav1.LeaveGroupResponse, error) {
	// Not implemented via gRPC - use binary protocol
	return &gofkav1.LeaveGroupResponse{}, fmt.Errorf("use binary protocol for leave group requests")
}
