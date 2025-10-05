package broker

import (
	"context"
	"fmt"
	"time"

	gofkav1 "github.com/user/gofka/api/v1"
)

// ReplicaFetch handles replica fetch requests from followers
func (b *Broker) ReplicaFetch(ctx context.Context, req *gofkav1.ReplicaFetchRequest) (*gofkav1.ReplicaFetchResponse, error) {
	// Get the log for the topic/partition
	log := b.GetLog(req.Topic, req.Partition)
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
func (b *Broker) ReplicaHeartbeat(ctx context.Context, req *gofkav1.ReplicaHeartbeatRequest) (*gofkav1.ReplicaHeartbeatResponse, error) {
	// Get the log for the topic/partition
	log := b.GetLog(req.Topic, req.Partition)
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
	rm := b.GetReplicationManager()
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
		LeaderId:      b.nodeID,
	}, nil
}
