package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	gofkav1 "github.com/prashanth8983/gofka/api/v1"
	"github.com/prashanth8983/gofka/pkg/crc"
	"github.com/prashanth8983/gofka/pkg/health"
	"github.com/prashanth8983/gofka/pkg/kraft"
	"github.com/prashanth8983/gofka/pkg/log"
	"github.com/prashanth8983/gofka/pkg/logger"
	"github.com/prashanth8983/gofka/pkg/metadata"
	"github.com/prashanth8983/gofka/pkg/metrics"
	"github.com/prashanth8983/gofka/pkg/network"
	"github.com/prashanth8983/gofka/pkg/network/protocol"
	"github.com/prashanth8983/gofka/pkg/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Broker handles message processing and storage.
type Broker struct {
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	// Components
	logs          map[string]*log.Log
	storage       *storage.DiskStorage
	server        *network.Server
	grpcServer    *grpc.Server
	consensus     *kraft.Consensus
	metadataStore *metadata.MetadataStore

	// New core infrastructure components
	replicationManager       *ReplicaManager
	consumerGroupCoordinator *ConsumerGroupCoordinator
	offsetManager            *OffsetManager
	crcValidator             *crc.MessageCRC
	healthService            *health.Service
	metricsServer            *http.Server

	// Configuration
	addr               string
	logDir             string
	raftAddr           string
	raftDir            string
	nodeID             string
	minInsyncReplicas  int32
	defaultAcks        int32
	defaultTimeoutMs   int32
}

// NewBroker creates a new Broker instance.
func NewBroker(nodeID, addr, logDir, raftAddr, raftDir string) (*Broker, error) {
	// Initialize structured logging
	logger.Init(logger.Config{
		Level:      "info",
		Encoding:   "json",
		OutputPath: "stdout",
		Component:  fmt.Sprintf("broker-%s", nodeID),
	})

	logger.Info("Starting broker", zap.String("node_id", nodeID), zap.String("addr", addr))

	// Input validation
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}
	if logDir == "" {
		return nil, fmt.Errorf("log directory cannot be empty")
	}
	if raftAddr == "" {
		return nil, fmt.Errorf("raft address cannot be empty")
	}
	if raftDir == "" {
		return nil, fmt.Errorf("raft directory cannot be empty")
	}

	ctx, cancel := context.WithCancel(context.Background())

	b := &Broker{
		ctx:               ctx,
		cancel:            cancel,
		logs:              make(map[string]*log.Log),
		addr:              addr,
		logDir:            logDir,
		raftAddr:          raftAddr,
		raftDir:           raftDir,
		nodeID:            nodeID,
		minInsyncReplicas: 1, // Default: at least 1 replica (leader) must ack
		defaultAcks:       1, // Default: wait for leader ack only
		defaultTimeoutMs:  5000, // Default: 5 second timeout
		crcValidator:      crc.NewMessageCRC(),
		healthService:     health.NewService(5 * time.Second),
	}

	storage, err := storage.NewDiskStorage(logDir)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create disk storage: %w", err)
	}

	// Extract port from addr for network server
	// Default to 9092 if no port specified
	port := 9092
	if addr != "" {
		if strings.Contains(addr, ":") {
			parts := strings.Split(addr, ":")
			if len(parts) == 2 && parts[1] != "" {
				if p, err := strconv.Atoi(parts[1]); err == nil {
					port = p
				}
			}
		}
	}
	server := network.NewServer(b, port)

	fsm := kraft.NewFSM()
	consensus, err := kraft.NewConsensus(nodeID, raftAddr, raftDir, fsm, false) // No cleanup needed for broker
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create consensus: %w", err)
	}

	b.storage = storage
	b.server = server
	b.consensus = consensus
	b.metadataStore = fsm.MetadataStore()

	// Initialize new core infrastructure components
	b.replicationManager = NewReplicaManager(b)
	b.consumerGroupCoordinator = NewConsumerGroupCoordinator(b)
	b.offsetManager = NewOffsetManager(b, filepath.Join(logDir, "offsets"))

	return b, nil
}

// Start starts the broker.
func (b *Broker) Start(bootstrap bool, peers []string) error {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return fmt.Errorf("broker already running")
	}
	b.running = true
	b.mu.Unlock()

	logger.Info("Starting broker", zap.String("node_id", b.nodeID), zap.Bool("bootstrap", bootstrap), zap.Strings("peers", peers))

	// If bootstrapping, cleanup existing Raft state first
	if bootstrap {
		if err := b.cleanupRaftState(); err != nil {
			logger.Warn("Failed to cleanup Raft state", zap.Error(err))
		}
	}

	// Add self to metadata store first
	b.metadataStore.AddBroker(&metadata.Broker{
		ID:   b.nodeID,
		Addr: b.addr,
	})

	// Start core infrastructure components first (they don't depend on consensus)
	if err := b.replicationManager.Start(); err != nil {
		return fmt.Errorf("failed to start replication manager: %w", err)
	}

	if err := b.consumerGroupCoordinator.Start(); err != nil {
		return fmt.Errorf("failed to start consumer group coordinator: %w", err)
	}

	if err := b.offsetManager.Start(); err != nil {
		return fmt.Errorf("failed to start offset manager: %w", err)
	}

	// Start network server early so broker can accept connections
	if err := b.server.Start(); err != nil {
		return fmt.Errorf("failed to start network server: %w", err)
	}

	// Start consensus in the background - it shouldn't block broker operation
	go func() {
		logger.Debug("Starting consensus in background")
		if err := b.consensus.Start(bootstrap, peers); err != nil {
			logger.Warn("Consensus failed to start, broker will continue in standalone mode", zap.Error(err))
		} else {
			logger.Info("Consensus started successfully")
		}
	}()

	// Start gRPC server (for cluster management and replication)
	if err := b.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// Register health checkers
	b.registerHealthCheckers()

	// Start metrics and health check server
	if err := b.startMetricsServer(); err != nil {
		logger.Warn("Failed to start metrics server", zap.Error(err))
		// Don't fail broker start if metrics server fails
	}

	// If not bootstrapping and we have peers, try to join the cluster
	if !bootstrap && len(peers) > 0 {
		go b.attemptJoinCluster(peers)
	}

	logger.Info("Broker started successfully",
		zap.String("broker_id", b.nodeID),
		zap.String("addr", b.addr),
		zap.Bool("bootstrap", bootstrap))

	// Update metrics
	metrics.BrokerUp.WithLabelValues(b.nodeID).Set(1)

	return nil
}

// startGRPCServer starts the gRPC server for cluster management
func (b *Broker) startGRPCServer() error {
	// Extract port from broker address
	port := 9092
	if b.addr != "" {
		if strings.Contains(b.addr, ":") {
			parts := strings.Split(b.addr, ":")
			if len(parts) == 2 && parts[1] != "" {
				if p, err := strconv.Atoi(parts[1]); err == nil {
					port = p
				}
			}
		}
	}

	// Use port + 1000 for gRPC (e.g., 9092 -> 10092)
	grpcPort := port + 1000
	grpcAddr := fmt.Sprintf(":%d", grpcPort)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", grpcAddr, err)
	}

	b.grpcServer = grpc.NewServer()
	gofkav1.RegisterGofkaServiceServer(b.grpcServer, &grpcBrokerWrapper{broker: b})

	go func() {
		logger.Info("gRPC server listening", zap.String("addr", grpcAddr))
		if err := b.grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// attemptJoinCluster tries to join an existing cluster
func (b *Broker) attemptJoinCluster(peers []string) {
	logger.Info("Attempting to join cluster", zap.Strings("peers", peers))

	// Wait a bit for the network server to be fully ready
	time.Sleep(2 * time.Second)

	// Try each peer to find the leader
	for _, peerRaftAddr := range peers {
		if peerRaftAddr == "" {
			continue
		}

		// Convert Raft address to broker address (port - 10000)
		brokerAddr := convertRaftAddrToBrokerAddr(peerRaftAddr)
		if brokerAddr == "" {
			logger.Warn("Could not convert Raft address", zap.String("raft_addr", peerRaftAddr))
			continue
		}

		logger.Debug("Sending join request", zap.String("broker_addr", brokerAddr))

		// Use the broker client to send join request
		client := NewBrokerClient()
		defer client.Close()

		if err := client.Connect(brokerAddr); err != nil {
			logger.Warn("Failed to connect to broker", zap.String("addr", brokerAddr), zap.Error(err))
			continue
		}

		// Send join cluster request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		grpcClient := client.clients[brokerAddr]
		resp, err := grpcClient.JoinCluster(ctx, &gofkav1.JoinClusterRequest{
			NodeId:        b.nodeID,
			RaftAddress:   b.raftAddr,
			BrokerAddress: b.addr,
		})

		if err != nil {
			logger.Warn("Join cluster RPC failed", zap.String("addr", brokerAddr), zap.Error(err))
			continue
		}

		if resp.Success {
			logger.Info("Successfully joined cluster", zap.String("via", brokerAddr))
			return
		}

		// If not the leader, try the leader address
		if !resp.IsLeader && resp.LeaderAddress != "" {
			logger.Debug("Peer is not leader, redirecting", zap.String("peer", brokerAddr), zap.String("leader", resp.LeaderAddress))

			// Convert leader's Raft address to gRPC address
			leaderGrpcAddr := convertRaftAddrToBrokerAddr(resp.LeaderAddress)
			if leaderGrpcAddr == "" {
				logger.Warn("Could not convert leader Raft address", zap.String("leader_addr", resp.LeaderAddress))
				continue
			}

			logger.Debug("Retrying join request to leader", zap.String("leader_addr", leaderGrpcAddr))

			// Create new client for leader
			leaderClient := NewBrokerClient()
			defer leaderClient.Close()

			leaderConn, err := grpc.Dial(leaderGrpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
			if err != nil {
				logger.Warn("Failed to connect to leader", zap.String("addr", leaderGrpcAddr), zap.Error(err))
				continue
			}
			defer leaderConn.Close()

			leaderGrpcClient := gofkav1.NewGofkaServiceClient(leaderConn)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			leaderResp, err := leaderGrpcClient.JoinCluster(ctx, &gofkav1.JoinClusterRequest{
				NodeId:        b.nodeID,
				RaftAddress:   b.raftAddr,
				BrokerAddress: b.addr,
			})

			if err != nil {
				logger.Warn("Failed to send join request to leader", zap.Error(err))
				continue
			}

			if leaderResp.Success {
				logger.Info("Successfully joined cluster via leader", zap.String("leader_addr", leaderGrpcAddr))
				return
			}

			logger.Warn("Leader join failed", zap.String("error", leaderResp.Error))
			continue
		}

		logger.Warn("Join failed", zap.String("addr", brokerAddr), zap.String("error", resp.Error))
	}

	logger.Warn("Could not join cluster through any peer")
}

// convertRaftAddrToBrokerAddr converts Raft address to gRPC broker address
func convertRaftAddrToBrokerAddr(raftAddr string) string {
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return ""
	}

	host := parts[0]
	var raftPort int
	if _, err := fmt.Sscanf(parts[1], "%d", &raftPort); err != nil {
		return ""
	}

	// Convert Raft port to broker port, then add 1000 for gRPC
	// e.g., raft:19092 -> broker:9092 -> gRPC:10092
	brokerPort := raftPort - 10000
	if brokerPort <= 0 {
		return ""
	}

	grpcPort := brokerPort + 1000

	return fmt.Sprintf("%s:%d", host, grpcPort)
}

// Stop stops the broker.
func (b *Broker) Stop() error {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return nil
	}
	b.running = false
	b.mu.Unlock()

	b.cancel()

	// Update metrics
	metrics.BrokerUp.WithLabelValues(b.nodeID).Set(0)

	// Stop metrics server
	if b.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.metricsServer.Shutdown(ctx); err != nil {
			logger.Warn("Failed to gracefully shutdown metrics server", zap.Error(err))
		}
	}

	// Stop gRPC server
	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}

	// Stop network server first to stop accepting new requests
	if err := b.server.Stop(); err != nil {
		return fmt.Errorf("failed to stop network server: %w", err)
	}

	// Stop new core infrastructure components
	if err := b.replicationManager.Stop(); err != nil {
		return fmt.Errorf("failed to stop replication manager: %w", err)
	}

	if err := b.consumerGroupCoordinator.Stop(); err != nil {
		return fmt.Errorf("failed to stop consumer group coordinator: %w", err)
	}

	if err := b.offsetManager.Stop(); err != nil {
		return fmt.Errorf("failed to stop offset manager: %w", err)
	}

	// Stop consensus last to allow other components to clean up
	if b.consensus != nil {
		if err := b.consensus.Close(); err != nil {
			return fmt.Errorf("failed to close consensus: %w", err)
		}
	}

	return nil
}

// logKey generates a unique key for the logs map based on topic and partition
func logKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func (b *Broker) getOrCreateLog(topic string) (*log.Log, error) {
	return b.getOrCreateLogForPartition(topic, 0)
}

func (b *Broker) getOrCreateLogForPartition(topic string, partition int32) (*log.Log, error) {
	key := logKey(topic, partition)

	b.mu.RLock()
	existingLog, ok := b.logs[key]
	b.mu.RUnlock()

	if ok {
		return existingLog, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double check if the log was created while we were waiting for the lock
	existingLog, ok = b.logs[key]
	if ok {
		return existingLog, nil
	}

	logDir := filepath.Join(b.logDir, topic, fmt.Sprintf("partition-%d", partition))
	newLog, err := log.NewLog(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create new log: %w", err)
	}

	b.logs[key] = newLog
	return newLog, nil
}

// ProduceMessage handles message production through Raft consensus.
func (b *Broker) ProduceMessage(topic string, message []byte) (int64, error) {
	return b.ProduceMessageToPartition(topic, 0, message)
}

// ProduceMessageToPartition handles message production to a specific partition
func (b *Broker) ProduceMessageToPartition(topic string, partition int32, message []byte) (int64, error) {
	offset, err := b.ProduceMessageWithAcksToPartition(topic, partition, message, b.defaultAcks, b.defaultTimeoutMs)
	if err == nil {
		logger.LogProducedMessage(topic, partition, offset, len(message))
		metrics.RecordProducedMessage(topic, partition, len(message))
	}
	return offset, err
}

// ProduceMessageWithAcks handles message production with ack configuration (defaults to partition 0)
func (b *Broker) ProduceMessageWithAcks(topic string, message []byte, acks, timeoutMs int32) (int64, error) {
	return b.ProduceMessageWithAcksToPartition(topic, 0, message, acks, timeoutMs)
}

// ProduceMessageWithAcksToPartition handles message production to a specific partition with ack configuration
func (b *Broker) ProduceMessageWithAcksToPartition(topic string, partitionID int32, message []byte, acks, timeoutMs int32) (int64, error) {
	// Auto-create topic and partition metadata if they don't exist
	topicMeta, exists := b.metadataStore.GetTopic(topic)
	if !exists {
		// Create topic with default settings
		topicMeta = &metadata.Topic{
			Name:              topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		b.metadataStore.AddTopic(topicMeta)

		// Create partition 0
		partition := &metadata.Partition{
			TopicName: topic,
			ID:        0,
			Leader:    b.nodeID,
			Replicas:  []string{b.nodeID},
		}
		b.metadataStore.AddPartition(partition)
	}

	// Validate partition exists
	if partitionID >= topicMeta.NumPartitions {
		return 0, fmt.Errorf("partition %d does not exist for topic %s (has %d partitions)", partitionID, topic, topicMeta.NumPartitions)
	}

	// Check if partition metadata exists, create if needed
	partition, _ := b.metadataStore.GetPartition(topic, partitionID)
	if partition == nil {
		// Create the partition metadata
		partition = &metadata.Partition{
			TopicName: topic,
			ID:        partitionID,
			Leader:    b.nodeID,
			Replicas:  []string{b.nodeID},
		}
		b.metadataStore.AddPartition(partition)
	}

	// Check if this broker is the leader for this partition
	if partition.Leader != b.nodeID {
		return 0, fmt.Errorf("not leader for partition %s-%d", topic, partitionID)
	}

	// Get the next offset for this topic-partition from the replicated state
	offset := b.metadataStore.GetNextOffsetForPartition(topic, partitionID)

	// Create a message replication command
	msg := &metadata.Message{
		Topic:     topic,
		Partition: partitionID,
		Offset:    offset,
		Data:      message,
	}

	cmd, err := metadata.NewCommand("replicate_message", msg)
	if err != nil {
		return 0, fmt.Errorf("failed to create replication command: %w", err)
	}

	// Apply through Raft consensus to replicate to all brokers
	if err := b.consensus.Apply(cmd.ToBytes()); err != nil {
		return 0, fmt.Errorf("failed to replicate message through consensus: %w", err)
	}

	// Also store locally for immediate availability (this will be consistent across all nodes)
	log, err := b.getOrCreateLogForPartition(topic, partitionID)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create log: %w", err)
	}

	// Store the message in local log with the assigned offset
	if _, err := log.Append(message); err != nil {
		return 0, fmt.Errorf("failed to append to local log: %w", err)
	}

	// Handle acks configuration
	switch acks {
	case 0:
		// No ack required - return immediately
		return offset, nil
	case 1:
		// Leader ack only - already written locally, return
		return offset, nil
	case -1:
		// All ISR acks required - check ISR count
		isr := b.replicationManager.GetISR(topic, partitionID)
		if int32(len(isr)) < b.minInsyncReplicas {
			return 0, fmt.Errorf("not enough in-sync replicas: %d < %d", len(isr), b.minInsyncReplicas)
		}
		// For now, just return since we're using Raft which ensures replication
		return offset, nil
	default:
		// Invalid acks value
		return 0, fmt.Errorf("invalid acks value: %d", acks)
	}
}

// ConsumeMessage handles message consumption from replicated state.
func (b *Broker) ConsumeMessage(topic string, partition int32, offset int64) ([]byte, error) {
	// First try to get message from replicated state (available on all brokers)
	if msg, found := b.metadataStore.GetMessageFromPartition(topic, partition, offset); found {
		logger.LogConsumedMessage(topic, partition, offset, "")
		metrics.RecordConsumedMessage(topic, partition, "", len(msg.Data))
		return msg.Data, nil
	}

	// Fallback to local log for backwards compatibility
	log, err := b.getOrCreateLogForPartition(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create log: %w", err)
	}

	data, err := log.Read(offset)
	if err == nil {
		logger.LogConsumedMessage(topic, partition, offset, "")
		metrics.RecordConsumedMessage(topic, partition, "", len(data))
	}
	return data, err
}

// GetBrokers returns broker metadata for network interface
func (b *Broker) GetBrokers() []network.BrokerMetadata {
	brokers := b.metadataStore.GetBrokers()
	result := make([]network.BrokerMetadata, len(brokers))
	for i, broker := range brokers {
		result[i] = network.BrokerMetadata{
			ID:   broker.ID,
			Addr: broker.Addr,
		}
	}
	return result
}

// GetTopics returns the list of topics in the cluster.
func (b *Broker) GetTopics() []string {
	topics := b.metadataStore.GetTopics()
	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = topic.Name
	}
	return topicNames
}

// GetPartitions returns partition metadata for network interface
func (b *Broker) GetPartitions(topicName string) []network.PartitionMetadata {
	partitions := b.metadataStore.GetPartitions(topicName)
	topic, exists := b.metadataStore.GetTopic(topicName)

	result := make([]network.PartitionMetadata, len(partitions))
	for i, partition := range partitions {
		result[i] = network.PartitionMetadata{
			ID:       partition.ID,
			Leader:   partition.Leader,
			Replicas: partition.Replicas,
		}
		if exists {
			result[i].NumPartitions = topic.NumPartitions
			result[i].ReplicationFactor = topic.ReplicationFactor
		}
	}
	return result
}

// CreateTopic creates a new topic in the cluster with the specified number of partitions.
func (b *Broker) CreateTopic(topicName string, numPartitions int32) error {
	// Validate partition count
	if numPartitions < 1 {
		numPartitions = 1
	}

	// Create topic metadata
	topic := &metadata.Topic{
		Name:              topicName,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1, // Default to 1 for now
	}

	b.metadataStore.AddTopic(topic)

	// Create all partitions for the topic
	for i := int32(0); i < numPartitions; i++ {
		partition := &metadata.Partition{
			TopicName: topicName,
			ID:        i,
			Leader:    b.nodeID,
			Replicas:  []string{b.nodeID},
		}
		b.metadataStore.AddPartition(partition)

		// Create the log for this partition to ensure the directory structure exists
		_, err := b.getOrCreateLogForPartition(topicName, i)
		if err != nil {
			return fmt.Errorf("failed to create log for topic %s partition %d: %w", topicName, i, err)
		}
	}

	return nil
}

// CreatePartition creates a new partition for a topic.
func (b *Broker) CreatePartition(topicName string, partitionID int32, leader string, replicas []string) error {
	partition := &metadata.Partition{
		TopicName: topicName,
		ID:        partitionID,
		Leader:    leader,
		Replicas:  replicas,
	}
	cmd, err := metadata.NewCommand("add_partition", partition)
	if err != nil {
		return err
	}
	cmdBytes := cmd.ToBytes()
	if cmdBytes == nil {
		return fmt.Errorf("failed to serialize partition command")
	}
	return b.consensus.Apply(cmdBytes)
}

// cleanupRaftState removes existing Raft state for clean bootstrap
func (b *Broker) cleanupRaftState() error {
	logger.Debug("Cleaning up existing broker Raft state")

	// Remove existing database files
	dbFiles := []string{
		filepath.Join(b.raftDir, "raft.db"),
		filepath.Join(b.raftDir, "stable.db"),
	}

	for _, dbFile := range dbFiles {
		if err := os.Remove(dbFile); err != nil && !os.IsNotExist(err) {
			logger.Warn("Could not remove Raft DB file", zap.String("file", dbFile), zap.Error(err))
		} else if err == nil {
			logger.Debug("Removed Raft DB file", zap.String("file", dbFile))
		}
	}

	// Clean existing snapshots but keep the directory
	snapshotDir := filepath.Join(b.raftDir, "snapshots")
	if err := os.RemoveAll(snapshotDir); err != nil && !os.IsNotExist(err) {
		logger.Warn("Could not remove snapshots directory", zap.Error(err))
	} else if err == nil {
		logger.Debug("Removed snapshots directory contents")
	}
	// Recreate the snapshots directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Warn("Could not recreate snapshots directory", zap.Error(err))
	}

	return nil
}

// GetOffsetManager returns the offset manager
func (b *Broker) GetOffsetManager() *OffsetManager {
	return b.offsetManager
}

// GetConsumerGroupCoordinator returns the consumer group coordinator
func (b *Broker) GetConsumerGroupCoordinator() *ConsumerGroupCoordinator {
	return b.consumerGroupCoordinator
}

// GetReplicationManager returns the replication manager
func (b *Broker) GetReplicationManager() *ReplicaManager {
	return b.replicationManager
}

// GetLog returns a log for a specific topic and partition
func (b *Broker) GetLog(topic string, partition int32) *log.Log {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := logKey(topic, partition)
	if log, ok := b.logs[key]; ok {
		return log
	}
	// Fallback to legacy topic-only key for backwards compatibility
	if partition == 0 {
		return b.logs[topic]
	}
	return nil
}

// getLog is an internal helper method for getting a log without exposing the broker
func (b *Broker) getLog(topic string, partition int32) *log.Log {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := logKey(topic, partition)
	if log, ok := b.logs[key]; ok {
		return log
	}
	// Fallback to legacy topic-only key for backwards compatibility
	if partition == 0 {
		return b.logs[topic]
	}
	return nil
}

// Protocol adapter methods to avoid import cycles

// GetProtocolBrokers returns brokers in protocol format
func (b *Broker) GetProtocolBrokers() []protocol.BrokerMetadata {
	brokers := b.GetBrokers()
	result := make([]protocol.BrokerMetadata, len(brokers))
	for i, broker := range brokers {
		result[i] = protocol.BrokerMetadata{
			ID:   broker.ID,
			Addr: broker.Addr,
		}
	}
	return result
}

// GetProtocolPartitions returns partitions in protocol format
func (b *Broker) GetProtocolPartitions(topicName string) []protocol.PartitionMetadata {
	partitions := b.GetPartitions(topicName)
	result := make([]protocol.PartitionMetadata, len(partitions))
	for i, partition := range partitions {
		result[i] = protocol.PartitionMetadata{
			ID:                partition.ID,
			Leader:            partition.Leader,
			Replicas:          partition.Replicas,
			NumPartitions:     partition.NumPartitions,
			ReplicationFactor: partition.ReplicationFactor,
		}
	}
	return result
}

// GetProtocolOffsetManager returns offset manager as protocol interface
func (b *Broker) GetProtocolOffsetManager() protocol.OffsetManagerInterface {
	return b.offsetManager
}

// GetProtocolConsumerGroupCoordinator returns consumer group coordinator as protocol interface
func (b *Broker) GetProtocolConsumerGroupCoordinator() protocol.ConsumerGroupCoordinatorInterface {
	return b.consumerGroupCoordinator
}

// GetBinaryProtocolHandler returns the binary protocol handler
func (b *Broker) GetBinaryProtocolHandler() network.BinaryProtocolHandler {
	return &protocolHandlerWrapper{broker: b}
}

// protocolHandlerWrapper wraps the protocol handler to avoid import cycles
type protocolHandlerWrapper struct {
	broker *Broker
}

func (w *protocolHandlerWrapper) HandleRequest(reader io.Reader, writer io.Writer) error {
	// Read all the data from the reader (the network server already consumed the size)
	// The reader contains: size(4) + actual message
	var size int32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return fmt.Errorf("failed to read size: %w", err)
	}

	msgBuf := make([]byte, size)
	if _, err := io.ReadFull(reader, msgBuf); err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	}

	// Use the wrapper to handle the request
	wrapper := protocol.NewRequestHandlerWrapper(w.broker)
	response, err := wrapper.HandleRequestWithoutSize(msgBuf)
	if err != nil {
		logger.Error("Protocol handler error", zap.Error(err))
		return err
	}

	// Write the response (already has size prefix)
	if _, err := writer.Write(response); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	return nil
}

// registerHealthCheckers registers all health check components
func (b *Broker) registerHealthCheckers() {
	// Register broker health checker
	b.healthService.RegisterComponent("broker", &health.BrokerHealthChecker{
		BrokerID: b.nodeID,
		IsLeader: func() bool {
			if b.consensus != nil {
				return b.consensus.IsLeader()
			}
			return false
		},
		TopicCount: func() int {
			return len(b.GetTopics())
		},
		GetState: func() string {
			if !b.running {
				return "stopped"
			}
			if b.consensus != nil && b.consensus.IsLeader() {
				return "leader"
			}
			return "follower"
		},
	})

	// Register Raft health checker if consensus is available
	if b.consensus != nil {
		b.healthService.RegisterComponent("raft", &health.RaftHealthChecker{
			NodeID: b.nodeID,
			GetState: func() string {
				// This would need to be implemented in the kraft package
				return "follower"
			},
			GetTerm: func() uint64 {
				// This would need to be implemented in the kraft package
				return 0
			},
			GetLeader: func() string {
				// This would need to be implemented in the kraft package
				return ""
			},
			GetPeerCount: func() int {
				// This would need to be implemented in the kraft package
				return len(b.metadataStore.GetBrokers()) - 1
			},
		})
	}

	// Register storage health checker
	b.healthService.RegisterComponent("storage", &health.StorageHealthChecker{
		GetDiskUsage: func() (used, total uint64) {
			// Simple disk usage check - would need proper implementation
			return 0, 1000000000 // 1GB for now
		},
		GetSegmentCount: func() int {
			count := 0
			for _, log := range b.logs {
				if log != nil {
					count++
				}
			}
			return count
		},
		GetOldestOffset: func() int64 {
			return 0
		},
		GetLatestOffset: func() int64 {
			var maxOffset int64
			for topic := range b.logs {
				offset := b.metadataStore.GetNextOffset(topic)
				if offset > maxOffset {
					maxOffset = offset
				}
			}
			return maxOffset
		},
	})
}

// startMetricsServer starts the HTTP server for metrics and health endpoints
func (b *Broker) startMetricsServer() error {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Health check endpoints
	mux.HandleFunc("/health", b.healthService.HTTPHandler())
	mux.HandleFunc("/health/live", health.LivenessHandler())
	mux.HandleFunc("/health/ready", b.healthService.ReadinessHandler())

	// Metrics port = broker port + 2000 (gRPC uses +1000)
	metricsPort := 8080
	if b.addr != "" && strings.Contains(b.addr, ":") {
		parts := strings.Split(b.addr, ":")
		if len(parts) == 2 {
			if port, err := strconv.Atoi(parts[1]); err == nil {
				metricsPort = port + 2000
			}
		}
	}

	b.metricsServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", metricsPort),
		Handler: mux,
	}

	go func() {
		logger.Info("Starting metrics server", zap.Int("port", metricsPort))
		if err := b.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server error", zap.Error(err))
		}
	}()

	return nil
}
