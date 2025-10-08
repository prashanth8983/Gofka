package broker

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	gofkav1 "github.com/user/gofka/api/v1"
	"github.com/user/gofka/pkg/kraft"
	"github.com/user/gofka/pkg/log"
	"github.com/user/gofka/pkg/metadata"
	"github.com/user/gofka/pkg/network"
	"github.com/user/gofka/pkg/network/protocol"
	"github.com/user/gofka/pkg/storage"
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

	fmt.Printf("Starting broker with node ID: %s, bootstrap: %v, peers: %v\n", b.nodeID, bootstrap, peers)

	// If bootstrapping, cleanup existing Raft state first
	if bootstrap {
		if err := b.cleanupRaftState(); err != nil {
			fmt.Printf("Warning: failed to cleanup Raft state: %v\n", err)
		}
	}

	// Start consensus first
	if err := b.consensus.Start(bootstrap, peers); err != nil {
		return fmt.Errorf("failed to start consensus: %w", err)
	}

	// Allow consensus to stabilize before proceeding
	time.Sleep(500 * time.Millisecond)

	// Add self to metadata store
	b.metadataStore.AddBroker(&metadata.Broker{
		ID:   b.nodeID,
		Addr: b.addr,
	})

	// Start new core infrastructure components
	if err := b.replicationManager.Start(); err != nil {
		return fmt.Errorf("failed to start replication manager: %w", err)
	}

	if err := b.consumerGroupCoordinator.Start(); err != nil {
		return fmt.Errorf("failed to start consumer group coordinator: %w", err)
	}

	if err := b.offsetManager.Start(); err != nil {
		return fmt.Errorf("failed to start offset manager: %w", err)
	}

	// Start network server (binary protocol)
	if err := b.server.Start(); err != nil {
		return fmt.Errorf("failed to start network server: %w", err)
	}

	// Start gRPC server (for cluster management and replication)
	if err := b.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// If not bootstrapping and we have peers, try to join the cluster
	if !bootstrap && len(peers) > 0 {
		go b.attemptJoinCluster(peers)
	}

	fmt.Printf("Broker started successfully on %s\n", b.addr)
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
		fmt.Printf("gRPC server listening on %s\n", grpcAddr)
		if err := b.grpcServer.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	return nil
}

// attemptJoinCluster tries to join an existing cluster
func (b *Broker) attemptJoinCluster(peers []string) {
	fmt.Printf("Attempting to join cluster via peers: %v\n", peers)

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
			fmt.Printf("Could not convert Raft address: %s\n", peerRaftAddr)
			continue
		}

		fmt.Printf("Sending join request to broker at %s\n", brokerAddr)

		// Use the broker client to send join request
		client := NewBrokerClient()
		defer client.Close()

		if err := client.Connect(brokerAddr); err != nil {
			fmt.Printf("Failed to connect to broker %s: %v\n", brokerAddr, err)
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
			fmt.Printf("Join cluster RPC failed for %s: %v\n", brokerAddr, err)
			continue
		}

		if resp.Success {
			fmt.Printf("Successfully joined cluster via %s\n", brokerAddr)
			return
		}

		// If not the leader, try the leader address
		if !resp.IsLeader && resp.LeaderAddress != "" {
			fmt.Printf("Peer %s is not leader, redirecting to leader at %s\n", brokerAddr, resp.LeaderAddress)

			// Convert leader's Raft address to gRPC address
			leaderGrpcAddr := convertRaftAddrToBrokerAddr(resp.LeaderAddress)
			if leaderGrpcAddr == "" {
				fmt.Printf("Could not convert leader Raft address: %s\n", resp.LeaderAddress)
				continue
			}

			fmt.Printf("Retrying join request to leader at %s\n", leaderGrpcAddr)

			// Create new client for leader
			leaderClient := NewBrokerClient()
			defer leaderClient.Close()

			leaderConn, err := grpc.Dial(leaderGrpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
			if err != nil {
				fmt.Printf("Failed to connect to leader %s: %v\n", leaderGrpcAddr, err)
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
				fmt.Printf("Failed to send join request to leader: %v\n", err)
				continue
			}

			if leaderResp.Success {
				fmt.Printf("Successfully joined cluster via leader at %s\n", leaderGrpcAddr)
				return
			}

			fmt.Printf("Leader join failed: %s\n", leaderResp.Error)
			continue
		}

		fmt.Printf("Join failed at %s: %s\n", brokerAddr, resp.Error)
	}

	fmt.Println("Warning: Could not join cluster through any peer")
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

func (b *Broker) getOrCreateLog(topic string) (*log.Log, error) {
	b.mu.RLock()
	existingLog, ok := b.logs[topic]
	b.mu.RUnlock()

	if ok {
		return existingLog, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double check if the log was created while we were waiting for the lock
	existingLog, ok = b.logs[topic]
	if ok {
		return existingLog, nil
	}

	logDir := filepath.Join(b.logDir, topic)
	newLog, err := log.NewLog(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create new log: %w", err)
	}

	b.logs[topic] = newLog
	return newLog, nil
}

// ProduceMessage handles message production through Raft consensus.
func (b *Broker) ProduceMessage(topic string, message []byte) (int64, error) {
	return b.ProduceMessageWithAcks(topic, message, b.defaultAcks, b.defaultTimeoutMs)
}

// ProduceMessageWithAcks handles message production with ack configuration
func (b *Broker) ProduceMessageWithAcks(topic string, message []byte, acks, timeoutMs int32) (int64, error) {
	// Auto-create topic and partition metadata if they don't exist
	if _, exists := b.metadataStore.GetTopic(topic); !exists {
		// Create topic with default settings
		topicMeta := &metadata.Topic{
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

	// Check if this broker is the leader for partition 0
	partition, _ := b.metadataStore.GetPartition(topic, 0)
	if partition != nil && partition.Leader != b.nodeID {
		return 0, fmt.Errorf("not leader for partition %s-0", topic)
	}

	// Get the next offset for this topic from the replicated state
	offset := b.metadataStore.GetNextOffset(topic)

	// Create a message replication command
	msg := &metadata.Message{
		Topic:     topic,
		Partition: 0, // For now, use partition 0
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
	log, err := b.getOrCreateLog(topic)
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
		isr := b.replicationManager.GetISR(topic, 0)
		if int32(len(isr)) < b.minInsyncReplicas {
			return 0, fmt.Errorf("not enough in-sync replicas: %d < %d", len(isr), b.minInsyncReplicas)
		}
		// TODO: Wait for all ISR to acknowledge
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
	if msg, found := b.metadataStore.GetMessage(topic, offset); found {
		return msg.Data, nil
	}
	
	// Fallback to local log for backwards compatibility
	log, err := b.getOrCreateLog(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create log: %w", err)
	}

	return log.Read(offset)
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

// CreateTopic creates a new topic in the cluster.
func (b *Broker) CreateTopic(topicName string, numPartitions int32) error {
	// For now, create topic locally for testing
	// TODO: Use consensus protocol for distributed topic creation
	topic := &metadata.Topic{
		Name:              topicName,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1, // Default to 1 for now
	}

	b.metadataStore.AddTopic(topic)

	// Create a default partition for the topic
	partition := &metadata.Partition{
		TopicName: topicName,
		ID:        0,
		Leader:    b.nodeID,
		Replicas:  []string{b.nodeID},
	}
	b.metadataStore.AddPartition(partition)

	// Create the log for this topic to ensure the directory structure exists
	_, err := b.getOrCreateLog(topicName)
	if err != nil {
		return fmt.Errorf("failed to create log for topic %s: %w", topicName, err)
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
	fmt.Println("Cleaning up existing broker Raft state...")

	// Remove existing database files
	dbFiles := []string{
		filepath.Join(b.raftDir, "raft.db"),
		filepath.Join(b.raftDir, "stable.db"),
	}

	for _, dbFile := range dbFiles {
		if err := os.Remove(dbFile); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: could not remove %s: %v\n", dbFile, err)
		} else if err == nil {
			fmt.Printf("Removed existing database file: %s\n", dbFile)
		}
	}

	// Clean existing snapshots but keep the directory
	snapshotDir := filepath.Join(b.raftDir, "snapshots")
	if err := os.RemoveAll(snapshotDir); err != nil && !os.IsNotExist(err) {
		fmt.Printf("Warning: could not remove snapshots directory: %v\n", err)
	} else if err == nil {
		fmt.Printf("Removed existing snapshots directory contents\n")
	}
	// Recreate the snapshots directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		fmt.Printf("Warning: could not recreate snapshots directory: %v\n", err)
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

	// For now, use topic as key (single partition support)
	// TODO: Support multiple partitions
	return b.logs[topic]
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
	handler := protocol.NewRequestHandler(w.broker)
	return handler.HandleRequest(reader, writer)
}
