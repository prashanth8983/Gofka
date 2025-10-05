package broker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/user/gofka/pkg/kraft"
	"github.com/user/gofka/pkg/log"
	"github.com/user/gofka/pkg/metadata"
	"github.com/user/gofka/pkg/network"
	"github.com/user/gofka/pkg/network/protocol"
	"github.com/user/gofka/pkg/storage"
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
	consensus     *kraft.Consensus
	metadataStore *metadata.MetadataStore

	// New core infrastructure components
	replicationManager       *ReplicaManager
	consumerGroupCoordinator *ConsumerGroupCoordinator
	offsetManager            *OffsetManager

	// Configuration
	addr     string
	logDir   string
	raftAddr string
	raftDir  string
	nodeID   string
}

// NewBroker creates a new Broker instance.
func NewBroker(nodeID, addr, logDir, raftAddr, raftDir string) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &Broker{
		ctx:      ctx,
		cancel:   cancel,
		logs:     make(map[string]*log.Log),
		addr:     addr,
		logDir:   logDir,
		raftAddr: raftAddr,
		raftDir:  raftDir,
		nodeID:   nodeID,
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

	// Start network server
	if err := b.server.Start(); err != nil {
		return fmt.Errorf("failed to start network server: %w", err)
	}

	fmt.Printf("Broker started successfully on %s\n", b.addr)
	return nil
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

	return offset, nil
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
