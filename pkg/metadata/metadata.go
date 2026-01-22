package metadata

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Broker represents the metadata for a Gofka broker.
type Broker struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Topic represents the metadata for a Gofka topic.
type Topic struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
}

// Message represents a message to be replicated across the cluster
type Message struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Data      []byte `json:"data"`
}

// Partition represents the metadata for a Gofka partition.
type Partition struct {
	TopicName string   `json:"topic_name"`
	ID        int32    `json:"id"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
}

// MetadataStore manages the cluster metadata.
type MetadataStore struct {
	mu         sync.RWMutex
	brokers    map[string]*Broker
	topics     map[string]*Topic
	partitions map[string]map[int32]*Partition // topic -> partition ID -> Partition
	messages   map[string]map[int64]*Message   // topic -> offset -> Message
}

// NewMetadataStore creates a new MetadataStore instance.
func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		brokers:    make(map[string]*Broker),
		topics:     make(map[string]*Topic),
		partitions: make(map[string]map[int32]*Partition),
		messages:   make(map[string]map[int64]*Message),
	}
}

// AddBroker adds a broker to the metadata store.
func (ms *MetadataStore) AddBroker(broker *Broker) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.brokers[broker.ID] = broker
}

// GetBroker gets a broker from the metadata store.
func (ms *MetadataStore) GetBroker(id string) (*Broker, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	broker, ok := ms.brokers[id]
	return broker, ok
}

// AddTopic adds a topic to the metadata store.
func (ms *MetadataStore) AddTopic(topic *Topic) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.topics[topic.Name] = topic
	ms.partitions[topic.Name] = make(map[int32]*Partition)
}

// GetTopic gets a topic from the metadata store.
func (ms *MetadataStore) GetTopic(name string) (*Topic, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	topic, ok := ms.topics[name]
	return topic, ok
}

// AddPartition adds a partition to the metadata store.
func (ms *MetadataStore) AddPartition(partition *Partition) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.partitions[partition.TopicName]; !ok {
		ms.partitions[partition.TopicName] = make(map[int32]*Partition)
	}
	ms.partitions[partition.TopicName][partition.ID] = partition
}

// GetPartition gets a partition from the metadata store.
func (ms *MetadataStore) GetPartition(topicName string, partitionID int32) (*Partition, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if partitions, ok := ms.partitions[topicName]; ok {
		partition, ok := partitions[partitionID]
		return partition, ok
	}
	return nil, false
}

// GetBrokers returns all brokers in the metadata store.
func (ms *MetadataStore) GetBrokers() []*Broker {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	brokers := make([]*Broker, 0, len(ms.brokers))
	for _, broker := range ms.brokers {
		brokers = append(brokers, broker)
	}
	return brokers
}

// GetTopics returns all topics in the metadata store.
func (ms *MetadataStore) GetTopics() []*Topic {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	topics := make([]*Topic, 0, len(ms.topics))
	for _, topic := range ms.topics {
		topics = append(topics, topic)
	}
	return topics
}

// GetPartitions returns all partitions for a given topic.
func (ms *MetadataStore) GetPartitions(topicName string) []*Partition {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if partitions, ok := ms.partitions[topicName]; ok {
		parts := make([]*Partition, 0, len(partitions))
		for _, part := range partitions {
			parts = append(parts, part)
		}
		return parts
	}
	return nil
}

// AddMessage adds a message to the metadata store.
func (ms *MetadataStore) AddMessage(msg *Message) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Use partition-aware key for new messages
	key := fmt.Sprintf("%s-%d", msg.Topic, msg.Partition)
	if _, ok := ms.messages[key]; !ok {
		ms.messages[key] = make(map[int64]*Message)
	}
	ms.messages[key][msg.Offset] = msg
}

// GetMessage retrieves a message from the metadata store (defaults to partition 0).
func (ms *MetadataStore) GetMessage(topic string, offset int64) (*Message, bool) {
	return ms.GetMessageFromPartition(topic, 0, offset)
}

// GetMessageFromPartition retrieves a message from a specific partition.
func (ms *MetadataStore) GetMessageFromPartition(topic string, partition int32, offset int64) (*Message, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Try partition-aware key first
	key := fmt.Sprintf("%s-%d", topic, partition)
	if messages, ok := ms.messages[key]; ok {
		msg, ok := messages[offset]
		return msg, ok
	}

	// Fallback to legacy topic-only key for backwards compatibility (partition 0 only)
	if partition == 0 {
		if messages, ok := ms.messages[topic]; ok {
			msg, ok := messages[offset]
			return msg, ok
		}
	}

	return nil, false
}

// GetNextOffset returns the next available offset for a topic (partition 0).
func (ms *MetadataStore) GetNextOffset(topic string) int64 {
	return ms.GetNextOffsetForPartition(topic, 0)
}

// GetNextOffsetForPartition returns the next available offset for a specific topic-partition.
func (ms *MetadataStore) GetNextOffsetForPartition(topic string, partition int32) int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Use a composite key for topic-partition
	key := fmt.Sprintf("%s-%d", topic, partition)
	if messages, ok := ms.messages[key]; ok {
		var maxOffset int64 = -1
		for offset := range messages {
			if offset > maxOffset {
				maxOffset = offset
			}
		}
		return maxOffset + 1
	}

	// Fallback to legacy topic-only key for backwards compatibility
	if messages, ok := ms.messages[topic]; ok && partition == 0 {
		var maxOffset int64 = -1
		for offset := range messages {
			if offset > maxOffset {
				maxOffset = offset
			}
		}
		return maxOffset + 1
	}

	return 0
}

// Command represents a command to be applied to the metadata store.
type Command struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ToBytes returns the byte representation of the command.
// Returns nil if marshaling fails.
func (c *Command) ToBytes() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		fmt.Printf("ERROR: failed to marshal command: %v\n", err)
		return nil
	}
	return data
}

// NewCommand creates a new Command.
func NewCommand(cmdType string, data interface{}) (*Command, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command data: %w", err)
	}
	return &Command{
		Type: cmdType,
		Data: dataBytes,
	}, nil
}

// ApplyCommand applies a command to the metadata store.
func (ms *MetadataStore) ApplyCommand(cmd *Command) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	switch cmd.Type {
	case "add_broker":
		var broker Broker
		if err := json.Unmarshal(cmd.Data, &broker); err != nil {
			return fmt.Errorf("failed to unmarshal broker data: %w", err)
		}
		ms.brokers[broker.ID] = &broker
	case "add_topic":
		var topic Topic
		if err := json.Unmarshal(cmd.Data, &topic); err != nil {
			return fmt.Errorf("failed to unmarshal topic data: %w", err)
		}
		ms.topics[topic.Name] = &topic
		ms.partitions[topic.Name] = make(map[int32]*Partition)
	case "add_partition":
		var partition Partition
		if err := json.Unmarshal(cmd.Data, &partition); err != nil {
			return fmt.Errorf("failed to unmarshal partition data: %w", err)
		}
		if _, ok := ms.partitions[partition.TopicName]; !ok {
			ms.partitions[partition.TopicName] = make(map[int32]*Partition)
		}
		ms.partitions[partition.TopicName][partition.ID] = &partition
	case "replicate_message":
		var message Message
		if err := json.Unmarshal(cmd.Data, &message); err != nil {
			return fmt.Errorf("failed to unmarshal message data: %w", err)
		}
		if _, ok := ms.messages[message.Topic]; !ok {
			ms.messages[message.Topic] = make(map[int64]*Message)
		}
		ms.messages[message.Topic][message.Offset] = &message
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
	return nil
}

// Snapshot returns a snapshot of the metadata store.
func (ms *MetadataStore) Snapshot() ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	data := struct {
		Brokers    map[string]*Broker
		Topics     map[string]*Topic
		Partitions map[string]map[int32]*Partition
		Messages   map[string]map[int64]*Message
	}{
		Brokers:    ms.brokers,
		Topics:     ms.topics,
		Partitions: ms.partitions,
		Messages:   ms.messages,
	}

	return json.Marshal(data)
}

// Restore restores the metadata store from a snapshot.
func (ms *MetadataStore) Restore(data []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var restoredData struct {
		Brokers    map[string]*Broker
		Topics     map[string]*Topic
		Partitions map[string]map[int32]*Partition
		Messages   map[string]map[int64]*Message
	}

	if err := json.Unmarshal(data, &restoredData); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot data: %w", err)
	}

	ms.brokers = restoredData.Brokers
	ms.topics = restoredData.Topics
	ms.partitions = restoredData.Partitions
	if restoredData.Messages != nil {
		ms.messages = restoredData.Messages
	} else {
		ms.messages = make(map[string]map[int64]*Message)
	}

	return nil
}
