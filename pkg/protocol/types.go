package protocol

// BrokerMetadata represents broker metadata
type BrokerMetadata struct {
	ID   string
	Addr string
}

// PartitionMetadata represents partition metadata
type PartitionMetadata struct {
	ID                int32
	Leader            string
	Replicas          []string
	NumPartitions     int32
	ReplicationFactor int32
}

// OffsetManagerInterface defines offset management operations
type OffsetManagerInterface interface {
	CommitOffset(group, topic string, partition int32, offset int64) error
	GetOffset(group, topic string, partition int32) (int64, error)
	GetEarliestOffset(topic string, partition int32) (int64, error)
	GetLatestOffset(topic string, partition int32) (int64, error)
}

// ConsumerGroupCoordinatorInterface defines consumer group operations
type ConsumerGroupCoordinatorInterface interface {
	JoinGroup(groupID, memberID, clientID string, topics []string) (string, error)
	LeaveGroup(groupID, memberID string) error
	Heartbeat(groupID, memberID string) error
	GetGroupState(groupID string) (interface{}, error)
}

// BrokerInterface defines the broker operations needed by protocol handlers
type BrokerInterface interface {
	ProduceMessage(topic string, message []byte) (int64, error)
	ConsumeMessage(topic string, partition int32, offset int64) ([]byte, error)
	GetTopics() []string
	GetProtocolBrokers() []BrokerMetadata
	GetProtocolPartitions(topicName string) []PartitionMetadata
	CreateTopic(name string, partitions int32) error
	GetProtocolOffsetManager() OffsetManagerInterface
	GetProtocolConsumerGroupCoordinator() ConsumerGroupCoordinatorInterface
}