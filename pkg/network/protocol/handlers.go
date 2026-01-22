package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// BrokerInterface defines the interface that the broker must implement
// This avoids import cycle between protocol and broker packages
type BrokerInterface interface {
	ProduceMessage(topic string, message []byte) (int64, error)
	ConsumeMessage(topic string, partition int32, offset int64) ([]byte, error)
	GetTopics() []string
	GetProtocolBrokers() []BrokerMetadata
	GetProtocolPartitions(topicName string) []PartitionMetadata
	GetProtocolOffsetManager() OffsetManagerInterface
	GetProtocolConsumerGroupCoordinator() ConsumerGroupCoordinatorInterface
}

// OffsetManagerInterface defines offset management operations
type OffsetManagerInterface interface {
	CommitOffset(groupID, topic string, partition int32, offset int64, metadata string) error
	FetchOffset(groupID, topic string, partition int32) (int64, error)
}

// ConsumerGroupInterface represents a consumer group
type ConsumerGroupInterface interface {
	GetGroupID() string
	GetGeneration() int32
	GetLeaderID() string
	GetMembers() map[string]ConsumerMemberInterface
	GetAssignments() map[string][]int32
}

// ConsumerMemberInterface represents a consumer group member
type ConsumerMemberInterface interface {
	GetMemberID() string
	GetSubscriptions() []string
	GetClientID() string
	GetClientHost() string
}

// ConsumerGroupCoordinatorInterface defines consumer group operations
type ConsumerGroupCoordinatorInterface interface {
	JoinGroup(groupID, memberID, clientID, clientHost string, sessionTimeout int32, subscriptions []string) (ConsumerGroupInterface, error)
	SyncGroup(groupID, memberID string, generation int32) (ConsumerGroupInterface, error)
	Heartbeat(groupID, memberID string, generation int32) error
	LeaveGroup(groupID, memberID string) error
	// ListGroups returns all consumer group IDs
	ListGroups() []string
	// GetGroup returns a consumer group by ID
	GetGroup(groupID string) (ConsumerGroupInterface, error)
}

// ConsumerGroupDetailInterface extends ConsumerGroupInterface with additional details
type ConsumerGroupDetailInterface interface {
	ConsumerGroupInterface
	GetProtocol() string
	GetState() string
}

// BrokerMetadata represents broker information
type BrokerMetadata struct {
	ID   string
	Addr string
}

// PartitionMetadata represents partition information
type PartitionMetadata struct {
	ID                int32
	Leader            string
	Replicas          []string
	NumPartitions     int32
	ReplicationFactor int32
}

// RequestHandler handles different types of Kafka protocol requests
type RequestHandler struct {
	broker BrokerInterface
}

// NewRequestHandler creates a new RequestHandler
func NewRequestHandler(broker BrokerInterface) *RequestHandler {
	return &RequestHandler{
		broker: broker,
	}
}

// HandleRequest routes requests to appropriate handlers based on API key
func (h *RequestHandler) HandleRequest(reader io.Reader, writer io.Writer) error {
	// Read request header
	header, err := h.readRequestHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to read request header: %w", err)
	}

	// Route to appropriate handler
	switch header.APIKey {
	case 0: // Produce
		return h.handleProduce(reader, writer, header)
	case 1: // Fetch
		return h.handleFetch(reader, writer, header)
	case 3: // Metadata
		return h.handleMetadata(reader, writer, header)
	case 8: // OffsetCommit
		return h.handleOffsetCommit(reader, writer, header)
	case 9: // OffsetFetch
		return h.handleOffsetFetch(reader, writer, header)
	case 10: // FindCoordinator
		return h.handleFindCoordinator(reader, writer, header)
	case 11: // JoinGroup
		return h.handleJoinGroup(reader, writer, header)
	case 12: // Heartbeat
		return h.handleHeartbeat(reader, writer, header)
	case 13: // LeaveGroup
		return h.handleLeaveGroup(reader, writer, header)
	case 14: // SyncGroup
		return h.handleSyncGroup(reader, writer, header)
	case 15: // DescribeGroups
		return h.handleDescribeGroups(reader, writer, header)
	case 16: // ListGroups
		return h.handleListGroups(reader, writer, header)
	default:
		return fmt.Errorf("unsupported API key: %d", header.APIKey)
	}
}

// RequestHeader represents the common header for all Kafka requests
type RequestHeader struct {
	Size          int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader represents the common header for all Kafka responses
type ResponseHeader struct {
	CorrelationID int32
}

// readRequestHeader reads the common request header
func (h *RequestHandler) readRequestHeader(reader io.Reader) (*RequestHeader, error) {
	header := &RequestHeader{}

	// Read size (4 bytes)
	if err := binary.Read(reader, binary.BigEndian, &header.Size); err != nil {
		return nil, fmt.Errorf("failed to read request size: %w", err)
	}

	// Read API key (2 bytes)
	if err := binary.Read(reader, binary.BigEndian, &header.APIKey); err != nil {
		return nil, fmt.Errorf("failed to read API key: %w", err)
	}

	// Read API version (2 bytes)
	if err := binary.Read(reader, binary.BigEndian, &header.APIVersion); err != nil {
		return nil, fmt.Errorf("failed to read API version: %w", err)
	}

	// Read correlation ID (4 bytes)
	if err := binary.Read(reader, binary.BigEndian, &header.CorrelationID); err != nil {
		return nil, fmt.Errorf("failed to read correlation ID: %w", err)
	}

	// Read client ID length and string
	var clientIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &clientIDLen); err != nil {
		return nil, fmt.Errorf("failed to read client ID length: %w", err)
	}

	if clientIDLen > 0 {
		clientIDBytes := make([]byte, clientIDLen)
		if _, err := io.ReadFull(reader, clientIDBytes); err != nil {
			return nil, fmt.Errorf("failed to read client ID: %w", err)
		}
		header.ClientID = string(clientIDBytes)
	}

	return header, nil
}

// writeResponseHeader writes the common response header
func (h *RequestHandler) writeResponseHeader(writer io.Writer, correlationID int32) error {
	header := ResponseHeader{
		CorrelationID: correlationID,
	}

	return binary.Write(writer, binary.BigEndian, header)
}

// writeErrorResponse writes an error response back to the client
func (h *RequestHandler) writeErrorResponse(writer io.Writer, header *RequestHeader, errorMsg string) error {
	// Write correlation ID
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write error flag (0 = error)
	errorFlag := byte(0)
	if err := binary.Write(writer, binary.BigEndian, errorFlag); err != nil {
		return fmt.Errorf("failed to write error flag: %w", err)
	}

	// Write error message length
	if err := binary.Write(writer, binary.BigEndian, int16(len(errorMsg))); err != nil {
		return fmt.Errorf("failed to write error message length: %w", err)
	}

	// Write error message
	if _, err := writer.Write([]byte(errorMsg)); err != nil {
		return fmt.Errorf("failed to write error message: %w", err)
	}

	return nil
}

// handleProduce handles produce requests (simplified Gofka protocol)
func (h *RequestHandler) handleProduce(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read topic name
	topic, err := h.readString(reader)
	if err != nil {
		return fmt.Errorf("failed to read topic: %w", err)
	}

	// Read partition
	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return fmt.Errorf("failed to read partition: %w", err)
	}

	// Read message length
	var messageLen int32
	if err := binary.Read(reader, binary.BigEndian, &messageLen); err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	// Read message
	message := make([]byte, messageLen)
	if _, err := io.ReadFull(reader, message); err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	}

	// Produce message
	offset, err := h.broker.ProduceMessage(topic, message)
	if err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("failed to produce message: %v", err))
	}

	// Write response: correlation_id + partition + offset
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	if err := binary.Write(writer, binary.BigEndian, partition); err != nil {
		return fmt.Errorf("failed to write partition: %w", err)
	}

	if err := binary.Write(writer, binary.BigEndian, offset); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}

	return nil
}

// ProduceRequest represents a produce request
type ProduceRequest struct {
	RequiredAcks int16
	Timeout      int32
	Topics       []TopicData
}

// TopicData represents data for a specific topic
type TopicData struct {
	Topic      string
	Partitions []PartitionData
}

// PartitionData represents data for a specific partition
type PartitionData struct {
	Partition int32
	Messages  []Message
}

// Message represents a single message
type Message struct {
	Key   []byte
	Value []byte
}

// readProduceRequest reads a produce request from the reader
func (h *RequestHandler) readProduceRequest(reader io.Reader) (*ProduceRequest, error) {
	request := &ProduceRequest{}

	// Read required acks
	if err := binary.Read(reader, binary.BigEndian, &request.RequiredAcks); err != nil {
		return nil, fmt.Errorf("failed to read required acks: %w", err)
	}

	// Read timeout
	if err := binary.Read(reader, binary.BigEndian, &request.Timeout); err != nil {
		return nil, fmt.Errorf("failed to read timeout: %w", err)
	}

	// Read topics array length
	var topicsLen int32
	if err := binary.Read(reader, binary.BigEndian, &topicsLen); err != nil {
		return nil, fmt.Errorf("failed to read topics length: %w", err)
	}

	request.Topics = make([]TopicData, topicsLen)
	for i := int32(0); i < topicsLen; i++ {
		topic, err := h.readTopicData(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read topic %d: %w", i, err)
		}
		request.Topics[i] = *topic
	}

	return request, nil
}

// readTopicData reads topic data from the reader
func (h *RequestHandler) readTopicData(reader io.Reader) (*TopicData, error) {
	topic := &TopicData{}

	// Read topic name
	topicName, err := h.readString(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic name: %w", err)
	}
	topic.Topic = topicName

	// Read partitions array length
	var partitionsLen int32
	if err := binary.Read(reader, binary.BigEndian, &partitionsLen); err != nil {
		return nil, fmt.Errorf("failed to read partitions length: %w", err)
	}

	topic.Partitions = make([]PartitionData, partitionsLen)
	for i := int32(0); i < partitionsLen; i++ {
		partition, err := h.readPartitionData(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read partition %d: %w", i, err)
		}
		topic.Partitions[i] = *partition
	}

	return topic, nil
}

// readPartitionData reads partition data from the reader
func (h *RequestHandler) readPartitionData(reader io.Reader) (*PartitionData, error) {
	partition := &PartitionData{}

	// Read partition ID
	if err := binary.Read(reader, binary.BigEndian, &partition.Partition); err != nil {
		return nil, fmt.Errorf("failed to read partition ID: %w", err)
	}

	// Read message set size
	var messageSetSize int32
	if err := binary.Read(reader, binary.BigEndian, &messageSetSize); err != nil {
		return nil, fmt.Errorf("failed to read message set size: %w", err)
	}

	// Read messages
	partition.Messages = make([]Message, 0)
	bytesRead := int32(0)
	for bytesRead < messageSetSize {
		message, err := h.readMessage(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}
		partition.Messages = append(partition.Messages, *message)
		bytesRead += int32(len(message.Key) + len(message.Value) + 8) // +8 for offset and message size
	}

	return partition, nil
}

// readMessage reads a single message from the reader
func (h *RequestHandler) readMessage(reader io.Reader) (*Message, error) {
	message := &Message{}

	// Read offset (8 bytes)
	var offset int64
	if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
		return nil, fmt.Errorf("failed to read message offset: %w", err)
	}

	// Read message size (4 bytes)
	var messageSize int32
	if err := binary.Read(reader, binary.BigEndian, &messageSize); err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	// Read CRC (4 bytes)
	var crc int32
	if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
		return nil, fmt.Errorf("failed to read message CRC: %w", err)
	}

	// Read magic byte (1 byte)
	var magic byte
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read message magic: %w", err)
	}

	// Read attributes (1 byte)
	var attributes byte
	if err := binary.Read(reader, binary.BigEndian, &attributes); err != nil {
		return nil, fmt.Errorf("failed to read message attributes: %w", err)
	}

	// Read key length and key
	key, err := h.readBytes(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read message key: %w", err)
	}
	message.Key = key

	// Read value length and value
	value, err := h.readBytes(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read message value: %w", err)
	}
	message.Value = value

	return message, nil
}

// readString reads a string from the reader
func (h *RequestHandler) readString(reader io.Reader) (string, error) {
	var length int16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}

	if length < 0 {
		return "", nil // null string
	}

	bytes := make([]byte, length)
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return "", fmt.Errorf("failed to read string bytes: %w", err)
	}

	return string(bytes), nil
}

// readBytes reads bytes from the reader
func (h *RequestHandler) readBytes(reader io.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read bytes length: %w", err)
	}

	if length < 0 {
		return nil, nil // null bytes
	}

	bytes := make([]byte, length)
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}

	return bytes, nil
}

// processProduceRequest processes a produce request and returns a response
func (h *RequestHandler) processProduceRequest(request *ProduceRequest) (*ProduceResponse, error) {
	response := &ProduceResponse{
		Topics: make([]TopicResponse, len(request.Topics)),
	}

	for i, topic := range request.Topics {
		topicResponse := TopicResponse{
			Topic:      topic.Topic,
			Partitions: make([]PartitionResponse, len(topic.Partitions)),
		}

		for j, partition := range topic.Partitions {
			// Produce messages to the broker
			offsets := make([]int64, len(partition.Messages))
			for k, message := range partition.Messages {
				// Combine key and value for now, in a real implementation you'd want to handle this properly
				combinedMessage := append(message.Key, message.Value...)
				offset, err := h.broker.ProduceMessage(topic.Topic, combinedMessage)
				if err != nil {
					return nil, fmt.Errorf("failed to produce message: %w", err)
				}
				offsets[k] = offset
			}

			partitionResponse := PartitionResponse{
				Partition: partition.Partition,
				ErrorCode: 0,                       // No error
				Offset:    offsets[len(offsets)-1], // Last offset
				Timestamp: time.Now().UnixMilli(),
			}

			topicResponse.Partitions[j] = partitionResponse
		}

		response.Topics[i] = topicResponse
	}

	return response, nil
}

// ProduceResponse represents a produce response
type ProduceResponse struct {
	Topics []TopicResponse
}

// TopicResponse represents a topic response
type TopicResponse struct {
	Topic      string
	Partitions []PartitionResponse
}

// PartitionResponse represents a partition response
type PartitionResponse struct {
	Partition int32
	ErrorCode int16
	Offset    int64
	Timestamp int64
}

// writeProduceResponse writes a produce response to the writer
func (h *RequestHandler) writeProduceResponse(writer io.Writer, response *ProduceResponse) error {
	// Write topics array length
	if err := binary.Write(writer, binary.BigEndian, int32(len(response.Topics))); err != nil {
		return fmt.Errorf("failed to write topics length: %w", err)
	}

	// Write each topic response
	for _, topic := range response.Topics {
		if err := h.writeTopicResponse(writer, topic); err != nil {
			return fmt.Errorf("failed to write topic response: %w", err)
		}
	}

	return nil
}

// writeTopicResponse writes a topic response to the writer
func (h *RequestHandler) writeTopicResponse(writer io.Writer, topic TopicResponse) error {
	// Write topic name
	if err := h.writeString(writer, topic.Topic); err != nil {
		return fmt.Errorf("failed to write topic name: %w", err)
	}

	// Write partitions array length
	if err := binary.Write(writer, binary.BigEndian, int32(len(topic.Partitions))); err != nil {
		return fmt.Errorf("failed to write partitions length: %w", err)
	}

	// Write each partition response
	for _, partition := range topic.Partitions {
		if err := h.writePartitionResponse(writer, partition); err != nil {
			return fmt.Errorf("failed to write partition response: %w", err)
		}
	}

	return nil
}

// writePartitionResponse writes a partition response to the writer
func (h *RequestHandler) writePartitionResponse(writer io.Writer, partition PartitionResponse) error {
	// Write partition ID
	if err := binary.Write(writer, binary.BigEndian, partition.Partition); err != nil {
		return fmt.Errorf("failed to write partition ID: %w", err)
	}

	// Write error code
	if err := binary.Write(writer, binary.BigEndian, partition.ErrorCode); err != nil {
		return fmt.Errorf("failed to write error code: %w", err)
	}

	// Write offset
	if err := binary.Write(writer, binary.BigEndian, partition.Offset); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}

	// Write timestamp
	if err := binary.Write(writer, binary.BigEndian, partition.Timestamp); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	return nil
}

// writeString writes a string to the writer
func (h *RequestHandler) writeString(writer io.Writer, s string) error {
	// Write string length
	if err := binary.Write(writer, binary.BigEndian, int16(len(s))); err != nil {
		return fmt.Errorf("failed to write string length: %w", err)
	}

	// Write string bytes
	if _, err := writer.Write([]byte(s)); err != nil {
		return fmt.Errorf("failed to write string bytes: %w", err)
	}

	return nil
}

// Placeholder handlers for other API operations
func (h *RequestHandler) handleFetch(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read fetch request (topic, partition, offset)
	var topic string
	var partition int32
	var offset int64

	// Read topic string length
	var topicLen int16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return fmt.Errorf("failed to read topic length: %w", err)
	}

	// Read topic string
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return fmt.Errorf("failed to read topic: %w", err)
	}
	topic = string(topicBytes)

	// Read partition
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return fmt.Errorf("failed to read partition: %w", err)
	}

	// Read offset
	if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
		return fmt.Errorf("failed to read offset: %w", err)
	}

	// Fetch message from broker
	message, err := h.broker.ConsumeMessage(topic, partition, offset)
	if err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("failed to fetch message: %v", err))
	}

	// Write response
	// Response format: correlation_id (4 bytes) | message_len (4 bytes) | message
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write message length
	messageLen := int32(len(message))
	if err := binary.Write(writer, binary.BigEndian, messageLen); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message
	if _, err := writer.Write(message); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleMetadata(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read number of topics requested
	var numTopics int32
	if err := binary.Read(reader, binary.BigEndian, &numTopics); err != nil {
		return fmt.Errorf("failed to read number of topics: %w", err)
	}

	// Read topic names
	requestedTopics := make([]string, numTopics)
	for i := int32(0); i < numTopics; i++ {
		var topicLen int16
		if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
			return fmt.Errorf("failed to read topic length: %w", err)
		}

		topicBytes := make([]byte, topicLen)
		if _, err := io.ReadFull(reader, topicBytes); err != nil {
			return fmt.Errorf("failed to read topic: %w", err)
		}
		requestedTopics[i] = string(topicBytes)
	}

	// Get broker metadata
	brokers := h.broker.GetProtocolBrokers()

	// Get topics (all topics if none requested)
	var topics []string
	if numTopics == 0 {
		topics = h.broker.GetTopics()
	} else {
		topics = requestedTopics
	}

	// Write response header
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write number of brokers
	if err := binary.Write(writer, binary.BigEndian, int32(len(brokers))); err != nil {
		return fmt.Errorf("failed to write broker count: %w", err)
	}

	// Write broker metadata
	for _, broker := range brokers {
		// Write broker ID length and ID
		if err := binary.Write(writer, binary.BigEndian, int16(len(broker.ID))); err != nil {
			return err
		}
		if _, err := writer.Write([]byte(broker.ID)); err != nil {
			return err
		}

		// Write broker address length and address
		if err := binary.Write(writer, binary.BigEndian, int16(len(broker.Addr))); err != nil {
			return err
		}
		if _, err := writer.Write([]byte(broker.Addr)); err != nil {
			return err
		}
	}

	// Write number of topics
	if err := binary.Write(writer, binary.BigEndian, int32(len(topics))); err != nil {
		return fmt.Errorf("failed to write topic count: %w", err)
	}

	// Write topic metadata
	for _, topic := range topics {
		partitions := h.broker.GetProtocolPartitions(topic)

		// Write topic name length and name
		if err := binary.Write(writer, binary.BigEndian, int16(len(topic))); err != nil {
			return err
		}
		if _, err := writer.Write([]byte(topic)); err != nil {
			return err
		}

		// Write number of partitions
		if err := binary.Write(writer, binary.BigEndian, int32(len(partitions))); err != nil {
			return err
		}

		// Write partition metadata
		for _, partition := range partitions {
			if err := binary.Write(writer, binary.BigEndian, partition.ID); err != nil {
				return err
			}

			// Write leader length and leader
			if err := binary.Write(writer, binary.BigEndian, int16(len(partition.Leader))); err != nil {
				return err
			}
			if _, err := writer.Write([]byte(partition.Leader)); err != nil {
				return err
			}

			// Write number of replicas
			if err := binary.Write(writer, binary.BigEndian, int32(len(partition.Replicas))); err != nil {
				return err
			}

			// Write replica IDs
			for _, replica := range partition.Replicas {
				if err := binary.Write(writer, binary.BigEndian, int16(len(replica))); err != nil {
					return err
				}
				if _, err := writer.Write([]byte(replica)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (h *RequestHandler) handleOffsetCommit(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id length and value
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read topic length and value
	var topicLen int16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return fmt.Errorf("failed to read topic: %w", err)
	}
	topic := string(topicBytes)

	// Read partition
	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return fmt.Errorf("failed to read partition: %w", err)
	}

	// Read offset
	var offset int64
	if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
		return fmt.Errorf("failed to read offset: %w", err)
	}

	// Read metadata length and value
	var metadataLen int16
	if err := binary.Read(reader, binary.BigEndian, &metadataLen); err != nil {
		return fmt.Errorf("failed to read metadata length: %w", err)
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(reader, metadataBytes); err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	metadata := string(metadataBytes)

	// Commit offset using offset manager
	offsetMgr := h.broker.GetProtocolOffsetManager()
	if err := offsetMgr.CommitOffset(groupID, topic, partition, offset, metadata); err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("failed to commit offset: %v", err))
	}

	// Write success response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Success flag (1 byte)
	success := byte(1)
	if err := binary.Write(writer, binary.BigEndian, success); err != nil {
		return fmt.Errorf("failed to write success flag: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleOffsetFetch(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id length and value
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read topic length and value
	var topicLen int16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return fmt.Errorf("failed to read topic: %w", err)
	}
	topic := string(topicBytes)

	// Read partition
	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return fmt.Errorf("failed to read partition: %w", err)
	}

	// Fetch offset using offset manager
	offsetMgr := h.broker.GetProtocolOffsetManager()
	offset, err := offsetMgr.FetchOffset(groupID, topic, partition)
	if err != nil {
		// If offset not found, return -1
		offset = -1
	}

	// Write response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write offset
	if err := binary.Write(writer, binary.BigEndian, offset); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleFindCoordinator(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read coordinator key (group ID or transactional ID)
	var keyLen int16
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return fmt.Errorf("failed to read key length: %w", err)
	}
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, keyBytes); err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}
	// keyType: 0 = group coordinator, 1 = transaction coordinator
	// For now, we only support group coordinators
	_ = string(keyBytes) // group ID

	// Get broker info - in Gofka, all brokers can be coordinators
	brokers := h.broker.GetProtocolBrokers()

	// Write response header
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write throttle time (0 for now)
	if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
		return fmt.Errorf("failed to write throttle time: %w", err)
	}

	// Write error code (0 = no error)
	if err := binary.Write(writer, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write error code: %w", err)
	}

	// Write error message (empty)
	if err := binary.Write(writer, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write error message length: %w", err)
	}

	// Use the first broker as coordinator (in production, this would be determined by hash)
	if len(brokers) > 0 {
		broker := brokers[0]

		// Write node ID (convert string ID to int32)
		// For simplicity, use 1 as default node ID
		if err := binary.Write(writer, binary.BigEndian, int32(1)); err != nil {
			return fmt.Errorf("failed to write node ID: %w", err)
		}

		// Extract host and port from broker address
		host := "localhost"
		port := int32(9092)
		if broker.Addr != "" {
			// Parse address (format: host:port)
			for i := len(broker.Addr) - 1; i >= 0; i-- {
				if broker.Addr[i] == ':' {
					host = broker.Addr[:i]
					fmt.Sscanf(broker.Addr[i+1:], "%d", &port)
					break
				}
			}
		}

		// Write host
		if err := binary.Write(writer, binary.BigEndian, int16(len(host))); err != nil {
			return fmt.Errorf("failed to write host length: %w", err)
		}
		if _, err := writer.Write([]byte(host)); err != nil {
			return fmt.Errorf("failed to write host: %w", err)
		}

		// Write port
		if err := binary.Write(writer, binary.BigEndian, port); err != nil {
			return fmt.Errorf("failed to write port: %w", err)
		}
	} else {
		// No brokers available - write error
		if err := binary.Write(writer, binary.BigEndian, int32(-1)); err != nil {
			return fmt.Errorf("failed to write node ID: %w", err)
		}
		if err := binary.Write(writer, binary.BigEndian, int16(0)); err != nil {
			return fmt.Errorf("failed to write host length: %w", err)
		}
		if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
			return fmt.Errorf("failed to write port: %w", err)
		}
	}

	return nil
}

func (h *RequestHandler) handleJoinGroup(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read member_id
	var memberIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &memberIDLen); err != nil {
		return fmt.Errorf("failed to read memberID length: %w", err)
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(reader, memberIDBytes); err != nil {
		return fmt.Errorf("failed to read memberID: %w", err)
	}
	memberID := string(memberIDBytes)

	// Read client_id
	var clientIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &clientIDLen); err != nil {
		return fmt.Errorf("failed to read clientID length: %w", err)
	}
	clientIDBytes := make([]byte, clientIDLen)
	if _, err := io.ReadFull(reader, clientIDBytes); err != nil {
		return fmt.Errorf("failed to read clientID: %w", err)
	}
	clientID := string(clientIDBytes)

	// Read client_host
	var clientHostLen int16
	if err := binary.Read(reader, binary.BigEndian, &clientHostLen); err != nil {
		return fmt.Errorf("failed to read clientHost length: %w", err)
	}
	clientHostBytes := make([]byte, clientHostLen)
	if _, err := io.ReadFull(reader, clientHostBytes); err != nil {
		return fmt.Errorf("failed to read clientHost: %w", err)
	}
	clientHost := string(clientHostBytes)

	// Read session timeout
	var sessionTimeout int32
	if err := binary.Read(reader, binary.BigEndian, &sessionTimeout); err != nil {
		return fmt.Errorf("failed to read session timeout: %w", err)
	}

	// Read subscriptions
	var numSubscriptions int32
	if err := binary.Read(reader, binary.BigEndian, &numSubscriptions); err != nil {
		return fmt.Errorf("failed to read subscriptions count: %w", err)
	}
	subscriptions := make([]string, numSubscriptions)
	for i := int32(0); i < numSubscriptions; i++ {
		var subLen int16
		if err := binary.Read(reader, binary.BigEndian, &subLen); err != nil {
			return fmt.Errorf("failed to read subscription length: %w", err)
		}
		subBytes := make([]byte, subLen)
		if _, err := io.ReadFull(reader, subBytes); err != nil {
			return fmt.Errorf("failed to read subscription: %w", err)
		}
		subscriptions[i] = string(subBytes)
	}

	// Join group using consumer group coordinator
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	group, err := coordinator.JoinGroup(groupID, memberID, clientID, clientHost, sessionTimeout, subscriptions)
	if err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("failed to join group: %v", err))
	}

	// Write response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write group_id
	groupIDResp := group.GetGroupID()
	if err := binary.Write(writer, binary.BigEndian, int16(len(groupIDResp))); err != nil {
		return err
	}
	if _, err := writer.Write([]byte(groupIDResp)); err != nil {
		return err
	}

	// Write generation
	if err := binary.Write(writer, binary.BigEndian, group.GetGeneration()); err != nil {
		return err
	}

	// Write member_id
	if err := binary.Write(writer, binary.BigEndian, int16(len(memberID))); err != nil {
		return err
	}
	if _, err := writer.Write([]byte(memberID)); err != nil {
		return err
	}

	// Write leader_id
	leaderID := group.GetLeaderID()
	if err := binary.Write(writer, binary.BigEndian, int16(len(leaderID))); err != nil {
		return err
	}
	if _, err := writer.Write([]byte(leaderID)); err != nil {
		return err
	}

	return nil
}

func (h *RequestHandler) handleHeartbeat(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read member_id
	var memberIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &memberIDLen); err != nil {
		return fmt.Errorf("failed to read memberID length: %w", err)
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(reader, memberIDBytes); err != nil {
		return fmt.Errorf("failed to read memberID: %w", err)
	}
	memberID := string(memberIDBytes)

	// Read generation
	var generation int32
	if err := binary.Read(reader, binary.BigEndian, &generation); err != nil {
		return fmt.Errorf("failed to read generation: %w", err)
	}

	// Send heartbeat to consumer group coordinator
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	if err := coordinator.Heartbeat(groupID, memberID, generation); err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("heartbeat failed: %v", err))
	}

	// Write success response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Success flag
	success := byte(1)
	if err := binary.Write(writer, binary.BigEndian, success); err != nil {
		return fmt.Errorf("failed to write success flag: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleLeaveGroup(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read member_id
	var memberIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &memberIDLen); err != nil {
		return fmt.Errorf("failed to read memberID length: %w", err)
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(reader, memberIDBytes); err != nil {
		return fmt.Errorf("failed to read memberID: %w", err)
	}
	memberID := string(memberIDBytes)

	// Leave group using consumer group coordinator
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	if err := coordinator.LeaveGroup(groupID, memberID); err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("failed to leave group: %v", err))
	}

	// Write success response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Success flag
	success := byte(1)
	if err := binary.Write(writer, binary.BigEndian, success); err != nil {
		return fmt.Errorf("failed to write success flag: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleSyncGroup(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read group_id
	var groupIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
		return fmt.Errorf("failed to read groupID length: %w", err)
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
		return fmt.Errorf("failed to read groupID: %w", err)
	}
	groupID := string(groupIDBytes)

	// Read member_id
	var memberIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &memberIDLen); err != nil {
		return fmt.Errorf("failed to read memberID length: %w", err)
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(reader, memberIDBytes); err != nil {
		return fmt.Errorf("failed to read memberID: %w", err)
	}
	memberID := string(memberIDBytes)

	// Read generation
	var generation int32
	if err := binary.Read(reader, binary.BigEndian, &generation); err != nil {
		return fmt.Errorf("failed to read generation: %w", err)
	}

	// Sync group using consumer group coordinator
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	assignment, err := coordinator.SyncGroup(groupID, memberID, generation)
	if err != nil {
		return h.writeErrorResponse(writer, header, fmt.Sprintf("sync group failed: %v", err))
	}

	// Write response
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write assigned partitions for this member
	assignments := assignment.GetAssignments()
	memberAssignment, ok := assignments[memberID]
	if !ok {
		memberAssignment = []int32{}
	}

	// Write number of assigned partitions
	if err := binary.Write(writer, binary.BigEndian, int32(len(memberAssignment))); err != nil {
		return fmt.Errorf("failed to write partition count: %w", err)
	}

	// Write partition IDs
	for _, partitionID := range memberAssignment {
		if err := binary.Write(writer, binary.BigEndian, partitionID); err != nil {
			return fmt.Errorf("failed to write partition ID: %w", err)
		}
	}

	// Write generation
	if err := binary.Write(writer, binary.BigEndian, assignment.GetGeneration()); err != nil {
		return fmt.Errorf("failed to write generation: %w", err)
	}

	return nil
}

func (h *RequestHandler) handleDescribeGroups(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Read number of group IDs
	var numGroups int32
	if err := binary.Read(reader, binary.BigEndian, &numGroups); err != nil {
		return fmt.Errorf("failed to read number of groups: %w", err)
	}

	// Read group IDs
	groupIDs := make([]string, numGroups)
	for i := int32(0); i < numGroups; i++ {
		var groupIDLen int16
		if err := binary.Read(reader, binary.BigEndian, &groupIDLen); err != nil {
			return fmt.Errorf("failed to read group ID length: %w", err)
		}
		groupIDBytes := make([]byte, groupIDLen)
		if _, err := io.ReadFull(reader, groupIDBytes); err != nil {
			return fmt.Errorf("failed to read group ID: %w", err)
		}
		groupIDs[i] = string(groupIDBytes)
	}

	// Write response header
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write throttle time (0 for now)
	if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
		return fmt.Errorf("failed to write throttle time: %w", err)
	}

	// Write number of groups
	if err := binary.Write(writer, binary.BigEndian, numGroups); err != nil {
		return fmt.Errorf("failed to write number of groups: %w", err)
	}

	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()

	// Write group descriptions
	for _, groupID := range groupIDs {
		// Write error code
		group, err := coordinator.GetGroup(groupID)
		if err != nil {
			// Group not found error code = 16
			if err := binary.Write(writer, binary.BigEndian, int16(16)); err != nil {
				return fmt.Errorf("failed to write error code: %w", err)
			}
			// Write group ID
			if err := h.writeString(writer, groupID); err != nil {
				return err
			}
			// Write empty state
			if err := h.writeString(writer, ""); err != nil {
				return err
			}
			// Write empty protocol type
			if err := h.writeString(writer, ""); err != nil {
				return err
			}
			// Write empty protocol
			if err := h.writeString(writer, ""); err != nil {
				return err
			}
			// Write 0 members
			if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
				return err
			}
			continue
		}

		// Group found - write success
		if err := binary.Write(writer, binary.BigEndian, int16(0)); err != nil {
			return fmt.Errorf("failed to write error code: %w", err)
		}

		// Write group ID
		if err := h.writeString(writer, groupID); err != nil {
			return err
		}

		// Write state (check if group implements ConsumerGroupDetailInterface)
		state := "Stable"
		protocol := "consumer"
		if detailGroup, ok := group.(ConsumerGroupDetailInterface); ok {
			state = detailGroup.GetState()
			protocol = detailGroup.GetProtocol()
		}
		if err := h.writeString(writer, state); err != nil {
			return err
		}

		// Write protocol type
		if err := h.writeString(writer, "consumer"); err != nil {
			return err
		}

		// Write protocol
		if err := h.writeString(writer, protocol); err != nil {
			return err
		}

		// Write members
		members := group.GetMembers()
		if err := binary.Write(writer, binary.BigEndian, int32(len(members))); err != nil {
			return fmt.Errorf("failed to write member count: %w", err)
		}

		assignments := group.GetAssignments()
		for memberID, member := range members {
			// Write member ID
			if err := h.writeString(writer, memberID); err != nil {
				return err
			}

			// Write client ID
			if err := h.writeString(writer, member.GetClientID()); err != nil {
				return err
			}

			// Write client host
			if err := h.writeString(writer, member.GetClientHost()); err != nil {
				return err
			}

			// Write member metadata (subscriptions as bytes)
			subscriptions := member.GetSubscriptions()
			metadataLen := 4 // 4 bytes for subscription count
			for _, sub := range subscriptions {
				metadataLen += 2 + len(sub) // 2 bytes length + string
			}
			if err := binary.Write(writer, binary.BigEndian, int32(metadataLen)); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, int32(len(subscriptions))); err != nil {
				return err
			}
			for _, sub := range subscriptions {
				if err := binary.Write(writer, binary.BigEndian, int16(len(sub))); err != nil {
					return err
				}
				if _, err := writer.Write([]byte(sub)); err != nil {
					return err
				}
			}

			// Write member assignment (partition assignments as bytes)
			memberAssignment, ok := assignments[memberID]
			if !ok {
				memberAssignment = []int32{}
			}
			assignmentLen := 4 + len(memberAssignment)*4 // count + partitions
			if err := binary.Write(writer, binary.BigEndian, int32(assignmentLen)); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, int32(len(memberAssignment))); err != nil {
				return err
			}
			for _, partition := range memberAssignment {
				if err := binary.Write(writer, binary.BigEndian, partition); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (h *RequestHandler) handleListGroups(reader io.Reader, writer io.Writer, header *RequestHeader) error {
	// Write response header
	if err := binary.Write(writer, binary.BigEndian, header.CorrelationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	// Write throttle time (0 for now)
	if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
		return fmt.Errorf("failed to write throttle time: %w", err)
	}

	// Write error code (0 = no error)
	if err := binary.Write(writer, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write error code: %w", err)
	}

	// Get list of groups from coordinator
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	groupIDs := coordinator.ListGroups()

	// Write number of groups
	if err := binary.Write(writer, binary.BigEndian, int32(len(groupIDs))); err != nil {
		return fmt.Errorf("failed to write group count: %w", err)
	}

	// Write each group
	for _, groupID := range groupIDs {
		// Write group ID
		if err := h.writeString(writer, groupID); err != nil {
			return err
		}

		// Write protocol type (consumer for all groups)
		if err := h.writeString(writer, "consumer"); err != nil {
			return err
		}
	}

	return nil
}
