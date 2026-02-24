package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/prashanth8983/gofka/pkg/logger"
	"go.uber.org/zap"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// API Keys (subset of Kafka protocol)
const (
	APIKeyProduce         = 0
	APIKeyFetch           = 1
	APIKeyListOffsets     = 2
	APIKeyMetadata        = 3
	APIKeyOffsetCommit    = 8
	APIKeyOffsetFetch     = 9
	APIKeyFindCoordinator = 10
	APIKeyJoinGroup       = 11
	APIKeyHeartbeat       = 12
	APIKeyLeaveGroup      = 13
	APIKeyCreateTopics    = 19
)

// RequestHandler handles binary protocol requests
type RequestHandler struct {
	broker BrokerInterface
}

// NewRequestHandler creates a new request handler
func NewRequestHandler(broker BrokerInterface) *RequestHandler {
	return &RequestHandler{
		broker: broker,
	}
}

// HandleRequest processes a binary protocol request
func (h *RequestHandler) HandleRequest(reader io.Reader, writer io.Writer) error {
	// Read the message size (already consumed by server)
	var messageSize int32
	if err := binary.Read(reader, binary.BigEndian, &messageSize); err != nil {
		return fmt.Errorf("failed to read message size: %w", err)
	}

	// Read API key
	var apiKey int16
	if err := binary.Read(reader, binary.BigEndian, &apiKey); err != nil {
		return fmt.Errorf("failed to read API key: %w", err)
	}

	// Read API version
	var apiVersion int16
	if err := binary.Read(reader, binary.BigEndian, &apiVersion); err != nil {
		return fmt.Errorf("failed to read API version: %w", err)
	}

	// Read correlation ID
	var correlationID int32
	if err := binary.Read(reader, binary.BigEndian, &correlationID); err != nil {
		return fmt.Errorf("failed to read correlation ID: %w", err)
	}

	// Read client ID length
	var clientIDLen int16
	if err := binary.Read(reader, binary.BigEndian, &clientIDLen); err != nil {
		return fmt.Errorf("failed to read client ID length: %w", err)
	}

	// Read client ID
	clientID := make([]byte, clientIDLen)
	if clientIDLen > 0 {
		if _, err := io.ReadFull(reader, clientID); err != nil {
			return fmt.Errorf("failed to read client ID: %w", err)
		}
	}

	start := time.Now()

	// Handle the request based on API key
	var response bytes.Buffer

	// Write correlation ID first
	if err := binary.Write(&response, binary.BigEndian, correlationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	var handlerErr error
	switch apiKey {
	case APIKeyMetadata:
		handlerErr = h.handleMetadata(reader, &response, apiVersion)
	case APIKeyProduce:
		handlerErr = h.handleProduce(reader, &response, apiVersion)
	case APIKeyFetch:
		handlerErr = h.handleFetch(reader, &response, apiVersion)
	case APIKeyListOffsets:
		handlerErr = h.handleListOffsets(reader, &response, apiVersion)
	case APIKeyCreateTopics:
		handlerErr = h.handleCreateTopics(reader, &response, apiVersion)
	case APIKeyOffsetCommit:
		handlerErr = h.handleOffsetCommit(reader, &response, apiVersion)
	case APIKeyOffsetFetch:
		handlerErr = h.handleOffsetFetch(reader, &response, apiVersion)
	case APIKeyJoinGroup:
		handlerErr = h.handleJoinGroup(reader, &response, apiVersion)
	case APIKeyHeartbeat:
		handlerErr = h.handleHeartbeat(reader, &response, apiVersion)
	case APIKeyLeaveGroup:
		handlerErr = h.handleLeaveGroup(reader, &response, apiVersion)
	default:
		logger.Warn("Unsupported API key", zap.Int16("api_key", apiKey))
		binary.Write(&response, binary.BigEndian, int16(-1))
		writeString(&response, fmt.Sprintf("Unsupported API key: %d", apiKey))
	}

	if handlerErr != nil {
		logger.Error("Request handler error", zap.Int16("api_key", apiKey), zap.Error(handlerErr))
		return handlerErr
	}

	// Write the complete response with size prefix
	responseBytes := response.Bytes()

	if err := binary.Write(writer, binary.BigEndian, int32(len(responseBytes))); err != nil {
		return fmt.Errorf("failed to write response size: %w", err)
	}

	if _, err := writer.Write(responseBytes); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	logger.Debug("Request handled",
		zap.Int16("api_key", apiKey),
		zap.Int32("correlation_id", correlationID),
		zap.Duration("duration", time.Since(start)),
		zap.Int("response_size", len(responseBytes)),
	)
	return nil
}

// handleMetadata handles metadata requests
func (h *RequestHandler) handleMetadata(reader io.Reader, response io.Writer, apiVersion int16) error {
	// Read request (for version 0, it's just number of topics)
	var numTopics int32
	if err := binary.Read(reader, binary.BigEndian, &numTopics); err != nil {
		return fmt.Errorf("failed to read num topics: %w", err)
	}

	// If numTopics > 0, read topic names (we'll ignore for now and return all)
	if numTopics > 0 {
		for i := int32(0); i < numTopics; i++ {
			var topicLen int16
			binary.Read(reader, binary.BigEndian, &topicLen)
			topic := make([]byte, topicLen)
			reader.Read(topic)
		}
	}

	// Get broker metadata
	brokers := h.broker.GetProtocolBrokers()

	// If no brokers, add ourselves
	if len(brokers) == 0 {
		brokers = []BrokerMetadata{
			{ID: "broker-1", Addr: "localhost:9092"},
		}
	}

	// Write number of brokers
	if err := binary.Write(response, binary.BigEndian, int32(len(brokers))); err != nil {
		return err
	}

	// Write each broker
	for _, broker := range brokers {
		// Node ID (convert string to int32)
		nodeID := int32(1) // Default to 1
		if broker.ID == "broker-1" {
			nodeID = 1
		}
		binary.Write(response, binary.BigEndian, nodeID)

		// Host
		writeString(response, "localhost")

		// Port
		binary.Write(response, binary.BigEndian, int32(9092))
	}

	// Get topics
	topics := h.broker.GetTopics()

	// Write number of topics
	binary.Write(response, binary.BigEndian, int32(len(topics)))

	// Write each topic
	for _, topicName := range topics {
		// Error code (0 = no error)
		binary.Write(response, binary.BigEndian, int16(0))

		// Topic name
		writeString(response, topicName)

		// Get partitions for this topic
		partitions := h.broker.GetProtocolPartitions(topicName)

		// If no partitions, create a default one
		if len(partitions) == 0 {
			partitions = []PartitionMetadata{
				{ID: 0, Leader: "broker-1", Replicas: []string{"broker-1"}},
			}
		}

		// Number of partitions
		binary.Write(response, binary.BigEndian, int32(len(partitions)))

		// Write each partition
		for _, partition := range partitions {
			// Error code
			binary.Write(response, binary.BigEndian, int16(0))

			// Partition ID
			binary.Write(response, binary.BigEndian, partition.ID)

			// Leader ID
			binary.Write(response, binary.BigEndian, int32(1)) // broker-1 = 1

			// Number of replicas
			binary.Write(response, binary.BigEndian, int32(len(partition.Replicas)))

			// Replica IDs
			for range partition.Replicas {
				binary.Write(response, binary.BigEndian, int32(1)) // All replicas on broker-1
			}

			// Number of ISR (in-sync replicas)
			binary.Write(response, binary.BigEndian, int32(len(partition.Replicas)))

			// ISR IDs
			for range partition.Replicas {
				binary.Write(response, binary.BigEndian, int32(1)) // All ISR on broker-1
			}
		}
	}

	logger.Debug("Metadata response sent", zap.Int("brokers", len(brokers)), zap.Int("topics", len(topics)))
	return nil
}

// handleProduce handles produce requests
func (h *RequestHandler) handleProduce(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read required acks
	var requiredAcks int16
	if err := binary.Read(reader, binary.BigEndian, &requiredAcks); err != nil {
		return fmt.Errorf("failed to read required acks: %w", err)
	}

	// Read timeout
	var timeout int32
	if err := binary.Read(reader, binary.BigEndian, &timeout); err != nil {
		return fmt.Errorf("failed to read timeout: %w", err)
	}

	// Read number of topics
	var numTopics int32
	if err := binary.Read(reader, binary.BigEndian, &numTopics); err != nil {
		return fmt.Errorf("failed to read number of topics: %w", err)
	}

	type topicResponse struct {
		topic      string
		partitions []struct {
			partition int32
			errorCode int16
			offset    int64
			timestamp int64
		}
	}
	responses := make([]topicResponse, 0, numTopics)

	// Process each topic
	for i := int32(0); i < numTopics; i++ {
		topicName := readString(reader)

		// Read number of partitions
		var numPartitions int32
		if err := binary.Read(reader, binary.BigEndian, &numPartitions); err != nil {
			return fmt.Errorf("failed to read number of partitions: %w", err)
		}

		resp := topicResponse{
			topic:      topicName,
			partitions: make([]struct {
				partition int32
				errorCode int16
				offset    int64
				timestamp int64
			}, numPartitions),
		}

		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
				return fmt.Errorf("failed to read partition: %w", err)
			}

			// Read message set size
			var messageSetSize int32
			if err := binary.Read(reader, binary.BigEndian, &messageSetSize); err != nil {
				return fmt.Errorf("failed to read message set size: %w", err)
			}

			// Read message set
			messageSet := make([]byte, messageSetSize)
			if _, err := io.ReadFull(reader, messageSet); err != nil {
				return fmt.Errorf("failed to read message set: %w", err)
			}

			// Produce the message
			offset, err := h.broker.ProduceMessage(topicName, messageSet)
			if err != nil {
				resp.partitions[j] = struct {
					partition int32
					errorCode int16
					offset    int64
					timestamp int64
				}{
					partition: partition,
					errorCode: 1, // Unknown error
					offset:    -1,
					timestamp: currentTimeMillis(),
				}
			} else {
				resp.partitions[j] = struct {
					partition int32
					errorCode int16
					offset    int64
					timestamp int64
				}{
					partition: partition,
					errorCode: 0,
					offset:    offset,
					timestamp: currentTimeMillis(),
				}
			}
		}
		responses = append(responses, resp)
	}

	// Write response
	// Write number of topics
	if err := binary.Write(writer, binary.BigEndian, int32(len(responses))); err != nil {
		return fmt.Errorf("failed to write number of topics: %w", err)
	}

	for _, resp := range responses {
		// Write topic name
		writeString(writer, resp.topic)

		// Write number of partitions
		if err := binary.Write(writer, binary.BigEndian, int32(len(resp.partitions))); err != nil {
			return fmt.Errorf("failed to write number of partitions: %w", err)
		}

		for _, partResp := range resp.partitions {
			// Write partition
			if err := binary.Write(writer, binary.BigEndian, partResp.partition); err != nil {
				return fmt.Errorf("failed to write partition: %w", err)
			}
			// Write error code
			if err := binary.Write(writer, binary.BigEndian, partResp.errorCode); err != nil {
				return fmt.Errorf("failed to write error code: %w", err)
			}
			// Write offset
			if err := binary.Write(writer, binary.BigEndian, partResp.offset); err != nil {
				return fmt.Errorf("failed to write offset: %w", err)
			}
			// Write timestamp (for version >= 2)
			if apiVersion >= 2 {
				if err := binary.Write(writer, binary.BigEndian, partResp.timestamp); err != nil {
					return fmt.Errorf("failed to write timestamp: %w", err)
				}
			}
		}
	}

	// Write throttle time for version >= 1
	if apiVersion >= 1 {
		if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
			return fmt.Errorf("failed to write throttle time: %w", err)
		}
	}

	return nil
}

// handleFetch handles fetch requests
func (h *RequestHandler) handleFetch(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read replica ID (usually -1 for consumers)
	var replicaID int32
	if err := binary.Read(reader, binary.BigEndian, &replicaID); err != nil {
		return fmt.Errorf("failed to read replica ID: %w", err)
	}

	// Read max wait time
	var maxWaitMs int32
	if err := binary.Read(reader, binary.BigEndian, &maxWaitMs); err != nil {
		return fmt.Errorf("failed to read max wait time: %w", err)
	}

	// Read min bytes
	var minBytes int32
	if err := binary.Read(reader, binary.BigEndian, &minBytes); err != nil {
		return fmt.Errorf("failed to read min bytes: %w", err)
	}

	// Read max bytes (for version >= 3)
	if apiVersion >= 3 {
		var maxBytes int32
		if err := binary.Read(reader, binary.BigEndian, &maxBytes); err != nil {
			return fmt.Errorf("failed to read max bytes: %w", err)
		}
	}

	// Read isolation level (for version >= 4)
	if apiVersion >= 4 {
		var isolationLevel int8
		if err := binary.Read(reader, binary.BigEndian, &isolationLevel); err != nil {
			return fmt.Errorf("failed to read isolation level: %w", err)
		}
	}

	// Read number of topics
	var numTopics int32
	if err := binary.Read(reader, binary.BigEndian, &numTopics); err != nil {
		return fmt.Errorf("failed to read number of topics: %w", err)
	}

	type partitionResponse struct {
		partition           int32
		errorCode           int16
		highWatermark       int64
		lastStableOffset    int64
		abortedTransactions []struct{}
		records             []byte
	}

	type topicResponse struct {
		topic      string
		partitions []partitionResponse
	}

	responses := make([]topicResponse, 0, numTopics)

	// Process each topic
	for i := int32(0); i < numTopics; i++ {
		topicName := readString(reader)

		// Read number of partitions
		var numPartitions int32
		if err := binary.Read(reader, binary.BigEndian, &numPartitions); err != nil {
			return fmt.Errorf("failed to read number of partitions: %w", err)
		}

		resp := topicResponse{
			topic:      topicName,
			partitions: make([]partitionResponse, numPartitions),
		}

		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
				return fmt.Errorf("failed to read partition: %w", err)
			}

			var fetchOffset int64
			if err := binary.Read(reader, binary.BigEndian, &fetchOffset); err != nil {
				return fmt.Errorf("failed to read fetch offset: %w", err)
			}

			// Read log start offset (for version >= 5)
			if apiVersion >= 5 {
				var logStartOffset int64
				if err := binary.Read(reader, binary.BigEndian, &logStartOffset); err != nil {
					return fmt.Errorf("failed to read log start offset: %w", err)
				}
			}

			var partitionMaxBytes int32
			if err := binary.Read(reader, binary.BigEndian, &partitionMaxBytes); err != nil {
				return fmt.Errorf("failed to read partition max bytes: %w", err)
			}

			// Fetch the message
			message, err := h.broker.ConsumeMessage(topicName, partition, fetchOffset)
			if err != nil {
				resp.partitions[j] = partitionResponse{
					partition:     partition,
					errorCode:     1, // Offset out of range or other error
					highWatermark: 0,
					records:       nil,
				}
			} else {
				resp.partitions[j] = partitionResponse{
					partition:     partition,
					errorCode:     0,
					highWatermark: fetchOffset + 1, // Simple implementation
					records:       message,
				}
			}
		}
		responses = append(responses, resp)
	}

	// Write throttle time (for version >= 1)
	if apiVersion >= 1 {
		if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
			return fmt.Errorf("failed to write throttle time: %w", err)
		}
	}

	// Write number of topics
	if err := binary.Write(writer, binary.BigEndian, int32(len(responses))); err != nil {
		return fmt.Errorf("failed to write number of topics: %w", err)
	}

	for _, resp := range responses {
		// Write topic name
		writeString(writer, resp.topic)

		// Write number of partitions
		if err := binary.Write(writer, binary.BigEndian, int32(len(resp.partitions))); err != nil {
			return fmt.Errorf("failed to write number of partitions: %w", err)
		}

		for _, partResp := range resp.partitions {
			// Write partition
			if err := binary.Write(writer, binary.BigEndian, partResp.partition); err != nil {
				return fmt.Errorf("failed to write partition: %w", err)
			}
			// Write error code
			if err := binary.Write(writer, binary.BigEndian, partResp.errorCode); err != nil {
				return fmt.Errorf("failed to write error code: %w", err)
			}
			// Write high watermark
			if err := binary.Write(writer, binary.BigEndian, partResp.highWatermark); err != nil {
				return fmt.Errorf("failed to write high watermark: %w", err)
			}

			// Write last stable offset (for version >= 4)
			if apiVersion >= 4 {
				if err := binary.Write(writer, binary.BigEndian, partResp.lastStableOffset); err != nil {
					return fmt.Errorf("failed to write last stable offset: %w", err)
				}
			}

			// Write log start offset (for version >= 5)
			if apiVersion >= 5 {
				if err := binary.Write(writer, binary.BigEndian, int64(0)); err != nil {
					return fmt.Errorf("failed to write log start offset: %w", err)
				}
			}

			// Write aborted transactions (for version >= 4)
			if apiVersion >= 4 {
				if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
					return fmt.Errorf("failed to write aborted transactions count: %w", err)
				}
			}

			// Write record set size and records
			recordLen := int32(0)
			if partResp.records != nil {
				recordLen = int32(len(partResp.records))
			}
			if err := binary.Write(writer, binary.BigEndian, recordLen); err != nil {
				return fmt.Errorf("failed to write record set size: %w", err)
			}
			if recordLen > 0 {
				if _, err := writer.Write(partResp.records); err != nil {
					return fmt.Errorf("failed to write records: %w", err)
				}
			}
		}
	}

	return nil
}

// handleListOffsets handles list offsets requests
func (h *RequestHandler) handleListOffsets(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read replica ID
	var replicaID int32
	if err := binary.Read(reader, binary.BigEndian, &replicaID); err != nil {
		return fmt.Errorf("failed to read replica ID: %w", err)
	}

	// Read isolation level (for version >= 2)
	if apiVersion >= 2 {
		var isolationLevel int8
		if err := binary.Read(reader, binary.BigEndian, &isolationLevel); err != nil {
			return fmt.Errorf("failed to read isolation level: %w", err)
		}
	}

	// Read number of topics
	var numTopics int32
	if err := binary.Read(reader, binary.BigEndian, &numTopics); err != nil {
		return fmt.Errorf("failed to read number of topics: %w", err)
	}

	type partitionResponse struct {
		partition int32
		errorCode int16
		timestamp int64
		offset    int64
	}

	type topicResponse struct {
		topic      string
		partitions []partitionResponse
	}

	responses := make([]topicResponse, 0, numTopics)
	offsetMgr := h.broker.GetProtocolOffsetManager()

	// Process each topic
	for i := int32(0); i < numTopics; i++ {
		topicName := readString(reader)

		// Read number of partitions
		var numPartitions int32
		if err := binary.Read(reader, binary.BigEndian, &numPartitions); err != nil {
			return fmt.Errorf("failed to read number of partitions: %w", err)
		}

		resp := topicResponse{
			topic:      topicName,
			partitions: make([]partitionResponse, numPartitions),
		}

		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
				return fmt.Errorf("failed to read partition: %w", err)
			}

			// Read timestamp (-1 = latest, -2 = earliest)
			var timestamp int64
			if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
				return fmt.Errorf("failed to read timestamp: %w", err)
			}

			// Get the appropriate offset based on timestamp
			var offset int64
			var err error

			if timestamp == -1 {
				// Latest offset
				offset, err = offsetMgr.GetLatestOffset(topicName, partition)
			} else if timestamp == -2 {
				// Earliest offset
				offset, err = offsetMgr.GetEarliestOffset(topicName, partition)
			} else {
				// For specific timestamp, return latest for now (would need time-based index)
				offset, err = offsetMgr.GetLatestOffset(topicName, partition)
			}

			if err != nil {
				resp.partitions[j] = partitionResponse{
					partition: partition,
					errorCode: 3, // Unknown topic or partition
					timestamp: -1,
					offset:    -1,
				}
			} else {
				resp.partitions[j] = partitionResponse{
					partition: partition,
					errorCode: 0,
					timestamp: timestamp,
					offset:    offset,
				}
			}
		}
		responses = append(responses, resp)
	}

	// Write throttle time (for version >= 2)
	if apiVersion >= 2 {
		if err := binary.Write(writer, binary.BigEndian, int32(0)); err != nil {
			return fmt.Errorf("failed to write throttle time: %w", err)
		}
	}

	// Write number of topics
	if err := binary.Write(writer, binary.BigEndian, int32(len(responses))); err != nil {
		return fmt.Errorf("failed to write number of topics: %w", err)
	}

	for _, resp := range responses {
		// Write topic name
		writeString(writer, resp.topic)

		// Write number of partitions
		if err := binary.Write(writer, binary.BigEndian, int32(len(resp.partitions))); err != nil {
			return fmt.Errorf("failed to write number of partitions: %w", err)
		}

		for _, partResp := range resp.partitions {
			// Write partition
			if err := binary.Write(writer, binary.BigEndian, partResp.partition); err != nil {
				return fmt.Errorf("failed to write partition: %w", err)
			}
			// Write error code
			if err := binary.Write(writer, binary.BigEndian, partResp.errorCode); err != nil {
				return fmt.Errorf("failed to write error code: %w", err)
			}

			// Version 0 returns multiple timestamps/offsets, version >= 1 returns single
			if apiVersion >= 1 {
				// Write timestamp
				if err := binary.Write(writer, binary.BigEndian, partResp.timestamp); err != nil {
					return fmt.Errorf("failed to write timestamp: %w", err)
				}
				// Write offset
				if err := binary.Write(writer, binary.BigEndian, partResp.offset); err != nil {
					return fmt.Errorf("failed to write offset: %w", err)
				}
			} else {
				// Version 0: write array of offsets
				if err := binary.Write(writer, binary.BigEndian, int32(1)); err != nil {
					return fmt.Errorf("failed to write offset count: %w", err)
				}
				if err := binary.Write(writer, binary.BigEndian, partResp.offset); err != nil {
					return fmt.Errorf("failed to write offset: %w", err)
				}
			}
		}
	}

	return nil
}

// handleCreateTopics handles create topics requests
func (h *RequestHandler) handleCreateTopics(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read number of topics
	var numTopics int32
	binary.Read(reader, binary.BigEndian, &numTopics)

	// For each topic
	for i := int32(0); i < numTopics; i++ {
		// Read topic name
		topicName := readString(reader)

		// Read num partitions
		var numPartitions int32
		binary.Read(reader, binary.BigEndian, &numPartitions)

		// Read replication factor
		var replicationFactor int16
		binary.Read(reader, binary.BigEndian, &replicationFactor)

		// Create the topic
		err := h.broker.CreateTopic(topicName, numPartitions)

		// Write response
		writeString(writer, topicName)
		if err != nil {
			binary.Write(writer, binary.BigEndian, int16(1)) // Error code
			writeString(writer, err.Error())
		} else {
			binary.Write(writer, binary.BigEndian, int16(0)) // Success
			writeString(writer, "")
		}
	}

	return nil
}

// handleOffsetCommit handles offset commit requests
func (h *RequestHandler) handleOffsetCommit(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read group ID
	groupID := readString(reader)

	// Read generation ID and member ID for newer versions
	var generationID int32
	var memberID string
	if apiVersion >= 1 {
		binary.Read(reader, binary.BigEndian, &generationID)
		memberID = readString(reader)
	}

	// Read number of topics
	var numTopics int32
	binary.Read(reader, binary.BigEndian, &numTopics)

	offsetMgr := h.broker.GetProtocolOffsetManager()

	// Write number of topics in response
	binary.Write(writer, binary.BigEndian, numTopics)

	for i := int32(0); i < numTopics; i++ {
		topicName := readString(reader)

		// Read number of partitions
		var numPartitions int32
		binary.Read(reader, binary.BigEndian, &numPartitions)

		// Write topic name
		writeString(writer, topicName)

		// Write number of partitions
		binary.Write(writer, binary.BigEndian, numPartitions)

		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			var offset int64
			binary.Read(reader, binary.BigEndian, &partition)
			binary.Read(reader, binary.BigEndian, &offset)

			// Commit the offset
			err := offsetMgr.CommitOffset(groupID, topicName, partition, offset)

			// Write partition
			binary.Write(writer, binary.BigEndian, partition)

			// Write error code
			if err != nil {
				binary.Write(writer, binary.BigEndian, int16(1))
			} else {
				binary.Write(writer, binary.BigEndian, int16(0))
			}
		}
	}

	_ = memberID // Suppress unused warning
	return nil
}

// handleOffsetFetch handles offset fetch requests
func (h *RequestHandler) handleOffsetFetch(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read group ID
	groupID := readString(reader)

	// Read number of topics
	var numTopics int32
	binary.Read(reader, binary.BigEndian, &numTopics)

	offsetMgr := h.broker.GetProtocolOffsetManager()

	// Write number of topics in response
	binary.Write(writer, binary.BigEndian, numTopics)

	for i := int32(0); i < numTopics; i++ {
		topicName := readString(reader)

		// Read number of partitions
		var numPartitions int32
		binary.Read(reader, binary.BigEndian, &numPartitions)

		// Write topic name
		writeString(writer, topicName)

		// Write number of partitions
		binary.Write(writer, binary.BigEndian, numPartitions)

		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			binary.Read(reader, binary.BigEndian, &partition)

			// Get the offset
			offset, err := offsetMgr.GetOffset(groupID, topicName, partition)

			// Write partition
			binary.Write(writer, binary.BigEndian, partition)

			// Write offset
			if err != nil {
				binary.Write(writer, binary.BigEndian, int64(-1)) // No offset
			} else {
				binary.Write(writer, binary.BigEndian, offset)
			}

			// Write metadata (empty string)
			writeString(writer, "")

			// Write error code
			if err != nil {
				binary.Write(writer, binary.BigEndian, int16(1))
			} else {
				binary.Write(writer, binary.BigEndian, int16(0))
			}
		}
	}

	return nil
}

// handleJoinGroup handles join group requests
func (h *RequestHandler) handleJoinGroup(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read group ID
	groupID := readString(reader)

	// Read session timeout
	var sessionTimeout int32
	binary.Read(reader, binary.BigEndian, &sessionTimeout)

	// Read member ID
	memberID := readString(reader)

	// Read protocol type
	protocolType := readString(reader)

	// Read group protocols
	var numProtocols int32
	binary.Read(reader, binary.BigEndian, &numProtocols)

	var topics []string
	for i := int32(0); i < numProtocols; i++ {
		protocolName := readString(reader)
		metadataLen := readInt32(reader)
		metadata := make([]byte, metadataLen)
		reader.Read(metadata)

		// Parse metadata to extract topics (simplified)
		// In a real implementation, this would parse the consumer protocol metadata
		topics = append(topics, "test-topic") // Default for now
		_ = protocolName
	}

	// Join the group
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	assignedMemberID, err := coordinator.JoinGroup(groupID, memberID, "", topics)

	if err != nil {
		// Write error response
		binary.Write(writer, binary.BigEndian, int16(1)) // Error code
	} else {
		// Write success response
		binary.Write(writer, binary.BigEndian, int16(0)) // Error code
	}

	// Write generation ID
	binary.Write(writer, binary.BigEndian, int32(1))

	// Write protocol
	writeString(writer, protocolType)

	// Write leader ID
	writeString(writer, assignedMemberID)

	// Write member ID
	writeString(writer, assignedMemberID)

	// Write members (empty for non-leaders)
	binary.Write(writer, binary.BigEndian, int32(0))

	return nil
}

// handleHeartbeat handles heartbeat requests
func (h *RequestHandler) handleHeartbeat(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read group ID
	groupID := readString(reader)

	// Read generation ID
	var generationID int32
	binary.Read(reader, binary.BigEndian, &generationID)

	// Read member ID
	memberID := readString(reader)

	// Send heartbeat
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	err := coordinator.Heartbeat(groupID, memberID)

	if err != nil {
		binary.Write(writer, binary.BigEndian, int16(1)) // Error code
	} else {
		binary.Write(writer, binary.BigEndian, int16(0)) // Success
	}

	return nil
}

// handleLeaveGroup handles leave group requests
func (h *RequestHandler) handleLeaveGroup(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// Read group ID
	groupID := readString(reader)

	// Read member ID
	memberID := readString(reader)

	// Leave the group
	coordinator := h.broker.GetProtocolConsumerGroupCoordinator()
	err := coordinator.LeaveGroup(groupID, memberID)

	if err != nil {
		binary.Write(writer, binary.BigEndian, int16(1)) // Error code
	} else {
		binary.Write(writer, binary.BigEndian, int16(0)) // Success
	}

	return nil
}

// Helper functions

func writeString(w io.Writer, s string) {
	binary.Write(w, binary.BigEndian, int16(len(s)))
	w.Write([]byte(s))
}

func readString(r io.Reader) string {
	var length int16
	binary.Read(r, binary.BigEndian, &length)
	if length <= 0 {
		return ""
	}
	buf := make([]byte, length)
	r.Read(buf)
	return string(buf)
}

func readInt32(r io.Reader) int32 {
	var val int32
	binary.Read(r, binary.BigEndian, &val)
	return val
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}