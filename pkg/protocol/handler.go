package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
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
	fmt.Println("RequestHandler.HandleRequest starting")

	// Read the message size (already consumed by server)
	var messageSize int32
	if err := binary.Read(reader, binary.BigEndian, &messageSize); err != nil {
		fmt.Printf("Failed to read message size: %v\n", err)
		return fmt.Errorf("failed to read message size: %w", err)
	}
	fmt.Printf("Read message size: %d\n", messageSize)

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

	fmt.Printf("Handling request: API=%d, Version=%d, Correlation=%d, ClientID=%s\n",
		apiKey, apiVersion, correlationID, string(clientID))

	// Handle the request based on API key
	var response bytes.Buffer

	// Write correlation ID first
	if err := binary.Write(&response, binary.BigEndian, correlationID); err != nil {
		return fmt.Errorf("failed to write correlation ID: %w", err)
	}

	switch apiKey {
	case APIKeyMetadata:
		if err := h.handleMetadata(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling metadata: %v\n", err)
			return err
		}
	case APIKeyProduce:
		if err := h.handleProduce(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling produce: %v\n", err)
			return err
		}
	case APIKeyFetch:
		if err := h.handleFetch(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling fetch: %v\n", err)
			return err
		}
	case APIKeyListOffsets:
		if err := h.handleListOffsets(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling list offsets: %v\n", err)
			return err
		}
	case APIKeyCreateTopics:
		if err := h.handleCreateTopics(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling create topics: %v\n", err)
			return err
		}
	case APIKeyOffsetCommit:
		if err := h.handleOffsetCommit(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling offset commit: %v\n", err)
			return err
		}
	case APIKeyOffsetFetch:
		if err := h.handleOffsetFetch(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling offset fetch: %v\n", err)
			return err
		}
	case APIKeyJoinGroup:
		if err := h.handleJoinGroup(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling join group: %v\n", err)
			return err
		}
	case APIKeyHeartbeat:
		if err := h.handleHeartbeat(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling heartbeat: %v\n", err)
			return err
		}
	case APIKeyLeaveGroup:
		if err := h.handleLeaveGroup(reader, &response, apiVersion); err != nil {
			fmt.Printf("Error handling leave group: %v\n", err)
			return err
		}
	default:
		fmt.Printf("Unsupported API key: %d\n", apiKey)
		// Write error response
		binary.Write(&response, binary.BigEndian, int16(-1)) // Error code
		writeString(&response, fmt.Sprintf("Unsupported API key: %d", apiKey))
	}

	// Write the complete response with size prefix
	responseBytes := response.Bytes()
	fmt.Printf("Response content: %d bytes, first bytes: %x\n", len(responseBytes), responseBytes[:min(20, len(responseBytes))])

	if err := binary.Write(writer, binary.BigEndian, int32(len(responseBytes))); err != nil {
		return fmt.Errorf("failed to write response size: %w", err)
	}

	if _, err := writer.Write(responseBytes); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	fmt.Printf("Wrote response with size prefix: total %d bytes (4 size + %d content)\n", 4+len(responseBytes), len(responseBytes))
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

	fmt.Printf("Sent metadata response with %d brokers and %d topics\n", len(brokers), len(topics))
	return nil
}

// handleProduce handles produce requests
func (h *RequestHandler) handleProduce(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// For now, return a simple acknowledgment
	// TODO: Implement full produce handling
	binary.Write(writer, binary.BigEndian, int32(0)) // Number of responses
	return nil
}

// handleFetch handles fetch requests
func (h *RequestHandler) handleFetch(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// For now, return empty messages
	// TODO: Implement full fetch handling
	binary.Write(writer, binary.BigEndian, int32(0)) // Number of responses
	return nil
}

// handleListOffsets handles list offsets requests
func (h *RequestHandler) handleListOffsets(reader io.Reader, writer io.Writer, apiVersion int16) error {
	// For now, return default offsets
	// TODO: Implement full list offsets handling
	binary.Write(writer, binary.BigEndian, int32(0)) // Number of responses
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