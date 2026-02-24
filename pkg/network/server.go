package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	gofkav1 "github.com/prashanth8983/gofka/api/v1"
	"github.com/prashanth8983/gofka/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// apiKeyName maps API key numbers to human-readable names
var apiKeyName = map[uint16]string{
	0: "Produce", 1: "Fetch", 3: "Metadata",
	8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator",
	11: "JoinGroup", 12: "Heartbeat", 13: "LeaveGroup",
	14: "SyncGroup", 15: "DescribeGroups", 16: "ListGroups",
}

// BinaryProtocolHandler handles binary protocol requests
type BinaryProtocolHandler interface {
	HandleRequest(reader io.Reader, writer io.Writer) error
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Broker interface for message handling
type Broker interface {
	ProduceMessage(topic string, message []byte) (int64, error)
	ConsumeMessage(topic string, partition int32, offset int64) ([]byte, error)
	GetTopics() []string
	GetBrokers() []BrokerMetadata
	GetPartitions(topicName string) []PartitionMetadata
	CreateTopic(name string, partitions int32) error
	GetBinaryProtocolHandler() BinaryProtocolHandler
}

// BrokerMetadata represents broker metadata for the interface
type BrokerMetadata struct {
	ID   string
	Addr string
}

// PartitionMetadata represents partition metadata for the interface
type PartitionMetadata struct {
	ID                int32
	Leader            string
	Replicas          []string
	NumPartitions     int32
	ReplicationFactor int32
}

// Server handles network connections and message routing
type Server struct {
	broker   Broker
	port     int
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewServer creates a new network server
func NewServer(broker Broker, port int) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		broker: broker,
		port:   port,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the server.
func (s *Server) Start() error {
	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	s.listener = listener

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					continue
				}
			}

			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.wg.Done()
	}()

	// Handle connection without peeking - directly process messages
	for {
		// Read message length (4 bytes)
		lenBuf := make([]byte, 4)
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err == io.EOF {
				return // Connection closed
			}
			// Ignore timeouts and continue
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		msgLen := binary.BigEndian.Uint32(lenBuf)

		// Validate message length
		if msgLen == 0 || msgLen > 100*1024*1024 { // Max 100MB
			logger.Warn("Invalid message length", zap.Uint32("length", msgLen))
			return
		}

		// Read the complete message
		msgBuf := make([]byte, msgLen)
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			logger.Debug("Failed to read message", zap.Error(err))
			return
		}

		// Process the message
		s.processGofkaMessage(conn, msgBuf)
	}
}

// isKafkaProtocol detects if the message follows Kafka protocol
func (s *Server) isKafkaProtocol(peekBuf []byte) bool {
	// Kafka protocol: first 4 bytes are message size (big endian)
	// The size should be reasonable (not too large)
	messageSize := binary.BigEndian.Uint32(peekBuf[:4])
	return messageSize > 0 && messageSize < 100*1024*1024 // Max 100MB
}

// handleKafkaConnection handles Kafka protocol connections
func (s *Server) handleKafkaConnection(conn net.Conn, peekBuf []byte) {
	logger.Debug("Handling Kafka protocol connection", zap.String("remote", conn.RemoteAddr().String()))

	// Create a buffer that includes the peeked bytes
	buf := make([]byte, 0, 1024)
	buf = append(buf, peekBuf...)

	// Read any remaining data
	tempBuf := make([]byte, 1024)
	for {
		n, err := conn.Read(tempBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Debug("Error reading Kafka request", zap.Error(err))
			return
		}
		buf = append(buf, tempBuf[:n]...)

		// Check if we have a complete message
		if len(buf) >= 4 {
			messageSize := int(binary.BigEndian.Uint32(buf[:4]))
			if len(buf) >= messageSize+4 {
				break // We have a complete message
			}
		}
	}

	// For now, just acknowledge that we received a Kafka protocol message
	// TODO: Implement full Kafka protocol handling
	logger.Debug("Received Kafka protocol message", zap.Int("size", len(buf)))

	// Send a simple acknowledgment
	response := fmt.Sprintf("Kafka protocol message received (%d bytes)\n", len(buf))
	conn.Write([]byte(response))
}

// Removed handleGofkaConnection - no longer needed after simplification

func (s *Server) processGofkaMessage(conn net.Conn, msgBuf []byte) {
	// Try binary protocol first (for Python/Go clients)
	if s.tryBinaryProtocol(conn, msgBuf) {
		return
	}

	// Fall back to protobuf protocol
	req := &gofkav1.Request{}
	if err := proto.Unmarshal(msgBuf, req); err != nil {
		logger.Warn("Failed to unmarshal protobuf request", zap.Error(err))
		s.sendErrorResponse(conn, "Invalid request format")
		return
	}

	// Handle the request
	resp, err := s.handleRequest(req)
	if err != nil {
		logger.Error("Failed to handle request", zap.Error(err))
		s.sendErrorResponse(conn, fmt.Sprintf("Request handling error: %v", err))
		return
	}

	// Marshal the response
	respBuf, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("Failed to marshal response", zap.Error(err))
		s.sendErrorResponse(conn, "Response serialization error")
		return
	}

	// Set write timeout
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	// Write the response length
	if err := binary.Write(conn, binary.BigEndian, uint32(len(respBuf))); err != nil {
		logger.Debug("Failed to write response length", zap.Error(err))
		return
	}

	// Write the response
	if _, err := conn.Write(respBuf); err != nil {
		logger.Debug("Failed to write response", zap.Error(err))
		return
	}
}

// tryBinaryProtocol attempts to handle the message using binary protocol handlers
func (s *Server) tryBinaryProtocol(conn net.Conn, msgBuf []byte) bool {
	// Check if this looks like a binary protocol message
	// Binary protocol starts with: api_key (2 bytes) + api_version (2 bytes)
	if len(msgBuf) < 4 {
		return false
	}

	// Check if API key is in valid range (0-50 to be safe)
	apiKey := binary.BigEndian.Uint16(msgBuf[0:2])
	if apiKey > 50 {
		return false // Not a valid API key, probably protobuf
	}

	// Get binary protocol handler from broker
	handler := s.broker.GetBinaryProtocolHandler()
	if handler == nil {
		return false
	}

	name := apiKeyName[apiKey]
	if name == "" {
		name = fmt.Sprintf("Unknown(%d)", apiKey)
	}
	logger.Debug("Request", zap.String("api", name), zap.Int("size", len(msgBuf)))

	// The protocol handler expects to read the size first, but we already consumed it.
	// Prepend the size to msgBuf so the handler can read it.
	fullMsg := make([]byte, 4+len(msgBuf))
	binary.BigEndian.PutUint32(fullMsg[0:4], uint32(len(msgBuf)))
	copy(fullMsg[4:], msgBuf)

	reader := bytes.NewReader(fullMsg)
	writer := &bytes.Buffer{}

	if err := handler.HandleRequest(reader, writer); err != nil {
		logger.Error("Binary protocol handler error", zap.String("api", name), zap.Error(err))
		conn.Close()
		return true
	}

	// Write response back to connection
	responseBytes := writer.Bytes()

	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Write(responseBytes); err != nil {
		logger.Warn("Failed to write response", zap.String("api", name), zap.Error(err))
	}

	return true
}

func (s *Server) sendErrorResponse(conn net.Conn, message string) {
	logger.Warn("Sending error response", zap.String("message", message))
	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	conn.Close()
}

func (s *Server) handleRequest(req *gofkav1.Request) (*gofkav1.Response, error) {
	switch r := req.Value.(type) {
	case *gofkav1.Request_ProduceRequest:
		return s.handleProduceRequest(r.ProduceRequest)
	case *gofkav1.Request_ConsumeRequest:
		return s.handleConsumeRequest(r.ConsumeRequest)
	case *gofkav1.Request_MetadataRequest:
		return s.handleMetadataRequest(r.MetadataRequest)
	case *gofkav1.Request_CreateTopicRequest:
		return s.handleCreateTopicRequest(r.CreateTopicRequest)
	default:
		return nil, fmt.Errorf("unknown request type: %T", r)
	}
}

func (s *Server) handleCreateTopicRequest(req *gofkav1.CreateTopicRequest) (*gofkav1.Response, error) {
	err := s.broker.CreateTopic(req.TopicName, req.NumPartitions)
	if err != nil {
		return &gofkav1.Response{
			Value: &gofkav1.Response_CreateTopicResponse{
				CreateTopicResponse: &gofkav1.CreateTopicResponse{
					Success: false,
					Message: err.Error(),
				},
			},
		}, nil
	}

	return &gofkav1.Response{
		Value: &gofkav1.Response_CreateTopicResponse{
			CreateTopicResponse: &gofkav1.CreateTopicResponse{
				Success: true,
				Message: fmt.Sprintf("Topic %s created successfully", req.TopicName),
			},
		},
	}, nil
}

func (s *Server) handleMetadataRequest(req *gofkav1.MetadataRequest) (*gofkav1.Response, error) {
	// Get brokers from the broker interface
	brokerMeta := s.broker.GetBrokers()
	brokers := make([]*gofkav1.BrokerMetadata, len(brokerMeta))
	for i, broker := range brokerMeta {
		brokers[i] = &gofkav1.BrokerMetadata{
			Id:   broker.ID,
			Addr: broker.Addr,
		}
	}

	// Get topics and their partitions
	topics := []*gofkav1.TopicMetadata{}
	for _, topicName := range s.broker.GetTopics() {
		partitionMeta := s.broker.GetPartitions(topicName)
		partitions := make([]*gofkav1.PartitionMetadata, len(partitionMeta))

		var numPartitions, replicationFactor int32 = 1, 1
		if len(partitionMeta) > 0 {
			numPartitions = partitionMeta[0].NumPartitions
			replicationFactor = partitionMeta[0].ReplicationFactor
		}

		for i, partition := range partitionMeta {
			partitions[i] = &gofkav1.PartitionMetadata{
				Id:       partition.ID,
				Leader:   partition.Leader,
				Replicas: partition.Replicas,
			}
		}

		topics = append(topics, &gofkav1.TopicMetadata{
			Name:              topicName,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			Partitions:        partitions,
		})
	}

	return &gofkav1.Response{
		Value: &gofkav1.Response_MetadataResponse{
			MetadataResponse: &gofkav1.MetadataResponse{
				Brokers: brokers,
				Topics:  topics,
			},
		},
	}, nil
}

func (s *Server) handleProduceRequest(req *gofkav1.ProduceRequest) (*gofkav1.Response, error) {
	var lastOffset int64
	for _, msg := range req.Messages {
		// msg is already []byte, so we can pass it directly
		offset, err := s.broker.ProduceMessage(req.Topic, msg)
		if err != nil {
			return nil, fmt.Errorf("failed to produce message: %w", err)
		}
		lastOffset = offset
	}

	return &gofkav1.Response{
		Value: &gofkav1.Response_ProduceResponse{
			ProduceResponse: &gofkav1.ProduceResponse{
				Offset: lastOffset,
			},
		},
	}, nil
}

func (s *Server) handleConsumeRequest(req *gofkav1.ConsumeRequest) (*gofkav1.Response, error) {
	// The original code had ConsumeMessage on the broker interface.
	// The new Broker interface has ConsumeMessage(topic string, partition int32, offset int64) ([]byte, error).
	// Assuming the topic name is part of the request and partition is 0.
	// For now, we'll pass the topic name and partition as 0.
	msg, err := s.broker.ConsumeMessage(req.Topic, req.Partition, req.Offset)
	if err != nil {
		errStr := err.Error()
		// Handle offset out of range gracefully - return empty messages instead of error
		if strings.Contains(errStr, "out of range") || strings.Contains(errStr, "not found in any segment") {
			return &gofkav1.Response{
				Value: &gofkav1.Response_ConsumeResponse{
					ConsumeResponse: &gofkav1.ConsumeResponse{
						Messages: [][]byte{}, // Return empty messages when offset doesn't exist yet
					},
				},
			}, nil
		}
		return nil, fmt.Errorf("failed to consume message: %w", err)
	}

	return &gofkav1.Response{
		Value: &gofkav1.Response_ConsumeResponse{
			ConsumeResponse: &gofkav1.ConsumeResponse{
				Messages: [][]byte{msg},
			},
		},
	}, nil
}
