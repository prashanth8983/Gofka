package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/prashanth8983/gofka/api/v1"
	"google.golang.org/protobuf/proto"
)

// Producer represents a Gofka producer client.
type Producer struct {
	mu            sync.RWMutex
	clusterClient *ClusterClient
	leaderConn    net.Conn
	leaderAddr    string
	ctx           context.Context
	cancel        context.CancelFunc

	// Configuration
	acks        int
	timeout     time.Duration
	retries     int
	compression string
}

// ProducerConfig holds configuration for a producer.
type ProducerConfig struct {
	Acks        int
	Timeout     time.Duration
	Retries     int
	Compression string
}

// NewProducer creates a new Producer instance.
func NewProducer(clusterClient *ClusterClient, config *ProducerConfig) *Producer {
	if config == nil {
		config = &ProducerConfig{
			Acks:        1,
			Timeout:     30 * time.Second,
			Retries:     3,
			Compression: "none",
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Producer{
		clusterClient: clusterClient,
		ctx:           ctx,
		cancel:        cancel,
		acks:          config.Acks,
		timeout:       config.Timeout,
		retries:       config.Retries,
		compression:   config.Compression,
	}
}

// Connect establishes a connection to the leader for a given topic and partition.
func (p *Producer) Connect(topic string, partition int32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If already connected to the leader, return
	if p.leaderConn != nil {
		return nil
	}

	// Get metadata for the topic
	metadataResp, err := p.clusterClient.GetMetadata([]string{topic})
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	// Find the topic and partition metadata
	var targetPartition *gofkav1.PartitionMetadata
	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			for _, part := range t.Partitions {
				if part.Id == partition {
					targetPartition = part
					break
				}
			}
			break
		}
	}

	if targetPartition == nil {
		return fmt.Errorf("topic %s or partition %d not found in metadata", topic, partition)
	}

	// Find the leader broker
	var leaderBroker *gofkav1.BrokerMetadata
	for _, b := range metadataResp.Brokers {
		if b.Id == targetPartition.Leader {
			leaderBroker = b
			break
		}
	}

	if leaderBroker == nil {
		return fmt.Errorf("leader broker %s not found in metadata", targetPartition.Leader)
	}

	// Connect to the leader broker
	conn, err := net.DialTimeout("tcp", leaderBroker.Addr, p.timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to leader broker %s: %w", leaderBroker.Addr, err)
	}

	p.leaderConn = conn
	p.leaderAddr = leaderBroker.Addr

	return nil
}

// Close closes the producer connection.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.leaderConn != nil {
		p.cancel()
		return p.leaderConn.Close()
	}
	return nil
}

// Produce sends a message to a Gofka topic.
func (p *Producer) Produce(topic string, message []byte) (int64, error) {
	// For simplicity, assume partition 0 for now.
	partition := int32(0)

	if err := p.Connect(topic, partition); err != nil {
		return 0, err
	}

	// Create the request
	req := &gofkav1.Request{
		Value: &gofkav1.Request_ProduceRequest{
			ProduceRequest: &gofkav1.ProduceRequest{
				Topic:    topic,
				Messages: [][]byte{message},
			},
		},
	}

	// Marshal the request
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Write the request length
	if err := binary.Write(p.leaderConn, binary.BigEndian, uint32(len(reqBuf))); err != nil {
		return 0, fmt.Errorf("failed to write request length: %w", err)
	}

	// Write the request
	if _, err := p.leaderConn.Write(reqBuf); err != nil {
		return 0, fmt.Errorf("failed to write request: %w", err)
	}

	// Read the response length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(p.leaderConn, lenBuf); err != nil {
		return 0, fmt.Errorf("failed to read response length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	// Read the response
	respBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(p.leaderConn, respBuf); err != nil {
		return 0, fmt.Errorf("failed to read response: %w", err)
	}

	// Unmarshal the response
	resp := &gofkav1.Response{}
	if err := proto.Unmarshal(respBuf, resp); err != nil {
		return 0, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Get the produce response
	produceResp, ok := resp.Value.(*gofkav1.Response_ProduceResponse)
	if !ok {
		return 0, fmt.Errorf("unexpected response type: %T", resp.Value)
	}

	return produceResp.ProduceResponse.Offset, nil
}
