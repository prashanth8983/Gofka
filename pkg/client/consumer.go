package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/user/gofka/api/v1"
	"google.golang.org/protobuf/proto"
)

// Consumer represents a Gofka consumer client.
type Consumer struct {
	mu            sync.RWMutex
	clusterClient *ClusterClient
	leaderConn    net.Conn
	leaderAddr    string
	ctx           context.Context
	cancel        context.CancelFunc

	// Consumer group
	groupID   string
	topic     string
	partition int32

	// Offset management
	offset     int64
	autoCommit bool

	// Configuration
	timeout     time.Duration
	batchSize   int
	PollTimeout time.Duration
}

// ConsumerConfig holds configuration for a consumer.
type ConsumerConfig struct {
	GroupID     string
	Topic       string
	Partition   int32
	AutoCommit  bool
	Timeout     time.Duration
	BatchSize   int
	PollTimeout time.Duration
}

// NewConsumer creates a new Consumer instance.
func NewConsumer(clusterClient *ClusterClient, config *ConsumerConfig) *Consumer {
	if config == nil {
		config = &ConsumerConfig{
			GroupID:     "default-group",
			Topic:       "",
			Partition:   0,
			AutoCommit:  true,
			Timeout:     30 * time.Second,
			BatchSize:   100,
			PollTimeout: 100 * time.Millisecond,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		clusterClient: clusterClient,
		ctx:           ctx,
		cancel:        cancel,
		groupID:       config.GroupID,
		topic:         config.Topic,
		partition:     config.Partition,
		autoCommit:    config.AutoCommit,
		timeout:       config.Timeout,
		batchSize:     config.BatchSize,
		PollTimeout:   config.PollTimeout,
	}
}

// Connect establishes a connection to the leader for a given topic and partition.
func (c *Consumer) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already connected to the leader, return
	if c.leaderConn != nil {
		return nil
	}

	// Get metadata for the topic
	metadataResp, err := c.clusterClient.GetMetadata([]string{c.topic})
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	// Find the topic and partition metadata
	var targetPartition *gofkav1.PartitionMetadata
	for _, t := range metadataResp.Topics {
		if t.Name == c.topic {
			for _, part := range t.Partitions {
				if part.Id == c.partition {
					targetPartition = part
					break
				}
			}
			break
		}
	}

	if targetPartition == nil {
		return fmt.Errorf("topic %s or partition %d not found in metadata", c.topic, c.partition)
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
	conn, err := net.DialTimeout("tcp", leaderBroker.Addr, c.timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to leader broker %s: %w", leaderBroker.Addr, err)
	}

	c.leaderConn = conn
	c.leaderAddr = leaderBroker.Addr

	return nil
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leaderConn != nil {
		c.cancel()
		return c.leaderConn.Close()
	}
	return nil
}

// Subscribe subscribes to a topic.
func (c *Consumer) Subscribe(topic string) error {
	c.topic = topic
	// TODO: Implement subscription logic
	return nil
}

// Consume starts consuming messages.
func (c *Consumer) Consume() ([][]byte, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	// Create the request
	req := &gofkav1.Request{
		Value: &gofkav1.Request_ConsumeRequest{
			ConsumeRequest: &gofkav1.ConsumeRequest{
				Topic:     c.topic,
				Partition: c.partition,
				Offset:    c.offset,
			},
		},
	}

	// Marshal the request
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Write the request length
	if err := binary.Write(c.leaderConn, binary.BigEndian, uint32(len(reqBuf))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	// Write the request
	if _, err := c.leaderConn.Write(reqBuf); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	// Read the response length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.leaderConn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	// Read the response
	respBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(c.leaderConn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Unmarshal the response
	resp := &gofkav1.Response{}
	if err := proto.Unmarshal(respBuf, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Get the consume response
	consumeResp, ok := resp.Value.(*gofkav1.Response_ConsumeResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", resp.Value)
	}

	// Update the offset
	c.offset += int64(len(consumeResp.ConsumeResponse.Messages))

	return consumeResp.ConsumeResponse.Messages, nil
}

// CommitOffset commits the current offset.
func (c *Consumer) CommitOffset(offset int64) error {
	c.offset = offset
	return nil
}
