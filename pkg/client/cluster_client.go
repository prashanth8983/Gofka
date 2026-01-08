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

// ClusterClient is a client for interacting with the Gofka cluster to fetch metadata.
type ClusterClient struct {
	mu     sync.RWMutex
	conn   net.Conn
	addr   string
	ctx    context.Context
	cancel context.CancelFunc

	// Configuration
	timeout time.Duration
}

// NewClusterClient creates a new ClusterClient instance.
func NewClusterClient(addr string, timeout time.Duration) *ClusterClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &ClusterClient{
		addr:    addr,
		ctx:     ctx,
		cancel:  cancel,
		timeout: timeout,
	}
}

// Connect establishes a connection to a broker.
func (c *ClusterClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}

	c.conn = conn
	return nil
}

// Close closes the client connection.
func (c *ClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.cancel()
		return c.conn.Close()
	}
	return nil
}

// GetMetadata fetches cluster metadata.
func (c *ClusterClient) GetMetadata(topics []string) (*gofkav1.MetadataResponse, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	req := &gofkav1.Request{
		Value: &gofkav1.Request_MetadataRequest{
			MetadataRequest: &gofkav1.MetadataRequest{
				Topics: topics,
			},
		},
	}

	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(reqBuf))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	if _, err := c.conn.Write(reqBuf); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	respBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	resp := &gofkav1.Response{}
	if err := proto.Unmarshal(respBuf, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	metadataResp, ok := resp.Value.(*gofkav1.Response_MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", resp.Value)
	}

	return metadataResp.MetadataResponse, nil
}

// CreateTopic sends a CreateTopicRequest to the broker.
func (c *ClusterClient) CreateTopic(topicName string, numPartitions, replicationFactor int32) (*gofkav1.CreateTopicResponse, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	req := &gofkav1.Request{
		Value: &gofkav1.Request_CreateTopicRequest{
			CreateTopicRequest: &gofkav1.CreateTopicRequest{
				TopicName:         topicName,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			},
		},
	}

	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(reqBuf))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	if _, err := c.conn.Write(reqBuf); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	respBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	resp := &gofkav1.Response{}
	if err := proto.Unmarshal(respBuf, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	createTopicResp, ok := resp.Value.(*gofkav1.Response_CreateTopicResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", resp.Value)
	}

	return createTopicResp.CreateTopicResponse, nil
}
