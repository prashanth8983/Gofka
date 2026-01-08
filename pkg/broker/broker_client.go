package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	gofkav1 "github.com/prashanth8983/gofka/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BrokerClient handles communication with other brokers
type BrokerClient struct {
	mu          sync.RWMutex
	connections map[string]*grpc.ClientConn
	clients     map[string]gofkav1.GofkaServiceClient
	timeout     time.Duration
}

// NewBrokerClient creates a new broker client
func NewBrokerClient() *BrokerClient {
	return &BrokerClient{
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]gofkav1.GofkaServiceClient),
		timeout:     30 * time.Second,
	}
}

// Connect establishes a connection to a broker
func (bc *BrokerClient) Connect(brokerAddr string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Check if already connected
	if _, exists := bc.connections[brokerAddr]; exists {
		return nil
	}

	// Parse address and add default port if needed
	host, port, err := net.SplitHostPort(brokerAddr)
	if err != nil {
		// No port specified, add default
		brokerAddr = net.JoinHostPort(brokerAddr, "9092")
		host = brokerAddr
	} else {
		brokerAddr = net.JoinHostPort(host, port)
	}

	// Create gRPC connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, brokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %w", brokerAddr, err)
	}

	bc.connections[brokerAddr] = conn
	bc.clients[brokerAddr] = gofkav1.NewGofkaServiceClient(conn)

	return nil
}

// Disconnect closes a connection to a broker
func (bc *BrokerClient) Disconnect(brokerAddr string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	conn, exists := bc.connections[brokerAddr]
	if !exists {
		return nil
	}

	err := conn.Close()
	delete(bc.connections, brokerAddr)
	delete(bc.clients, brokerAddr)

	return err
}

// Close closes all connections
func (bc *BrokerClient) Close() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	var lastErr error
	for addr, conn := range bc.connections {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
		delete(bc.connections, addr)
		delete(bc.clients, addr)
	}

	return lastErr
}

// FetchFromLeader fetches records from a leader broker
func (bc *BrokerClient) FetchFromLeader(brokerAddr, topic string, partition int32, fetchOffset, maxBytes int64, replicaID string) (*gofkav1.ReplicaFetchResponse, error) {
	bc.mu.RLock()
	client, exists := bc.clients[brokerAddr]
	bc.mu.RUnlock()

	if !exists {
		// Try to connect
		if err := bc.Connect(brokerAddr); err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %w", err)
		}
		bc.mu.RLock()
		client = bc.clients[brokerAddr]
		bc.mu.RUnlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), bc.timeout)
	defer cancel()

	req := &gofkav1.ReplicaFetchRequest{
		Topic:       topic,
		Partition:   partition,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
		ReplicaId:   replicaID,
	}

	resp, err := client.ReplicaFetch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replica fetch failed: %w", err)
	}

	return resp, nil
}

// SendHeartbeatToLeader sends a heartbeat to the leader
func (bc *BrokerClient) SendHeartbeatToLeader(brokerAddr, replicaID, topic string, partition int32, currentOffset int64) (*gofkav1.ReplicaHeartbeatResponse, error) {
	bc.mu.RLock()
	client, exists := bc.clients[brokerAddr]
	bc.mu.RUnlock()

	if !exists {
		// Try to connect
		if err := bc.Connect(brokerAddr); err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %w", err)
		}
		bc.mu.RLock()
		client = bc.clients[brokerAddr]
		bc.mu.RUnlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &gofkav1.ReplicaHeartbeatRequest{
		ReplicaId:     replicaID,
		Topic:         topic,
		Partition:     partition,
		CurrentOffset: currentOffset,
	}

	resp, err := client.ReplicaHeartbeat(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replica heartbeat failed: %w", err)
	}

	return resp, nil
}

// GetMetadata fetches metadata from a broker
func (bc *BrokerClient) GetMetadata(brokerAddr string, topics []string) (*gofkav1.MetadataResponse, error) {
	bc.mu.RLock()
	client, exists := bc.clients[brokerAddr]
	bc.mu.RUnlock()

	if !exists {
		// Try to connect
		if err := bc.Connect(brokerAddr); err != nil {
			return nil, fmt.Errorf("failed to connect to broker: %w", err)
		}
		bc.mu.RLock()
		client = bc.clients[brokerAddr]
		bc.mu.RUnlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), bc.timeout)
	defer cancel()

	req := &gofkav1.MetadataRequest{
		Topics: topics,
	}

	resp, err := client.GetMetadata(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("metadata fetch failed: %w", err)
	}

	return resp, nil
}
