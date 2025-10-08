package broker

import (
	"os"
	"testing"
	"time"
)

func TestNewBroker(t *testing.T) {
	addr := ":9092"
	logDir := "/tmp/gofka/test-logs-9092"
	raftAddr := "127.0.0.1:19092"
	raftDir := "/tmp/gofka/test-raft-9092"
	defer os.RemoveAll(logDir)
	defer os.RemoveAll(raftDir)
	broker, err := NewBroker("test-broker-1", addr, logDir, raftAddr, raftDir)

	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	if broker == nil {
		t.Fatal("Broker should not be nil")
	}

	if broker.addr != addr {
		t.Errorf("Expected addr %s, got %s", addr, broker.addr)
	}
}

func TestBrokerStartStop(t *testing.T) {
	addr := ":9093"
	logDir := "/tmp/gofka/test-logs-9093"
	raftAddr := "127.0.0.1:19093"
	raftDir := "/tmp/gofka/test-raft-9093"
	defer os.RemoveAll(logDir)
	defer os.RemoveAll(raftDir)
	broker, err := NewBroker("test-broker-2", addr, logDir, raftAddr, raftDir)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Test start
	if err := broker.Start(true, []string{raftAddr}); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}

	// Test stop
	if err := broker.Stop(); err != nil {
		t.Fatalf("Failed to stop broker: %v", err)
	}
}

func TestBrokerDoubleStart(t *testing.T) {
	addr := ":9094"
	logDir := "/tmp/gofka/test-logs-9094"
	raftAddr := "127.0.0.1:19094"
	raftDir := "/tmp/gofka/test-raft-9094"
	defer os.RemoveAll(logDir)
	defer os.RemoveAll(raftDir)
	broker, err := NewBroker("test-broker-3", addr, logDir, raftAddr, raftDir)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Start first time
	if err := broker.Start(true, []string{raftAddr}); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}

	// Try to start again
	if err := broker.Start(false, nil); err == nil {
		t.Error("Expected error when starting already running broker")
	}

	// Clean up
	broker.Stop()
}

func TestBrokerProduceConsume(t *testing.T) {
	addr := ":9095"
	logDir := "/tmp/gofka/test-logs-9095"
	raftAddr := "127.0.0.1:19095"
	raftDir := "/tmp/gofka/test-raft-9095"
	defer os.RemoveAll(logDir)
	defer os.RemoveAll(raftDir)
	
	broker, err := NewBroker("test-broker-4", addr, logDir, raftAddr, raftDir)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Start broker
	if err := broker.Start(true, []string{raftAddr}); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// Wait for broker to be ready
	time.Sleep(1 * time.Second)

	// Test message production
	topic := "test-topic"
	message := []byte("Hello, Gofka!")
	
	offset, err := broker.ProduceMessage(topic, message)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}
	
	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}

	// Test message consumption
	consumedMessage, err := broker.ConsumeMessage(topic, 0, offset)
	if err != nil {
		t.Fatalf("Failed to consume message: %v", err)
	}
	
	if string(consumedMessage) != string(message) {
		t.Errorf("Expected message %s, got %s", string(message), string(consumedMessage))
	}
}

func TestBrokerTopicCreation(t *testing.T) {
	addr := ":9096"
	logDir := "/tmp/gofka/test-logs-9096"
	raftAddr := "127.0.0.1:19096"
	raftDir := "/tmp/gofka/test-raft-9096"
	defer os.RemoveAll(logDir)
	defer os.RemoveAll(raftDir)
	
	broker, err := NewBroker("test-broker-5", addr, logDir, raftAddr, raftDir)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Start broker
	if err := broker.Start(true, []string{raftAddr}); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// Wait for broker to be ready
	time.Sleep(1 * time.Second)

	// Test topic creation
	topicName := "test-topic-creation"
	err = broker.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Verify topic exists
	topics := broker.GetTopics()
	found := false
	for _, topic := range topics {
		if topic == topicName {
			found = true
			break
		}
	}
	
	if !found {
		t.Errorf("Topic %s not found in topics list: %v", topicName, topics)
	}
}

func TestBrokerInvalidInputs(t *testing.T) {
	// Test with invalid addresses
	_, err := NewBroker("", ":9097", "/tmp/test", "127.0.0.1:19097", "/tmp/test")
	if err == nil {
		t.Error("Expected error for empty node ID")
	}

	// Test with invalid log directory
	_, err = NewBroker("test", ":9098", "", "127.0.0.1:19098", "/tmp/test")
	if err == nil {
		t.Error("Expected error for empty log directory")
	}
}
