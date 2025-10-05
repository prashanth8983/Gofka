package broker

import (
	"os"
	"testing"
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
