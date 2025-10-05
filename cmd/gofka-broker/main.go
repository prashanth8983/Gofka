package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/user/gofka/pkg/broker"
)

func main() {
	var nodeID string
	var addr string
	var logDir string
	var raftAddr string
	var raftDir string
	var peers string
	var bootstrap bool

	flag.StringVar(&nodeID, "node.id", "gofka-1", "Node ID")
	flag.StringVar(&addr, "addr", ":9092", "Broker address")
	flag.StringVar(&logDir, "log.dir", "/tmp/gofka/logs", "Log directory")
	flag.StringVar(&raftAddr, "raft.addr", ":19092", "Raft address")
	flag.StringVar(&raftDir, "raft.dir", "/tmp/gofka/raft", "Raft directory")
	flag.StringVar(&peers, "peers", "", "Comma-separated list of peer addresses")
	flag.BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the cluster")
	flag.Parse()

	fmt.Println("Gofka Broker starting...")
	fmt.Printf("Configuration: nodeID=%s, addr=%s, raftAddr=%s, bootstrap=%v, peers=%s\n",
		nodeID, addr, raftAddr, bootstrap, peers)

	// Create broker
	b, err := broker.NewBroker(nodeID, addr, logDir, raftAddr, raftDir)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// Parse peers and filter out empty strings
	var peerList []string
	if peers != "" {
		for _, peer := range strings.Split(peers, ",") {
			if strings.TrimSpace(peer) != "" {
				peerList = append(peerList, strings.TrimSpace(peer))
			}
		}
	}

	fmt.Printf("Starting broker with %d peers: %v\n", len(peerList), peerList)

	// Start broker
	if err := b.Start(bootstrap, peerList); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	fmt.Printf("Broker started on %s\n", addr)

	// Wait for shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		fmt.Printf("Received signal %v, shutting down...\n", sig)
	case <-ctx.Done():
		fmt.Println("Context cancelled, shutting down...")
	}

	// Stop broker
	if err := b.Stop(); err != nil {
		log.Printf("Error stopping broker: %v", err)
	}

	fmt.Println("Broker stopped")
}
