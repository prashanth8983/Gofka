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

	"github.com/user/gofka/pkg/controller"
)

func main() {
	var nodeID string
	var raftAddr string
	var httpAddr string
	var raftDir string
	var peers string
	var bootstrap bool

	flag.StringVar(&nodeID, "node.id", "gofka-controller-1", "Controller node ID")
	flag.StringVar(&raftAddr, "raft.addr", ":19093", "Raft address")
	flag.StringVar(&httpAddr, "http.addr", ":19094", "HTTP address for leader checks")
	flag.StringVar(&raftDir, "raft.dir", "/tmp/gofka/controller-raft", "Raft directory")
	flag.StringVar(&peers, "peers", "", "Comma-separated list of peer addresses")
	flag.BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the cluster")
	flag.Parse()

	fmt.Println("Gofka Controller starting...")

	controller, err := controller.NewController(nodeID, raftAddr, httpAddr, raftDir, bootstrap)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	if err := controller.Start(bootstrap, strings.Split(peers, ",")); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	fmt.Printf("Controller started on %s with Raft address %s, HTTP address %s\n", nodeID, raftAddr, httpAddr)

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

	if err := controller.Stop(); err != nil {
		log.Printf("Error stopping controller: %v", err)
	}

	fmt.Println("Controller stopped")
}
