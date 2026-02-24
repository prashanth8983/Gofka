package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prashanth8983/gofka/pkg/broker"
	"github.com/prashanth8983/gofka/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	var nodeID string
	var addr string
	var logDir string
	var raftAddr string
	var raftDir string
	var peers string
	var bootstrap bool
	var logLevel string
	var logEncoding string

	flag.StringVar(&nodeID, "node.id", "gofka-1", "Node ID")
	flag.StringVar(&addr, "addr", ":9092", "Broker address")
	flag.StringVar(&logDir, "log.dir", "/tmp/gofka/logs", "Log directory")
	flag.StringVar(&raftAddr, "raft.addr", ":19092", "Raft address")
	flag.StringVar(&raftDir, "raft.dir", "/tmp/gofka/raft", "Raft directory")
	flag.StringVar(&peers, "peers", "", "Comma-separated list of peer addresses")
	flag.BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the cluster")
	flag.StringVar(&logLevel, "log.level", "info", "Log level: debug, info, warn, error")
	flag.StringVar(&logEncoding, "log.encoding", "console", "Log encoding: json, console")
	flag.Parse()

	// Initialize logger before anything else so all components use the right level
	logger.Init(logger.Config{
		Level:      logLevel,
		Encoding:   logEncoding,
		OutputPath: "stdout",
		Component:  fmt.Sprintf("broker-%s", nodeID),
	})

	logger.Info("Gofka Broker starting",
		zap.String("node_id", nodeID),
		zap.String("addr", addr),
		zap.String("raft_addr", raftAddr),
		zap.Bool("bootstrap", bootstrap),
		zap.String("log_level", logLevel),
	)

	// Create broker
	b, err := broker.NewBroker(nodeID, addr, logDir, raftAddr, raftDir)
	if err != nil {
		logger.Fatal("Failed to create broker", zap.Error(err))
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

	// Start broker
	if err := b.Start(bootstrap, peerList); err != nil {
		logger.Fatal("Failed to start broker", zap.Error(err))
	}

	logger.Info("Broker started", zap.String("addr", addr))

	// Wait for shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case <-ctx.Done():
		logger.Info("Context cancelled, shutting down")
	}

	// Stop broker
	if err := b.Stop(); err != nil {
		logger.Error("Error stopping broker", zap.Error(err))
	}

	logger.Info("Broker stopped")
}
