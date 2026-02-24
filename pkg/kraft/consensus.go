package kraft

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/prashanth8983/gofka/pkg/logger"
	"github.com/prashanth8983/gofka/pkg/metadata"
	"go.uber.org/zap"
)

// Consensus manages the KRaft consensus protocol for metadata.
type Consensus struct {
	raft      *raft.Raft
	localID   raft.ServerID
	localAddr raft.ServerAddress
	config    *raft.Config
	transport raft.Transport
	snapshots raft.SnapshotStore
	logStore  raft.LogStore
	fsm       raft.FSM
	raftDir   string
}

// NewConsensus creates a new Consensus instance.
func NewConsensus(nodeID, raftAddr, raftDir string, fsm raft.FSM, cleanup bool) (*Consensus, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// Use shorter timeouts for faster single-node elections
	// Single-node clusters can elect themselves quickly
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 2000 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond
	config.MaxAppendEntries = 64
	config.SnapshotInterval = 120 * time.Second
	config.SnapshotThreshold = 8192

	// Leader lease timeout should be shorter for faster elections
	config.LeaderLeaseTimeout = 500 * time.Millisecond

	// Additional stability settings
	config.ShutdownOnRemove = true
	config.TrailingLogs = 10000

	// Raft internal logging level — WARN keeps it quiet in normal operation
	config.LogLevel = "WARN"

	// Create directories if they don't exist
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %w", err)
	}

	// If cleanup is requested, remove existing state before creating new stores
	if cleanup {
		if err := cleanupExistingState(raftDir); err != nil {
			return nil, fmt.Errorf("failed to cleanup existing state: %w", err)
		}
	}

	// Create snapshots subdirectory
	snapshotsDir := filepath.Join(raftDir, "snapshots")
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshots directory: %w", err)
	}

	// Create transport with proper bind and advertise addresses
	// For local development, use "127.0.0.1" + port for binding
	var localBindAddr string
	var advertiseAddr string

	if strings.HasPrefix(raftAddr, ":") {
		localBindAddr = "127.0.0.1" + raftAddr
		advertiseAddr = "127.0.0.1" + raftAddr
	} else if strings.Contains(raftAddr, ":") {
		parts := strings.Split(raftAddr, ":")
		if len(parts) == 2 {
			localBindAddr = "127.0.0.1:" + parts[1]
			advertiseAddr = "127.0.0.1:" + parts[1]
		} else {
			localBindAddr = "127.0.0.1:19092" // fallback
			advertiseAddr = "127.0.0.1:19092" // fallback
		}
	} else {
		localBindAddr = "127.0.0.1:19092" // fallback
		advertiseAddr = "127.0.0.1:19092" // fallback
	}

	logger.Debug("Raft transport config", zap.String("bind", localBindAddr), zap.String("advertise", advertiseAddr))

	// Create a new resolved address for the advertise address
	advertiseTCPAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertise address: %w", err)
	}

	transport, err := raft.NewTCPTransport(localBindAddr, advertiseTCPAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create separate stores for log and stable storage
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt stable store: %w", err)
	}

	// Create the Raft instance with separate log and stable stores
	raftInstance, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}

	return &Consensus{
		raft:      raftInstance,
		localID:   config.LocalID,
		localAddr: raft.ServerAddress(advertiseAddr),
		config:    config,
		transport: transport,
		snapshots: snapshots,
		logStore:  logStore,
		fsm:       fsm,
		raftDir:   raftDir,
	}, nil
}

// cleanupExistingState removes existing Raft state to ensure clean bootstrap
func cleanupExistingState(raftDir string) error {
	logger.Debug("Cleaning up existing Raft state")

	// Remove existing database files
	dbFiles := []string{
		filepath.Join(raftDir, "raft.db"),
		filepath.Join(raftDir, "stable.db"),
	}

	for _, dbFile := range dbFiles {
		if err := os.Remove(dbFile); err != nil && !os.IsNotExist(err) {
			logger.Warn("Could not remove Raft DB", zap.String("file", dbFile), zap.Error(err))
		}
	}

	// Clean existing snapshots but keep the directory
	snapshotDir := filepath.Join(raftDir, "snapshots")
	if err := os.RemoveAll(snapshotDir); err != nil && !os.IsNotExist(err) {
		logger.Warn("Could not remove snapshots directory", zap.Error(err))
	}
	// Recreate the snapshots directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Warn("Could not recreate snapshots directory", zap.Error(err))
	}

	return nil
}

// Start starts the consensus protocol.
func (c *Consensus) Start(bootstrap bool, peers []string) error {
	logger.Info("Starting consensus",
		zap.Bool("bootstrap", bootstrap),
		zap.Strings("peers", peers),
		zap.String("node_id", string(c.localID)),
		zap.String("addr", string(c.localAddr)),
	)

	if c.raft == nil {
		return fmt.Errorf("raft instance is nil, cannot start consensus")
	}

	logger.Debug("Current Raft state", zap.String("state", c.raft.State().String()))

	// Check if cluster is already properly configured
	config := c.raft.GetConfiguration()
	if config.Error() == nil {
		servers := config.Configuration().Servers
		logger.Debug("Current cluster configuration", zap.Int("server_count", len(servers)))

		// If we have servers, the cluster is already bootstrapped
		if len(servers) > 0 {
			// For single-node bootstrap, if we're the only server, we can proceed
			if bootstrap && len(servers) == 1 {
				onlyServer := servers[0]
				if onlyServer.ID == c.localID && onlyServer.Address == c.localAddr {
					logger.Debug("Single-node cluster detected, proceeding with bootstrap")
				} else {
					logger.Debug("Cluster has different server, skipping bootstrap")
					return nil
				}
			} else if !bootstrap {
				logger.Debug("Cluster already configured, not bootstrapping")
				return nil
			} else {
				logger.Debug("Cluster already has servers, not bootstrapping", zap.Int("server_count", len(servers)))
				return nil
			}
		} else {
			logger.Debug("Cluster not yet bootstrapped, proceeding")
		}
	} else {
		logger.Debug("Could not get cluster configuration, proceeding with bootstrap", zap.Error(config.Error()))
	}

	if bootstrap {
		// Wait a bit for any existing Raft operations to complete
		time.Sleep(500 * time.Millisecond)

		// For single-node bootstrap, add self to the configuration
		configuration := raft.Configuration{}

		// Always add self for single-node bootstrap
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.localID,
			Address: c.localAddr,
		})

		// If additional peers are provided, add them too (for multi-node clusters)
		addedAddresses := make(map[string]bool)
		addedAddresses[string(c.localAddr)] = true

		for _, peer := range peers {
			if peer == "" || peer == string(c.localAddr) || addedAddresses[peer] {
				continue
			}

			peerID := raft.ServerID(peer)
			peerAddr := raft.ServerAddress(peer)

			configuration.Servers = append(configuration.Servers, raft.Server{
				ID:      peerID,
				Address: peerAddr,
			})
			addedAddresses[peer] = true
			logger.Debug("Adding peer to cluster", zap.String("peer_id", peer))
		}

		logger.Info("Bootstrapping Raft cluster", zap.Int("server_count", len(configuration.Servers)))

		bootstrapFuture := c.raft.BootstrapCluster(configuration)
		if bootstrapFuture.Error() != nil {
			return fmt.Errorf("failed to bootstrap cluster: %w", bootstrapFuture.Error())
		}

		// Wait for bootstrap to complete
		time.Sleep(1000 * time.Millisecond)

		// Verify bootstrap was successful
		config = c.raft.GetConfiguration()
		if config.Error() != nil {
			return fmt.Errorf("failed to verify bootstrap: %w", config.Error())
		}

		logger.Info("Raft cluster bootstrapped successfully",
			zap.Int("server_count", len(config.Configuration().Servers)),
		)
	} else {
		// Try to join the existing cluster via peers
		if len(peers) > 0 {
			if err := c.joinCluster(peers); err != nil {
				logger.Warn("Failed to join cluster, starting in follower mode", zap.Error(err))
			}
		} else {
			logger.Info("No peers provided, starting in follower mode")
		}
	}

	// Wait for the Raft state to stabilize
	logger.Debug("Waiting for Raft state to stabilize")
	stableState := false
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		state := c.raft.State()

		// For single-node clusters, we expect to become leader
		if bootstrap && state == raft.Leader {
			stableState = true
			break
		}

		// For multi-node clusters, either leader or follower is acceptable
		if !bootstrap && (state == raft.Leader || state == raft.Follower) {
			stableState = true
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if !stableState {
		logger.Warn("Raft cluster did not stabilize within expected time")
	}

	// Final status
	finalState := c.raft.State()
	finalLeader := c.raft.Leader()
	logger.Info("Raft state settled",
		zap.String("state", finalState.String()),
		zap.String("leader", string(finalLeader)),
	)

	if bootstrap && finalState != raft.Leader {
		logger.Warn("Single-node cluster did not become leader", zap.String("state", finalState.String()))
	}

	return nil
}

// Apply applies a command to the state machine.
func (c *Consensus) Apply(command []byte) error {
	future := c.raft.Apply(command, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

// Raft returns the underlying Raft instance.
func (c *Consensus) Raft() *raft.Raft {
	return c.raft
}

// Close properly shuts down the consensus and cleans up resources.
func (c *Consensus) Close() error {
	if c.raft != nil {
		// Wait a bit for any pending operations to complete
		time.Sleep(100 * time.Millisecond)

		// Shutdown the Raft instance gracefully
		if err := c.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("failed to shutdown raft: %w", err)
		}

		// Wait a bit more for cleanup to complete
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// FSM is a finite state machine that applies commands to a key-value store.
type FSM struct {
	metadataStore *metadata.MetadataStore
}

// NewFSM creates a new FSM instance.
func NewFSM() *FSM {
	return &FSM{
		metadataStore: metadata.NewMetadataStore(),
	}
}

// Apply applies a command to the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd metadata.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		logger.Error("Failed to unmarshal FSM command", zap.Error(err))
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	if err := f.metadataStore.ApplyCommand(&cmd); err != nil {
		logger.Error("Failed to apply FSM command", zap.Error(err))
		return fmt.Errorf("failed to apply command: %w", err)
	}
	return nil
}

// Snapshot returns a snapshot of the FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.metadataStore.Snapshot()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	return f.metadataStore.Restore(data)
}

// MetadataStore returns the underlying metadata store.
func (f *FSM) MetadataStore() *metadata.MetadataStore {
	return f.metadataStore
}

// fsmSnapshot implements the raft.FSMSnapshot interface.
type fsmSnapshot struct {
	data []byte
}

// Persist persists the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(f.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release releases the snapshot.
func (f *fsmSnapshot) Release() {}

// joinCluster attempts to join an existing cluster by contacting peers
func (c *Consensus) joinCluster(peers []string) error {
	logger.Debug("Attempting to join cluster", zap.Int("peer_count", len(peers)))

	for _, peerRaftAddr := range peers {
		if peerRaftAddr == "" || peerRaftAddr == string(c.localAddr) {
			continue
		}

		brokerAddr := convertRaftToBrokerAddr(peerRaftAddr)
		if brokerAddr == "" {
			logger.Warn("Could not convert Raft address to broker address", zap.String("raft_addr", peerRaftAddr))
			continue
		}

		logger.Debug("Join request will be sent to broker", zap.String("broker_addr", brokerAddr))
		return nil // Return success, the broker will handle the actual join
	}

	return fmt.Errorf("could not find suitable peer to join cluster")
}

// convertRaftToBrokerAddr converts a Raft address to a broker address
// Convention: broker port = raft port - 10000
func convertRaftToBrokerAddr(raftAddr string) string {
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return ""
	}

	host := parts[0]
	raftPort := parts[1]

	var port int
	if _, err := fmt.Sscanf(raftPort, "%d", &port); err != nil {
		return ""
	}

	brokerPort := port - 10000
	if brokerPort <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", host, brokerPort)
}

// AddVoter adds a new voting member to the cluster (must be called on the leader)
func (c *Consensus) AddVoter(id raft.ServerID, address raft.ServerAddress) error {
	if c.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot add voter")
	}

	logger.Info("Adding voter to cluster", zap.String("id", string(id)), zap.String("addr", string(address)))

	future := c.raft.AddVoter(id, address, 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	logger.Info("Successfully added voter", zap.String("id", string(id)))
	return nil
}

// GetLeader returns the current leader address
func (c *Consensus) GetLeader() raft.ServerAddress {
	return c.raft.Leader()
}

// IsLeader returns true if this node is the leader
func (c *Consensus) IsLeader() bool {
	return c.raft.State() == raft.Leader
}
