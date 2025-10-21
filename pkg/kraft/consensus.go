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
	"github.com/user/gofka/pkg/metadata"
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

	// Enable debug logging by setting log level
	config.LogLevel = "DEBUG"

	// For single-node clusters, we'll handle quorum in the bootstrap logic

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

	fmt.Printf("Raft transport: bind=%s, advertise=%s\n", localBindAddr, advertiseAddr)

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
	fmt.Println("Cleaning up existing Raft state...")

	// Remove existing database files
	dbFiles := []string{
		filepath.Join(raftDir, "raft.db"),
		filepath.Join(raftDir, "stable.db"),
	}

	for _, dbFile := range dbFiles {
		if err := os.Remove(dbFile); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: could not remove %s: %v\n", dbFile, err)
		} else if err == nil {
			fmt.Printf("Removed existing database file: %s\n", dbFile)
		}
	}

	// Clean existing snapshots but keep the directory
	snapshotDir := filepath.Join(raftDir, "snapshots")
	if err := os.RemoveAll(snapshotDir); err != nil && !os.IsNotExist(err) {
		fmt.Printf("Warning: could not remove snapshots directory: %v\n", err)
	} else if err == nil {
		fmt.Printf("Removed existing snapshots directory contents\n")
	}
	// Recreate the snapshots directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		fmt.Printf("Warning: could not recreate snapshots directory: %v\n", err)
	}

	return nil
}

// Start starts the consensus protocol.
func (c *Consensus) Start(bootstrap bool, peers []string) error {
	fmt.Printf("Consensus.Start called with bootstrap=%v, peers=%v\n", bootstrap, peers)
	fmt.Printf("Local node: ID=%s, Address=%s\n", c.localID, c.localAddr)

	if c.raft == nil {
		return fmt.Errorf("raft instance is nil, cannot start consensus")
	}

	fmt.Printf("Current Raft state: %s\n", c.raft.State())

	// Check if cluster is already properly configured
	config := c.raft.GetConfiguration()
	if config.Error() == nil {
		servers := config.Configuration().Servers
		fmt.Printf("Current cluster configuration has %d servers: %+v\n", len(servers), servers)

		// If we have servers, the cluster is already bootstrapped
		if len(servers) > 0 {
			fmt.Printf("Cluster already bootstrapped with %d servers, checking if bootstrap is still needed\n", len(servers))

			// For single-node bootstrap, if we're the only server, we can proceed
			if bootstrap && len(servers) == 1 {
				// Check if we're the only server and we're trying to bootstrap
				onlyServer := servers[0]
				if onlyServer.ID == c.localID && onlyServer.Address == c.localAddr {
					fmt.Printf("Single-node cluster detected, proceeding with bootstrap\n")
				} else {
					fmt.Printf("Cluster has different server, skipping bootstrap\n")
					return nil
				}
			} else if !bootstrap {
				fmt.Printf("Cluster already configured, not bootstrapping\n")
				return nil
			} else {
				fmt.Printf("Cluster has %d servers, not bootstrapping\n", len(servers))
				return nil
			}
		} else {
			fmt.Printf("Cluster not yet bootstrapped (no servers), proceeding with bootstrap\n")
		}
	} else {
		fmt.Printf("Could not get cluster configuration: %v, proceeding with bootstrap\n", config.Error())
	}

	if bootstrap {
		// For single-node bootstrap, we need to ensure clean state
		fmt.Println("Preparing for single-node bootstrap...")

		// Wait a bit for any existing Raft operations to complete
		time.Sleep(500 * time.Millisecond)

		// For single-node bootstrap, add self to the configuration
		configuration := raft.Configuration{}

		// Always add self for single-node bootstrap
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.localID,
			Address: c.localAddr,
		})
		fmt.Printf("Bootstrapping single-node cluster with ID: %s, Address: %s\n", c.localID, c.localAddr)

		// If additional peers are provided, add them too (for multi-node clusters)
		// But avoid adding duplicates
		addedAddresses := make(map[string]bool)
		addedAddresses[string(c.localAddr)] = true

		for _, peer := range peers {
			if peer == "" {
				continue
			}

			// Skip if this peer address is the same as our own
			if peer == string(c.localAddr) {
				fmt.Printf("Skipping duplicate peer address (same as local): %s\n", peer)
				continue
			}

			// Skip if we already added this address
			if addedAddresses[peer] {
				fmt.Printf("Skipping duplicate peer address: %s\n", peer)
				continue
			}

			// For now, use the peer address as both ID and address
			// In a real implementation, you'd want to separate node IDs from addresses
			peerID := raft.ServerID(peer)
			peerAddr := raft.ServerAddress(peer)

			configuration.Servers = append(configuration.Servers, raft.Server{
				ID:      peerID,
				Address: peerAddr,
			})
			addedAddresses[peer] = true
			fmt.Printf("Adding peer to cluster: ID: %s, Address: %s\n", peerID, peerAddr)
		}

		fmt.Printf("Bootstrapping Raft cluster with %d servers\n", len(configuration.Servers))
		fmt.Printf("Configuration: %+v\n", configuration)

		// Bootstrap the cluster
		fmt.Println("Bootstrapping Raft cluster...")
		bootstrapFuture := c.raft.BootstrapCluster(configuration)
		if bootstrapFuture.Error() != nil {
			fmt.Printf("Bootstrap error: %v\n", bootstrapFuture.Error())
			return fmt.Errorf("failed to bootstrap cluster: %w", bootstrapFuture.Error())
		}

		// Wait for bootstrap to complete
		fmt.Println("Waiting for bootstrap to complete...")
		time.Sleep(1000 * time.Millisecond)

		// Verify bootstrap was successful
		config = c.raft.GetConfiguration()
		if config.Error() != nil {
			return fmt.Errorf("failed to verify bootstrap: %w", config.Error())
		}

		bootstrapServers := config.Configuration().Servers
		fmt.Printf("Bootstrap verification: cluster now has %d servers\n", len(bootstrapServers))
		fmt.Println("Raft cluster bootstrapped successfully")
	} else {
		fmt.Println("Bootstrap not requested, attempting to join existing cluster")

		// Try to join the existing cluster via peers
		if len(peers) > 0 {
			if err := c.joinCluster(peers); err != nil {
				fmt.Printf("Warning: Failed to join cluster: %v\n", err)
				fmt.Println("Will start in follower mode and wait for cluster discovery")
			}
		} else {
			fmt.Println("No peers provided, will start in follower mode")
		}
	}

	// Wait for the Raft state to stabilize and provide status updates
	fmt.Println("Waiting for Raft state to stabilize...")
	stableState := false
	for i := 0; i < 50; i++ { // Wait up to 5 seconds with more conservative timing
		state := c.raft.State()
		leader := c.raft.Leader()
		fmt.Printf("Raft state after %d*100ms: %s, Leader: %s\n", i+1, state, leader)

		// For single-node clusters, we expect to become leader
		if bootstrap && state == raft.Leader {
			fmt.Printf("Raft cluster stabilized in Leader state\n")
			stableState = true
			break
		}

		// For multi-node clusters, either leader or follower is acceptable
		if !bootstrap && (state == raft.Leader || state == raft.Follower) {
			fmt.Printf("Raft cluster stabilized in %s state\n", state)
			stableState = true
			break
		}

		// If we're in candidate state, election is in progress
		if state == raft.Candidate {
			fmt.Printf("Election in progress (candidate state)\n")
		}

		time.Sleep(100 * time.Millisecond)
	}

	if !stableState {
		fmt.Printf("Warning: Raft cluster did not stabilize within expected time\n")
	}

	// Final status check
	finalState := c.raft.State()
	finalLeader := c.raft.Leader()
	fmt.Printf("Final Raft state: %s, Leader: %s\n", finalState, finalLeader)

	// For single-node bootstrap, we should be the leader
	if bootstrap && finalState != raft.Leader {
		fmt.Printf("Warning: Single-node cluster did not become leader, final state: %s\n", finalState)
		// Check if there are any configuration issues
		if config.Error() == nil {
			servers := config.Configuration().Servers
			fmt.Printf("Current cluster configuration: %+v\n", servers)
		}
	} else if bootstrap && finalState == raft.Leader {
		fmt.Printf("Success: Single-node cluster is now leader\n")
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
		fmt.Printf("ERROR: failed to unmarshal command: %s\n", err.Error())
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	if err := f.metadataStore.ApplyCommand(&cmd); err != nil {
		fmt.Printf("ERROR: failed to apply command: %s\n", err.Error())
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
	fmt.Printf("Attempting to join cluster via %d peers: %v\n", len(peers), peers)

	// We need to convert Raft addresses to broker addresses for gRPC
	// For now, we'll try to contact the Raft addresses directly
	// In production, you'd want a service discovery mechanism

	// Try each peer to find one that can help us join
	for _, peerRaftAddr := range peers {
		if peerRaftAddr == "" || peerRaftAddr == string(c.localAddr) {
			continue
		}

		fmt.Printf("Attempting to join via peer %s...\n", peerRaftAddr)

		// Convert Raft address to broker address (subtract 10000 from port)
		// This is a simple convention: broker port = raft port - 10000
		// e.g., raft:19092 -> broker:9092
		brokerAddr := convertRaftToBrokerAddr(peerRaftAddr)
		if brokerAddr == "" {
			fmt.Printf("Could not convert Raft address to broker address: %s\n", peerRaftAddr)
			continue
		}

		fmt.Printf("Converted Raft address %s to broker address %s\n", peerRaftAddr, brokerAddr)

		// Try to send join request via gRPC
		// This will be handled by the broker's JoinCluster RPC handler
		// For now, we'll just log and wait - the actual gRPC call needs to be made from broker layer
		fmt.Printf("Join request should be sent to broker at %s\n", brokerAddr)
		fmt.Printf("Note: Join will be completed asynchronously via broker's gRPC client\n")

		return nil // Return success, the broker will handle the actual join
	}

	return fmt.Errorf("could not find suitable peer to join cluster")
}

// convertRaftToBrokerAddr converts a Raft address to a broker address
// Convention: broker port = raft port - 10000
func convertRaftToBrokerAddr(raftAddr string) string {
	// Parse the Raft address to extract host and port
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return ""
	}

	host := parts[0]
	raftPort := parts[1]

	// Convert port string to int
	var port int
	if _, err := fmt.Sscanf(raftPort, "%d", &port); err != nil {
		return ""
	}

	// Calculate broker port (raft port - 10000)
	brokerPort := port - 10000
	if brokerPort <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", host, brokerPort)
}

// AddVoter adds a new voting member to the cluster (must be called on the leader)
func (c *Consensus) AddVoter(id raft.ServerID, address raft.ServerAddress) error {
	// Check if we're the leader
	if c.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot add voter")
	}

	fmt.Printf("Leader adding voter: ID=%s, Address=%s\n", id, address)

	// Add the voter to the cluster
	future := c.raft.AddVoter(id, address, 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	fmt.Printf("Successfully added voter %s to cluster\n", id)
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
