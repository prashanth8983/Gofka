package controller

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/user/gofka/pkg/kraft"
	"github.com/user/gofka/pkg/metadata"
)

// Controller manages the cluster metadata.
type Controller struct {
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	// Components
	consensus     *kraft.Consensus
	metadataStore *metadata.MetadataStore

	// Configuration
	nodeID   string
	raftAddr string
	httpAddr string // New field for HTTP server address
	raftDir  string

	// Raft instance (for leader checks)
	raft *raft.Raft
}

// NewController creates a new Controller instance.
func NewController(nodeID, raftAddr, httpAddr, raftDir string, bootstrap bool) (*Controller, error) {
	ctx, cancel := context.WithCancel(context.Background())

	fsm := kraft.NewFSM()
	consensus, err := kraft.NewConsensus(nodeID, raftAddr, raftDir, fsm, bootstrap) // Cleanup if bootstrap
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create consensus: %w", err)
	}

	return &Controller{
		ctx:           ctx,
		cancel:        cancel,
		nodeID:        nodeID,
		raftAddr:      raftAddr,
		httpAddr:      httpAddr,
		raftDir:       raftDir,
		consensus:     consensus,
		metadataStore: fsm.MetadataStore(),
		raft:          consensus.Raft(), // Get the underlying Raft instance
	}, nil
}

// Start starts the controller.
func (c *Controller) Start(bootstrap bool, peers []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("controller already running")
	}
	c.running = true

	// Start consensus protocol
	if err := c.consensus.Start(bootstrap, peers); err != nil {
		return fmt.Errorf("failed to start consensus: %w", err)
	}

	// Wait for consensus to stabilize and become leader (for bootstrap mode)
	if bootstrap {
		fmt.Println("Waiting for controller to become leader...")

		// Check the current configuration
		config := c.raft.GetConfiguration()
		if config.Error() == nil {
			servers := config.Configuration().Servers
			fmt.Printf("Current Raft configuration: %+v\n", servers)
		}

		// Wait longer for leadership election (up to 15 seconds)
		// The broker takes about 1.7 seconds, so give more time for the controller
		for i := 0; i < 150; i++ { // Wait up to 15 seconds
			state := c.raft.State()
			leader := c.raft.Leader()
			fmt.Printf("Raft state: %s, Leader: %s\n", state, leader)

			if state == raft.Leader {
				fmt.Printf("Controller is now leader (state: %s)\n", state)
				break
			}

			// If we're still a follower, check if we have a leader
			if state == raft.Follower && leader != "" {
				fmt.Printf("Following leader: %s\n", leader)
			}

			// Check if we're in candidate state (election in progress)
			if state == raft.Candidate {
				fmt.Printf("Election in progress (candidate state)\n")
			}

			// If we've been waiting a long time, check for errors
			if i > 50 && state == raft.Follower && leader == "" {
				fmt.Printf("Warning: Still in follower state with no leader after %d seconds\n", i/10)
				// Check if there are any Raft errors
				if config.Error() != nil {
					fmt.Printf("Raft configuration error: %v\n", config.Error())
				}
			}

			time.Sleep(100 * time.Millisecond)
		}

		// Final check
		finalState := c.raft.State()
		if finalState != raft.Leader {
			fmt.Printf("Warning: Controller did not become leader, final state: %s\n", finalState)
			// Don't return error, just log warning - the system might still work
		} else {
			fmt.Printf("Success: Controller is now leader\n")
		}
	} else {
		// For non-bootstrap mode, just wait a bit for stabilization
		time.Sleep(1000 * time.Millisecond)
	}

	// Start HTTP server for leader checks
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		leaderAddr := c.raft.Leader()
		if leaderAddr == "" {
			http.Error(w, "no leader", http.StatusNotFound)
			return
		}
		w.Write([]byte(string(leaderAddr)))
	})

	go func() {
		if err := http.ListenAndServe(c.httpAddr, nil); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Controller started on %s with Raft address %s, HTTP address %s", c.nodeID, c.raftAddr, c.httpAddr)

	return nil
}

// Stop stops the controller.
func (c *Controller) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}
	c.running = false

	c.cancel()

	// Stop consensus gracefully
	if c.consensus != nil {
		if err := c.consensus.Close(); err != nil {
			log.Printf("Error closing consensus: %v", err)
		}
	}

	log.Println("Controller stopped")

	return nil
}

// CreateTopic creates a new topic in the cluster.
func (c *Controller) CreateTopic(topicName string, numPartitions, replicationFactor int32) error {
	topic := &metadata.Topic{
		Name:              topicName,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
	cmd, err := metadata.NewCommand("add_topic", topic)
	if err != nil {
		return err
	}
	cmdBytes := cmd.ToBytes()
	if cmdBytes == nil {
		return fmt.Errorf("failed to serialize topic command")
	}
	return c.consensus.Apply(cmdBytes)
}

// AddBroker adds a broker to the cluster metadata.
func (c *Controller) AddBroker(brokerID, brokerAddr string) error {
	broker := &metadata.Broker{
		ID:   brokerID,
		Addr: brokerAddr,
	}
	cmd, err := metadata.NewCommand("add_broker", broker)
	if err != nil {
		return err
	}
	cmdBytes := cmd.ToBytes()
	if cmdBytes == nil {
		return fmt.Errorf("failed to serialize broker command")
	}
	return c.consensus.Apply(cmdBytes)
}
