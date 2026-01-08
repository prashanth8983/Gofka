package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Status represents the health status
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// Component represents a system component's health
type Component struct {
	Name        string                 `json:"name"`
	Status      Status                 `json:"status"`
	Message     string                 `json:"message,omitempty"`
	LastChecked time.Time              `json:"last_checked"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// HealthChecker interface for components that can report health
type HealthChecker interface {
	CheckHealth() Component
}

// Service manages health checks for all components
type Service struct {
	mu         sync.RWMutex
	components map[string]HealthChecker
	cache      map[string]Component
	cacheTTL   time.Duration
}

// NewService creates a new health service
func NewService(cacheTTL time.Duration) *Service {
	return &Service{
		components: make(map[string]HealthChecker),
		cache:      make(map[string]Component),
		cacheTTL:   cacheTTL,
	}
}

// RegisterComponent registers a component for health checking
func (s *Service) RegisterComponent(name string, checker HealthChecker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.components[name] = checker
}

// CheckHealth checks the health of all components
func (s *Service) CheckHealth() map[string]Component {
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make(map[string]Component)

	for name, checker := range s.components {
		// Check cache
		if cached, ok := s.cache[name]; ok {
			if time.Since(cached.LastChecked) < s.cacheTTL {
				results[name] = cached
				continue
			}
		}

		// Perform health check
		component := checker.CheckHealth()
		component.LastChecked = time.Now()

		// Update cache
		s.cache[name] = component
		results[name] = component
	}

	return results
}

// GetOverallHealth returns the overall system health
func (s *Service) GetOverallHealth() Status {
	components := s.CheckHealth()

	hasUnhealthy := false
	hasDegraded := false

	for _, component := range components {
		switch component.Status {
		case StatusUnhealthy:
			hasUnhealthy = true
		case StatusDegraded:
			hasDegraded = true
		}
	}

	if hasUnhealthy {
		return StatusUnhealthy
	}
	if hasDegraded {
		return StatusDegraded
	}
	return StatusHealthy
}

// HTTPHandler returns an HTTP handler for health checks
func (s *Service) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		components := s.CheckHealth()
		overall := s.GetOverallHealth()

		response := map[string]interface{}{
			"status":     overall,
			"timestamp":  time.Now(),
			"components": components,
		}

		// Set status code based on health
		statusCode := http.StatusOK
		if overall == StatusUnhealthy {
			statusCode = http.StatusServiceUnavailable
		} else if overall == StatusDegraded {
			statusCode = http.StatusOK // Still operational but degraded
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(response)
	}
}

// LivenessHandler returns a simple liveness probe handler
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "alive",
			"timestamp": time.Now(),
		})
	}
}

// ReadinessHandler returns a readiness probe handler
func (s *Service) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		overall := s.GetOverallHealth()

		response := map[string]interface{}{
			"ready":     overall == StatusHealthy,
			"status":    overall,
			"timestamp": time.Now(),
		}

		statusCode := http.StatusOK
		if overall != StatusHealthy {
			statusCode = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(response)
	}
}

// BrokerHealthChecker checks broker health
type BrokerHealthChecker struct {
	BrokerID   string
	IsLeader   func() bool
	TopicCount func() int
	GetState   func() string
}

func (b *BrokerHealthChecker) CheckHealth() Component {
	state := b.GetState()
	isHealthy := state == "running" || state == "leader" || state == "follower"

	status := StatusHealthy
	message := fmt.Sprintf("Broker %s is %s", b.BrokerID, state)

	if !isHealthy {
		status = StatusUnhealthy
		message = fmt.Sprintf("Broker %s is in unhealthy state: %s", b.BrokerID, state)
	}

	return Component{
		Name:    "broker",
		Status:  status,
		Message: message,
		Details: map[string]interface{}{
			"broker_id":   b.BrokerID,
			"is_leader":   b.IsLeader(),
			"state":       state,
			"topic_count": b.TopicCount(),
		},
	}
}

// RaftHealthChecker checks Raft consensus health
type RaftHealthChecker struct {
	NodeID       string
	GetState     func() string
	GetTerm      func() uint64
	GetLeader    func() string
	GetPeerCount func() int
}

func (r *RaftHealthChecker) CheckHealth() Component {
	state := r.GetState()
	leader := r.GetLeader()
	peerCount := r.GetPeerCount()

	status := StatusHealthy
	message := fmt.Sprintf("Raft node %s is %s", r.NodeID, state)

	// Check if we have a leader
	if leader == "" && state != "Leader" {
		status = StatusDegraded
		message = "No leader elected"
	}

	// Check if we have enough peers
	if peerCount == 0 && state != "Leader" {
		status = StatusDegraded
		message = "No peers connected"
	}

	return Component{
		Name:    "raft",
		Status:  status,
		Message: message,
		Details: map[string]interface{}{
			"node_id":    r.NodeID,
			"state":      state,
			"term":       r.GetTerm(),
			"leader":     leader,
			"peer_count": peerCount,
		},
	}
}

// StorageHealthChecker checks storage health
type StorageHealthChecker struct {
	GetDiskUsage     func() (used, total uint64)
	GetSegmentCount  func() int
	GetOldestOffset  func() int64
	GetLatestOffset  func() int64
}

func (s *StorageHealthChecker) CheckHealth() Component {
	used, total := s.GetDiskUsage()
	usagePercent := float64(used) / float64(total) * 100

	status := StatusHealthy
	message := fmt.Sprintf("Storage usage: %.1f%%", usagePercent)

	if usagePercent > 90 {
		status = StatusUnhealthy
		message = fmt.Sprintf("Storage critically full: %.1f%%", usagePercent)
	} else if usagePercent > 75 {
		status = StatusDegraded
		message = fmt.Sprintf("Storage usage high: %.1f%%", usagePercent)
	}

	return Component{
		Name:    "storage",
		Status:  status,
		Message: message,
		Details: map[string]interface{}{
			"disk_used_bytes":  used,
			"disk_total_bytes": total,
			"usage_percent":    usagePercent,
			"segment_count":    s.GetSegmentCount(),
			"oldest_offset":    s.GetOldestOffset(),
			"latest_offset":    s.GetLatestOffset(),
		},
	}
}