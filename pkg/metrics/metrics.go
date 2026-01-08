package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Messages metrics
	MessagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_messages_produced_total",
		Help: "Total number of messages produced",
	}, []string{"topic", "partition"})

	MessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_messages_consumed_total",
		Help: "Total number of messages consumed",
	}, []string{"topic", "partition", "group"})

	MessageSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gofka_message_size_bytes",
		Help:    "Size of messages in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 2, 10), // 100B to 100KB
	}, []string{"topic", "type"}) // type = produce/consume

	// Consumer group metrics
	ConsumerGroupMembers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_consumer_group_members",
		Help: "Number of members in consumer groups",
	}, []string{"group"})

	ConsumerGroupRebalances = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_consumer_group_rebalances_total",
		Help: "Total number of consumer group rebalances",
	}, []string{"group"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_consumer_lag",
		Help: "Consumer lag in number of messages",
	}, []string{"group", "topic", "partition"})

	// Storage metrics
	LogSegments = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_log_segments",
		Help: "Number of log segments",
	}, []string{"topic", "partition"})

	LogSizeBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_log_size_bytes",
		Help: "Size of log in bytes",
	}, []string{"topic", "partition"})

	OffsetCommits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_offset_commits_total",
		Help: "Total number of offset commits",
	}, []string{"group", "topic", "partition"})

	// Network metrics
	RequestsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_requests_received_total",
		Help: "Total number of requests received",
	}, []string{"api_key", "api_version"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gofka_request_duration_seconds",
		Help:    "Request duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"api_key"})

	RequestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_request_errors_total",
		Help: "Total number of request errors",
	}, []string{"api_key", "error_code"})

	BytesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_bytes_received_total",
		Help: "Total number of bytes received",
	}, []string{"api_key"})

	BytesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gofka_bytes_sent_total",
		Help: "Total number of bytes sent",
	}, []string{"api_key"})

	// Raft consensus metrics
	RaftState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_raft_state",
		Help: "Current Raft state (0=Follower, 1=Candidate, 2=Leader)",
	}, []string{"node_id"})

	RaftTerm = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_raft_term",
		Help: "Current Raft term",
	}, []string{"node_id"})

	RaftCommitIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_raft_commit_index",
		Help: "Current Raft commit index",
	}, []string{"node_id"})

	RaftAppliedIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_raft_applied_index",
		Help: "Current Raft applied index",
	}, []string{"node_id"})

	RaftPeers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_raft_peers",
		Help: "Number of Raft peers",
	}, []string{"node_id"})

	// Broker health metrics
	BrokerUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_broker_up",
		Help: "Whether the broker is up (1) or down (0)",
	}, []string{"broker_id"})

	TopicsCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gofka_topics_total",
		Help: "Total number of topics",
	})

	PartitionsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofka_partitions_total",
		Help: "Total number of partitions per topic",
	}, []string{"topic"})

	// Connection metrics
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gofka_active_connections",
		Help: "Number of active client connections",
	})

	ConnectionsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gofka_connections_accepted_total",
		Help: "Total number of connections accepted",
	})

	ConnectionsClosed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gofka_connections_closed_total",
		Help: "Total number of connections closed",
	})
)

// RecordProducedMessage records a produced message
func RecordProducedMessage(topic string, partition int32, size int) {
	MessagesProduced.WithLabelValues(topic, string(partition)).Inc()
	MessageSize.WithLabelValues(topic, "produce").Observe(float64(size))
}

// RecordConsumedMessage records a consumed message
func RecordConsumedMessage(topic string, partition int32, group string, size int) {
	MessagesConsumed.WithLabelValues(topic, string(partition), group).Inc()
	MessageSize.WithLabelValues(topic, "consume").Observe(float64(size))
}

// RecordRequest records a request
func RecordRequest(apiKey string, apiVersion string, duration float64, bytesIn int, bytesOut int, err error) {
	RequestsReceived.WithLabelValues(apiKey, apiVersion).Inc()
	RequestDuration.WithLabelValues(apiKey).Observe(duration)
	BytesReceived.WithLabelValues(apiKey).Add(float64(bytesIn))
	BytesSent.WithLabelValues(apiKey).Add(float64(bytesOut))

	if err != nil {
		RequestErrors.WithLabelValues(apiKey, err.Error()).Inc()
	}
}

// UpdateRaftMetrics updates Raft consensus metrics
func UpdateRaftMetrics(nodeID string, state int, term uint64, commitIndex uint64, appliedIndex uint64, peers int) {
	RaftState.WithLabelValues(nodeID).Set(float64(state))
	RaftTerm.WithLabelValues(nodeID).Set(float64(term))
	RaftCommitIndex.WithLabelValues(nodeID).Set(float64(commitIndex))
	RaftAppliedIndex.WithLabelValues(nodeID).Set(float64(appliedIndex))
	RaftPeers.WithLabelValues(nodeID).Set(float64(peers))
}