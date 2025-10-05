# Gofka Replication Architecture

## Overview

Gofka implements a hybrid replication architecture that combines **KRaft consensus** for metadata management with **leader-follower replication** for data streams. This design provides strong consistency for cluster metadata while enabling high-throughput message replication.

## Architecture Components

### 1. Replication Manager (`pkg/broker/replication.go`)

The `ReplicaManager` coordinates data replication across brokers:

- **Leader State Tracking**: Maintains ISR (In-Sync Replicas), high water marks, and log end offsets
- **Follower State Tracking**: Tracks connection to leader, fetch progress, and sync status
- **ISR Monitoring**: Continuously monitors replica health and updates ISR membership
- **Heartbeat System**: Periodic health checks between leaders and followers

### 2. Inter-Broker Communication (`pkg/broker/broker_client.go`)

The `BrokerClient` provides gRPC-based communication between brokers:

- **Connection Pooling**: Maintains persistent connections to peer brokers
- **Replica Fetch**: Fetches log records from leaders
- **Heartbeat Protocol**: Sends keepalive and offset updates to leaders
- **Automatic Reconnection**: Handles connection failures gracefully

### 3. Replication Protocol

#### ReplicaFetchRequest/Response

Followers fetch data from leaders using this protocol:

```protobuf
message ReplicaFetchRequest {
  string topic = 1;
  int32 partition = 2;
  int64 fetch_offset = 3;      // Offset to start fetching from
  int64 max_bytes = 4;          // Maximum bytes to fetch
  string replica_id = 5;        // Follower's broker ID
}

message ReplicaFetchResponse {
  string topic = 1;
  int32 partition = 2;
  int64 high_watermark = 3;     // HWM on leader
  int64 log_end_offset = 4;     // LEO on leader
  repeated LogRecord records = 5; // Batch of records
  string error = 6;
}
```

#### ReplicaHeartbeatRequest/Response

Followers send heartbeats to leaders:

```protobuf
message ReplicaHeartbeatRequest {
  string replica_id = 1;
  string topic = 2;
  int32 partition = 3;
  int64 current_offset = 4;     // Follower's current offset
}

message ReplicaHeartbeatResponse {
  bool success = 1;
  int64 high_watermark = 2;
  string leader_id = 3;
  string error = 4;
}
```

## Replication Flow

### Leader-Side Operations

1. **Message Production**:
   - Leader receives produce request
   - Appends to local log
   - Updates high water mark
   - Returns ack based on configuration

2. **Serving Fetch Requests**:
   - Receives fetch request from follower
   - Reads batch of records from log
   - Returns records with HWM and LEO
   - Updates follower's last-seen timestamp

3. **ISR Management**:
   - Monitors follower heartbeats
   - Checks if replicas are within timeout
   - Updates ISR membership
   - Enforces `min.insync.replicas` on produce

### Follower-Side Operations

1. **Continuous Fetching**:
   - Runs fetch loop every 100ms
   - Fetches from current offset
   - Appends records to local log
   - Updates local HWM and LEO

2. **Heartbeat**:
   - Sends heartbeat every 1s
   - Reports current offset
   - Receives leader's HWM
   - Maintains ISR membership

3. **Catch-Up**:
   - Fetches in batches (up to 1MB)
   - Handles log gaps automatically
   - Tracks catch-up progress
   - Updates `IsInSync` status

## Acks Configuration

Gofka supports three acknowledgment modes:

### `acks = 0` (No Acknowledgment)
- Producer doesn't wait for any acknowledgment
- Fastest, but no durability guarantee
- Use for non-critical, high-throughput scenarios

### `acks = 1` (Leader Acknowledgment) - Default
- Wait for leader to write to local log
- Balanced performance and durability
- Risk: Data loss if leader fails before replication

### `acks = -1` or `acks = all` (All ISR Acknowledgment)
- Wait for all in-sync replicas to acknowledge
- Strongest durability guarantee
- Enforces `min.insync.replicas` check
- Currently uses KRaft consensus for guarantees

## Min In-Sync Replicas

The `min.insync.replicas` setting ensures minimum replication level:

- **Default**: 1 (leader only)
- **Recommended**: 2 for production (leader + 1 follower)
- **Behavior**: Produce requests fail if ISR count < min.insync.replicas
- **Check**: Only enforced when `acks = -1`

## High Water Mark (HWM)

The high water mark indicates the highest offset that is replicated to all ISR members:

- **Leader**: Tracks HWM based on follower progress
- **Follower**: Receives HWM from leader during fetch
- **Consumers**: Only read up to HWM (ensures consistency)
- **Current**: HWM = LEO (simplified, will be enhanced)

## Testing

### Multi-Broker Cluster Test

Run the replication test script:

```bash
./scripts/testing/test_multi_broker_replication.sh
```

This script:
1. Starts 3 brokers (1 leader + 2 followers)
2. Produces messages to the leader
3. Verifies replication across all brokers
4. Checks for errors and health

### Manual Testing

1. **Start Broker 1 (Bootstrap)**:
```bash
go run cmd/gofka-broker/main.go \
  --node.id=broker-1 \
  --addr=localhost:9092 \
  --raft.addr=localhost:19092 \
  --bootstrap
```

2. **Start Broker 2 (Follower)**:
```bash
go run cmd/gofka-broker/main.go \
  --node.id=broker-2 \
  --addr=localhost:9093 \
  --raft.addr=localhost:19093 \
  --peers=localhost:19092
```

3. **Produce Messages**:
```bash
# Using Python client
cd clients/python
python3 examples/simple_producer.py
```

4. **Verify Replication**:
```bash
# Check logs on both brokers
ls -la /tmp/gofka/broker-1/logs/test-topic/
ls -la /tmp/gofka/broker-2/logs/test-topic/
```

## Current Limitations

1. **Single Partition**: Currently only partition 0 is fully implemented
2. **HWM Calculation**: Simplified to HWM = LEO; needs proper ISR-based calculation
3. **Failover**: Manual intervention required for leader changes
4. **Lag Metrics**: No exposed metrics for replica lag yet
5. **Backpressure**: No flow control on fetch requests

## Future Enhancements

1. **Advanced HWM**: Track follower offsets and calculate HWM properly
2. **Leader Failover**: Automatic leader election and promotion
3. **Metrics**: Expose replication lag, ISR changes, fetch latency
4. **Quotas**: Rate limiting on replication traffic
5. **Compression**: Compress fetch responses
6. **Batching**: Optimize batch sizes based on network conditions

## Implementation Details

### File Structure

```
pkg/broker/
├── replication.go         # ReplicaManager (503 lines)
├── broker_client.go       # Inter-broker client (192 lines)
├── grpc_handlers.go       # gRPC handlers (107 lines)
└── broker.go              # ProduceMessageWithAcks (updated)

pkg/log/
└── log.go                 # ReadBatch, GetLogEndOffset (updated)

api/v1/
└── gofka.proto           # ReplicaFetch/Heartbeat messages
```

### Key Methods

#### ReplicaManager
- `followerFetchLoop()`: Main replication loop for followers
- `fetchFromLeader()`: Fetch batch from leader and append locally
- `GetISR()`: Get current in-sync replicas
- `monitorISR()`: Update ISR based on health checks

#### BrokerClient
- `FetchFromLeader()`: Send fetch request to leader
- `SendHeartbeatToLeader()`: Send heartbeat with current offset
- `Connect()`: Establish gRPC connection to broker

#### Broker
- `ProduceMessageWithAcks()`: Produce with configurable acks
- `ReplicaFetch()`: Handle fetch requests from followers (gRPC)
- `ReplicaHeartbeat()`: Handle heartbeat from followers (gRPC)

## Debugging

### Enable Verbose Logging

Add debug prints in key locations:

```go
// In followerFetchLoop
fmt.Printf("Fetched %d records from %s for %s-%d\n",
    len(resp.Records), leaderAddr, topic, partition)

// In monitorISR
fmt.Printf("ISR for %s: %d replicas\n", key, len(newISR))
```

### Common Issues

1. **Follower not replicating**: Check network connectivity and gRPC errors
2. **ISR shrinking**: Check follower fetch timeouts and lag
3. **Acks timeout**: Verify ISR count meets min.insync.replicas
4. **Connection errors**: Ensure all brokers are reachable on advertised addresses

## References

- [Kafka Replication Design](https://kafka.apache.org/documentation/#replication)
- [KRaft: Kafka's New Consensus Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
- [Protocol Buffers](https://protobuf.dev/)
