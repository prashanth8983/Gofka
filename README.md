# Gofka

A high-performance distributed message broker built in Go, inspired by Apache Kafka's architecture while offering a simpler operational model.

## Features

- **KRaft Consensus**: Built-in Raft-based metadata management, eliminating external dependencies like ZooKeeper
- **High Throughput**: Zero-copy reads, message batching, and multiple compression algorithms (gzip, snappy, lz4)
- **Strong Durability**: Configurable replication with ISR tracking and CRC32 data integrity validation
- **Consumer Groups**: Automatic partition assignment with sticky assignments and rebalancing
- **Production Ready**: Comprehensive metrics (Prometheus), structured logging, and health checks
- **Python Client**: Full-featured async client with connection pooling and SSL/TLS support

## Quick Start

### Installation

```bash
# Clone and build
git clone https://github.com/prashanth8983/gofka.git
cd gofka
make build

# Or install with Go
go install github.com/prashanth8983/gofka/cmd/gofka-broker@latest
go install github.com/prashanth8983/gofka/cmd/gofka-controller@latest
```

### Running a Single Node

```bash
# Start controller (metadata management)
gofka-controller \
  --node.id=controller-1 \
  --raft.addr=localhost:19093 \
  --raft.dir=/tmp/gofka/controller-raft \
  --bootstrap

# Start broker (message storage and serving)
gofka-broker \
  --node.id=broker-1 \
  --addr=localhost:9092 \
  --log.dir=/tmp/gofka/broker-logs \
  --raft.addr=localhost:19092 \
  --raft.dir=/tmp/gofka/broker-raft \
  --peers=localhost:19093
```

### Using the Python Client

```bash
pip install gofka-python
```

```python
from gofka import Producer, Consumer

# Producing messages
async with Producer("localhost:9092") as producer:
    await producer.send("my-topic", b"Hello, Gofka!")

# Consuming messages
async with Consumer("localhost:9092", "my-group", ["my-topic"]) as consumer:
    async for message in consumer:
        print(f"Received: {message.value}")
        await consumer.commit()
```

## Architecture

Gofka uses a distributed architecture with separated control and data planes:

- **Controllers**: Manage cluster metadata using Raft consensus
- **Brokers**: Store and serve messages, handle replication
- **Topics**: Logical channels divided into partitions
- **Partitions**: Ordered, immutable sequence of messages

### Message Flow

1. Producers send messages to partition leaders
2. Leaders replicate to followers maintaining ISR
3. Consumers read from leaders or ISR replicas
4. Consumer groups coordinate for parallel consumption

## Monitoring

Gofka exposes operational metrics and health endpoints:

```bash
# Prometheus metrics
curl http://localhost:8080/metrics

# Health status
curl http://localhost:8080/health

# Kubernetes probes
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

Key metrics include:
- `gofka_messages_produced_total` - Message production rate
- `gofka_consumer_lag` - Consumer group lag
- `gofka_broker_up` - Broker availability
- `gofka_raft_state` - Consensus state

## Configuration

### Broker Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--node.id` | Unique broker identifier | Required |
| `--addr` | Listen address for clients | localhost:9092 |
| `--log.dir` | Directory for message logs | Required |
| `--raft.addr` | Raft consensus address | Required |
| `--min.insync.replicas` | Minimum ISR for writes | 1 |

### Topic Configuration

Topics are auto-created on first use or can be configured:

```bash
# Future CLI (in development)
gofka topic create --name events --partitions 10 --replication-factor 3
```

## Performance

Benchmarked on commodity hardware (8 core, 16GB RAM, SSD):

- **Throughput**: 100K+ messages/sec per broker
- **Latency**: p99 < 10ms at moderate load
- **Compression**: 70% reduction with snappy
- **Recovery**: < 30s for leader election

## Development

### Building from Source

```bash
# Prerequisites
go version  # Go 1.23+
protoc --version  # Protocol Buffers 3.x

# Build
make build
make test
make proto  # Regenerate protobuf files
```

### Project Structure

```
gofka/
├── cmd/              # Entry points
│   ├── gofka-broker/
│   └── gofka-controller/
├── pkg/              # Core packages
│   ├── broker/       # Message broker implementation
│   ├── kraft/        # Raft consensus
│   ├── log/          # Segment-based storage
│   ├── network/      # Binary protocol
│   ├── metrics/      # Prometheus metrics
│   └── health/       # Health checks
├── api/              # Protocol definitions
└── docs/             # Documentation
```

### Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.

Key areas for contribution:
- CLI administrative tools
- Additional client libraries (Java, Node.js, Rust)
- Security features (ACLs, encryption at rest)
- Stream processing capabilities

## Comparison with Apache Kafka

| Feature | Gofka | Apache Kafka |
|---------|-------|--------------|
| Metadata Management | Built-in Raft | ZooKeeper/KRaft |
| Protocol | Simplified Binary | Complex Binary |
| Languages | Go + Python | Java + Many |
| Minimum Nodes | 1 | 3+ |
| Configuration | Minimal | Extensive |
| Performance | High | Very High |
| Maturity | Early | Production |

## License

MIT License - See [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/prashanth8983/gofka/issues)
- **Discussions**: [GitHub Discussions](https://github.com/prashanth8983/gofka/discussions)

## Roadmap

- [x] Core broker with replication
- [x] Consumer groups and offset management
- [x] Production monitoring (metrics, logging, health)
- [x] Python client with async support
- [ ] CLI administration tools
- [ ] Security (TLS, SASL, ACLs)
- [ ] Transactions and exactly-once semantics
- [ ] Stream processing framework

---

Built with ❤️ in Go