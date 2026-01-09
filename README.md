# Gofka

A distributed message broker in Go, inspired by Kafka but simpler to run.

```
┌──────────────────────────────────────────────────────────────────────┐
│                           GOFKA CLUSTER                              │
│                                                                      │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │
│   │ Controller  │◄───►│ Controller  │◄───►│ Controller  │           │
│   │   (Raft)    │     │   (Raft)    │     │   (Raft)    │           │
│   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘           │
│          │                   │                   │                   │
│          └───────────────────┼───────────────────┘                   │
│                              │ metadata                              │
│          ┌───────────────────┼───────────────────┐                   │
│          ▼                   ▼                   ▼                   │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │
│   │   Broker    │     │   Broker    │     │   Broker    │           │
│   │  [P1,P3]    │◄───►│  [P2,P1]    │◄───►│  [P3,P2]    │           │
│   └─────────────┘     └─────────────┘     └─────────────┘           │
│          ▲                   ▲                   ▲                   │
└──────────┼───────────────────┼───────────────────┼───────────────────┘
           │                   │                   │
    ┌──────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
    │  Producer   │     │  Consumer   │     │  Consumer   │
    └─────────────┘     │   Group     │     │   Group     │
                        └─────────────┘     └─────────────┘
```

## What it does

- **No ZooKeeper** - Uses built-in Raft for metadata (like Kafka KRaft)
- **Message replication** - Configurable ISR for durability
- **Consumer groups** - Automatic partition assignment
- **Compression** - gzip, snappy, lz4
- **Monitoring** - Prometheus metrics + health checks

## Quick Start

```bash
# Build
git clone https://github.com/prashanth8983/gofka.git
cd gofka && make build

# Start controller
./gofka-controller --node.id=c1 --raft.addr=:19093 --bootstrap

# Start broker
./gofka-broker --node.id=b1 --addr=:9092 --log.dir=/tmp/gofka --peers=localhost:19093
```

## Python Client

```bash
pip install gofka-python
```

```python
from gofka import Producer, Consumer

# Send
async with Producer("localhost:9092") as p:
    await p.send("orders", b'{"id": 123}')

# Receive
async with Consumer("localhost:9092", "my-group", ["orders"]) as c:
    async for msg in c:
        print(msg.value)
        await c.commit()
```

## How it works

```
Producer                          Broker                         Consumer
   │                                │                                │
   │─── send("topic", msg) ────────►│                                │
   │                                │── replicate to ISR ──►│        │
   │                                │◄── ack ──────────────│        │
   │◄── ack ────────────────────────│                                │
   │                                │                                │
   │                                │◄── poll("topic") ─────────────│
   │                                │─── messages ─────────────────►│
   │                                │◄── commit(offset) ────────────│
```

## Project Structure

```
gofka/
├── cmd/
│   ├── gofka-broker/      # Message storage & serving
│   └── gofka-controller/  # Cluster metadata (Raft)
├── pkg/
│   ├── broker/            # Core broker logic
│   ├── kraft/             # Raft consensus
│   ├── log/               # Segment storage
│   └── network/           # Wire protocol
└── examples/
    ├── simple-producer/
    └── simple-consumer/
```

## Config

| Flag | What it does |
|------|--------------|
| `--node.id` | Unique node name |
| `--addr` | Client listen address |
| `--log.dir` | Where messages are stored |
| `--min.insync.replicas` | Min replicas for ack |

## Monitoring

```bash
curl localhost:8080/metrics  # Prometheus
curl localhost:8080/health   # Health check
```

## vs Kafka

| | Gofka | Kafka |
|---|---|---|
| Min nodes | 1 | 3+ |
| Dependencies | None | ZooKeeper (or KRaft) |
| Config | Minimal | Lots |
| Written in | Go | Java |

## License

MIT
