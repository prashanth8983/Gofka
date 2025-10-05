# API Documentation

## Protocol

Gofka uses Protocol Buffers for communication between clients and brokers. The protocol definitions are in `api/v1/gofka.proto`.

## Client Operations

### Topic Management

```go
client := client.NewClusterClient("localhost:9092", 10*time.Second)
defer client.Close()

// Create topic
resp, err := client.CreateTopic("my-topic", 1, 1)
if err != nil {
    log.Fatal(err)
}
```

### Producer

```go
producer := client.NewProducer(clusterClient, &client.ProducerConfig{
    Acks:    1,
    Timeout: 30 * time.Second,
})
defer producer.Close()

// Connect to topic/partition
err := producer.Connect("my-topic", 0)
if err != nil {
    log.Fatal(err)
}

// Send message
offset, err := producer.Produce("my-topic", []byte("Hello, Gofka!"))
if err != nil {
    log.Fatal(err)
}
```

### Consumer

```go
consumer := client.NewConsumer(clusterClient, &client.ConsumerConfig{
    Group:   "my-group",
    Timeout: 30 * time.Second,
})
defer consumer.Close()

// Subscribe to topic/partition
err := consumer.Subscribe("my-topic", 0)
if err != nil {
    log.Fatal(err)
}

// Consume messages
for {
    msg, err := consumer.Consume()
    if err != nil {
        log.Printf("Error: %v", err)
        break
    }
    fmt.Printf("Received: %s\n", string(msg.Value))
}
```

## Configuration

### Broker Configuration

- `--node.id`: Unique node identifier
- `--addr`: Broker listen address
- `--log.dir`: Log storage directory
- `--raft.addr`: Raft consensus address
- `--raft.dir`: Raft state directory
- `--bootstrap`: Bootstrap single-node cluster
- `--peers`: Comma-separated peer addresses

### Controller Configuration

- `--node.id`: Unique controller identifier
- `--raft.addr`: Raft consensus address
- `--http.addr`: HTTP API address
- `--raft.dir`: Raft state directory
- `--bootstrap`: Bootstrap controller cluster
- `--peers`: Comma-separated peer addresses