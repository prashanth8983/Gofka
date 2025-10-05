# Gofka Examples

This directory contains example applications demonstrating how to use the Gofka client libraries.

## Examples

### Simple Producer

A basic producer that sends a single message to a topic.

```bash
go run examples/simple-producer/main.go -broker localhost:9092 -topic test-topic -message "Hello, Gofka!"
```

### Simple Consumer

A basic consumer that subscribes to a topic and consumes messages.

```bash
go run examples/simple-consumer/main.go -broker localhost:9092 -topic test-topic -group test-group
```

## Running the Examples

1. Start the Gofka broker:
   ```bash
   make run-broker
   ```

2. In another terminal, run the producer:
   ```bash
   go run examples/simple-producer/main.go
   ```

3. In a third terminal, run the consumer:
   ```bash
   go run examples/simple-consumer/main.go
   ```

## Configuration

All examples support command-line flags for configuration:

- `-broker`: Broker address (default: localhost:9092)
- `-topic`: Topic name (default: test-topic)
- `-message`: Message to send (producer only)
- `-group`: Consumer group ID (consumer only)

## Building Examples

To build the examples:

```bash
# Build producer
go build -o bin/simple-producer examples/simple-producer/main.go

# Build consumer
go build -o bin/simple-consumer examples/simple-consumer/main.go
``` 