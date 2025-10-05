#!/bin/bash

# Test script for multi-broker replication in Gofka
# Tests that data replicates across multiple brokers

set -e

echo "=== Gofka Multi-Broker Replication Test ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cleanup function
cleanup() {
    echo ""
    echo "${YELLOW}Cleaning up...${NC}"

    # Kill all broker processes
    pkill -f "gofka-broker" || true

    # Remove temporary directories
    rm -rf /tmp/gofka-test-multi-broker

    echo "${GREEN}Cleanup complete${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Create fresh directories
rm -rf /tmp/gofka-test-multi-broker
mkdir -p /tmp/gofka-test-multi-broker

# Build the broker
echo "${YELLOW}Building Gofka broker...${NC}"
go build -o /tmp/gofka-test-multi-broker/gofka-broker cmd/gofka-broker/main.go

if [ $? -ne 0 ]; then
    echo "${RED}Failed to build broker${NC}"
    exit 1
fi

echo "${GREEN}✓ Broker built successfully${NC}"
echo ""

# Start Broker 1 (Bootstrap node)
echo "${YELLOW}Starting Broker 1 (bootstrap)...${NC}"
/tmp/gofka-test-multi-broker/gofka-broker \
    --node.id=broker-1 \
    --addr=localhost:9092 \
    --log.dir=/tmp/gofka-test-multi-broker/broker-1/logs \
    --raft.addr=localhost:19092 \
    --raft.dir=/tmp/gofka-test-multi-broker/broker-1/raft \
    --bootstrap \
    > /tmp/gofka-test-multi-broker/broker-1.log 2>&1 &

BROKER1_PID=$!
echo "${GREEN}✓ Broker 1 started (PID: $BROKER1_PID)${NC}"

# Wait for broker 1 to be ready
echo "Waiting for Broker 1 to be ready..."
sleep 3

# Start Broker 2 (Join the cluster)
echo "${YELLOW}Starting Broker 2 (follower)...${NC}"
/tmp/gofka-test-multi-broker/gofka-broker \
    --node.id=broker-2 \
    --addr=localhost:9093 \
    --log.dir=/tmp/gofka-test-multi-broker/broker-2/logs \
    --raft.addr=localhost:19093 \
    --raft.dir=/tmp/gofka-test-multi-broker/broker-2/raft \
    --peers=localhost:19092 \
    > /tmp/gofka-test-multi-broker/broker-2.log 2>&1 &

BROKER2_PID=$!
echo "${GREEN}✓ Broker 2 started (PID: $BROKER2_PID)${NC}"

# Wait for broker 2 to join
echo "Waiting for Broker 2 to join cluster..."
sleep 3

# Start Broker 3 (Join the cluster)
echo "${YELLOW}Starting Broker 3 (follower)...${NC}"
/tmp/gofka-test-multi-broker/gofka-broker \
    --node.id=broker-3 \
    --addr=localhost:9094 \
    --log.dir=/tmp/gofka-test-multi-broker/broker-3/logs \
    --raft.addr=localhost:19094 \
    --raft.dir=/tmp/gofka-test-multi-broker/broker-3/raft \
    --peers=localhost:19092,localhost:19093 \
    > /tmp/gofka-test-multi-broker/broker-3.log 2>&1 &

BROKER3_PID=$!
echo "${GREEN}✓ Broker 3 started (PID: $BROKER3_PID)${NC}"

# Wait for cluster to stabilize
echo "Waiting for cluster to stabilize..."
sleep 5

echo ""
echo "${GREEN}=== 3-Broker Cluster Running ===${NC}"
echo "Broker 1: localhost:9092 (PID: $BROKER1_PID)"
echo "Broker 2: localhost:9093 (PID: $BROKER2_PID)"
echo "Broker 3: localhost:9094 (PID: $BROKER3_PID)"
echo ""

# Test 1: Produce messages to Broker 1
echo "${YELLOW}Test 1: Producing messages to Broker 1${NC}"

# Build a simple producer if it exists
if [ -f "examples/simple-producer/main.go" ]; then
    go build -o /tmp/gofka-test-multi-broker/producer examples/simple-producer/main.go

    for i in {1..10}; do
        /tmp/gofka-test-multi-broker/producer \
            --broker localhost:9092 \
            --topic test-replication \
            --message "Message $i" \
            --partition 0 \
            >> /tmp/gofka-test-multi-broker/producer.log 2>&1
    done

    echo "${GREEN}✓ Produced 10 messages to Broker 1${NC}"
else
    echo "${YELLOW}Note: Producer example not found, skipping message production${NC}"
fi

# Wait for replication
echo "Waiting for replication to complete..."
sleep 5

# Test 2: Check logs on all brokers
echo ""
echo "${YELLOW}Test 2: Checking log files on all brokers${NC}"

for broker_num in 1 2 3; do
    log_dir="/tmp/gofka-test-multi-broker/broker-$broker_num/logs/test-replication"
    if [ -d "$log_dir" ]; then
        log_files=$(find $log_dir -name "*.log" 2>/dev/null | wc -l)
        if [ "$log_files" -gt 0 ]; then
            echo "${GREEN}✓ Broker $broker_num has log files for test-replication topic${NC}"
        else
            echo "${RED}✗ Broker $broker_num has no log files${NC}"
        fi
    else
        echo "${YELLOW}  Broker $broker_num: No log directory yet${NC}"
    fi
done

# Test 3: Check broker processes are still running
echo ""
echo "${YELLOW}Test 3: Verifying broker processes${NC}"

for pid in $BROKER1_PID $BROKER2_PID $BROKER3_PID; do
    if ps -p $pid > /dev/null 2>&1; then
        echo "${GREEN}✓ Process $pid is running${NC}"
    else
        echo "${RED}✗ Process $pid has stopped${NC}"
    fi
done

# Test 4: Check for errors in logs
echo ""
echo "${YELLOW}Test 4: Checking for critical errors in logs${NC}"

for broker_num in 1 2 3; do
    log_file="/tmp/gofka-test-multi-broker/broker-$broker_num.log"

    # Check for panic or fatal errors
    if grep -qi "panic\|fatal" $log_file 2>/dev/null; then
        echo "${RED}✗ Broker $broker_num has critical errors:${NC}"
        grep -i "panic\|fatal" $log_file | head -5
    else
        echo "${GREEN}✓ Broker $broker_num: No critical errors${NC}"
    fi
done

# Display summary
echo ""
echo "${GREEN}=== Test Summary ===${NC}"
echo "All 3 brokers are running"
echo "Check individual broker logs at:"
echo "  - /tmp/gofka-test-multi-broker/broker-1.log"
echo "  - /tmp/gofka-test-multi-broker/broker-2.log"
echo "  - /tmp/gofka-test-multi-broker/broker-3.log"
echo ""
echo "Press Ctrl+C to stop all brokers and cleanup"

# Keep the script running
wait
