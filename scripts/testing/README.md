# Testing Scripts

This directory contains testing scripts for validating Gofka functionality.

## Available Tests

### `test_gofka_simple.sh`
Tests a standalone broker in bootstrap mode with basic producer/consumer operations.
- ✅ Single-node cluster setup
- ✅ Topic creation
- ✅ Message production
- ⚠️ Message consumption (partial)

**Usage:**
```bash
./scripts/testing/test_gofka_simple.sh
```

### `test_gofka.sh`
Tests a full controller + broker cluster setup.
- Controller with KRaft consensus
- Broker connecting to controller
- Topic management through controller
- Producer/consumer operations

**Usage:**
```bash
./scripts/testing/test_gofka.sh
```

### `test_broker_simple.sh`
Basic broker startup and leadership test.

### `test_broker_fixed.sh`
Broker test with transaction rollback fixes.

## Test Results

Current status of test scripts:
- **Broker startup**: ✅ Working
- **Leadership election**: ✅ Working  
- **Topic creation**: ✅ Working
- **Message production**: ✅ Working
- **Message consumption**: ⚠️ Needs refinement

## Running Tests

All test scripts are executable and include cleanup procedures. They create temporary directories under `/tmp/gofka/` which are cleaned up automatically.

To run all tests:
```bash
cd scripts/testing
for test in test_*.sh; do
    echo "Running $test..."
    ./$test
    echo "---"
done
```