# Development Guide

## Project Structure

```
gofka/
├── api/v1/              # Protocol buffer definitions
├── cmd/                 # Main applications
│   ├── gofka-broker/    # Broker executable
│   └── gofka-controller/ # Controller executable
├── pkg/                 # Core packages
│   ├── broker/          # Broker implementation
│   ├── client/          # Client library
│   ├── controller/      # Controller implementation
│   ├── kraft/           # KRaft consensus protocol
│   ├── log/             # Log storage and segments
│   ├── metadata/        # Cluster metadata management
│   ├── network/         # Network protocol handling
│   ├── storage/         # Storage abstraction
│   └── streams/         # Stream processing (future)
├── examples/            # Example applications
├── scripts/testing/     # Test scripts
├── build/               # Built binaries
├── docs/                # Documentation
└── tools/               # Development and deployment tools
```

## Building

```bash
# Build all components
make build

# Build specific components
go build -o build/gofka-broker cmd/gofka-broker/main.go
go build -o build/gofka-controller cmd/gofka-controller/main.go
```

## Testing

```bash
# Run simple broker test
./scripts/testing/test_gofka_simple.sh

# Run full cluster test
./scripts/testing/test_gofka.sh
```

## Development Workflow

1. Make changes to the code
2. Run tests to ensure functionality
3. Build and test locally
4. Commit changes

## Key Components

- **Broker**: Handles message storage and client requests
- **Controller**: Manages cluster metadata using KRaft consensus
- **Client**: Producer and consumer libraries
- **Storage**: Log-based message storage with segments