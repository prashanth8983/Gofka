.PHONY: build clean test broker controller all

# Build variables
BINARY_DIR=build
BROKER_BINARY=$(BINARY_DIR)/gofka-broker
CONTROLLER_BINARY=$(BINARY_DIR)/gofka-controller

# Go variables
GO=go
GOFLAGS=-trimpath

# Default target
all: build

# Create binary directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build broker
broker: $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(BROKER_BINARY) ./cmd/gofka-broker

# Build controller
controller: $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(CONTROLLER_BINARY) ./cmd/gofka-controller

# Build all binaries
build: broker controller

# Run tests
test:
	$(GO) test ./...

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR)

# Install dependencies
deps:
	$(GO) mod download
	$(GO) mod tidy

# Format code
fmt:
	$(GO) fmt ./...

# Run linter
lint:
	$(GO) vet ./...

# Generate protobuf code
proto:
	protoc --go_out=. --go_opt=paths=source_relative api/v1/gofka.proto

# Run broker
run-broker: broker
	./$(BROKER_BINARY)

# Run controller
run-controller: controller
	./$(CONTROLLER_BINARY)

# Help target
help:
	@echo "Available targets:"
	@echo "  build        - Build all binaries"
	@echo "  broker       - Build broker binary"
	@echo "  controller   - Build controller binary"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  deps         - Install dependencies"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  proto        - Generate protobuf code"
	@echo "  run-broker   - Build and run broker"
	@echo "  run-controller - Build and run controller" 