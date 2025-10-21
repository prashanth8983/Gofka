# Gofka Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive unit tests for broker and log packages
- Input validation for broker constructor
- Error handling improvements throughout the codebase
- Test coverage for message production and consumption
- Test coverage for topic creation and management
- Test coverage for log segment management and persistence

### Changed
- Replaced deprecated `ioutil.ReadDir` with `os.ReadDir` in log package
- Improved error messages with more descriptive context
- Enhanced test suite with edge case coverage

### Fixed
- Fixed deprecation warning for `ioutil` package usage
- Improved error handling in broker initialization
- Fixed potential nil pointer dereferences in test scenarios

### Security
- Added input validation to prevent empty or invalid configuration parameters

## [0.2.0] - 2025-01-XX

### Added
- Complete replication system with leader-follower architecture
- Consumer group coordination with heartbeat monitoring
- Offset management with persistence
- Binary protocol implementation for client communication
- Python client library with async support
- gRPC service for inter-broker communication
- KRaft consensus protocol for metadata management
- Multi-broker cluster support
- ISR (In-Sync Replicas) monitoring
- Configurable acknowledgment levels (0, 1, all)
- Auto-topic creation on first produce
- Comprehensive testing infrastructure

### Changed
- Migrated from ZooKeeper to KRaft for metadata management
- Improved network protocol efficiency
- Enhanced error handling and logging

### Fixed
- Consumer partition assignment issues
- Broker startup sequence and error handling
- Memory leaks in long-running processes

## [0.1.0] - 2025-01-XX

### Added
- Initial implementation of Gofka message broker
- Basic produce/consume functionality
- Simple log storage with segment-based architecture
- Network server for client communication
- Basic metadata management
- Initial test suite

### Security
- Basic input validation for message production
- Safe file handling for log segments


