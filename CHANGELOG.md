# Changelog

All notable changes to Gofka will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2025-11-19

### Added
- CRC32 data integrity validation for all messages
- Prometheus metrics exposure (40+ metrics)
- Structured logging with Zap
- Health check endpoints (/health, /health/live, /health/ready)
- Metrics server on port 8080
- Comprehensive monitoring and observability

### Changed
- Enhanced broker with production monitoring capabilities
- Improved error handling and logging throughout

## [0.2.0] - 2025-11-15

### Added
- Consumer group coordinator with sticky assignments
- Offset manager with leader epoch tracking
- Message compression (gzip, snappy, lz4)
- Message headers support
- Metadata persistence for consumer groups
- Static group membership
- Python client v0.2.0 with async support

### Changed
- Improved replication with ISR tracking
- Enhanced binary protocol efficiency

## [0.1.0] - 2025-11-01

### Added
- Core broker implementation with KRaft consensus
- Binary protocol for client communication
- Basic replication with leader-follower model
- Python client library
- Consumer groups with partition assignment
- Offset management
- Segment-based log storage


