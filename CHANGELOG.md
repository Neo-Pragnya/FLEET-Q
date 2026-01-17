# Changelog

All notable changes to FLEET-Q will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release preparation
- Comprehensive documentation with MkDocs
- PyPI package configuration

## [0.1.0] - 2026-01-01

### Added
- Initial FLEET-Q implementation
- Brokerless distributed task queue architecture
- Snowflake-based coordination system
- Leader election and health management
- Atomic step claiming with exponential backoff
- Recovery system for orphaned tasks
- FastAPI REST API endpoints
- Docker container support
- Comprehensive test suite with property-based testing
- SQLite simulation mode for development
- Multi-pod deployment support with docker-compose
- Structured logging and observability
- Configuration via environment variables
- Background service loops (heartbeat, claiming, recovery)
- Idempotency utilities and patterns
- Step lifecycle management
- Capacity-aware task distribution
- Graceful shutdown handling

### Core Features
- **Storage Backends**: Snowflake production storage and SQLite simulation
- **API Endpoints**: Health, submit, status, admin endpoints
- **Background Services**: Heartbeat, claim, execute, and recovery loops
- **Leader Election**: Deterministic leader selection based on eldest pod
- **Recovery System**: Dead pod detection and orphaned task requeuing
- **Backoff Utility**: Reusable exponential backoff with jitter
- **Configuration**: Environment-driven configuration with validation
- **Observability**: Structured logging, metrics, and health checks
- **Testing**: Unit tests, integration tests, and property-based tests
- **Documentation**: Comprehensive guides and API documentation
- **Examples**: Usage examples and step implementation patterns

### Technical Details
- **Python**: 3.9+ support with full type hints
- **Dependencies**: FastAPI, Snowflake connector, SQLite, Pydantic
- **Architecture**: Single-process container with async/await
- **Database Schema**: Two-table design (POD_HEALTH, STEP_TRACKER)
- **Deployment**: Docker, Kubernetes, and EKS ready
- **Monitoring**: Health endpoints and admin interfaces
- **Security**: Non-root container execution, input validation

### Documentation
- Installation and quick start guides
- Architecture overview and design principles
- API reference with examples
- Deployment guides for Docker and Kubernetes
- Operations guides for monitoring and troubleshooting
- Development guides for contributing and testing
- Utility documentation for backoff and storage
- Example implementations and patterns
- Comparison with alternative solutions

[Unreleased]: https://github.com/fleet-q/fleet-q/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/fleet-q/fleet-q/releases/tag/v0.1.0