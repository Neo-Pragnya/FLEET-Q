# FLEET-Q

**Federated Leaderless Execution & Elastic Tasking Queue**

[![PyPI version](https://badge.fury.io/py/fleet-q.svg)](https://badge.fury.io/py/fleet-q)
[![Python versions](https://img.shields.io/pypi/pyversions/fleet-q.svg)](https://pypi.org/project/fleet-q/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-fleet--q.readthedocs.io-blue)](https://fleet-q.readthedocs.io)

A brokerless distributed task queue designed specifically for EKS environments where pods cannot communicate directly with each other. FLEET-Q uses Snowflake as the sole coordination mechanism and implements a leaderless architecture with elected recovery coordination.

## 🚀 Key Features

- **🔧 Brokerless Architecture**: No message broker required - uses Snowflake for coordination
- **🏗️ EKS Optimized**: Designed for container environments with network isolation
- **📈 Elastic Scaling**: Pods can join and leave dynamically with automatic load balancing
- **👑 Leader-based Recovery**: Automatic detection and recovery of orphaned tasks
- **⚡ Atomic Operations**: Database-mediated coordination prevents race conditions
- **📊 Comprehensive Observability**: Structured logging and metrics for operational visibility
- **🐳 Container Ready**: Single-process container with proper health checks
- **🔒 Production Ready**: Comprehensive error handling, retries, and monitoring

## 🎯 Perfect For

- **EKS/Kubernetes environments** with pod-to-pod communication restrictions
- **Snowflake-centric architectures** leveraging existing database infrastructure  
- **High-reliability applications** requiring strong consistency and audit trails
- **Enterprise environments** with strict compliance and security requirements

## 📦 Quick Start

### Installation

```bash
pip install fleet-q
```

### Basic Usage

```python
import asyncio
import httpx

async def main():
    # FLEET-Q runs as a service, interact via HTTP API
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit a task
    response = await client.post("/submit", json={
        "type": "data_processing",
        "args": {"input_file": "data.csv"},
        "metadata": {"user": "alice"}
    })
    
    step_id = response.json()["step_id"]
    print(f"Submitted task: {step_id}")
    
    # Check status
    status = await client.get(f"/status/{step_id}")
    print(f"Status: {status.json()}")
    
    await client.aclose()

asyncio.run(main())
```

### Configuration

Configure FLEET-Q entirely through environment variables:

```bash
# Required Snowflake connection
export FLEET_Q_SNOWFLAKE_ACCOUNT="your-account"
export FLEET_Q_SNOWFLAKE_USER="your-user"  
export FLEET_Q_SNOWFLAKE_PASSWORD="your-password"
export FLEET_Q_SNOWFLAKE_DATABASE="your-database"

# Optional configuration
export FLEET_Q_MAX_PARALLELISM="10"
export FLEET_Q_LOG_LEVEL="INFO"
export FLEET_Q_LOG_FORMAT="json"
```

### Running

```bash
# As a command
fleet-q

# Or as a module  
python -m fleet_q.main

# With Docker
docker run -p 8000:8000 --env-file .env fleet-q
```

## 🏗️ Architecture

FLEET-Q implements a unique hybrid architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    EKS Cluster                              │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ Pod 1       │  │ Pod 2       │  │ Pod N       │        │
│  │ (Leader +   │  │ (Worker)    │  │ (Worker)    │        │
│  │  Worker)    │  │             │  │             │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
│         │                 │                 │              │
│         └─────────────────┼─────────────────┘              │
└─────────────────────────────────────────────────────────────┘
                            │
                   ┌─────────────────┐
                   │   Snowflake     │
                   │   Database      │
                   │                 │
                   │ • POD_HEALTH    │
                   │ • STEP_TRACKER  │
                   └─────────────────┘
```

### Core Components

- **Distributed Workers**: All pods claim and execute tasks independently
- **Leader Election**: Automatic leader selection for recovery operations  
- **Database Coordination**: All communication through Snowflake transactions
- **Local Recovery**: Ephemeral SQLite for recovery cycle tracking

## 📚 Documentation

- **[Installation Guide](https://fleet-q.readthedocs.io/en/latest/getting-started/installation/)** - Detailed installation instructions
- **[Quick Start](https://fleet-q.readthedocs.io/en/latest/getting-started/quick-start/)** - Get up and running quickly
- **[Architecture Overview](https://fleet-q.readthedocs.io/en/latest/architecture/overview/)** - Understanding FLEET-Q's design
- **[API Reference](https://fleet-q.readthedocs.io/en/latest/api/endpoints/)** - Complete API documentation
- **[Deployment Guide](https://fleet-q.readthedocs.io/en/latest/operations/deployment/)** - Production deployment
- **[Examples](https://fleet-q.readthedocs.io/en/latest/examples/basic-usage/)** - Practical usage examples

## 🔧 Development

This project uses [uv](https://github.com/astral-sh/uv) for dependency management:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install with all optional dependencies
uv sync --extra all

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=fleet_q

# Format code
uv run black fleet_q tests
uv run isort fleet_q tests

# Type checking
uv run mypy fleet_q
```

## 🆚 Why FLEET-Q?

Traditional task queues like Celery, RQ, and Dramatiq assume:
- Message broker availability (Redis, RabbitMQ)
- Direct pod-to-pod communication
- Additional infrastructure to manage

FLEET-Q is designed for environments where these assumptions don't hold:

| Feature | Traditional Queues | FLEET-Q |
|---------|-------------------|---------|
| **Broker** | Required (Redis/RabbitMQ) | None (uses database) |
| **Networking** | Pod-to-broker + pod-to-pod | Database-only |
| **Infrastructure** | Queue + Broker + Workers | Workers + Database |
| **Failure Recovery** | Broker-dependent | Database-coordinated |
| **Consistency** | Eventually consistent | Strongly consistent |
| **Audit Trail** | Limited | Complete (database) |

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://fleet-q.readthedocs.io/en/latest/development/contributing/) for details.

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

FLEET-Q was inspired by the need for reliable task processing in constrained Kubernetes environments. Special thanks to the teams who provided feedback and requirements that shaped this design.

---

**Ready to get started?** Check out our [Quick Start Guide](https://fleet-q.readthedocs.io/en/latest/getting-started/quick-start/) or explore the [examples](https://fleet-q.readthedocs.io/en/latest/examples/basic-usage/)!