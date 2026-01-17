# Quick Start

Get FLEET-Q up and running in minutes with this quick start guide.

## Prerequisites

- FLEET-Q installed (see [Installation](installation.md))
- Snowflake account with appropriate permissions
- Environment configured with Snowflake credentials

## Basic Setup

### 1. Configure Environment

Create a `.env` file with your Snowflake credentials:

```bash
# Required Snowflake connection
FLEET_Q_SNOWFLAKE_ACCOUNT=your-account
FLEET_Q_SNOWFLAKE_USER=your-username
FLEET_Q_SNOWFLAKE_PASSWORD=your-password
FLEET_Q_SNOWFLAKE_DATABASE=your-database
FLEET_Q_SNOWFLAKE_SCHEMA=PUBLIC
FLEET_Q_SNOWFLAKE_WAREHOUSE=COMPUTE_WH
FLEET_Q_SNOWFLAKE_ROLE=ACCOUNTADMIN

# Optional configuration
FLEET_Q_MAX_PARALLELISM=10
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json
```

### 2. Start FLEET-Q

```bash
# Load environment and start
source .env
fleet-q

# Or using Python module
python -m fleet_q.main
```

You should see output similar to:

```
2024-01-01T12:00:00.000Z [INFO] Starting FLEET-Q application
2024-01-01T12:00:00.100Z [INFO] Pod registered: pod-hostname-abc123
2024-01-01T12:00:00.200Z [INFO] Leader elected: pod-hostname-abc123
2024-01-01T12:00:00.300Z [INFO] FastAPI server started on http://0.0.0.0:8000
2024-01-01T12:00:00.400Z [INFO] Background loops started
```

### 3. Verify Installation

Check that FLEET-Q is running:

```bash
# Health check
curl http://localhost:8000/health

# Expected response:
{
  "pod_id": "pod-hostname-abc123",
  "status": "healthy",
  "uptime_seconds": 30.5,
  "current_capacity": 0,
  "max_capacity": 8,
  "is_leader": true,
  "version": "1.0.0"
}
```

## Submit Your First Step

### Using curl

```bash
# Submit a simple step
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{
    "type": "hello_world",
    "args": {"message": "Hello, FLEET-Q!"},
    "metadata": {"user": "quickstart"}
  }'

# Expected response:
{
  "step_id": "step_20240101120000_abc12345",
  "status": "submitted",
  "message": "Step step_20240101120000_abc12345 submitted successfully"
}
```

### Using Python

```python
import httpx

client = httpx.Client(base_url="http://localhost:8000")

# Submit a step
response = client.post("/submit", json={
    "type": "data_processing",
    "args": {"input_file": "data.csv"},
    "metadata": {"user": "alice"},
    "priority": 5
})

step_id = response.json()["step_id"]
print(f"Submitted step: {step_id}")

# Check status
status = client.get(f"/status/{step_id}").json()
print(f"Status: {status['status']}")
```

## Check Step Status

```bash
# Get step status
curl http://localhost:8000/status/step_20240101120000_abc12345

# Expected response:
{
  "step_id": "step_20240101120000_abc12345",
  "status": "completed",
  "claimed_by": "pod-hostname-abc123",
  "last_update_ts": "2024-01-01T12:00:30.123456",
  "retry_count": 0,
  "priority": 0,
  "created_ts": "2024-01-01T12:00:00.000000",
  "max_retries": 3
}
```

## Monitor the Queue

### Queue Statistics

```bash
# Get queue stats
curl http://localhost:8000/admin/queue

# Expected response:
{
  "pending_steps": 0,
  "claimed_steps": 0,
  "completed_steps": 1,
  "failed_steps": 0,
  "active_pods": 1,
  "current_leader": "pod-hostname-abc123"
}
```

### Leader Information

```bash
# Get leader info
curl http://localhost:8000/admin/leader

# Expected response:
{
  "current_leader": "pod-hostname-abc123",
  "leader_birth_ts": "2024-01-01T12:00:00.000000",
  "active_pods": 1,
  "last_election_check": "2024-01-01T12:01:00.000000"
}
```

## Multi-Pod Setup

To see FLEET-Q's distributed capabilities, run multiple pods:

### Using Docker Compose

```yaml
# docker-compose.yml
version: '3.8'
services:
  fleet-q-leader:
    image: fleet-q:latest
    ports:
      - "8000:8000"
    environment:
      - FLEET_Q_SNOWFLAKE_ACCOUNT=${FLEET_Q_SNOWFLAKE_ACCOUNT}
      - FLEET_Q_SNOWFLAKE_USER=${FLEET_Q_SNOWFLAKE_USER}
      - FLEET_Q_SNOWFLAKE_PASSWORD=${FLEET_Q_SNOWFLAKE_PASSWORD}
      - FLEET_Q_SNOWFLAKE_DATABASE=${FLEET_Q_SNOWFLAKE_DATABASE}
      - FLEET_Q_POD_ID=pod-leader
      - FLEET_Q_MAX_PARALLELISM=10

  fleet-q-worker-1:
    image: fleet-q:latest
    ports:
      - "8001:8000"
    environment:
      - FLEET_Q_SNOWFLAKE_ACCOUNT=${FLEET_Q_SNOWFLAKE_ACCOUNT}
      - FLEET_Q_SNOWFLAKE_USER=${FLEET_Q_SNOWFLAKE_USER}
      - FLEET_Q_SNOWFLAKE_PASSWORD=${FLEET_Q_SNOWFLAKE_PASSWORD}
      - FLEET_Q_SNOWFLAKE_DATABASE=${FLEET_Q_SNOWFLAKE_DATABASE}
      - FLEET_Q_POD_ID=pod-worker-1
      - FLEET_Q_MAX_PARALLELISM=8

  fleet-q-worker-2:
    image: fleet-q:latest
    ports:
      - "8002:8000"
    environment:
      - FLEET_Q_SNOWFLAKE_ACCOUNT=${FLEET_Q_SNOWFLAKE_ACCOUNT}
      - FLEET_Q_SNOWFLAKE_USER=${FLEET_Q_SNOWFLAKE_USER}
      - FLEET_Q_SNOWFLAKE_PASSWORD=${FLEET_Q_SNOWFLAKE_PASSWORD}
      - FLEET_Q_SNOWFLAKE_DATABASE=${FLEET_Q_SNOWFLAKE_DATABASE}
      - FLEET_Q_POD_ID=pod-worker-2
      - FLEET_Q_MAX_PARALLELISM=6
```

Start the multi-pod setup:

```bash
# Start all pods
docker-compose up -d

# Check leader election
curl http://localhost:8000/admin/leader

# Submit work to any pod
curl -X POST http://localhost:8001/submit \
  -H "Content-Type: application/json" \
  -d '{"type": "test", "args": {"data": "distributed"}}'
```

## Development Mode

For development, you can use SQLite simulation mode:

```bash
# Set simulation mode
export FLEET_Q_STORAGE_MODE=sqlite
export FLEET_Q_SQLITE_DB_PATH=./fleet_q_dev.db

# Start in development mode
fleet-q --dev
```

This runs FLEET-Q with:
- SQLite instead of Snowflake
- Faster intervals for quick iteration
- Debug logging enabled
- Hot reloading (if supported)

## Next Steps

Now that you have FLEET-Q running:

1. **[Learn about Configuration](configuration.md)** - Customize FLEET-Q for your needs
2. **[Explore the API](../api/endpoints.md)** - Understand all available endpoints
3. **[See Examples](../examples/basic-usage.md)** - Learn common usage patterns
4. **[Deploy to Production](../operations/deployment.md)** - Production deployment guide

## Troubleshooting

### Common Issues

**Connection refused on port 8000**
- Check if FLEET-Q is running: `ps aux | grep fleet-q`
- Verify port is not in use: `lsof -i :8000`

**Snowflake connection errors**
- Verify credentials in environment variables
- Check network connectivity to Snowflake
- Ensure user has required permissions

**Steps not processing**
- Check pod capacity: `curl http://localhost:8000/health`
- Verify step format matches expected schema
- Check logs for error messages

For more troubleshooting help, see the [Troubleshooting Guide](../operations/troubleshooting.md).