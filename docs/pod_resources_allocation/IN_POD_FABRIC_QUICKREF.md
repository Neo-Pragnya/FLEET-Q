# In-Pod Execution Fabric - Quick Reference

## Module Overview

| Module | Purpose | Key Classes |
|--------|---------|-------------|
| `zeromq_utils.py` | ZeroMQ messaging patterns | `ZMQRouter`, `ZMQDealer`, `ZMQPush`, `ZMQPull` |
| `sqlite_outbox.py` | Durable side effects | `SQLiteOutbox`, `StepUpdate`, `ResultIntent` |
| `apscheduler_utils.py` | Time-based triggers | `APSchedulerManager`, `CommonJobs` |
| `iohub.py` | Central coordinator | `IOHub`, `IOHubClient`, `AIMDConfig` |
| `control_plane_runner.py` | Orchestrator | `ControlPlaneRunner`, `ControlPlaneConfig` |

## Common Patterns

### 1. Start Control Plane Runner

```python
from fleet_q.control_plane_runner import ControlPlaneRunner, ControlPlaneConfig

config = ControlPlaneConfig(pod_id="my-pod")
runner = ControlPlaneRunner(config)
await runner.run_forever()
```

### 2. Worker with IOHub Client

```python
from fleet_q.iohub import IOHubClient

client = IOHubClient(iohub_address, worker_id="worker-1")

# Request permit
if await client.request_permit():
    # Do work
    result = await call_bedrock(...)
    
    # Report success
    await client.report_success(latency=0.5)
    
    # Enqueue result
    await client.enqueue_result(step_id, table_name, data)
```

### 3. Add Scheduled Job

```python
runner.add_custom_job(
    my_task_func,
    job_id="my-job",
    job_type="interval",
    minutes=10
)
```

### 4. FastAPI Integration

```python
from fastapi import FastAPI
from fleet_q.control_plane_runner import ControlPlaneRunner

@asynccontextmanager
async def lifespan(app: FastAPI):
    runner = ControlPlaneRunner(config)
    started = await runner.start()  # Only one worker becomes runner
    yield
    if started:
        await runner.stop()

app = FastAPI(lifespan=lifespan)
```

## ZeroMQ Socket Patterns

| Pattern | Bind Side | Connect Side | Use Case |
|---------|-----------|--------------|----------|
| ROUTER/DEALER | IOHub | Workers | Request/reply with routing |
| PUSH/PULL | Producer | Consumers | Load-balanced pipeline |
| PUB/SUB | Publisher | Subscribers | Broadcast notifications |

## AIMD Parameters

| Parameter | Default | Recommendation |
|-----------|---------|----------------|
| `initial_max_inflight` | 10 | Start conservative (5-10) |
| `max_max_inflight` | 100 | Set based on rate limits |
| `increase_rate` | 1.0 | Don't exceed 2.0 |
| `decrease_factor` | 0.5 | Keep between 0.5-0.75 |
| `success_streak_threshold` | 5 | 3-10 depending on latency |

## Outbox Tables

| Table | What to Flush | Frequency |
|-------|---------------|-----------|
| `outbox_step_updates` | Status transitions | 15-30s |
| `outbox_results` | Final payloads | 15-30s |
| `outbox_sharepoint_ops` | File operations | 30-60s |

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Multiple control planes | Lease not working | Check SQLite file permissions |
| All permits denied | AIMD stuck low | Reset pressure state |
| Outbox growing | Flusher not running | Check scheduler jobs |
| ZeroMQ errors | Address mismatch | Verify IOHub address |

## Environment Variables

```bash
# Required
export POD_ID="my-pod"

# Optional
export FLEET_Q_OUTBOX_DB="/tmp/fleetq_outbox.db"
export FLEET_Q_IOHUB_ADDRESS="ipc:///tmp/fleetq-iohub.ipc"
export FLEET_Q_LEASE_TTL="30"
export FLEET_Q_AIMD_INITIAL="10"
export FLEET_Q_AIMD_MAX="100"
```

## Deployment Checklist

- [ ] Set POD_ID from Kubernetes downward API
- [ ] Mount persistent volume for SQLite outbox
- [ ] Configure lease TTL (≥30s)
- [ ] Set AIMD bounds appropriately
- [ ] Enable metrics logging
- [ ] Test lease failover
- [ ] Monitor outbox growth
- [ ] Set up log aggregation

## Performance Targets

| Metric | Target |
|--------|--------|
| Permit request latency | < 5ms |
| AIMD adjustment | < 100ms |
| Outbox flush (100 items) | 100-500ms |
| Lease renewal | < 50ms |
| Worker startup | 1-2s |
| Graceful shutdown | 5-10s |

## Useful Commands

```bash
# Run demo
python examples/in_pod_fabric_demo.py

# Run FastAPI integration
uvicorn examples.fastapi_integration:app --workers 4

# Check SQLite outbox
sqlite3 /tmp/fleetq_outbox.db "SELECT * FROM outbox_step_updates LIMIT 10;"

# Monitor logs
tail -f /var/log/fleetq/control_plane.log

# Test IOHub connection
python -c "from fleet_q.iohub import IOHubClient; import asyncio; asyncio.run(IOHubClient('ipc:///tmp/fleetq-iohub.ipc', 'test').request_permit())"
```

## Links

- [Complete Guide](IN_POD_FABRIC_GUIDE.md)
- [Integration Example](../examples/in_pod_fabric_demo.py)
- [FastAPI Integration](../examples/fastapi_integration.py)
- [Design Document](pod_resources_allocation/In-Pod-Exection-Fabric.md)
