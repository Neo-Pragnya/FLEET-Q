# Control Plane Quick Reference

## 🚀 Quick Start

### 1. Enable Control Plane

```bash
export FLEET_Q_ENABLE_CONTROL_PLANE=true
export FLEET_Q_CONTROL_PLANE_FLUSH_INTERVAL=20.0
```

### 2. Start FLEET-Q

```bash
uvicorn fleet_q.quickstart.main:app --host 0.0.0.0 --port 8000
```

### 3. Submit Bulk Write

```python
import httpx
import asyncio

async def submit():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/control-plane/write",
            json={
                "writer_type": "snowflake",
                "destination": "MY_TABLE",
                "data": {"col1": "value1", "col2": "value2"},
                "orm_type": "sqlalchemy"
            }
        )
        print(response.json())

asyncio.run(submit())
```

---

## 📋 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/control-plane/write` | POST | Submit bulk write operation |
| `/control-plane/stats` | GET | Get control plane statistics |
| `/control-plane/flush` | POST | Manually trigger flush |
| `/control-plane/maintenance` | POST | Trigger database maintenance |

---

## 🎛️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_ENABLE_CONTROL_PLANE` | `true` | Enable/disable control plane |
| `FLEET_Q_CONTROL_PLANE_FLUSH_INTERVAL` | `20.0` | Flush interval (seconds) |
| `FLEET_Q_CONTROL_PLANE_MAINTENANCE_INTERVAL` | `3600.0` | Maintenance interval (seconds) |
| `FLEET_Q_CONTROL_PLANE_BASE_PATH` | `/tmp/fleetq` | Base path for pod databases |
| `FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE` | `1000` | Max batch size before flush |
| `FLEET_Q_CONTROL_PLANE_MIN_WRITERS` | `1` | Minimum writer pool size |
| `FLEET_Q_CONTROL_PLANE_MAX_WRITERS` | `8` | Maximum writer pool size |

---

## 💾 Writer Types

| Type | Description | Use Case |
|------|-------------|----------|
| `snowflake` | Snowflake database | Data warehouse writes |
| `sharepoint` | SharePoint uploads | Document management |
| `bedrock` | AWS Bedrock | Batch AI inference |
| `s3` | AWS S3 | Object storage |
| `local_db` | Local SQLite | Local caching |

---

## 🔄 Batching Behavior

Operations are grouped by:
1. **Writer Type** (snowflake, sharepoint, etc.)
2. **Destination** (table name, bucket, etc.)
3. **ORM Type** (sqlalchemy, django, raw, etc.)

**Example:**

```
Input: 100 operations
├─ snowflake:EVENTS:sqlalchemy → Batch 1 (60 ops)
├─ snowflake:EVENTS:django → Batch 2 (30 ops)
└─ sharepoint:docs:default → Batch 3 (10 ops)

After 20s: 3 separate batch writes
```

---

## 📊 Monitoring

### Check Status

```bash
curl http://localhost:8000/control-plane/stats | jq
```

### Key Metrics

```bash
# Pending operations
curl http://localhost:8000/control-plane/stats | jq '.buffer_stats.total_buffered'

# Active writers
curl http://localhost:8000/control-plane/stats | jq '.writer_pool.active_workers'

# Queue depth
curl http://localhost:8000/control-plane/stats | jq '.writer_pool.queue_depth'

# Database size
curl http://localhost:8000/control-plane/stats | jq '.storage_stats.database_size_bytes'
```

---

## ⚡ Dynamic Scaling

| Queue Depth | Active Writers | Action |
|-------------|----------------|--------|
| < 50 | 1 | Scale down |
| 50-100 | 1 | Maintain |
| 100-500 | 2-3 | Scale up |
| 500-1000 | 4-5 | Scale up |
| > 1000 | 6-8 (max) | Scale to max |

---

## 🛠️ Troubleshooting

### Operations Not Flushing

```bash
# Check if control plane is running
curl http://localhost:8000/control-plane/stats | jq '.running'

# Manually trigger flush
curl -X POST http://localhost:8000/control-plane/flush
```

### High Queue Depth

```bash
# Check queue depth
curl http://localhost:8000/control-plane/stats | jq '.writer_pool.queue_depth'

# Increase max writers
export FLEET_Q_CONTROL_PLANE_MAX_WRITERS=16
```

### Large Database

```bash
# Check database size
curl http://localhost:8000/control-plane/stats | jq '.storage_stats.database_size_bytes'

# Trigger maintenance
curl -X POST http://localhost:8000/control-plane/maintenance
```

---

## 📦 Request Examples

### Basic Write

```json
{
    "writer_type": "snowflake",
    "destination": "EVENTS_TABLE",
    "data": {
        "event_id": 123,
        "event_type": "user_login",
        "timestamp": "2024-01-01T10:00:00"
    }
}
```

### With ORM Type

```json
{
    "writer_type": "snowflake",
    "destination": "USERS",
    "data": {
        "user_id": 1,
        "name": "Alice"
    },
    "orm_type": "sqlalchemy"
}
```

### With Priority

```json
{
    "writer_type": "sharepoint",
    "destination": "documents/reports",
    "data": {
        "file_name": "report.pdf",
        "content": "base64..."
    },
    "priority": 1
}
```

---

## 🔍 Response Examples

### Write Response

```json
{
    "operation_id": "op_1234567890_a1b2c3d4",
    "status": "queued",
    "message": "Operation queued for bulk processing (will flush in ~20s)"
}
```

### Stats Response

```json
{
    "enabled": true,
    "pod_id": "pod-123",
    "running": true,
    "buffer_stats": {
        "total_buffered": 150,
        "buffer_count": 3,
        "buffers": {
            "snowflake:EVENTS_TABLE:sqlalchemy": 100,
            "sharepoint:documents:default": 30,
            "bedrock:inference:default": 20
        },
        "last_flush_ago": 12.5
    },
    "storage_stats": {
        "pending_operations": 150,
        "completed_operations": 5000,
        "database_size_bytes": 1048576
    },
    "writer_pool": {
        "active_workers": 3,
        "queue_depth": 5
    }
}
```

### Flush Response

```json
{
    "message": "Flushed 3 batches",
    "batches_flushed": 3,
    "total_operations": 150
}
```

---

## 🎯 Best Practices

### 1. Choose Flush Interval

| Use Case | Interval | Rationale |
|----------|----------|-----------|
| Real-time dashboards | 15s | Low latency |
| Batch analytics | 30s | High efficiency |
| Mixed workload | 20s | Balanced (default) |

### 2. Set Batch Size

| Data Type | Batch Size | Rationale |
|-----------|------------|-----------|
| Small records (events) | 5000 | Maximize throughput |
| Medium records (users) | 1000 | Default |
| Large objects (documents) | 100 | Limit memory |

### 3. Configure Writer Pool

| Workload | Min | Max | Rationale |
|----------|-----|-----|-----------|
| Low volume | 1 | 4 | Conserve resources |
| High volume | 2 | 16 | Maximize throughput |
| Bursty | 1 | 8 | Default, balanced |

---

## 🧪 Testing

### Run Examples

```bash
# All examples
python fleet_q/quickstart/example_control_plane.py

# Or run specific examples in Python
python -c "
import asyncio
from example_control_plane import example_basic_bulk_writes
asyncio.run(example_basic_bulk_writes())
"
```

### Load Testing

```bash
# Submit 10,000 operations
for i in {1..10000}; do
  curl -X POST http://localhost:8000/control-plane/write \
    -H "Content-Type: application/json" \
    -d "{\"writer_type\":\"snowflake\",\"destination\":\"TEST_TABLE\",\"data\":{\"id\":$i}}"
done

# Monitor scaling
watch -n 1 'curl -s http://localhost:8000/control-plane/stats | jq ".writer_pool"'
```

---

## 🔗 Related Documentation

- [Full Documentation](CONTROL_PLANE_README.md)
- [Example Usage](example_control_plane.py)
- [Configuration Reference](config.py)
- [Main Application](main.py)

---

## 📞 Common Commands

```bash
# Check if control plane is enabled
curl http://localhost:8000 | jq '.control_plane_enabled'

# Get full statistics
curl http://localhost:8000/control-plane/stats | jq

# Force immediate flush
curl -X POST http://localhost:8000/control-plane/flush

# Run maintenance
curl -X POST http://localhost:8000/control-plane/maintenance

# Submit single write
curl -X POST http://localhost:8000/control-plane/write \
  -H "Content-Type: application/json" \
  -d '{
    "writer_type": "snowflake",
    "destination": "MY_TABLE",
    "data": {"col1": "value1"}
  }'
```

---

## 🎓 Learning Path

1. **Read** [CONTROL_PLANE_README.md](CONTROL_PLANE_README.md) - Full documentation
2. **Run** [example_control_plane.py](example_control_plane.py) - See it in action
3. **Experiment** - Try different configurations
4. **Monitor** - Watch scaling behavior
5. **Customize** - Add your own writers

---

## ⚙️ Architecture Summary

```
FastAPI Worker Thread
    ↓
Control Plane Worker (lifespan managed)
    ↓
┌─────────────────────────┐
│ Write Buffer Manager    │  ← In-memory buffers
│ (groups by type+dest+orm)│
└─────────────────────────┘
    ↓ (flush every 20s or when full)
┌─────────────────────────┐
│ Dynamic Writer Pool     │  ← Auto-scaling workers
│ (1-8 workers)           │
└─────────────────────────┘
    ↓ (persists operations)
┌─────────────────────────┐
│ Pod-Scoped SQLite       │  ← /tmp/fleetq/{pod_id}/local.db
│ (recovery + tracking)   │
└─────────────────────────┘
    ↓ (bulk writes)
┌─────────────────────────┐
│ Destination             │  ← Snowflake, SharePoint, etc.
└─────────────────────────┘
```

---

## 📚 Key Concepts

- **Control Plane**: Designated FastAPI worker managing bulk operations
- **Batching**: Pooling operations for 15-30s before writing
- **ORM-Agnostic**: Groups by ORM type for correct serialization
- **Dynamic Scaling**: Auto-adjusts writers based on queue depth
- **Pod-Scoped**: Each pod has isolated SQLite database
- **Maintenance**: Periodic cleanup of old records

---

Last Updated: 2024
Version: 1.0.0
