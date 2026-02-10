# FLEET-Q Control Plane Worker

## Overview

The Control Plane Worker is a specialized component that runs within one of the FastAPI uvicorn worker threads and handles **bulk read/write operations** with dynamic scaling. This architecture enables efficient batch processing while maintaining the single-pod model.

## Key Features

### 🎯 **Bulk Operation Management**
- Pools write operations for 15-30 seconds before batch execution
- ORM-agnostic batching (groups by destination and ORM type)
- Supports multiple writers: Snowflake, SharePoint, Bedrock, S3, Local DB

### 📊 **Dynamic Writer Scaling**
- Automatically scales writers based on queue depth
- Configurable min/max worker limits (default: 1-8 workers)
- Intelligent scaling policy:
  - Queue < 100: 1 writer
  - Queue 100-500: 2-3 writers
  - Queue 500-1000: 4-5 writers
  - Queue > 1000: 6-8 writers (max)

### 💾 **Pod-Scoped SQLite Storage**
- Each pod gets isolated database: `/tmp/fleetq/{pod_id}/local.db`
- Automatic directory creation
- Persistent buffer for recovery
- Periodic maintenance and cleanup

### 🔄 **Automatic Flushing**
- Configurable flush intervals (default: 20 seconds)
- Flushes when batch size reaches threshold
- Manual flush API available

### 🧹 **Maintenance & Cleanup**
- Periodic cleanup of old records (default: 1 hour)
- Database optimization
- Statistics tracking

## Architecture

```
FastAPI (uvicorn worker threads)
    ↓
Control Plane Worker (1 designated thread)
    ↓
┌─────────────────────────────────────┐
│   Write Buffer Manager              │
│   ├─ Buffer: snowflake:MY_TABLE     │
│   ├─ Buffer: sharepoint:docs        │
│   └─ Buffer: bedrock:inference      │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│   Dynamic Writer Pool               │
│   ├─ SnowflakeBulkWriter (2 active)│
│   ├─ SharePointBulkWriter (1 active)│
│   └─ BedrockBulkWriter (1 active)   │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│   Pod-Scoped SQLite                 │
│   Path: /tmp/fleetq/{pod_id}/local.db│
│   ├─ write_buffer                   │
│   ├─ batch_history                  │
│   └─ maintenance_log                │
└─────────────────────────────────────┘
```

## Configuration

### Environment Variables

```bash
# Enable/disable control plane
export FLEET_Q_ENABLE_CONTROL_PLANE=true

# Flush interval (seconds) - time to pool operations
export FLEET_Q_CONTROL_PLANE_FLUSH_INTERVAL=20.0

# Maintenance interval (seconds)
export FLEET_Q_CONTROL_PLANE_MAINTENANCE_INTERVAL=3600.0

# Base path for pod SQLite databases
export FLEET_Q_CONTROL_PLANE_BASE_PATH=/tmp/fleetq

# Maximum batch size before forced flush
export FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE=1000

# Writer pool scaling limits
export FLEET_Q_CONTROL_PLANE_MIN_WRITERS=1
export FLEET_Q_CONTROL_PLANE_MAX_WRITERS=8
```

### Configuration in Code

```python
from fleet_q.quickstart.config import FleetQConfig, load_config

config = load_config()

# Control plane settings
print(f"Control Plane: {config.enable_control_plane}")
print(f"Flush Interval: {config.control_plane_flush_interval}s")
print(f"Base Path: {config.control_plane_base_path}")
```

## Usage

### 1. Submit Bulk Write Operations

```python
import httpx
import asyncio

async def submit_bulk_writes():
    """Submit multiple write operations for bulk processing"""
    
    async with httpx.AsyncClient() as client:
        # Submit operations to different destinations
        operations = [
            {
                "writer_type": "snowflake",
                "destination": "EVENTS_TABLE",
                "data": {"event": "user_login", "user_id": 123, "timestamp": "2024-01-01T10:00:00"},
                "orm_type": "sqlalchemy",
                "priority": 1
            },
            {
                "writer_type": "snowflake",
                "destination": "EVENTS_TABLE",
                "data": {"event": "user_logout", "user_id": 123, "timestamp": "2024-01-01T11:00:00"},
                "orm_type": "sqlalchemy",
                "priority": 1
            },
            {
                "writer_type": "sharepoint",
                "destination": "documents/reports",
                "data": {"file_name": "report.pdf", "content": "base64..."},
                "priority": 0
            }
        ]
        
        for op in operations:
            response = await client.post(
                "http://localhost:8000/control-plane/write",
                json=op
            )
            result = response.json()
            print(f"Submitted: {result['operation_id']} - {result['message']}")

# Run
asyncio.run(submit_bulk_writes())
```

### 2. Check Control Plane Statistics

```python
import httpx
import asyncio

async def get_stats():
    """Get control plane statistics"""
    
    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8000/control-plane/stats")
        stats = response.json()
        
        print(f"Pod ID: {stats['pod_id']}")
        print(f"Running: {stats['running']}")
        print(f"\nBuffer Stats:")
        print(f"  Total Buffered: {stats['buffer_stats']['total_buffered']}")
        print(f"  Buffer Count: {stats['buffer_stats']['buffer_count']}")
        print(f"  Last Flush: {stats['buffer_stats']['last_flush_ago']:.1f}s ago")
        print(f"\nWriter Pool:")
        print(f"  Active Workers: {stats['writer_pool']['active_workers']}")
        print(f"  Queue Depth: {stats['writer_pool']['queue_depth']}")
        print(f"\nStorage Stats:")
        print(f"  Pending Operations: {stats['storage_stats']['pending_operations']}")
        print(f"  Completed Operations: {stats['storage_stats']['completed_operations']}")
        print(f"  Database Size: {stats['storage_stats']['database_size_bytes']} bytes")

asyncio.run(get_stats())
```

### 3. Manual Flush

```python
import httpx
import asyncio

async def trigger_flush():
    """Manually trigger flush of all buffers"""
    
    async with httpx.AsyncClient() as client:
        response = await client.post("http://localhost:8000/control-plane/flush")
        result = response.json()
        
        print(f"Message: {result['message']}")
        print(f"Batches Flushed: {result['batches_flushed']}")
        print(f"Total Operations: {result['total_operations']}")

asyncio.run(trigger_flush())
```

### 4. Database Maintenance

```python
import httpx
import asyncio

async def trigger_maintenance():
    """Manually trigger database maintenance"""
    
    async with httpx.AsyncClient() as client:
        response = await client.post("http://localhost:8000/control-plane/maintenance")
        result = response.json()
        
        print(f"Message: {result['message']}")
        print(f"Storage Stats: {result['storage_stats']}")

asyncio.run(trigger_maintenance())
```

## API Endpoints

### POST /control-plane/write
Submit a write operation for bulk processing.

**Request Body:**
```json
{
    "writer_type": "snowflake",
    "destination": "MY_TABLE",
    "data": {"col1": "value1", "col2": "value2"},
    "orm_type": "sqlalchemy",
    "priority": 1
}
```

**Response:**
```json
{
    "operation_id": "op_1234567890_a1b2c3d4",
    "status": "queued",
    "message": "Operation queued for bulk processing (will flush in ~20s)"
}
```

### GET /control-plane/stats
Get control plane statistics.

**Response:**
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
        "destination_counts": {
            "EVENTS_TABLE": 100,
            "documents": 30,
            "inference": 20
        },
        "database_size_bytes": 1048576,
        "database_path": "/tmp/fleetq/pod-123/local.db"
    },
    "writer_pool": {
        "active_workers": 3,
        "queue_depth": 5
    }
}
```

### POST /control-plane/flush
Manually trigger flush of all buffers.

**Response:**
```json
{
    "message": "Flushed 3 batches",
    "batches_flushed": 3,
    "total_operations": 150
}
```

### POST /control-plane/maintenance
Manually trigger database maintenance.

**Response:**
```json
{
    "message": "Maintenance completed successfully",
    "storage_stats": {
        "pending_operations": 150,
        "completed_operations": 4500,
        "database_size_bytes": 524288
    }
}
```

## ORM-Agnostic Batching

The control plane groups operations by three keys:
1. **Writer Type**: snowflake, sharepoint, bedrock, etc.
2. **Destination**: table name, bucket, endpoint, etc.
3. **ORM Type**: sqlalchemy, django, peewee, raw, etc. (optional)

### Example: SQLAlchemy Operations

```python
# All these will be batched together
operations = [
    {
        "writer_type": "snowflake",
        "destination": "USERS",
        "orm_type": "sqlalchemy",
        "data": {"user_id": 1, "name": "Alice"}
    },
    {
        "writer_type": "snowflake",
        "destination": "USERS",
        "orm_type": "sqlalchemy",
        "data": {"user_id": 2, "name": "Bob"}
    }
]
# After 15-30 seconds: Batch insert to Snowflake USERS table
```

### Example: Mixed ORM Types

```python
# These will be in separate batches
operations = [
    # Batch 1: SQLAlchemy → USERS
    {
        "writer_type": "snowflake",
        "destination": "USERS",
        "orm_type": "sqlalchemy",
        "data": {"user_id": 1, "name": "Alice"}
    },
    # Batch 2: Django → USERS
    {
        "writer_type": "snowflake",
        "destination": "USERS",
        "orm_type": "django",
        "data": {"user_id": 2, "name": "Bob"}
    }
]
```

## Scaling Behavior

### Queue-Based Scaling

The control plane monitors queue depth and automatically scales writers:

```
Queue Depth    │ Active Writers │ Scale Action
───────────────┼────────────────┼──────────────
< 50           │ 1              │ Scale down
50-100         │ 1              │ Maintain
100-500        │ 2-3            │ Scale up
500-1000       │ 4-5            │ Scale up
> 1000         │ 6-8 (max)      │ Scale up to max
```

### Scaling Example

```python
# Initial state: 1 writer, queue depth = 0
await submit_bulk_write(...)  # Queue depth = 1
# Still 1 writer

# Submit 150 operations
for i in range(150):
    await submit_bulk_write(...)
# Queue depth = 150 → Scale up to 2-3 writers

# Submit 1000 more operations
for i in range(1000):
    await submit_bulk_write(...)
# Queue depth = 1150 → Scale up to 8 writers (max)

# Wait for processing...
# Queue depth decreases → Gradually scale down
```

## Pod-Scoped Storage

### Directory Structure

```
/tmp/fleetq/
├── pod-001/
│   └── local.db          # Pod 001's database
├── pod-002/
│   └── local.db          # Pod 002's database
└── pod-003/
    └── local.db          # Pod 003's database
```

### Database Schema

```sql
-- Write buffer (pending operations)
CREATE TABLE write_buffer (
    operation_id TEXT PRIMARY KEY,
    writer_type TEXT NOT NULL,
    destination TEXT NOT NULL,
    orm_type TEXT,
    data TEXT NOT NULL,
    priority INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    status TEXT DEFAULT 'pending'
);

-- Batch history (completed batches)
CREATE TABLE batch_history (
    batch_id TEXT PRIMARY KEY,
    writer_type TEXT NOT NULL,
    destination TEXT NOT NULL,
    orm_type TEXT,
    operation_count INTEGER NOT NULL,
    created_at REAL NOT NULL,
    completed_at REAL,
    status TEXT DEFAULT 'pending',
    error TEXT
);

-- Maintenance log
CREATE TABLE maintenance_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    timestamp REAL NOT NULL,
    details TEXT
);
```

## Implementing Custom Writers

### Example: Custom S3 Writer

```python
from control_plane import BulkWriter, WriterType, WriteBatch

class S3BulkWriter(BulkWriter):
    """Bulk writer for S3"""
    
    def __init__(self, boto3_client):
        super().__init__(WriterType.S3)
        self.s3_client = boto3_client
    
    async def write_batch(self, batch: WriteBatch) -> bool:
        """Write batch to S3"""
        try:
            logger.info(f"Writing batch {batch.batch_id} to S3 bucket {batch.destination}")
            
            # Batch upload using S3 multipart or batch API
            for operation in batch.operations:
                key = operation.data.get('key')
                body = operation.data.get('body')
                
                self.s3_client.put_object(
                    Bucket=batch.destination,
                    Key=key,
                    Body=body
                )
            
            self.operations_completed += batch.size()
            return True
            
        except Exception as e:
            logger.error(f"Failed to write batch {batch.batch_id}: {e}")
            self.operations_failed += batch.size()
            return False
```

### Register Custom Writer

```python
# In control_plane.py DynamicWriterPool.writer_worker()
writers = {
    WriterType.SNOWFLAKE: SnowflakeBulkWriter(storage_conn),
    WriterType.SHAREPOINT: SharePointBulkWriter(),
    WriterType.BEDROCK: BedrockBulkWriter(),
    WriterType.S3: S3BulkWriter(boto3_client),  # Add custom writer
}
```

## Monitoring

### Health Checks

```bash
# Check control plane status
curl http://localhost:8000/control-plane/stats | jq '.running'

# Check buffer size
curl http://localhost:8000/control-plane/stats | jq '.buffer_stats.total_buffered'

# Check active writers
curl http://localhost:8000/control-plane/stats | jq '.writer_pool.active_workers'
```

### Prometheus Metrics (Future Enhancement)

```python
# Example metrics to add:
control_plane_operations_total{writer_type="snowflake", status="success"}
control_plane_operations_total{writer_type="snowflake", status="failed"}
control_plane_buffer_size{destination="EVENTS_TABLE", orm_type="sqlalchemy"}
control_plane_active_writers
control_plane_queue_depth
control_plane_flush_duration_seconds
```

## Best Practices

### 1. Choose Appropriate Flush Interval
- **Fast writes (15s)**: Real-time dashboards, notifications
- **Balanced (20s)**: Default for most use cases
- **Efficient (30s)**: Batch analytics, large datasets

### 2. Set Batch Size Limits
```python
# For large objects (documents, images)
FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE=100

# For small records (events, logs)
FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE=5000
```

### 3. Configure Writer Pool
```python
# For high throughput
FLEET_Q_CONTROL_PLANE_MIN_WRITERS=2
FLEET_Q_CONTROL_PLANE_MAX_WRITERS=16

# For low throughput
FLEET_Q_CONTROL_PLANE_MIN_WRITERS=1
FLEET_Q_CONTROL_PLANE_MAX_WRITERS=4
```

### 4. Monitor Queue Depth
```python
import asyncio

async def monitor_queue():
    while True:
        stats = await get_control_plane_stats()
        queue_depth = stats['writer_pool']['queue_depth']
        
        if queue_depth > 1000:
            print(f"⚠️  High queue depth: {queue_depth}")
        
        await asyncio.sleep(60)
```

### 5. Handle Failures
```python
# Check batch history for failures
SELECT * FROM batch_history WHERE status = 'failed';

# Retry failed batches
SELECT * FROM write_buffer WHERE status = 'pending' AND created_at < ?;
```

## Troubleshooting

### Issue: Operations Not Flushing

**Symptoms:** Operations queued but never written

**Check:**
```python
# 1. Is control plane running?
stats = await get_control_plane_stats()
print(stats['running'])  # Should be True

# 2. Check buffer size
print(stats['buffer_stats']['total_buffered'])

# 3. Check last flush time
print(stats['buffer_stats']['last_flush_ago'])
```

**Fix:**
```bash
# Manually trigger flush
curl -X POST http://localhost:8000/control-plane/flush
```

### Issue: High Queue Depth

**Symptoms:** Queue depth keeps growing

**Check:**
```python
# Check active writers
stats = await get_control_plane_stats()
print(stats['writer_pool']['active_workers'])
print(stats['writer_pool']['queue_depth'])
```

**Fix:**
```bash
# Increase max writers
export FLEET_Q_CONTROL_PLANE_MAX_WRITERS=16

# Restart application
```

### Issue: Database Growing Too Large

**Symptoms:** SQLite file size > 1GB

**Check:**
```python
stats = await get_control_plane_stats()
print(stats['storage_stats']['database_size_bytes'] / 1024 / 1024)  # MB
```

**Fix:**
```bash
# Trigger maintenance
curl -X POST http://localhost:8000/control-plane/maintenance

# Reduce cleanup age
export FLEET_Q_CONTROL_PLANE_MAINTENANCE_INTERVAL=1800  # 30 minutes
```

## Performance Considerations

### Throughput

With default settings:
- **15-30s flush interval**: ~1000-5000 ops/batch
- **8 max writers**: ~8000-40000 ops/minute
- **Multiple pods**: Linear scaling

### Latency

- **Minimum latency**: `flush_interval` (15-30s)
- **Maximum latency**: `flush_interval + batch_write_time`
- **Typical**: 20-25 seconds for most operations

### Memory Usage

- **Per operation**: ~1KB (in-memory + SQLite)
- **1000 operations**: ~1MB
- **10000 operations**: ~10MB

## Future Enhancements

1. **Bulk Read Operations**: Similar batching for reads
2. **Priority Queues**: Separate queues for different priorities
3. **Compression**: Compress data in SQLite
4. **Distributed Coordination**: ZooKeeper/etcd for multi-pod control plane
5. **Streaming Writes**: Support for streaming large datasets
6. **Dead Letter Queue**: Failed operations handling
7. **Metrics Export**: Prometheus/OpenTelemetry integration
8. **Web UI**: Dashboard for monitoring control plane

## Summary

The Control Plane Worker provides:
- ✅ Bulk operation batching with configurable pooling windows
- ✅ ORM-agnostic grouping for efficient writes
- ✅ Dynamic writer scaling based on demand
- ✅ Pod-scoped SQLite for isolated storage
- ✅ Automatic maintenance and cleanup
- ✅ RESTful API for operations and monitoring
- ✅ Support for multiple writer types (Snowflake, SharePoint, Bedrock, etc.)

This enables efficient bulk processing within a single-pod architecture that scales horizontally across multiple pods.
