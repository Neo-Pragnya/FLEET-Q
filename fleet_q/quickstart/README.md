# FLEET-Q Quickstart Implementation

This quickstart implementation demonstrates the complete FLEET-Q (Federated Leaderless Execution & Elastic Tasking Queue) system - a distributed task queue that uses only Snowflake for coordination, without requiring traditional message brokers like Redis or RabbitMQ.

## 🎯 What is FLEET-Q?

FLEET-Q is a distributed task queue designed for environments where:
- **Pods cannot communicate directly** (no pod-to-pod networking)
- **Only Snowflake is available** as a shared coordination store
- **Tasks must be resilient** to worker failures
- **Automatic scaling** is needed as workers join/leave

### Key Features

✅ **Brokerless Architecture** - Uses only Snowflake for coordination  
✅ **Atomic Task Claiming** - Prevents duplicate processing via transactions  
✅ **Elastic Capacity** - Workers auto-scale based on available capacity  
✅ **Leader-Assisted Recovery** - Automatically recovers orphaned tasks  
✅ **Exponential Backoff** - Handles contention gracefully  
✅ **Dead Letter Queue** - Tracks failed tasks with retry policies

## 📦 Files Overview

```
quickstart/
├── schema.sql          # Snowflake table schemas (POD_HEALTH, STEP_TRACKER)
├── config.py           # Configuration management (env vars)
├── backoff.py          # Exponential backoff decorator (reusable)
├── storage.py          # Snowflake + SQLite storage abstractions
├── queue.py            # Core queue operations (submit, claim, complete, fail)
├── worker.py           # Worker loops (heartbeat, claim, execute)
├── leader.py           # Leader election and recovery logic
├── main.py             # FastAPI application (entry point)
├── raquel_patterns.py  # Raquel-inspired design patterns (reference)
└── README.md           # This file
```

## 🏗️ Architecture Overview

### Core Components

1. **Global Snowflake Tables**
   - `POD_HEALTH`: Tracks pod heartbeats and leader election
   - `STEP_TRACKER`: The global task queue

2. **Worker Loops** (runs on all pods)
   - **Heartbeat Loop**: Keeps pod alive in `POD_HEALTH`
   - **Claim Loop**: Atomically claims pending tasks based on capacity
   - **Execute Loop**: Processes claimed tasks asynchronously

3. **Leader Recovery** (runs only on leader pod)
   - Detects dead pods via stale heartbeats
   - Finds orphaned tasks (claimed by dead pods)
   - Builds ephemeral local DLQ (SQLite)
   - Resubmits eligible tasks (respecting retry limits)
   - Drops local DLQ after processing

### Task Lifecycle

```
┌─────────┐
│ pending │ ───> Worker claims atomically
└─────────┘
     │
     v
┌─────────┐
│ claimed │ ───> Worker executes task
└─────────┘
     │
     ├──> Success ──> ┌───────────┐
     │                │ completed │
     │                └───────────┘
     │
     └──> Failure ──> ┌────────┐
                      │ failed │
                      └────────┘
                           │
                           v
                      Leader recovery
                      (requeue or terminal)
```

**Note**: See [raquel_patterns.py](raquel_patterns.py) for detailed examples of how FLEET-Q implements Raquel-inspired design patterns including transaction-centered correctness, clean API design, and retry semantics.

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Snowflake account with database/schema/warehouse access
- Environment variables configured (see below)

### 1. Set Up Snowflake Schema

Run the SQL commands in `schema.sql` to create the required tables:

```sql
-- In your Snowflake console
USE DATABASE your_database;
USE SCHEMA your_schema;

-- Run schema.sql contents
```

### 2. Install Dependencies

```bash
pip install fastapi uvicorn snowflake-connector-python pydantic
```

### 3. Configure Environment Variables

Create a `.env` file or export these variables:

```bash
# Required
export FLEET_Q_POD_ID="pod-001"  # Unique identifier for this pod
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
export SNOWFLAKE_WAREHOUSE="your_warehouse"

# Optional (with defaults)
export FLEET_Q_HEARTBEAT_INTERVAL="10"       # seconds
export FLEET_Q_CLAIM_INTERVAL="5"            # seconds
export FLEET_Q_RECOVERY_INTERVAL="30"        # seconds
export FLEET_Q_MAX_PARALLELISM="8"           # max concurrent tasks
export FLEET_Q_CAPACITY_THRESHOLD="0.8"      # claim up to 80% capacity
export FLEET_Q_DEAD_POD_THRESHOLD="60"       # seconds before pod considered dead
export FLEET_Q_MAX_RETRIES="3"               # max retry attempts per task
```

### 4. Run the Application

```bash
python main.py
```

Or with uvicorn:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## 📡 API Endpoints

### Submit a Task

```bash
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {
      "task_type": "process_data",
      "file": "data.csv"
    },
    "priority": 1
  }'
```

Response:
```json
{
  "step_id": "step-abc123...",
  "status": "submitted"
}
```

### Check Task Status

```bash
curl http://localhost:8000/status/step-abc123...
```

Response:
```json
{
  "step_id": "step-abc123...",
  "status": "completed",
  "claimed_by": "pod-001",
  "payload": {"task_type": "process_data", "file": "data.csv"},
  "retry_count": 0,
  "priority": 1,
  "created_ts": "2026-01-17 12:34:56",
  "last_update_ts": "2026-01-17 12:35:01"
}
```

### Get Queue Statistics

```bash
curl http://localhost:8000/admin/queue
```

Response:
```json
{
  "pending": 10,
  "claimed": 5,
  "completed": 100,
  "failed": 2
}
```

### Get Leader Information

```bash
curl http://localhost:8000/admin/leader
```

Response:
```json
{
  "leader_pod_id": "pod-001",
  "this_pod_id": "pod-002",
  "is_leader": false,
  "total_alive_pods": 3,
  "alive_pods": ["pod-001", "pod-002", "pod-003"]
}
```

### Manually Trigger Recovery (Leader Only)

```bash
curl -X POST http://localhost:8000/admin/recovery/run
```

Response:
```json
{
  "message": "Recovery cycle completed successfully",
  "stats": {
    "dead_pods": 1,
    "total_orphans": 5,
    "resubmitted": 4,
    "terminal_failures": 1
  }
}
```

## 🔧 Customizing Task Execution

Edit the `execute_task()` function in `main.py` to implement your custom task logic:

```python
def execute_task(step: Dict[str, Any]) -> Any:
    """Custom task execution logic"""
    step_id = step['STEP_ID']
    payload = step['PAYLOAD']
    
    task_type = payload.get('task_type')
    
    if task_type == 'my_custom_task':
        # Your custom logic here
        result = do_something(payload)
        return result
    
    # ... handle other task types ...
```

## 🔄 Using the Exponential Backoff Decorator

The `backoff.py` module provides a powerful, reusable exponential backoff decorator that you can apply to **any function** - not just queue operations. It's designed to be a general-purpose retry mechanism for handling transient failures.

### Basic Usage

Apply the `@with_backoff` decorator to any function that might fail:

```python
from backoff import with_backoff

@with_backoff(max_attempts=3, base_delay_ms=100)
def unreliable_api_call():
    response = requests.get("https://api.example.com/data")
    response.raise_for_status()
    return response.json()
```

### Configuration Options

The decorator is highly configurable:

```python
@with_backoff(
    max_attempts=5,              # Maximum number of retry attempts
    base_delay_ms=50,            # Base delay in milliseconds
    max_delay_ms=5000,           # Maximum delay cap in milliseconds
    jitter=True,                 # Apply full jitter to delays
    retry_on=(ConnectionError, TimeoutError),  # Only retry on specific exceptions
    on_retry=my_callback_func,   # Callback function on each retry
    timeout_total_s=30.0         # Total timeout for all attempts
)
def my_function():
    # Your code here
    pass
```

### Real-World Examples

#### Example 1: Database Connections with Backoff

```python
from backoff import with_backoff
import psycopg2

@with_backoff(
    max_attempts=5,
    base_delay_ms=100,
    retry_on=(psycopg2.OperationalError,),
    on_retry=lambda attempt, delay, exc: print(f"DB connection failed, retry {attempt}")
)
def connect_to_database():
    """Connect to PostgreSQL with automatic retries"""
    return psycopg2.connect(
        host="db.example.com",
        database="mydb",
        user="user",
        password="password"
    )

# Usage
conn = connect_to_database()  # Automatically retries on connection failure
```

#### Example 2: External API Calls

```python
import requests
from backoff import with_backoff

@with_backoff(
    max_attempts=3,
    base_delay_ms=200,
    max_delay_ms=2000,
    retry_on=(requests.exceptions.RequestException,)
)
def fetch_user_data(user_id: str):
    """Fetch user data from external API with retries"""
    response = requests.get(f"https://api.example.com/users/{user_id}")
    response.raise_for_status()
    return response.json()

# Usage
user = fetch_user_data("user-123")
```

#### Example 3: Async Functions (FastAPI, AsyncIO)

The decorator works seamlessly with async functions:

```python
import httpx
from backoff import with_backoff

@with_backoff(
    max_attempts=5,
    base_delay_ms=100,
    retry_on=(httpx.RequestError, httpx.HTTPStatusError)
)
async def async_api_call(url: str):
    """Async HTTP request with automatic retries"""
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

# Usage in async context
data = await async_api_call("https://api.example.com/data")
```

#### Example 4: File Operations with Retry

```python
from backoff import with_backoff
import os

@with_backoff(
    max_attempts=3,
    base_delay_ms=500,
    retry_on=(IOError, OSError)
)
def write_to_network_drive(filepath: str, data: str):
    """Write to network drive with retries on I/O errors"""
    with open(filepath, 'w') as f:
        f.write(data)
    return True

# Usage
write_to_network_drive("/mnt/network/data.txt", "Important data")
```

### Custom Retry Logic with Callbacks

Use the `on_retry` callback to implement custom behavior on each retry:

```python
from backoff import with_backoff
import logging

logger = logging.getLogger(__name__)

def log_retry(attempt: int, delay: float, exception: Exception):
    """Custom callback for retry events"""
    logger.warning(
        f"Retry attempt {attempt} after {delay:.2f}s due to: {exception}"
    )
    # Could also:
    # - Send metrics to monitoring system
    # - Update progress bar
    # - Send alerts after N attempts
    # - Log to external service

@with_backoff(
    max_attempts=5,
    base_delay_ms=100,
    on_retry=log_retry
)
def critical_operation():
    # Your code here
    pass
```

### Extending the Backoff Strategy

#### Custom Backoff Algorithm

You can extend the backoff module with custom delay calculations:

```python
# In backoff.py, add a new strategy
def calculate_delay_linear(attempt: int, config: BackoffConfig) -> float:
    """Linear backoff instead of exponential"""
    delay_ms = min(config.base_delay_ms * (attempt + 1), config.max_delay_ms)
    if config.jitter:
        delay_ms = random.uniform(0, delay_ms)
    return delay_ms / 1000.0

def calculate_delay_fibonacci(attempt: int, config: BackoffConfig) -> float:
    """Fibonacci backoff sequence"""
    def fib(n):
        if n <= 1:
            return n
        return fib(n-1) + fib(n-2)
    
    delay_ms = min(config.base_delay_ms * fib(attempt), config.max_delay_ms)
    if config.jitter:
        delay_ms = random.uniform(0, delay_ms)
    return delay_ms / 1000.0
```

#### Custom Retry Predicate

Create a decorator with custom retry logic:

```python
from backoff import with_backoff

def should_retry_on_status(exception: Exception) -> bool:
    """Custom predicate: only retry on specific HTTP status codes"""
    if isinstance(exception, requests.exceptions.HTTPError):
        # Retry on 429 (rate limit), 500, 502, 503, 504
        return exception.response.status_code in [429, 500, 502, 503, 504]
    return True

@with_backoff(
    max_attempts=5,
    base_delay_ms=1000,
    retry_on=requests.exceptions.HTTPError
)
def smart_api_call(url: str):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
```

#### Conditional Backoff Strategies

Apply different backoff strategies based on context:

```python
from backoff import with_backoff
from functools import partial

# Define different backoff profiles
aggressive_backoff = partial(
    with_backoff,
    max_attempts=10,
    base_delay_ms=50,
    max_delay_ms=1000
)

conservative_backoff = partial(
    with_backoff,
    max_attempts=3,
    base_delay_ms=1000,
    max_delay_ms=10000
)

# Apply based on operation criticality
@aggressive_backoff()
def high_priority_task():
    """Retry quickly and frequently"""
    pass

@conservative_backoff()
def low_priority_task():
    """Retry slowly with longer delays"""
    pass
```

### Integration with FLEET-Q

FLEET-Q already uses the backoff decorator internally for queue operations. You can also use it in your custom task executors:

```python
from backoff import with_backoff
from typing import Dict, Any

@with_backoff(
    max_attempts=3,
    base_delay_ms=500,
    retry_on=(ConnectionError, TimeoutError)
)
def execute_task(step: Dict[str, Any]) -> Any:
    """Task executor with automatic retries for transient failures"""
    step_id = step['STEP_ID']
    payload = step['PAYLOAD']
    
    # Call external service
    result = external_service.process(payload)
    return result
```

### Best Practices

1. **Choose Appropriate Exception Types**: Only retry on transient failures
   ```python
   # Good - specific transient errors
   @with_backoff(retry_on=(ConnectionError, TimeoutError))
   
   # Bad - might retry on permanent failures
   @with_backoff(retry_on=Exception)
   ```

2. **Set Reasonable Timeouts**: Prevent infinite retry loops
   ```python
   @with_backoff(max_attempts=5, timeout_total_s=30.0)
   ```

3. **Use Jitter**: Prevents thundering herd in distributed systems
   ```python
   @with_backoff(jitter=True)  # Always recommended
   ```

4. **Log Retry Attempts**: Aid debugging with callbacks
   ```python
   @with_backoff(on_retry=lambda a, d, e: logger.warning(f"Retry {a}: {e}"))
   ```

5. **Test Backoff Behavior**: Verify retry logic under failure conditions
   ```python
   # Test that function retries and eventually succeeds/fails
   mock_service.side_effect = [ConnectionError(), ConnectionError(), {"status": "ok"}]
   result = my_retryable_function()
   assert result == {"status": "ok"}
   ```

### Performance Considerations

- **Exponential backoff** is ideal for distributed systems to reduce contention
- **Linear backoff** works well for rate-limited APIs
- **Base delay** should be low enough for responsiveness but high enough to let issues resolve
- **Max delay** prevents waiting too long on persistent failures
- **Jitter** is crucial in distributed systems to prevent synchronized retries

## 🧪 Testing Locally

### Simulate Multiple Pods

Run multiple instances with different pod IDs:

```bash
# Terminal 1
export FLEET_Q_POD_ID="pod-001"
python main.py

# Terminal 2
export FLEET_Q_POD_ID="pod-002"
python main.py --port 8001

# Terminal 3
export FLEET_Q_POD_ID="pod-003"
python main.py --port 8002
```

### Submit Test Tasks

```bash
# Submit 10 test tasks
for i in {1..10}; do
  curl -X POST http://localhost:8000/submit \
    -H "Content-Type: application/json" \
    -d "{\"payload\": {\"task_type\": \"example_task\", \"id\": $i}}"
done
```

### Monitor Queue

```bash
# Watch queue stats
watch -n 2 'curl -s http://localhost:8000/admin/queue | jq'
```

## 🎯 How It Works

### 1. Workers Claim Tasks Atomically

Workers use Snowflake's MERGE statement to atomically claim pending tasks:

```sql
MERGE INTO STEP_TRACKER AS target
USING (
    SELECT step_id
    FROM STEP_TRACKER
    WHERE status = 'pending'
    ORDER BY priority DESC, created_ts ASC
    LIMIT 5
) AS source
ON target.step_id = source.step_id
WHEN MATCHED AND target.status = 'pending' THEN
    UPDATE SET 
        status = 'claimed',
        claimed_by = 'pod-001',
        last_update_ts = CURRENT_TIMESTAMP()
```

This ensures no two workers claim the same task.

### 2. Leader Election is Deterministic

The leader is always the **oldest alive pod** (earliest `birth_ts`):

```sql
SELECT pod_id
FROM POD_HEALTH
WHERE status = 'up'
AND last_heartbeat_ts > CURRENT_TIMESTAMP() - INTERVAL '60 seconds'
ORDER BY birth_ts ASC
LIMIT 1
```

All pods agree on the leader without direct communication.

### 3. Recovery Handles Dead Pods

When the leader detects a dead pod:

1. Finds orphaned tasks (claimed by dead pod)
2. Creates local SQLite DLQ table
3. Applies retry policy (check `retry_count` vs `max_retries`)
4. Resubmits eligible tasks to `pending`
5. Marks terminal failures as `failed`
6. Drops local DLQ table

### 4. Exponential Backoff Prevents Stampedes

When claim conflicts occur (multiple workers trying to claim same tasks), the backoff decorator automatically retries with increasing delays:

```
Attempt 1: 50ms delay
Attempt 2: 100-150ms delay (with jitter)
Attempt 3: 200-300ms delay (with jitter)
Attempt 4: 400-650ms delay (with jitter)
Attempt 5: 800-1300ms delay (with jitter)
```

## 🔍 Monitoring & Observability

### Logging

All components log to stdout with structured messages:

```
2026-01-17 12:34:56 - worker - INFO - Claimed 3 steps
2026-01-17 12:34:57 - worker - INFO - Starting execution of step step-abc123...
2026-01-17 12:34:59 - worker - INFO - Completed step step-abc123...
2026-01-17 12:35:00 - leader - INFO - Recovery cycle completed: {...}
```

### Metrics to Track

- **Queue depth**: `SELECT COUNT(*) FROM STEP_TRACKER WHERE status = 'pending'`
- **Inflight tasks**: `SELECT COUNT(*) FROM STEP_TRACKER WHERE status = 'claimed'`
- **Completed tasks**: `SELECT COUNT(*) FROM STEP_TRACKER WHERE status = 'completed'`
- **Failed tasks**: `SELECT COUNT(*) FROM STEP_TRACKER WHERE status = 'failed'`
- **Alive pods**: `SELECT COUNT(*) FROM POD_HEALTH WHERE status = 'up' AND last_heartbeat_ts > NOW() - INTERVAL '60 seconds'`

## 🐳 Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install fastapi uvicorn snowflake-connector-python pydantic

# Copy application files
COPY quickstart/ /app/

# Expose port
EXPOSE 8000

# Run application
CMD ["python", "main.py"]
```

Build and run:

```bash
docker build -t fleet-q:latest .

docker run -d \
  -p 8000:8000 \
  -e FLEET_Q_POD_ID="pod-001" \
  -e SNOWFLAKE_ACCOUNT="your_account" \
  -e SNOWFLAKE_USER="your_user" \
  -e SNOWFLAKE_PASSWORD="your_password" \
  -e SNOWFLAKE_DATABASE="your_database" \
  -e SNOWFLAKE_SCHEMA="your_schema" \
  -e SNOWFLAKE_WAREHOUSE="your_warehouse" \
  fleet-q:latest
```

## ☸️ Kubernetes Deployment

Deploy multiple pods with unique pod IDs:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-q
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fleet-q
  template:
    metadata:
      labels:
        app: fleet-q
    spec:
      containers:
      - name: fleet-q
        image: fleet-q:latest
        env:
        - name: FLEET_Q_POD_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: SNOWFLAKE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: snowflake-creds
              key: account
        # ... other env vars from secrets ...
        ports:
        - containerPort: 8000
```

## 🔒 Security Best Practices

1. **Use Secrets Management**: Store Snowflake credentials in Kubernetes secrets or AWS Secrets Manager
2. **Enable SSL**: Use HTTPS for API endpoints in production
3. **Implement Authentication**: Add API key or OAuth validation to endpoints
4. **Network Policies**: Restrict pod-to-pod communication (even though FLEET-Q doesn't require it)
5. **Audit Logging**: Log all task submissions and status changes

## 🚨 Troubleshooting

### Issue: Tasks stuck in "claimed" status

**Cause**: Worker died mid-execution  
**Solution**: Leader recovery will automatically requeue these after `dead_pod_threshold` seconds

### Issue: Multiple pods claiming same task

**Cause**: Transaction isolation issue  
**Solution**: Verify Snowflake transaction isolation level and check for connection issues

### Issue: Leader not running recovery

**Cause**: Leader election timing or heartbeat failure  
**Solution**: Check logs for leader election results and heartbeat status

### Issue: High retry counts

**Cause**: Tasks consistently failing  
**Solution**: Review task execution logs and check `max_retries` configuration

## 🎓 Design Philosophy: Lessons from Raquel

FLEET-Q's implementation draws inspiration from [Raquel](https://github.com/vduseev/raquel), a proven Python job queue library that uses SQL tables as the foundation for distributed task processing. While Raquel is designed for general-purpose job queuing, FLEET-Q adapts its core principles for broker-less, Snowflake-backed distributed systems.

### What We Learned from Raquel

| Raquel Principle | How FLEET-Q Applies It |
|-----------------|------------------------|
| **SQL-backed queue semantics** | `STEP_TRACKER` serves as the single source of truth for all task state. No external message broker needed. |
| **Clean enqueue/dequeue API** | Simple, intuitive methods: `submit_step()`, `claim_steps()`, `complete_step()`, `fail_step()`, `requeue_step()` |
| **Transaction-centered correctness** | Atomic MERGE operations for claiming ensure no duplicate processing. State transitions are predictable and safe. |
| **Retry semantics** | Leader applies intelligent retry policies with exponential backoff. Failed tasks are requeued until max retries. |
| **Context managers** | Transaction handling follows Raquel's pattern for clean resource management |
| **Status constants** | Well-defined status values (`pending`, `claimed`, `completed`, `failed`) mirror Raquel's state machine |

### Key Differences from Raquel

While inspired by Raquel, FLEET-Q extends the concepts for distributed systems:

| Aspect | Raquel | FLEET-Q |
|--------|--------|---------|
| **Architecture** | Single-process or thread-based workers | Multi-pod distributed workers (EKS) |
| **Coordination** | Shared SQL database (PostgreSQL, SQLite) | Snowflake as global coordination store |
| **Claiming** | Row-level locks with SELECT...FOR UPDATE | MERGE-based atomic claims (Snowflake-safe) |
| **Recovery** | Reclaim based on timeouts | Leader-based recovery using pod health detection |
| **Capacity** | Static worker pool | Dynamic elastic capacity (pods scale up/down) |
| **Networking** | Workers can communicate | No pod-to-pod communication allowed |
| **DLQ** | Not built-in | Leader maintains ephemeral local SQLite DLQ |

### Transaction-Centered Correctness in Practice

Following Raquel's emphasis on transaction correctness, FLEET-Q ensures atomicity:

```python
# Atomic claim using MERGE (no race conditions)
MERGE INTO STEP_TRACKER AS target
USING (
    SELECT step_id FROM STEP_TRACKER
    WHERE status = 'pending'
    ORDER BY priority DESC, created_ts ASC
    LIMIT 5
) AS source
ON target.step_id = source.step_id
WHEN MATCHED AND target.status = 'pending' THEN
    UPDATE SET status = 'claimed', claimed_by = 'pod-001'
```

This guarantees:
- ✅ Each step is claimed by exactly one worker
- ✅ No lost updates under concurrent access
- ✅ Predictable state transitions
- ✅ Safe retry behavior

### Why Not Just Use Raquel?

Excellent question! Raquel is fantastic for many use cases, but FLEET-Q addresses specific constraints:

1. **No pod-to-pod networking**: Raquel assumes workers can coordinate; FLEET-Q works without it
2. **Snowflake-specific optimizations**: Uses Snowflake's MERGE instead of PostgreSQL's row locks
3. **Leader-based recovery**: Handles pod failures without relying on task timeouts
4. **Elastic scaling**: Capacity-aware claiming adapts as pods join/leave the cluster
5. **EKS-optimized**: Designed for Kubernetes deployments where traditional brokers aren't available

### Acknowledgments

We're grateful to the Raquel project for demonstrating that **SQL-backed task queues can be simple, reliable, and production-ready**. FLEET-Q builds on these principles while adapting them for distributed, broker-less environments.

**Learn more about Raquel**: [https://github.com/vduseev/raquel](https://github.com/vduseev/raquel)

## 📚 Further Reading

- [FLEET-Q Concept Documentation](../docs/FLEET-Q%20-%20Federated%20Leaderless%20Execution%20%26%20Elastic%20Tasking%20Queue.md)
- [Snowflake Transaction Isolation](https://docs.snowflake.com/en/sql-reference/transactions.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Raquel - SQL-based Job Queue](https://github.com/vduseev/raquel)

## 🤝 Contributing

This is a reference implementation. Feel free to customize it for your use case:

- Add custom task types
- Implement priority queues
- Add task dependencies
- Integrate with monitoring systems (Prometheus, DataDog, etc.)
- Add task scheduling (cron-like)

## 📄 License

See main repository LICENSE file.

---

**FLEET-Q** - Distributed task queuing, simplified. 🚀
