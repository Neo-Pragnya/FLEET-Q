# REST API Endpoints

FLEET-Q provides a comprehensive REST API for task submission, status monitoring, and administrative operations.

## Base URL

All API endpoints are relative to the base URL where FLEET-Q is running:

```
http://localhost:8000  # Default local development
https://fleet-q.example.com  # Production deployment
```

## Authentication

Currently, FLEET-Q does not implement authentication. In production environments, you should:

- Use a reverse proxy (nginx, ALB) for authentication
- Implement network-level security (VPC, security groups)
- Consider API gateway integration for advanced auth

## Core Endpoints

### Health Check

#### `GET /health`

Returns the current health status of the pod.

**Response:**
```json
{
  "pod_id": "pod-hostname-abc123",
  "status": "healthy",
  "uptime_seconds": 3600.5,
  "current_capacity": 3,
  "max_capacity": 8,
  "is_leader": true,
  "version": "1.0.0"
}
```

**Response Fields:**
- `pod_id`: Unique identifier for this pod
- `status`: Health status (`healthy`, `unhealthy`)
- `uptime_seconds`: Time since pod started
- `current_capacity`: Currently executing steps
- `max_capacity`: Maximum concurrent steps (80% of max parallelism)
- `is_leader`: Whether this pod is the current leader
- `version`: FLEET-Q version

**Status Codes:**
- `200 OK`: Pod is healthy
- `503 Service Unavailable`: Pod is unhealthy

**Example:**
```bash
curl http://localhost:8000/health
```

### Step Submission

#### `POST /submit`

Submits a new step to the queue for processing.

**Request Body:**
```json
{
  "type": "data_processing",
  "args": {
    "input_file": "data.csv",
    "output_format": "json"
  },
  "metadata": {
    "user": "alice",
    "department": "analytics"
  },
  "priority": 5,
  "max_retries": 3
}
```

**Request Fields:**
- `type` (required): Step type identifier
- `args` (required): Step arguments as key-value pairs
- `metadata` (optional): Additional metadata
- `priority` (optional): Priority level (higher = more important, default: 0)
- `max_retries` (optional): Maximum retry attempts (default: 3)

**Response:**
```json
{
  "step_id": "step_20240101120000_abc12345",
  "status": "submitted",
  "message": "Step step_20240101120000_abc12345 submitted successfully"
}
```

**Status Codes:**
- `201 Created`: Step submitted successfully
- `400 Bad Request`: Invalid request body
- `500 Internal Server Error`: Database error

**Example:**
```bash
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{
    "type": "hello_world",
    "args": {"message": "Hello, FLEET-Q!"},
    "priority": 1
  }'
```

### Step Status

#### `GET /status/{step_id}`

Retrieves the current status of a specific step.

**Path Parameters:**
- `step_id`: The unique step identifier

**Response:**
```json
{
  "step_id": "step_20240101120000_abc12345",
  "status": "claimed",
  "claimed_by": "pod-worker-def456",
  "last_update_ts": "2024-01-01T12:05:30.123456",
  "retry_count": 0,
  "priority": 5,
  "created_ts": "2024-01-01T12:00:00.000000",
  "max_retries": 3,
  "payload": {
    "type": "data_processing",
    "args": {"input_file": "data.csv"},
    "metadata": {"user": "alice"}
  }
}
```

**Response Fields:**
- `step_id`: Unique step identifier
- `status`: Current status (`pending`, `claimed`, `completed`, `failed`)
- `claimed_by`: Pod ID that claimed the step (if claimed)
- `last_update_ts`: Timestamp of last status update
- `retry_count`: Number of retry attempts
- `priority`: Step priority level
- `created_ts`: Step creation timestamp
- `max_retries`: Maximum allowed retries
- `payload`: Original step payload

**Status Codes:**
- `200 OK`: Step found and returned
- `404 Not Found`: Step not found
- `500 Internal Server Error`: Database error

**Example:**
```bash
curl http://localhost:8000/status/step_20240101120000_abc12345
```

### Idempotency Key

#### `GET /idempotency/{step_id}`

Returns the idempotency key for a step, used for safe re-execution.

**Path Parameters:**
- `step_id`: The unique step identifier

**Response:**
```json
{
  "step_id": "step_20240101120000_abc12345",
  "idempotency_key": "idem_abc12345_def67890"
}
```

**Status Codes:**
- `200 OK`: Idempotency key generated
- `404 Not Found`: Step not found
- `500 Internal Server Error`: Key generation error

**Example:**
```bash
curl http://localhost:8000/idempotency/step_20240101120000_abc12345
```

## Administrative Endpoints

### Leader Information

#### `GET /admin/leader`

Returns information about the current leader election status.

**Response:**
```json
{
  "current_leader": "pod-leader-abc123",
  "leader_birth_ts": "2024-01-01T10:00:00.000000",
  "active_pods": 5,
  "last_election_check": "2024-01-01T12:05:00.000000",
  "election_details": {
    "eligible_pods": [
      {
        "pod_id": "pod-leader-abc123",
        "birth_ts": "2024-01-01T10:00:00.000000",
        "last_heartbeat_ts": "2024-01-01T12:05:00.000000",
        "is_leader": true
      },
      {
        "pod_id": "pod-worker-def456",
        "birth_ts": "2024-01-01T10:01:00.000000",
        "last_heartbeat_ts": "2024-01-01T12:04:58.000000",
        "is_leader": false
      }
    ]
  }
}
```

**Response Fields:**
- `current_leader`: Pod ID of current leader
- `leader_birth_ts`: Birth timestamp of leader
- `active_pods`: Number of active pods
- `last_election_check`: Last leader election check
- `election_details`: Detailed election information

**Status Codes:**
- `200 OK`: Leader information returned
- `500 Internal Server Error`: Database error

**Example:**
```bash
curl http://localhost:8000/admin/leader
```

### Queue Statistics

#### `GET /admin/queue`

Returns comprehensive queue statistics and metrics.

**Response:**
```json
{
  "pending_steps": 25,
  "claimed_steps": 8,
  "completed_steps": 142,
  "failed_steps": 3,
  "active_pods": 5,
  "current_leader": "pod-leader-abc123",
  "queue_depth_by_priority": {
    "0": 15,
    "1": 8,
    "5": 2
  },
  "steps_by_type": {
    "data_processing": 12,
    "email_notification": 8,
    "report_generation": 5
  },
  "pod_capacity": {
    "pod-leader-abc123": {"current": 3, "max": 8},
    "pod-worker-def456": {"current": 2, "max": 6},
    "pod-worker-ghi789": {"current": 3, "max": 6}
  }
}
```

**Response Fields:**
- `pending_steps`: Steps waiting to be claimed
- `claimed_steps`: Steps currently being processed
- `completed_steps`: Successfully completed steps
- `failed_steps`: Failed steps (terminal failures)
- `active_pods`: Number of active pods
- `current_leader`: Current leader pod ID
- `queue_depth_by_priority`: Pending steps grouped by priority
- `steps_by_type`: Step counts by type
- `pod_capacity`: Current and maximum capacity per pod

**Status Codes:**
- `200 OK`: Statistics returned
- `500 Internal Server Error`: Database error

**Example:**
```bash
curl http://localhost:8000/admin/queue
```

### Manual Recovery

#### `POST /admin/recovery/run`

Triggers a manual recovery cycle. Only available on the leader pod.

**Request Body:** (optional)
```json
{
  "force": false,
  "dry_run": false
}
```

**Request Fields:**
- `force` (optional): Force recovery even if recent cycle completed
- `dry_run` (optional): Perform recovery analysis without making changes

**Response:**
```json
{
  "status": "completed",
  "message": "Manual recovery cycle completed successfully",
  "recovery_summary": {
    "cycle_id": "recovery_1704110400",
    "dead_pods_found": 2,
    "orphaned_steps_found": 12,
    "steps_requeued": 10,
    "steps_marked_terminal": 2,
    "duration_ms": 1500,
    "dead_pods": ["pod-dead-1", "pod-dead-2"],
    "requeued_steps": [
      "step_20240101120000_abc123",
      "step_20240101120001_def456"
    ],
    "terminal_steps": [
      "step_20240101120002_ghi789"
    ]
  }
}
```

**Status Codes:**
- `200 OK`: Recovery completed successfully
- `403 Forbidden`: Not the leader pod
- `409 Conflict`: Recovery already in progress
- `500 Internal Server Error`: Recovery failed

**Example:**
```bash
# Trigger manual recovery
curl -X POST http://localhost:8000/admin/recovery/run

# Dry run recovery
curl -X POST http://localhost:8000/admin/recovery/run \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

## Error Responses

All endpoints return consistent error responses:

```json
{
  "detail": "Step not found",
  "error_type": "not_found",
  "timestamp": "2024-01-01T12:00:00.000000Z",
  "path": "/status/invalid_step_id"
}
```

**Error Fields:**
- `detail`: Human-readable error description
- `error_type`: Machine-readable error category
- `timestamp`: Error occurrence timestamp
- `path`: Request path that caused the error

### Common Error Types

- `validation_error`: Invalid request data
- `not_found`: Resource not found
- `permission_denied`: Insufficient permissions
- `conflict`: Resource conflict (e.g., recovery in progress)
- `internal_error`: Server-side error
- `database_error`: Database operation failed

## Rate Limiting

FLEET-Q does not implement built-in rate limiting. For production deployments:

- Use a reverse proxy (nginx) for rate limiting
- Implement API gateway rate limiting
- Monitor queue depth to prevent overload

## Monitoring and Metrics

### Health Check Integration

The `/health` endpoint is designed for container orchestration:

```yaml
# Kubernetes liveness probe
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10

# Kubernetes readiness probe
readinessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Metrics Collection

Use the admin endpoints for metrics collection:

```bash
# Collect queue metrics
curl -s http://localhost:8000/admin/queue | jq '.pending_steps'

# Monitor leader status
curl -s http://localhost:8000/admin/leader | jq '.current_leader'

# Check pod capacity
curl -s http://localhost:8000/health | jq '.current_capacity'
```

## Client Libraries

### Python Client Example

```python
import httpx
from typing import Dict, Any, Optional

class FleetQClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.client = httpx.Client(base_url=base_url)
    
    def submit_step(self, step_type: str, args: Dict[str, Any], 
                   priority: int = 0, max_retries: int = 3) -> str:
        response = self.client.post("/submit", json={
            "type": step_type,
            "args": args,
            "priority": priority,
            "max_retries": max_retries
        })
        response.raise_for_status()
        return response.json()["step_id"]
    
    def get_step_status(self, step_id: str) -> Dict[str, Any]:
        response = self.client.get(f"/status/{step_id}")
        response.raise_for_status()
        return response.json()
    
    def get_queue_stats(self) -> Dict[str, Any]:
        response = self.client.get("/admin/queue")
        response.raise_for_status()
        return response.json()

# Usage
client = FleetQClient()
step_id = client.submit_step("data_processing", {"file": "data.csv"})
status = client.get_step_status(step_id)
```

### JavaScript Client Example

```javascript
class FleetQClient {
    constructor(baseUrl = 'http://localhost:8000') {
        this.baseUrl = baseUrl;
    }
    
    async submitStep(type, args, priority = 0, maxRetries = 3) {
        const response = await fetch(`${this.baseUrl}/submit`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                type,
                args,
                priority,
                max_retries: maxRetries
            })
        });
        
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        return data.step_id;
    }
    
    async getStepStatus(stepId) {
        const response = await fetch(`${this.baseUrl}/status/${stepId}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
    }
}

// Usage
const client = new FleetQClient();
const stepId = await client.submitStep('data_processing', {file: 'data.csv'});
const status = await client.getStepStatus(stepId);
```

## Next Steps

- **[Data Models](models.md)** - Detailed data model documentation
- **[Error Handling](errors.md)** - Complete error handling guide
- **[Examples](../examples/basic-usage.md)** - Practical usage examples