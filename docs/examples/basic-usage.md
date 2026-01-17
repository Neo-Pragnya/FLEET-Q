# Basic Usage Examples

This guide provides practical examples of using FLEET-Q for common task processing scenarios.

## Simple Task Submission

### Basic Step Submission

```python
import httpx
import asyncio

async def submit_simple_task():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit a simple task
    response = await client.post("/submit", json={
        "type": "hello_world",
        "args": {"message": "Hello, FLEET-Q!"},
        "metadata": {"user": "alice"}
    })
    
    step_data = response.json()
    step_id = step_data["step_id"]
    print(f"Submitted step: {step_id}")
    
    # Wait for completion
    while True:
        status_response = await client.get(f"/status/{step_id}")
        status = status_response.json()
        
        print(f"Step {step_id} status: {status['status']}")
        
        if status["status"] in ["completed", "failed"]:
            break
        
        await asyncio.sleep(1)
    
    await client.aclose()

# Run the example
asyncio.run(submit_simple_task())
```

### Batch Task Submission

```python
import httpx
import asyncio

async def submit_batch_tasks():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit multiple tasks
    tasks = [
        {"type": "data_processing", "args": {"file": f"data_{i}.csv"}}
        for i in range(10)
    ]
    
    step_ids = []
    for task in tasks:
        response = await client.post("/submit", json=task)
        step_id = response.json()["step_id"]
        step_ids.append(step_id)
        print(f"Submitted: {step_id}")
    
    # Monitor all tasks
    completed = set()
    while len(completed) < len(step_ids):
        for step_id in step_ids:
            if step_id in completed:
                continue
                
            status_response = await client.get(f"/status/{step_id}")
            status = status_response.json()
            
            if status["status"] in ["completed", "failed"]:
                completed.add(step_id)
                print(f"Finished: {step_id} -> {status['status']}")
        
        await asyncio.sleep(2)
    
    print(f"All {len(step_ids)} tasks completed!")
    await client.aclose()

asyncio.run(submit_batch_tasks())
```

## Priority-Based Processing

### High Priority Tasks

```python
import httpx
import asyncio

async def priority_example():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit low priority tasks first
    low_priority_tasks = []
    for i in range(5):
        response = await client.post("/submit", json={
            "type": "background_job",
            "args": {"task_id": i},
            "priority": 1  # Low priority
        })
        low_priority_tasks.append(response.json()["step_id"])
    
    # Submit high priority task
    response = await client.post("/submit", json={
        "type": "urgent_task",
        "args": {"alert": "critical"},
        "priority": 10  # High priority
    })
    urgent_step_id = response.json()["step_id"]
    
    print(f"Submitted urgent task: {urgent_step_id}")
    print(f"Submitted {len(low_priority_tasks)} background tasks")
    
    # Monitor which completes first
    while True:
        # Check urgent task
        urgent_status = await client.get(f"/status/{urgent_step_id}")
        urgent_data = urgent_status.json()
        
        if urgent_data["status"] == "completed":
            print("✅ Urgent task completed first (as expected)")
            break
        
        # Check if any low priority task completed first
        for step_id in low_priority_tasks:
            status_response = await client.get(f"/status/{step_id}")
            status = status_response.json()
            if status["status"] == "completed":
                print("⚠️ Low priority task completed before urgent task")
                break
        
        await asyncio.sleep(0.5)
    
    await client.aclose()

asyncio.run(priority_example())
```

## Error Handling and Retries

### Retry Configuration

```python
import httpx
import asyncio

async def retry_example():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit a task that might fail
    response = await client.post("/submit", json={
        "type": "flaky_operation",
        "args": {"failure_rate": 0.7},
        "max_retries": 5  # Allow up to 5 retries
    })
    
    step_id = response.json()["step_id"]
    print(f"Submitted flaky task: {step_id}")
    
    # Monitor retry behavior
    last_retry_count = 0
    while True:
        status_response = await client.get(f"/status/{step_id}")
        status = status_response.json()
        
        if status["retry_count"] > last_retry_count:
            print(f"Task failed, retry #{status['retry_count']}")
            last_retry_count = status["retry_count"]
        
        if status["status"] == "completed":
            print(f"✅ Task succeeded after {status['retry_count']} retries")
            break
        elif status["status"] == "failed":
            print(f"❌ Task failed permanently after {status['retry_count']} retries")
            break
        
        await asyncio.sleep(1)
    
    await client.aclose()

asyncio.run(retry_example())
```

## Monitoring and Observability

### Queue Monitoring

```python
import httpx
import asyncio
import json

async def monitor_queue():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    print("📊 FLEET-Q Queue Monitor")
    print("=" * 50)
    
    while True:
        try:
            # Get queue statistics
            queue_response = await client.get("/admin/queue")
            queue_stats = queue_response.json()
            
            # Get leader information
            leader_response = await client.get("/admin/leader")
            leader_info = leader_response.json()
            
            # Display current status
            print(f"\r🔄 Pending: {queue_stats['pending_steps']:3d} | "
                  f"Processing: {queue_stats['claimed_steps']:3d} | "
                  f"Completed: {queue_stats['completed_steps']:4d} | "
                  f"Failed: {queue_stats['failed_steps']:3d} | "
                  f"Leader: {leader_info['current_leader']}", end="")
            
            await asyncio.sleep(2)
            
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            await asyncio.sleep(5)
    
    await client.aclose()

# Run with: python monitor.py
if __name__ == "__main__":
    asyncio.run(monitor_queue())
```

### Health Check Monitoring

```python
import httpx
import asyncio
from datetime import datetime

async def health_monitor():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    print("🏥 FLEET-Q Health Monitor")
    print("=" * 50)
    
    while True:
        try:
            response = await client.get("/health")
            health = response.json()
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            status_icon = "✅" if health["status"] == "healthy" else "❌"
            leader_icon = "👑" if health["is_leader"] else "👤"
            
            print(f"{timestamp} {status_icon} {leader_icon} "
                  f"Pod: {health['pod_id'][:12]}... | "
                  f"Uptime: {health['uptime_seconds']:6.1f}s | "
                  f"Capacity: {health['current_capacity']}/{health['max_capacity']}")
            
            await asyncio.sleep(5)
            
        except KeyboardInterrupt:
            print("\n👋 Health monitoring stopped")
            break
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            await asyncio.sleep(10)
    
    await client.aclose()

asyncio.run(health_monitor())
```

## Load Testing

### Simple Load Test

```python
import httpx
import asyncio
import time
import random

async def load_test(concurrent_tasks: int = 50, duration_seconds: int = 60):
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    print(f"🚀 Starting load test: {concurrent_tasks} concurrent tasks for {duration_seconds}s")
    
    submitted = 0
    completed = 0
    failed = 0
    start_time = time.time()
    
    async def submit_task():
        nonlocal submitted, completed, failed
        
        try:
            # Submit a task
            response = await client.post("/submit", json={
                "type": "load_test",
                "args": {
                    "duration": random.uniform(0.1, 2.0),
                    "task_id": submitted
                },
                "priority": random.randint(0, 5)
            })
            
            step_id = response.json()["step_id"]
            submitted += 1
            
            # Wait for completion
            while True:
                status_response = await client.get(f"/status/{step_id}")
                status = status_response.json()
                
                if status["status"] == "completed":
                    completed += 1
                    break
                elif status["status"] == "failed":
                    failed += 1
                    break
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            print(f"Task error: {e}")
            failed += 1
    
    # Start concurrent tasks
    tasks = []
    end_time = start_time + duration_seconds
    
    while time.time() < end_time:
        # Maintain concurrent task count
        while len(tasks) < concurrent_tasks and time.time() < end_time:
            task = asyncio.create_task(submit_task())
            tasks.append(task)
        
        # Remove completed tasks
        tasks = [t for t in tasks if not t.done()]
        
        # Progress update
        elapsed = time.time() - start_time
        print(f"\r⏱️  {elapsed:5.1f}s | "
              f"Submitted: {submitted:3d} | "
              f"Completed: {completed:3d} | "
              f"Failed: {failed:3d} | "
              f"Active: {len(tasks):2d}", end="")
        
        await asyncio.sleep(1)
    
    # Wait for remaining tasks
    print("\n⏳ Waiting for remaining tasks...")
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # Final statistics
    total_time = time.time() - start_time
    throughput = completed / total_time
    
    print(f"\n📈 Load Test Results:")
    print(f"   Duration: {total_time:.1f}s")
    print(f"   Submitted: {submitted}")
    print(f"   Completed: {completed}")
    print(f"   Failed: {failed}")
    print(f"   Throughput: {throughput:.2f} tasks/second")
    print(f"   Success Rate: {(completed/submitted)*100:.1f}%")
    
    await client.aclose()

# Run load test
asyncio.run(load_test(concurrent_tasks=20, duration_seconds=30))
```

## Multi-Pod Scenarios

### Leader Election Testing

```python
import httpx
import asyncio

async def test_leader_election():
    # Connect to multiple pods
    pods = [
        httpx.AsyncClient(base_url="http://localhost:8000"),  # Pod 1
        httpx.AsyncClient(base_url="http://localhost:8001"),  # Pod 2
        httpx.AsyncClient(base_url="http://localhost:8002"),  # Pod 3
    ]
    
    print("👑 Testing Leader Election")
    print("=" * 40)
    
    # Check initial leader status
    for i, client in enumerate(pods):
        try:
            health_response = await client.get("/health")
            health = health_response.json()
            
            leader_response = await client.get("/admin/leader")
            leader_info = leader_response.json()
            
            status = "👑 LEADER" if health["is_leader"] else "👤 Worker"
            print(f"Pod {i+1}: {health['pod_id'][:12]}... - {status}")
            
        except Exception as e:
            print(f"Pod {i+1}: ❌ Unavailable ({e})")
    
    print(f"\nCurrent Leader: {leader_info['current_leader']}")
    print(f"Active Pods: {leader_info['active_pods']}")
    
    # Submit work to different pods
    print("\n📤 Submitting work to different pods...")
    for i, client in enumerate(pods):
        try:
            response = await client.post("/submit", json={
                "type": "multi_pod_test",
                "args": {"submitted_to_pod": i+1},
                "metadata": {"test": "leader_election"}
            })
            step_id = response.json()["step_id"]
            print(f"Pod {i+1}: Submitted {step_id}")
        except Exception as e:
            print(f"Pod {i+1}: Failed to submit ({e})")
    
    # Close connections
    for client in pods:
        await client.aclose()

asyncio.run(test_leader_election())
```

## Custom Step Types

### Implementing Custom Step Processors

```python
# This would be part of your FLEET-Q extension
from fleet_q.models import StepPayload
import asyncio
import httpx

class CustomStepProcessor:
    """Example custom step processor"""
    
    async def process_data_analysis(self, payload: StepPayload) -> dict:
        """Process data analysis steps"""
        input_file = payload.args["input_file"]
        analysis_type = payload.args.get("analysis_type", "basic")
        
        print(f"🔍 Analyzing {input_file} with {analysis_type} analysis")
        
        # Simulate processing time
        await asyncio.sleep(2)
        
        return {
            "status": "completed",
            "results": {
                "file": input_file,
                "analysis_type": analysis_type,
                "records_processed": 1000,
                "insights": ["trend_up", "seasonal_pattern"]
            }
        }
    
    async def process_email_notification(self, payload: StepPayload) -> dict:
        """Process email notification steps"""
        recipient = payload.args["recipient"]
        subject = payload.args["subject"]
        
        print(f"📧 Sending email to {recipient}: {subject}")
        
        # Simulate email sending
        await asyncio.sleep(0.5)
        
        return {
            "status": "completed",
            "results": {
                "recipient": recipient,
                "subject": subject,
                "sent_at": "2024-01-01T12:00:00Z",
                "message_id": "msg_123456"
            }
        }

# Example usage
async def submit_custom_steps():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit data analysis step
    analysis_response = await client.post("/submit", json={
        "type": "data_analysis",
        "args": {
            "input_file": "sales_data_2024.csv",
            "analysis_type": "advanced"
        },
        "metadata": {"department": "analytics"},
        "priority": 5
    })
    
    # Submit email notification step
    email_response = await client.post("/submit", json={
        "type": "email_notification",
        "args": {
            "recipient": "manager@company.com",
            "subject": "Daily Sales Report Ready"
        },
        "metadata": {"automated": True},
        "priority": 3
    })
    
    print(f"Submitted analysis: {analysis_response.json()['step_id']}")
    print(f"Submitted email: {email_response.json()['step_id']}")
    
    await client.aclose()

asyncio.run(submit_custom_steps())
```

## Error Scenarios and Recovery

### Simulating Pod Failures

```python
import httpx
import asyncio
import subprocess

async def test_pod_failure_recovery():
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    print("💥 Testing Pod Failure Recovery")
    print("=" * 40)
    
    # Submit some long-running tasks
    step_ids = []
    for i in range(5):
        response = await client.post("/submit", json={
            "type": "long_running_task",
            "args": {"duration": 30, "task_id": i},
            "metadata": {"test": "failure_recovery"}
        })
        step_ids.append(response.json()["step_id"])
    
    print(f"📤 Submitted {len(step_ids)} long-running tasks")
    
    # Wait for tasks to be claimed
    await asyncio.sleep(5)
    
    # Check which tasks are claimed
    claimed_tasks = []
    for step_id in step_ids:
        status_response = await client.get(f"/status/{step_id}")
        status = status_response.json()
        if status["status"] == "claimed":
            claimed_tasks.append((step_id, status["claimed_by"]))
    
    print(f"🔒 {len(claimed_tasks)} tasks claimed")
    
    if claimed_tasks:
        # Simulate pod failure (this would be done externally)
        print("💥 Simulating pod failure...")
        print("   (In real scenario, you would kill a pod here)")
        
        # Wait for leader to detect failure and recover
        print("⏳ Waiting for recovery (this may take a few minutes)...")
        await asyncio.sleep(180)  # Wait for dead pod threshold
        
        # Trigger manual recovery
        try:
            recovery_response = await client.post("/admin/recovery/run")
            recovery_data = recovery_response.json()
            print(f"🔧 Manual recovery triggered: {recovery_data['status']}")
        except Exception as e:
            print(f"❌ Recovery failed: {e}")
        
        # Check if tasks were requeued
        requeued = 0
        for step_id, original_claimer in claimed_tasks:
            status_response = await client.get(f"/status/{step_id}")
            status = status_response.json()
            
            if status["status"] == "pending":
                requeued += 1
                print(f"✅ Task {step_id} requeued")
            elif status["claimed_by"] != original_claimer:
                print(f"🔄 Task {step_id} reclaimed by {status['claimed_by']}")
        
        print(f"📊 Recovery summary: {requeued} tasks requeued")
    
    await client.aclose()

# Note: This test requires multiple pods and manual intervention
# asyncio.run(test_pod_failure_recovery())
```

## Integration Examples

### FastAPI Integration

```python
from fastapi import FastAPI, BackgroundTasks
import httpx

app = FastAPI()
fleet_q_client = httpx.AsyncClient(base_url="http://localhost:8000")

@app.post("/process-data")
async def process_data(file_path: str, background_tasks: BackgroundTasks):
    """Submit data processing to FLEET-Q"""
    
    # Submit to FLEET-Q
    response = await fleet_q_client.post("/submit", json={
        "type": "data_processing",
        "args": {"file_path": file_path},
        "metadata": {"source": "api"},
        "priority": 5
    })
    
    step_id = response.json()["step_id"]
    
    return {
        "message": "Data processing started",
        "step_id": step_id,
        "status_url": f"/status/{step_id}"
    }

@app.get("/status/{step_id}")
async def get_status(step_id: str):
    """Get processing status from FLEET-Q"""
    
    response = await fleet_q_client.get(f"/status/{step_id}")
    return response.json()

@app.get("/queue-stats")
async def queue_stats():
    """Get FLEET-Q queue statistics"""
    
    response = await fleet_q_client.get("/admin/queue")
    return response.json()
```

### Celery Migration Example

```python
# Before (Celery)
from celery import Celery

celery_app = Celery('myapp')

@celery_app.task
def process_data(file_path):
    # Process data
    return {"status": "completed"}

# Submit task
result = process_data.delay("data.csv")

# After (FLEET-Q)
import httpx

async def process_data_fleet_q(file_path):
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit task
    response = await client.post("/submit", json={
        "type": "process_data",
        "args": {"file_path": file_path}
    })
    
    step_id = response.json()["step_id"]
    
    # Wait for completion (or return step_id for async handling)
    while True:
        status_response = await client.get(f"/status/{step_id}")
        status = status_response.json()
        
        if status["status"] in ["completed", "failed"]:
            await client.aclose()
            return status
        
        await asyncio.sleep(1)

# Usage
result = await process_data_fleet_q("data.csv")
```

These examples demonstrate the flexibility and power of FLEET-Q for various task processing scenarios. The key advantages include:

- **Simplicity**: No broker setup required
- **Reliability**: Built-in retry and recovery mechanisms
- **Observability**: Rich monitoring and status information
- **Scalability**: Horizontal scaling with automatic load balancing
- **Flexibility**: Support for custom step types and priorities

## Next Steps

- **[Step Implementation](step-implementation.md)** - Learn how to implement custom step processors
- **[Idempotency Patterns](idempotency.md)** - Ensure safe task re-execution
- **[Environment Configurations](environments.md)** - Configure FLEET-Q for different environments