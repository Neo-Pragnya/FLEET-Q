"""
FastAPI Integration Example for FLEET-Q In-Pod Execution Fabric

Demonstrates how to integrate the Control Plane Runner with FastAPI multi-worker deployment.

Architecture:
- FastAPI runs with multiple workers (e.g., 4 workers via gunicorn/uvicorn)
- Each worker process tries to acquire the lease
- Only ONE becomes the Control Plane Runner
- Others serve HTTP requests normally
- All can enqueue work via IOHub client

Deployment:
    # Using uvicorn
    uvicorn fastapi_integration:app --workers 4 --host 0.0.0.0 --port 8000
    
    # Using gunicorn
    gunicorn fastapi_integration:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import asyncio
import logging
import os
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

# FLEET-Q imports
from fleet_q.control_plane_runner import ControlPlaneRunner, ControlPlaneConfig
from fleet_q.iohub import IOHubClient, AIMDConfig
from fleet_q.sqlite_outbox import SQLiteOutbox, ResultIntent
from fleet_q.zeromq_utils import create_ipc_address

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Global State (Process-Local)
# ============================================================================

control_plane_runner: Optional[ControlPlaneRunner] = None
is_control_plane: bool = False
iohub_address: str = create_ipc_address("iohub-fastapi")
outbox_db_path: str = "/tmp/fleetq_fastapi.db"


# ============================================================================
# FastAPI Lifespan (Startup/Shutdown)
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.
    
    Each worker process executes this on startup.
    Only the lease holder becomes the Control Plane Runner.
    """
    global control_plane_runner, is_control_plane
    
    process_id = os.getpid()
    pod_id = os.environ.get('POD_ID', 'fastapi-pod')
    
    logger.info(f"Process {process_id} starting up...")
    
    # Try to become Control Plane Runner
    config = ControlPlaneConfig(
        pod_id=pod_id,
        process_id=process_id,
        outbox_db_path=outbox_db_path,
        iohub_address=iohub_address,
        lease_ttl_seconds=30,
        lease_renewal_interval_seconds=10,
        enable_claim_loop=False,  # Optional: integrate with claim service
        enable_heartbeat_loop=False,  # Optional: integrate with health service
        aimd_config=AIMDConfig(
            initial_max_inflight=10,
            max_max_inflight=50
        )
    )
    
    runner = ControlPlaneRunner(config)
    
    # Try to start (acquires lease)
    started = await runner.start()
    
    if started:
        # This process is the Control Plane Runner
        is_control_plane = True
        control_plane_runner = runner
        logger.info(f"✅ Process {process_id} is CONTROL PLANE RUNNER")
    else:
        # Another process holds the lease
        is_control_plane = False
        logger.info(f"ℹ️  Process {process_id} is a regular API worker")
    
    # Yield to application
    yield
    
    # Shutdown
    if is_control_plane and control_plane_runner:
        logger.info(f"Process {process_id} shutting down Control Plane Runner...")
        await control_plane_runner.stop()


# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(
    title="FLEET-Q FastAPI Integration",
    description="Multi-worker FastAPI with In-Pod Execution Fabric",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# Request Models
# ============================================================================

class TaskSubmitRequest(BaseModel):
    """Request to submit a task"""
    task_type: str
    prompt: str
    document: str
    metadata: Optional[Dict[str, Any]] = None


class TaskSubmitResponse(BaseModel):
    """Response after task submission"""
    task_id: str
    status: str
    message: str


class StatusResponse(BaseModel):
    """System status response"""
    process_id: int
    is_control_plane: bool
    control_plane_status: Optional[Dict[str, Any]] = None


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "FLEET-Q FastAPI Integration",
        "process_id": os.getpid(),
        "is_control_plane": is_control_plane,
        "status": "healthy"
    }


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """
    Get system status.
    
    Returns process info and control plane status (if this is the runner).
    """
    status_data = {
        "process_id": os.getpid(),
        "is_control_plane": is_control_plane
    }
    
    if is_control_plane and control_plane_runner:
        status_data["control_plane_status"] = control_plane_runner.get_status()
    
    return status_data


@app.post("/tasks/submit", response_model=TaskSubmitResponse)
async def submit_task(request: TaskSubmitRequest, background_tasks: BackgroundTasks):
    """
    Submit a task for processing.
    
    This endpoint can be called on ANY FastAPI worker.
    The task will be coordinated through IOHub.
    """
    import uuid
    task_id = str(uuid.uuid4())
    
    logger.info(f"Received task submission: {task_id} (process={os.getpid()})")
    
    # Queue task for background processing
    background_tasks.add_task(
        process_task_with_iohub,
        task_id=task_id,
        task_type=request.task_type,
        prompt=request.prompt,
        document=request.document,
        metadata=request.metadata
    )
    
    return TaskSubmitResponse(
        task_id=task_id,
        status="queued",
        message=f"Task queued for processing (worker pid={os.getpid()})"
    )


@app.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """
    Get task status.
    
    In production, this would query the outbox or Snowflake for task status.
    """
    # TODO: Implement actual status lookup
    outbox = SQLiteOutbox(outbox_db_path)
    
    # For now, return mock status
    return {
        "task_id": task_id,
        "status": "processing",
        "message": "Task status lookup not fully implemented"
    }


@app.get("/metrics")
async def get_metrics():
    """
    Get system metrics.
    
    Aggregates metrics from outbox and IOHub (if control plane).
    """
    outbox = SQLiteOutbox(outbox_db_path)
    stats = outbox.get_stats()
    
    metrics = {
        "process_id": os.getpid(),
        "is_control_plane": is_control_plane,
        "outbox_stats": {
            "pending_updates": stats['step_updates']['pending'],
            "pending_results": stats['results']['pending'],
            "pending_sharepoint_ops": stats['sharepoint_ops']['pending']
        }
    }
    
    if is_control_plane and control_plane_runner:
        iohub_status = control_plane_runner.iohub.get_status()
        metrics["iohub_metrics"] = {
            "max_inflight": iohub_status['aimd']['max_inflight'],
            "current_inflight": iohub_status['aimd']['current_inflight'],
            "permits_granted": iohub_status['permits']['granted'],
            "permits_denied": iohub_status['permits']['denied'],
            "total_requests": iohub_status['requests']['total'],
            "success_rate": (
                iohub_status['requests']['successes'] / iohub_status['requests']['total']
                if iohub_status['requests']['total'] > 0 else 0
            )
        }
    
    return metrics


# ============================================================================
# Background Task Processing
# ============================================================================

async def process_task_with_iohub(
    task_id: str,
    task_type: str,
    prompt: str,
    document: str,
    metadata: Optional[Dict[str, Any]]
):
    """
    Process task using IOHub for permit control.
    
    This function runs in FastAPI background tasks.
    It connects to IOHub (running in Control Plane Runner) for coordination.
    """
    worker_id = f"api-worker-{os.getpid()}-{task_id[:8]}"
    
    logger.info(f"Processing task {task_id} with worker {worker_id}")
    
    # Create IOHub client
    client = IOHubClient(iohub_address, worker_id)
    
    try:
        # Request permit
        permit_acquired = False
        for attempt in range(5):
            if await client.request_permit(timeout_ms=2000):
                permit_acquired = True
                break
            logger.debug(f"Worker {worker_id} waiting for permit (attempt {attempt + 1})")
            await asyncio.sleep(0.5)
        
        if not permit_acquired:
            logger.error(f"Worker {worker_id} failed to acquire permit for task {task_id}")
            return
        
        # Simulate Bedrock call
        import time
        start_time = time.time()
        
        try:
            # Mock Bedrock call
            await asyncio.sleep(0.5)  # Simulate I/O
            result = {
                'summary': f'Summary of document (type={task_type})',
                'task_id': task_id
            }
            
            latency = time.time() - start_time
            
            # Report success
            await client.report_success(latency=latency)
            
            # Enqueue result
            await client.enqueue_result(
                step_id=task_id,
                table_name='API_RESULTS',
                record_data={
                    'task_id': task_id,
                    'task_type': task_type,
                    'result': result,
                    'latency': latency
                }
            )
            
            logger.info(f"Task {task_id} completed successfully in {latency:.2f}s")
        
        except Exception as e:
            error_str = str(e)
            
            if 'ThrottlingException' in error_str or '429' in error_str:
                await client.report_throttle()
                logger.warning(f"Task {task_id} throttled")
            else:
                await client.report_error(error_str)
                logger.error(f"Task {task_id} error: {e}")
    
    finally:
        client.close()


# ============================================================================
# Run Instructions
# ============================================================================

if __name__ == "__main__":
    """
    Run with uvicorn:
        uvicorn fastapi_integration:app --workers 4 --host 0.0.0.0 --port 8000
    
    Or with gunicorn:
        gunicorn fastapi_integration:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
    
    Test:
        # Check status
        curl http://localhost:8000/status
        
        # Submit task
        curl -X POST http://localhost:8000/tasks/submit \
          -H "Content-Type: application/json" \
          -d '{"task_type": "summarize", "prompt": "Summarize this", "document": "Document content..."}'
        
        # Get metrics
        curl http://localhost:8000/metrics
    """
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
