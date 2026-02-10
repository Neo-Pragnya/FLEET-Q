"""
FLEET-Q FastAPI Application

Main entry point that ties together all components:
- FastAPI web server for API endpoints
- Worker loops (heartbeat, claim, execute)
- Leader recovery loop
- Task execution logic
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config import FleetQConfig, load_config
from control_plane import ControlPlaneWorker, get_control_plane, initialize_control_plane
from leader import LeaderElection, LeaderRecovery
from queue import QueueOperations
from storage import LocalStorage, SnowflakeStorage
from throttle import AdaptiveThrottle, ThrottleConfig, get_or_create_throttle, with_throttle
from worker import HealthMonitor, WorkerLoops

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Global state (initialized at startup)
# ============================================================================
config: Optional[FleetQConfig] = None
storage: Optional[SnowflakeStorage] = None
local_storage: Optional[LocalStorage] = None
queue_ops: Optional[QueueOperations] = None
worker_loops: Optional[WorkerLoops] = None
health_monitor: Optional[HealthMonitor] = None
leader_recovery: Optional[LeaderRecovery] = None
leader_election: Optional[LeaderElection] = None
control_plane: Optional[ControlPlaneWorker] = None

# Adaptive throttles for external APIs
bedrock_throttle: Optional[AdaptiveThrottle] = None
external_api_throttle: Optional[AdaptiveThrottle] = None


# ============================================================================
# Task Executor - Customize this for your use case
# ============================================================================

# Example: Create throttle for Bedrock API (will be initialized at startup)
# This throttle will be shared across all tasks that call Bedrock
def get_bedrock_throttle() -> AdaptiveThrottle:
    """Get or create Bedrock API throttle"""
    return get_or_create_throttle("bedrock", ThrottleConfig(
        initial_limit=10,
        min_limit=1,
        max_limit=100,
        additive_increase=1,
        multiplicative_decrease=0.5,
        enable_latency_tracking=True,
        success_threshold=10
    ))


@with_throttle(
    throttle=get_bedrock_throttle(),
    throttle_exceptions=(Exception,),  # Replace with actual Bedrock throttle exceptions
)
async def call_bedrock_api(payload: Dict[str, Any]) -> Any:
    """
    Example function that calls Bedrock API with adaptive throttling.
    
    The @with_throttle decorator automatically:
    - Acquires a throttle slot before execution
    - Records success/failure outcomes
    - Adjusts concurrency limits dynamically
    - Tracks latency for pressure sensing
    """
    # Simulate Bedrock API call
    import asyncio
    await asyncio.sleep(0.1)  # Replace with actual bedrock call
    
    # Example: Check for throttle errors
    # if response.status_code == 429:
    #     raise BotoThrottleException("Rate limit exceeded")
    
    return {"status": "success", "result": "bedrock response"}


def execute_task(step: Dict[str, Any]) -> Any:
    """
    Custom task execution logic with adaptive throttling support.
    
    This is where you implement your actual task processing.
    The step contains:
    - STEP_ID: unique step identifier
    - PAYLOAD: task details (task_type, args, etc.)
    - RETRY_COUNT: number of retries so far
    - PRIORITY: task priority
    
    Args:
        step: Step details from the queue
        
    Returns:
        Task result (any serializable value)
        
    Raises:
        Exception: If task execution fails
    """
    step_id = step['STEP_ID']
    payload = step['PAYLOAD']
    
    logger.info(f"Executing step {step_id}")
    logger.debug(f"Payload: {payload}")
    
    # Extract task type
    task_type = payload.get('task_type', 'unknown')
    
    # Route to appropriate handler
    if task_type == 'bedrock_task':
        # Use adaptive throttling for Bedrock calls
        throttle = get_bedrock_throttle()
        
        # Manual throttle usage (alternative to decorator)
        with throttle.acquire_sync():
            import time
            start = time.time()
            
            try:
                # Call Bedrock API
                logger.info(f"Calling Bedrock API with throttle (current limit: {throttle.max_inflight})")
                time.sleep(0.5)  # Replace with actual bedrock call
                
                # Record success
                latency = time.time() - start
                throttle.record_success(latency=latency)
                
                result = {'status': 'completed', 'message': 'Bedrock task executed'}
            
            except Exception as e:
                # Check if it's a throttle error
                if 'throttle' in str(e).lower() or 'rate limit' in str(e).lower():
                    throttle.record_throttle()
                logger.error(f"Bedrock call failed: {e}")
                raise
    
    elif task_type == 'example_task':
        # Example: process some data (no throttling needed)
        import time
        time.sleep(1)  # Simulate work
        result = {'status': 'completed', 'message': 'Example task executed'}
        
    elif task_type == 'process_data':
        # Example: process a file
        file_path = payload.get('file')
        logger.info(f"Processing file: {file_path}")
        # ... your processing logic ...
        result = {'status': 'completed', 'file': file_path}
        
    else:
        logger.warning(f"Unknown task type: {task_type}")
        result = {'status': 'completed', 'message': f'Unknown task type: {task_type}'}
    
    logger.info(f"Step {step_id} completed with result: {result}")
    return result


# ============================================================================
# Application Lifecycle
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager
    Handles startup and shutdown
    """
    global config, storage, local_storage, queue_ops, worker_loops
    global health_monitor, leader_recovery, leader_election, control_plane
    
    logger.info("=" * 80)
    logger.info("FLEET-Q Starting...")
    logger.info("=" * 80)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        logger.info(f"Pod ID: {config.pod_id}")
        logger.info(f"Max parallelism: {config.max_parallelism}")
        logger.info(f"Capacity threshold: {config.capacity_threshold}")
        logger.info(f"Control Plane: {'Enabled' if config.enable_control_plane else 'Disabled'}")
        
        # Initialize storage
        logger.info("Initializing storage...")
        storage = SnowflakeStorage(config)
        local_storage = LocalStorage(config.local_db_path)
        
        # Initialize queue operations
        logger.info("Initializing queue operations...")
        queue_ops = QueueOperations(config, storage)
        
        # Initialize health monitor
        logger.info("Initializing health monitor...")
        health_monitor = HealthMonitor(config, storage)
        
        # Initialize leader components
        logger.info("Initializing leader components...")
        leader_election = LeaderElection(config, health_monitor)
        leader_recovery = LeaderRecovery(
            config, storage, local_storage, queue_ops, health_monitor
        )
        
        # Initialize worker loops
        logger.info("Initializing worker loops...")
        worker_loops = WorkerLoops(config, storage, queue_ops, execute_task)
        
        # Initialize Control Plane (if enabled)
        if config.enable_control_plane:
            logger.info("Initializing Control Plane Worker...")
            control_plane = initialize_control_plane(
                pod_id=config.pod_id,
                storage_conn=storage,
                flush_interval=config.control_plane_flush_interval,
                maintenance_interval=config.control_plane_maintenance_interval,
                base_path=config.control_plane_base_path
            )
            logger.info(f"  Flush interval: {config.control_plane_flush_interval}s")
            logger.info(f"  Base path: {config.control_plane_base_path}/{config.pod_id}")
            logger.info(f"  Writer pool: {config.control_plane_min_writers}-{config.control_plane_max_writers} workers")
        
        # Start background loops
        logger.info("Starting background loops...")
        await worker_loops.start()
        await leader_recovery.start()
        
        if control_plane:
            await control_plane.start()
            logger.info("Control Plane Worker started")
        
        logger.info("=" * 80)
        logger.info("FLEET-Q Started Successfully!")
        logger.info("=" * 80)
        
        # Yield control to the application
        yield
        
    finally:
        # Shutdown
        logger.info("=" * 80)
        logger.info("FLEET-Q Shutting down...")
        logger.info("=" * 80)
        
        if control_plane:
            await control_plane.stop()
            logger.info("Control Plane stopped")
        
        if worker_loops:
            await worker_loops.stop()
        
        if leader_recovery:
            await leader_recovery.stop()
        
        if storage:
            storage.close()
        
        logger.info("FLEET-Q Shutdown complete")


# ============================================================================
# FastAPI Application
# ============================================================================
app = FastAPI(
    title="FLEET-Q",
    description="Federated Leaderless Execution & Elastic Tasking Queue",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# Request/Response Models
# ============================================================================
class SubmitStepRequest(BaseModel):
    payload: Dict[str, Any]
    step_id: Optional[str] = None
    priority: int = 0


class SubmitStepResponse(BaseModel):
    step_id: str
    status: str = "submitted"


class StepStatusResponse(BaseModel):
    step_id: str
    status: str
    claimed_by: Optional[str]
    payload: Dict[str, Any]
    retry_count: int
    priority: int
    created_ts: str
    last_update_ts: str


class QueueStatsResponse(BaseModel):
    pending: int
    claimed: int
    completed: int
    failed: int


class LeaderInfoResponse(BaseModel):
    leader_pod_id: Optional[str]
    this_pod_id: str
    is_leader: bool
    total_alive_pods: int
    alive_pods: list


class RecoveryResponse(BaseModel):
    message: str
    stats: Optional[Dict[str, Any]]


class ThrottleStatsResponse(BaseModel):
    throttles: Dict[str, Dict[str, Any]]


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "pod_id": config.pod_id,
        "service": "FLEET-Q"
    }


@app.post("/submit", response_model=SubmitStepResponse)
async def submit_step(request: SubmitStepRequest):
    """
    Submit a new step to the queue
    
    Example payload:
    {
        "payload": {
            "task_type": "process_data",
            "file": "data.csv"
        },
        "priority": 1
    }
    """
    try:
        step_id = queue_ops.submit_step(
            payload=request.payload,
            step_id=request.step_id,
            priority=request.priority
        )
        
        return SubmitStepResponse(step_id=step_id)
    
    except Exception as e:
        logger.error(f"Failed to submit step: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{step_id}", response_model=StepStatusResponse)
async def get_step_status(step_id: str):
    """Get the current status of a step"""
    try:
        step = queue_ops.get_step_status(step_id)
        
        if not step:
            raise HTTPException(status_code=404, detail=f"Step {step_id} not found")
        
        return StepStatusResponse(
            step_id=step['STEP_ID'],
            status=step['STATUS'],
            claimed_by=step.get('CLAIMED_BY'),
            payload=step['PAYLOAD'],
            retry_count=step['RETRY_COUNT'],
            priority=step['PRIORITY'],
            created_ts=str(step['CREATED_TS']),
            last_update_ts=str(step['LAST_UPDATE_TS'])
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get step status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/queue", response_model=QueueStatsResponse)
async def get_queue_stats():
    """Get queue statistics"""
    try:
        stats = queue_ops.get_queue_stats()
        return QueueStatsResponse(**stats)
    
    except Exception as e:
        logger.error(f"Failed to get queue stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/leader", response_model=LeaderInfoResponse)
async def get_leader_info():
    """Get information about current leadership"""
    try:
        info = leader_election.get_leader_info()
        return LeaderInfoResponse(**info)
    
    except Exception as e:
        logger.error(f"Failed to get leader info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/recovery/run", response_model=RecoveryResponse)
async def trigger_recovery():
    """
    Manually trigger a recovery cycle
    
    Only works if this pod is the current leader.
    """
    try:
        if not leader_election.am_i_leader():
            return RecoveryResponse(
                message="This pod is not the leader. Recovery can only be triggered by the leader.",
                stats=None
            )
        
        # Run recovery in executor to avoid blocking
        loop = asyncio.get_event_loop()
        stats = await loop.run_in_executor(None, leader_recovery.run_recovery_cycle)
        
        return RecoveryResponse(
            message="Recovery cycle completed successfully",
            stats=stats
        )
    
    except Exception as e:
        logger.error(f"Failed to run recovery: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/throttle", response_model=ThrottleStatsResponse)
async def get_throttle_stats():
    """
    Get statistics for all registered adaptive throttles
    
    Shows current limits, in-flight requests, success rates, etc.
    """
    try:
        from throttle import get_all_throttles
        
        throttles = get_all_throttles()
        stats = {
            name: throttle.get_stats()
            for name, throttle in throttles.items()
        }
        
        return ThrottleStatsResponse(throttles=stats)
    
    except Exception as e:
        logger.error(f"Failed to get throttle stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/throttle/{throttle_name}/reset")
async def reset_throttle(throttle_name: str):
    """
    Reset a specific throttle to its initial state
    
    Useful for manual intervention or testing.
    """
    try:
        from throttle import get_all_throttles
        
        throttles = get_all_throttles()
        
        if throttle_name not in throttles:
            raise HTTPException(
                status_code=404,
                detail=f"Throttle '{throttle_name}' not found"
            )
        
        throttle = throttles[throttle_name]
        throttle.reset()
        
        return {
            "message": f"Throttle '{throttle_name}' reset successfully",
            "stats": throttle.get_stats()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reset throttle: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Control Plane Endpoints
# ============================================================================

class BulkWriteRequest(BaseModel):
    writer_type: str  # "snowflake", "sharepoint", "bedrock", etc.
    destination: str  # table name, bucket, etc.
    data: Dict[str, Any]
    orm_type: Optional[str] = None
    priority: int = 0


class BulkWriteResponse(BaseModel):
    operation_id: str
    status: str = "queued"
    message: str = "Operation queued for bulk processing"


class ControlPlaneStatsResponse(BaseModel):
    enabled: bool
    pod_id: str
    running: bool
    buffer_stats: Dict[str, Any]
    storage_stats: Dict[str, Any]
    writer_pool: Dict[str, Any]


@app.post("/control-plane/write", response_model=BulkWriteResponse)
async def submit_bulk_write(request: BulkWriteRequest):
    """
    Submit a write operation to the control plane for bulk processing.
    
    Operations are batched and written in bulk after pooling for 15-30 seconds.
    
    Example:
    {
        "writer_type": "snowflake",
        "destination": "MY_TABLE",
        "data": {"col1": "value1", "col2": "value2"},
        "orm_type": "sqlalchemy",
        "priority": 1
    }
    """
    if not control_plane:
        raise HTTPException(
            status_code=503,
            detail="Control plane not enabled or not initialized"
        )
    
    try:
        from control_plane import WriterType, submit_bulk_write
        
        # Map string to WriterType enum
        writer_type_map = {
            "snowflake": WriterType.SNOWFLAKE,
            "sharepoint": WriterType.SHAREPOINT,
            "bedrock": WriterType.BEDROCK,
            "s3": WriterType.S3,
            "local_db": WriterType.LOCAL_DB
        }
        
        writer_type = writer_type_map.get(request.writer_type.lower())
        if not writer_type:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid writer_type. Must be one of: {list(writer_type_map.keys())}"
            )
        
        operation_id = await submit_bulk_write(
            writer_type=writer_type,
            destination=request.destination,
            data=request.data,
            orm_type=request.orm_type,
            priority=request.priority
        )
        
        return BulkWriteResponse(
            operation_id=operation_id,
            status="queued",
            message=f"Operation queued for bulk processing (will flush in ~{config.control_plane_flush_interval}s)"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit bulk write: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/control-plane/stats", response_model=ControlPlaneStatsResponse)
async def get_control_plane_stats():
    """
    Get control plane statistics and status.
    
    Returns information about:
    - Buffer sizes by destination/ORM
    - Pending operations
    - Active writer pool size
    - Queue depth
    """
    if not control_plane:
        return ControlPlaneStatsResponse(
            enabled=False,
            pod_id=config.pod_id,
            running=False,
            buffer_stats={},
            storage_stats={},
            writer_pool={}
        )
    
    try:
        stats = control_plane.get_stats()
        
        return ControlPlaneStatsResponse(
            enabled=True,
            pod_id=stats['pod_id'],
            running=stats['running'],
            buffer_stats=stats['buffer_stats'],
            storage_stats=stats['storage_stats'],
            writer_pool=stats['writer_pool']
        )
    
    except Exception as e:
        logger.error(f"Failed to get control plane stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/control-plane/flush")
async def trigger_flush():
    """
    Manually trigger a flush of all write buffers.
    
    Useful for testing or when immediate writes are needed.
    """
    if not control_plane:
        raise HTTPException(
            status_code=503,
            detail="Control plane not enabled or not initialized"
        )
    
    try:
        # Get batches to flush
        batches = control_plane.buffer_manager.get_batches_to_flush()
        
        # Submit to writer pool
        for batch in batches:
            await control_plane.writer_pool.submit_batch(batch)
            control_plane.local_storage.record_batch(batch, status='queued')
        
        # Scale workers if needed
        await control_plane.writer_pool.scale_workers(control_plane.storage_conn)
        
        return {
            "message": f"Flushed {len(batches)} batches",
            "batches_flushed": len(batches),
            "total_operations": sum(batch.size() for batch in batches)
        }
    
    except Exception as e:
        logger.error(f"Failed to trigger flush: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/control-plane/maintenance")
async def trigger_maintenance():
    """
    Manually trigger database maintenance.
    
    Cleans up old completed operations and batch history.
    """
    if not control_plane:
        raise HTTPException(
            status_code=503,
            detail="Control plane not enabled or not initialized"
        )
    
    try:
        control_plane.local_storage.cleanup_old_records(max_age_seconds=86400)
        stats = control_plane.local_storage.get_stats()
        
        return {
            "message": "Maintenance completed successfully",
            "storage_stats": stats
        }
    
    except Exception as e:
        logger.error(f"Failed to trigger maintenance: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def root():
    """Root endpoint with basic info"""
    control_plane_enabled = control_plane is not None and control_plane.running
    
    endpoints = {
        "health": "/health",
        "submit": "POST /submit",
        "status": "GET /status/{step_id}",
        "queue_stats": "GET /admin/queue",
        "leader_info": "GET /admin/leader",
        "trigger_recovery": "POST /admin/recovery/run",
        "throttle_stats": "GET /admin/throttle",
        "reset_throttle": "POST /admin/throttle/{throttle_name}/reset"
    }
    
    if control_plane_enabled:
        endpoints.update({
            "control_plane_write": "POST /control-plane/write",
            "control_plane_stats": "GET /control-plane/stats",
            "control_plane_flush": "POST /control-plane/flush",
            "control_plane_maintenance": "POST /control-plane/maintenance"
        })
    
    return {
        "service": "FLEET-Q",
        "description": "Federated Leaderless Execution & Elastic Tasking Queue",
        "version": "1.0.0",
        "pod_id": config.pod_id,
        "control_plane_enabled": control_plane_enabled,
        "endpoints": endpoints
    }


# ============================================================================
# Main Entry Point
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment or use default
    port = int(os.getenv("PORT", "8000"))
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=False  # Set to True for development
    )
