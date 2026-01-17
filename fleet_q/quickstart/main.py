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
from leader import LeaderElection, LeaderRecovery
from queue import QueueOperations
from storage import LocalStorage, SnowflakeStorage
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


# ============================================================================
# Task Executor - Customize this for your use case
# ============================================================================
def execute_task(step: Dict[str, Any]) -> Any:
    """
    Custom task execution logic
    
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
    if task_type == 'example_task':
        # Example: process some data
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
    global health_monitor, leader_recovery, leader_election
    
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
        
        # Start background loops
        logger.info("Starting background loops...")
        await worker_loops.start()
        await leader_recovery.start()
        
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


@app.get("/")
async def root():
    """Root endpoint with basic info"""
    return {
        "service": "FLEET-Q",
        "description": "Federated Leaderless Execution & Elastic Tasking Queue",
        "version": "1.0.0",
        "pod_id": config.pod_id,
        "endpoints": {
            "health": "/health",
            "submit": "POST /submit",
            "status": "GET /status/{step_id}",
            "queue_stats": "GET /admin/queue",
            "leader_info": "GET /admin/leader",
            "trigger_recovery": "POST /admin/recovery/run"
        }
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
