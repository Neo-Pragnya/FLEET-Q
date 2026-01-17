"""
FastAPI application for FLEET-Q.

Provides REST endpoints for step submission, status queries, and
administrative operations. 
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse

from .models import (
    StepSubmissionRequest, 
    StepStatusResponse, 
    HealthResponse,
    QueueStatsResponse,
    LeaderInfoResponse
)
from .step_service import StepService, StepSubmissionError, StepValidationError
from .leader_service import LeaderElectionService
from .storage import StorageInterface
from .config import FleetQConfig

logger = logging.getLogger(__name__)


class FleetQAPI:
    """
    FastAPI application wrapper for FLEET-Q.
    
    Provides REST endpoints and manages dependencies like storage
    and step service instances.
    """
    
    def __init__(self, storage: StorageInterface, config: FleetQConfig, sqlite_storage=None):
        """
        Initialize the FastAPI application.
        
        Args:
            storage: Storage backend for operations
            config: Application configuration
            sqlite_storage: SQLite storage for recovery operations (optional)
        """
        self.storage = storage
        self.config = config
        self.sqlite_storage = sqlite_storage
        self.step_service = StepService(storage, config)
        self.leader_service = LeaderElectionService(storage, config)
        self.app = FastAPI(
            title="FLEET-Q Distributed Task Queue",
            description="Brokerless distributed task queue for EKS environments",
            version="1.0.0"
        )
        self.startup_time = datetime.utcnow()
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register all API routes."""
        
        @self.app.get("/health", response_model=HealthResponse)
        async def health():
            """
            Get pod health status.
            
            Returns current pod health information including capacity
            and operational status.
            """
            try:
                uptime = (datetime.utcnow() - self.startup_time).total_seconds()
                
                # Check if this pod is the leader
                is_leader = await self.leader_service.am_i_leader()
                
                return HealthResponse(
                    pod_id=self.config.pod_id,
                    status="healthy",
                    uptime_seconds=uptime,
                    current_capacity=0,  # TODO: Implement capacity tracking
                    max_capacity=self.config.max_concurrent_steps,
                    is_leader=is_leader,
                    version="1.0.0"
                )
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=500, detail="Health check failed")
        
        @self.app.post("/submit", response_model=dict)
        async def submit_step(request: StepSubmissionRequest):
            """
            Submit a new step to the queue.
            
            Creates a new step with unique ID and stores it in the
            STEP_TRACKER table with pending status.
            """
            try:
                step_id = await self.step_service.submit_step(request)
                
                return {
                    "step_id": step_id,
                    "status": "submitted",
                    "message": f"Step {step_id} submitted successfully"
                }
                
            except StepValidationError as e:
                logger.warning(f"Step validation failed: {e}")
                raise HTTPException(status_code=400, detail=str(e))
            except StepSubmissionError as e:
                logger.error(f"Step submission failed: {e}")
                raise HTTPException(status_code=500, detail="Step submission failed")
            except Exception as e:
                logger.error(f"Unexpected error during step submission: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.get("/status/{step_id}", response_model=StepStatusResponse)
        async def get_step_status(step_id: str):
            """
            Get the current status of a step.
            
            Returns detailed information about the step including
            its current status, retry count, and timestamps.
            """
            try:
                step = await self.step_service.get_step_status(step_id)
                
                if not step:
                    raise HTTPException(status_code=404, detail=f"Step {step_id} not found")
                
                return StepStatusResponse(
                    step_id=step.step_id,
                    status=step.status,
                    claimed_by=step.claimed_by,
                    last_update_ts=step.last_update_ts,
                    retry_count=step.retry_count,
                    priority=step.priority,
                    created_ts=step.created_ts or step.last_update_ts,
                    max_retries=step.max_retries
                )
                
            except HTTPException:
                # Re-raise HTTP exceptions as-is
                raise
            except Exception as e:
                logger.error(f"Failed to get step status for {step_id}: {e}")
                raise HTTPException(status_code=500, detail="Status retrieval failed")
        
        @self.app.get("/admin/queue", response_model=QueueStatsResponse)
        async def get_queue_stats():
            """
            Get current queue statistics.
            
            Returns counts of steps by status and other queue metrics.
            """
            try:
                stats = await self.step_service.get_queue_statistics()
                
                # Get leader information
                leader_info = await self.leader_service.get_leader_info()
                
                return QueueStatsResponse(
                    pending_steps=stats.get('pending', 0),
                    claimed_steps=stats.get('claimed', 0),
                    completed_steps=stats.get('completed', 0),
                    failed_steps=stats.get('failed', 0),
                    active_pods=leader_info.get('active_pods_count', 1),
                    current_leader=leader_info.get('current_leader')
                )
                
            except Exception as e:
                logger.error(f"Failed to get queue statistics: {e}")
                raise HTTPException(status_code=500, detail="Statistics retrieval failed")
        
        @self.app.get("/idempotency/{step_id}")
        async def get_idempotency_key(step_id: str):
            """
            Get the idempotency key for a step.
            
            Returns the idempotency key that can be used by step
            implementations to ensure idempotent execution.
            """
            try:
                # Validate that the step exists
                exists = await self.step_service.validate_step_exists(step_id)
                if not exists:
                    raise HTTPException(status_code=404, detail=f"Step {step_id} not found")
                
                idempotency_key = self.step_service.get_idempotency_key(step_id)
                
                return {
                    "step_id": step_id,
                    "idempotency_key": idempotency_key
                }
                
            except HTTPException:
                # Re-raise HTTP exceptions as-is
                raise
            except Exception as e:
                logger.error(f"Failed to get idempotency key for {step_id}: {e}")
                raise HTTPException(status_code=500, detail="Idempotency key retrieval failed")
        
        @self.app.get("/admin/leader", response_model=LeaderInfoResponse)
        async def get_leader_info():
            """
            Get leader election information.
            
            Returns detailed information about the current leader,
            active pods, and election status for administrative monitoring.
            """
            try:
                leader_info_response = await self.leader_service.get_leader_info_response()
                
                logger.debug(
                    f"Leader info requested",
                    extra={
                        "current_leader": leader_info_response.current_leader,
                        "active_pods": leader_info_response.active_pods,
                        "requesting_pod": self.config.pod_id
                    }
                )
                
                return leader_info_response
                
            except Exception as e:
                logger.error(f"Failed to get leader information: {e}")
                raise HTTPException(status_code=500, detail="Leader information retrieval failed")
        
        @self.app.post("/admin/recovery/run")
        async def trigger_manual_recovery():
            """
            Trigger manual recovery cycle.
            
            Forces an immediate recovery cycle to detect dead pods and
            requeue orphaned steps. Only available to the current leader pod.
            """
            try:
                # Check if this pod is the leader
                is_leader = await self.leader_service.am_i_leader()
                if not is_leader:
                    current_leader = await self.leader_service.determine_leader()
                    raise HTTPException(
                        status_code=403, 
                        detail=f"Manual recovery can only be triggered by the leader pod. Current leader: {current_leader or 'none'}"
                    )
                
                logger.info(f"Manual recovery triggered by pod {self.config.pod_id}")
                
                # Check if we have SQLite storage available
                if not self.sqlite_storage:
                    # Create temporary SQLite storage for this operation
                    from .sqlite_storage import SQLiteStorage
                    temp_sqlite = SQLiteStorage(self.config)
                    await temp_sqlite.initialize()
                    sqlite_storage = temp_sqlite
                else:
                    sqlite_storage = self.sqlite_storage
                
                # Import recovery service here to avoid circular imports
                from .recovery_service import create_recovery_service
                
                # Create a temporary recovery service instance
                recovery_service = create_recovery_service(
                    self.storage,
                    sqlite_storage,
                    self.config
                )
                
                # Trigger recovery cycle
                recovery_summary = await recovery_service.run_recovery_cycle()
                
                # Clean up temporary SQLite storage if we created one
                if not self.sqlite_storage and sqlite_storage:
                    await sqlite_storage.close()
                
                logger.info(
                    f"Manual recovery completed",
                    extra={
                        "cycle_id": recovery_summary.cycle_id,
                        "dead_pods": len(recovery_summary.dead_pods),
                        "orphaned_steps": recovery_summary.orphaned_steps_found,
                        "requeued_steps": recovery_summary.steps_requeued,
                        "terminal_steps": recovery_summary.steps_marked_terminal,
                        "duration_ms": recovery_summary.recovery_duration_ms
                    }
                )
                
                return {
                    "status": "completed",
                    "message": "Manual recovery cycle completed successfully",
                    "recovery_summary": {
                        "cycle_id": recovery_summary.cycle_id,
                        "dead_pods_found": len(recovery_summary.dead_pods),
                        "orphaned_steps_found": recovery_summary.orphaned_steps_found,
                        "steps_requeued": recovery_summary.steps_requeued,
                        "steps_marked_terminal": recovery_summary.steps_marked_terminal,
                        "duration_ms": recovery_summary.recovery_duration_ms,
                        "dead_pods": recovery_summary.dead_pods
                    }
                }
                
            except HTTPException:
                # Re-raise HTTP exceptions as-is
                raise
            except Exception as e:
                logger.error(f"Manual recovery failed: {e}")
                raise HTTPException(status_code=500, detail=f"Manual recovery failed: {str(e)}")
        
        # Add exception handlers
        @self.app.exception_handler(StepValidationError)
        async def validation_error_handler(request, exc):
            """Handle step validation errors."""
            return JSONResponse(
                status_code=400,
                content={"detail": str(exc), "error_type": "validation_error"}
            )
        
        @self.app.exception_handler(StepSubmissionError)
        async def submission_error_handler(request, exc):
            """Handle step submission errors."""
            return JSONResponse(
                status_code=500,
                content={"detail": str(exc), "error_type": "submission_error"}
            )


def create_app(storage: StorageInterface, config: FleetQConfig, sqlite_storage=None) -> FastAPI:
    """
    Factory function to create a FastAPI application.
    
    Args:
        storage: Storage backend for operations
        config: Application configuration
        sqlite_storage: SQLite storage for recovery operations (optional)
        
    Returns:
        FastAPI: Configured FastAPI application
    """
    api = FleetQAPI(storage, config, sqlite_storage)
    return api.app