"""
Data models for FLEET-Q system.

Defines the core data structures used throughout the system including
Step, PodHealth, and related enums and types.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass
from pydantic import BaseModel, Field


class StepStatus(str, Enum):
    """Step execution status enumeration."""
    PENDING = "pending"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"


class PodStatus(str, Enum):
    """Pod health status enumeration."""
    UP = "up"
    DOWN = "down"


@dataclass
class StepPayload:
    """
    Payload data for a step/task.
    
    Contains the task type, arguments, and metadata needed for execution.
    """
    type: str  # Task type identifier
    args: Dict[str, Any]  # Task arguments
    metadata: Dict[str, Any]  # Additional metadata


@dataclass
class Step:
    """
    Represents a single task/step in the queue system.
    
    Tracks the complete lifecycle of a step from submission to completion.
    """
    step_id: str
    status: StepStatus
    claimed_by: Optional[str]
    last_update_ts: datetime
    payload: StepPayload
    retry_count: int = 0
    priority: int = 0
    created_ts: Optional[datetime] = None
    max_retries: int = 3


@dataclass
class PodHealth:
    """
    Represents the health status of a pod in the system.
    
    Used for leader election and dead pod detection.
    """
    pod_id: str
    birth_ts: datetime
    last_heartbeat_ts: datetime
    status: PodStatus
    meta: Dict[str, Any]  # CPU, threads, version, etc.


@dataclass
class DLQRecord:
    """
    Dead Letter Queue record for tracking orphaned steps during recovery.
    
    Used in local SQLite tables during recovery cycles.
    """
    step_id: str
    original_payload: StepPayload
    claimed_by: str
    claim_timestamp: datetime
    recovery_timestamp: datetime
    reason: str  # dead_pod | timeout | poison
    retry_count: int
    max_retries: int
    action: str  # requeue | terminal


@dataclass
class RecoverySummary:
    """
    Summary information about a recovery cycle.
    
    Used for logging and operational visibility.
    """
    cycle_id: str
    dead_pods: list[str]
    orphaned_steps_found: int
    steps_requeued: int
    steps_marked_terminal: int
    recovery_duration_ms: int


# Pydantic models for API requests/responses
class StepSubmissionRequest(BaseModel):
    """Request model for step submission API."""
    type: str = Field(..., description="Task type identifier")
    args: Dict[str, Any] = Field(default_factory=dict, description="Task arguments")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    priority: int = Field(default=0, description="Step priority (higher = more priority)")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")


class StepStatusResponse(BaseModel):
    """Response model for step status queries."""
    step_id: str
    status: StepStatus
    claimed_by: Optional[str]
    last_update_ts: datetime
    retry_count: int
    priority: int
    created_ts: datetime
    max_retries: int


class HealthResponse(BaseModel):
    """Response model for pod health endpoint."""
    pod_id: str
    status: str
    uptime_seconds: float
    current_capacity: int
    max_capacity: int
    is_leader: bool
    version: str


class QueueStatsResponse(BaseModel):
    """Response model for queue statistics."""
    pending_steps: int
    claimed_steps: int
    completed_steps: int
    failed_steps: int
    active_pods: int
    current_leader: Optional[str]


class LeaderInfoResponse(BaseModel):
    """Response model for leader election information."""
    current_leader: Optional[str]
    leader_birth_ts: Optional[datetime]
    active_pods: int
    last_election_check: datetime