"""
Storage interface abstractions for FLEET-Q.

Defines abstract base classes for Snowflake and SQLite operations, along with
serialization/deserialization utilities for Snowflake VARIANT types.

This module provides the foundational storage abstractions that will be
implemented by concrete Snowflake and SQLite backends.
"""

import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from .models import Step, PodHealth, DLQRecord, StepPayload, StepStatus, PodStatus


class VariantSerializer:
    """
    Utility class for serializing/deserializing data for Snowflake VARIANT types.
    
    Handles conversion between Python objects and JSON strings that can be
    stored in Snowflake VARIANT columns.
    """
    
    @staticmethod
    def serialize(data: Union[Dict[str, Any], StepPayload, Any]) -> str:
        """
        Serialize Python data to JSON string for VARIANT storage.
        
        Args:
            data: Python object to serialize (dict, StepPayload, or other JSON-serializable)
            
        Returns:
            str: JSON string representation
            
        Raises:
            TypeError: If data is not JSON serializable
        """
        if isinstance(data, StepPayload):
            # Convert StepPayload to dict for serialization
            return json.dumps({
                "type": data.type,
                "args": data.args,
                "metadata": data.metadata
            })
        elif isinstance(data, dict):
            return json.dumps(data)
        else:
            # Handle other types that might be JSON serializable
            return json.dumps(data)
    
    @staticmethod
    def deserialize_step_payload(variant_str: str) -> StepPayload:
        """
        Deserialize JSON string to StepPayload object.
        
        Args:
            variant_str: JSON string from VARIANT column
            
        Returns:
            StepPayload: Deserialized payload object
            
        Raises:
            ValueError: If JSON is invalid or missing required fields
        """
        try:
            data = json.loads(variant_str)
            return StepPayload(
                type=data["type"],
                args=data.get("args", {}),
                metadata=data.get("metadata", {})
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid step payload JSON: {e}") from e
    
    @staticmethod
    def deserialize_dict(variant_str: str) -> Dict[str, Any]:
        """
        Deserialize JSON string to dictionary.
        
        Args:
            variant_str: JSON string from VARIANT column
            
        Returns:
            Dict[str, Any]: Deserialized dictionary
            
        Raises:
            ValueError: If JSON is invalid
        """
        try:
            return json.loads(variant_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e


class StorageInterface(ABC):
    """
    Abstract base class defining the storage interface for FLEET-Q operations.
    
    This interface defines all storage operations needed by the distributed
    task queue system, including step management, pod health tracking,
    and recovery operations.
    """
    
    @abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the storage backend.
        
        This method should set up connections, create tables if needed,
        and perform any other initialization required.
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """
        Close the storage backend and clean up resources.
        """
        pass
    
    # Step Management Operations
    
    @abstractmethod
    async def create_step(self, step: Step) -> None:
        """
        Create a new step in the storage backend.
        
        Args:
            step: Step object to create
            
        Raises:
            StorageError: If step creation fails
        """
        pass
    
    @abstractmethod
    async def get_step(self, step_id: str) -> Optional[Step]:
        """
        Retrieve a step by its ID.
        
        Args:
            step_id: Unique identifier for the step
            
        Returns:
            Optional[Step]: Step object if found, None otherwise
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def claim_steps(self, pod_id: str, limit: int) -> List[Step]:
        """
        Atomically claim available steps for execution.
        
        This operation should use database transactions to ensure atomicity
        and prevent race conditions between multiple pods.
        
        Args:
            pod_id: ID of the pod claiming the steps
            limit: Maximum number of steps to claim
            
        Returns:
            List[Step]: List of successfully claimed steps
            
        Raises:
            StorageError: If claiming fails
        """
        pass
    
    @abstractmethod
    async def update_step_status(
        self, 
        step_id: str, 
        status: StepStatus, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update the status of a step.
        
        Args:
            step_id: ID of the step to update
            status: New status for the step
            metadata: Optional metadata to include in the update
            
        Raises:
            StorageError: If update fails
        """
        pass
    
    @abstractmethod
    async def complete_step(
        self, 
        step_id: str, 
        result_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a step as completed with optional result metadata.
        
        Atomically transitions step from claimed to completed status,
        clears claimed_by field, and updates timestamp.
        
        Args:
            step_id: ID of the step to complete
            result_metadata: Optional metadata about the execution result
            
        Raises:
            StorageError: If completion fails
        """
        pass
    
    @abstractmethod
    async def fail_step(
        self, 
        step_id: str, 
        error_message: Optional[str] = None,
        error_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a step as failed with optional error information.
        
        Atomically transitions step from claimed to failed status,
        clears claimed_by field, and updates timestamp.
        
        Args:
            step_id: ID of the step to fail
            error_message: Optional error message describing the failure
            error_metadata: Optional metadata about the failure
            
        Raises:
            StorageError: If failure update fails
        """
        pass
    
    @abstractmethod
    async def get_steps_by_status(self, status: StepStatus) -> List[Step]:
        """
        Retrieve all steps with a specific status.
        
        Args:
            status: Status to filter by
            
        Returns:
            List[Step]: List of steps with the specified status
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_steps_claimed_by_pods(self, pod_ids: List[str]) -> List[Step]:
        """
        Retrieve all steps claimed by specific pods.
        
        Used during recovery to find orphaned steps.
        
        Args:
            pod_ids: List of pod IDs to search for
            
        Returns:
            List[Step]: List of steps claimed by the specified pods
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def requeue_steps(self, step_ids: List[str]) -> None:
        """
        Requeue steps back to pending status with incremented retry count.
        
        Args:
            step_ids: List of step IDs to requeue
            
        Raises:
            StorageError: If requeuing fails
        """
        pass
    
    # Pod Health Management Operations
    
    @abstractmethod
    async def upsert_pod_health(self, pod_health: PodHealth) -> None:
        """
        Insert or update pod health record.
        
        Args:
            pod_health: PodHealth object to upsert
            
        Raises:
            StorageError: If upsert fails
        """
        pass
    
    @abstractmethod
    async def get_pod_health(self, pod_id: str) -> Optional[PodHealth]:
        """
        Retrieve pod health record by ID.
        
        Args:
            pod_id: ID of the pod
            
        Returns:
            Optional[PodHealth]: Pod health record if found, None otherwise
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_all_pod_health(self) -> List[PodHealth]:
        """
        Retrieve all pod health records.
        
        Returns:
            List[PodHealth]: List of all pod health records
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_active_pods(self, heartbeat_threshold_seconds: int) -> List[PodHealth]:
        """
        Retrieve pods that are considered active based on heartbeat threshold.
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            List[PodHealth]: List of active pod health records
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_dead_pods(self, heartbeat_threshold_seconds: int) -> List[PodHealth]:
        """
        Get pods that are considered dead based on stale heartbeats.
        
        A pod is considered dead if its last_heartbeat_ts is older than
        the specified threshold and its status is not already 'down'.
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat before pod is considered dead
            
        Returns:
            List[PodHealth]: List of dead pod health records
            
        Raises:
            StorageError: If the query fails
        """
        pass
    
    @abstractmethod
    async def mark_pods_dead(self, pod_ids: List[str]) -> None:
        """
        Mark specified pods as dead by updating their status.
        
        Args:
            pod_ids: List of pod IDs to mark as dead
            
        Raises:
            StorageError: If update fails
        """
        pass
    
    @abstractmethod
    async def determine_leader(self, heartbeat_threshold_seconds: int) -> Optional[str]:
        """
        Determine the current leader pod based on eldest pod with fresh heartbeat.
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            Optional[str]: Pod ID of the current leader, None if no eligible pods
            
        Raises:
            StorageError: If leader determination fails
        """
        pass
    
    @abstractmethod
    async def am_i_leader(self, pod_id: str, heartbeat_threshold_seconds: int) -> bool:
        """
        Check if the specified pod is the current leader.
        
        Args:
            pod_id: Pod ID to check for leadership
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            bool: True if the pod is the leader, False otherwise
            
        Raises:
            StorageError: If leader check fails
        """
        pass
    
    @abstractmethod
    async def get_leader_info(self, heartbeat_threshold_seconds: int) -> Dict[str, Any]:
        """
        Get comprehensive leader election information.
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            Dict[str, Any]: Leader information including current leader, active pods, etc.
            
        Raises:
            StorageError: If leader info retrieval fails
        """
        pass
    
    # Statistics and Monitoring Operations
    
    @abstractmethod
    async def get_queue_stats(self) -> Dict[str, int]:
        """
        Get queue statistics including step counts by status.
        
        Returns:
            Dict[str, int]: Dictionary with counts for each step status
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    # Leader Election Operations
    
    @abstractmethod
    async def determine_leader(self, heartbeat_threshold_seconds: int) -> Optional[str]:
        """
        Determine the current leader based on eldest pod with fresh heartbeat.
        
        Leader election rule: The leader is the pod with the earliest birth_ts
        among all pods with status='up' and fresh heartbeat (within threshold).
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            Optional[str]: Pod ID of the current leader, None if no eligible pods
            
        Raises:
            StorageError: If leader determination fails
        """
        pass
    
    @abstractmethod
    async def am_i_leader(self, pod_id: str, heartbeat_threshold_seconds: int) -> bool:
        """
        Check if the specified pod is the current leader.
        
        Args:
            pod_id: ID of the pod to check
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            bool: True if the pod is the leader, False otherwise
            
        Raises:
            StorageError: If leader check fails
        """
        pass
    
    @abstractmethod
    async def get_leader_info(self, heartbeat_threshold_seconds: int) -> Dict[str, Any]:
        """
        Get comprehensive leader election information.
        
        Returns information about the current leader, active pods, and election status.
        
        Args:
            heartbeat_threshold_seconds: Maximum age of heartbeat to consider active
            
        Returns:
            Dict[str, Any]: Leader information including current leader, active pods count, etc.
            
        Raises:
            StorageError: If leader info retrieval fails
        """
        pass


class SQLiteStorageInterface(ABC):
    """
    Abstract base class for local SQLite storage operations.
    
    Used for ephemeral storage during recovery cycles, particularly
    for Dead Letter Queue (DLQ) management.
    """
    
    @abstractmethod
    async def initialize(self, db_path: str) -> None:
        """
        Initialize SQLite database connection.
        
        Args:
            db_path: Path to SQLite database file (or ":memory:" for in-memory)
            
        Raises:
            StorageError: If initialization fails
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """
        Close SQLite database connection.
        """
        pass
    
    # DLQ Table Management
    
    @abstractmethod
    async def create_dlq_table(self, cycle_id: str) -> None:
        """
        Create a DLQ table for a specific recovery cycle.
        
        Args:
            cycle_id: Unique identifier for the recovery cycle
            
        Raises:
            StorageError: If table creation fails
        """
        pass
    
    @abstractmethod
    async def drop_dlq_table(self, cycle_id: str) -> None:
        """
        Drop a DLQ table for a specific recovery cycle.
        
        Args:
            cycle_id: Unique identifier for the recovery cycle
            
        Raises:
            StorageError: If table drop fails
        """
        pass
    
    @abstractmethod
    async def insert_dlq_record(self, cycle_id: str, record: DLQRecord) -> None:
        """
        Insert a record into the DLQ table.
        
        Args:
            cycle_id: Recovery cycle identifier
            record: DLQ record to insert
            
        Raises:
            StorageError: If insertion fails
        """
        pass
    
    @abstractmethod
    async def get_dlq_records(self, cycle_id: str) -> List[DLQRecord]:
        """
        Retrieve all records from a DLQ table.
        
        Args:
            cycle_id: Recovery cycle identifier
            
        Returns:
            List[DLQRecord]: List of DLQ records
            
        Raises:
            StorageError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_dlq_records_by_action(self, cycle_id: str, action: str) -> List[DLQRecord]:
        """
        Retrieve DLQ records filtered by action type.
        
        Args:
            cycle_id: Recovery cycle identifier
            action: Action type to filter by ("requeue" or "terminal")
            
        Returns:
            List[DLQRecord]: List of filtered DLQ records
            
        Raises:
            StorageError: If retrieval fails
        """
        pass


class StorageError(Exception):
    """
    Base exception class for storage-related errors.
    
    Used to wrap database-specific exceptions and provide consistent
    error handling across different storage backends.
    """
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """
        Initialize storage error.
        
        Args:
            message: Human-readable error message
            original_error: Original exception that caused this error
        """
        super().__init__(message)
        self.original_error = original_error


class TransactionConflictError(StorageError):
    """
    Exception raised when a database transaction conflicts with another.
    
    This is typically used for step claiming conflicts where multiple
    pods attempt to claim the same steps simultaneously.
    """
    pass


class ConnectionError(StorageError):
    """
    Exception raised when database connection fails.
    
    Used for network timeouts, authentication failures, and other
    connection-related issues.
    """
    pass


# Utility functions for common storage operations

def generate_idempotency_key(step_id: str) -> str:
    """
    Generate an idempotency key from a step ID.
    
    This key can be used to ensure idempotent step execution by
    checking if work has already been completed for this step.
    
    Args:
        step_id: Unique step identifier
        
    Returns:
        str: Idempotency key derived from step_id
    """
    # Simple implementation - in production might want to use a hash
    # or more sophisticated key generation
    return f"idempotent_{step_id}"


def create_step_from_submission(
    step_id: str,
    payload: StepPayload,
    priority: int = 0,
    max_retries: int = 3
) -> Step:
    """
    Create a Step object from submission parameters.
    
    Args:
        step_id: Unique identifier for the step
        payload: Step payload containing type, args, and metadata
        priority: Step priority (higher = more priority)
        max_retries: Maximum number of retry attempts
        
    Returns:
        Step: Newly created step object with pending status
    """
    now = datetime.utcnow()
    return Step(
        step_id=step_id,
        status=StepStatus.PENDING,
        claimed_by=None,
        last_update_ts=now,
        payload=payload,
        retry_count=0,
        priority=priority,
        created_ts=now,
        max_retries=max_retries
    )


def create_pod_health_record(
    pod_id: str,
    metadata: Optional[Dict[str, Any]] = None
) -> PodHealth:
    """
    Create a PodHealth record for a new pod.
    
    Args:
        pod_id: Unique identifier for the pod
        metadata: Optional metadata about the pod (CPU, threads, version, etc.)
        
    Returns:
        PodHealth: Newly created pod health record with UP status
    """
    now = datetime.utcnow()
    return PodHealth(
        pod_id=pod_id,
        birth_ts=now,
        last_heartbeat_ts=now,
        status=PodStatus.UP,
        meta=metadata or {}
    )


def create_dlq_record(
    step: Step,
    reason: str,
    action: str,
    recovery_timestamp: Optional[datetime] = None
) -> DLQRecord:
    """
    Create a DLQ record from a step during recovery.
    
    Args:
        step: Step that is being processed during recovery
        reason: Reason for DLQ entry ("dead_pod", "timeout", "poison")
        action: Action to take ("requeue" or "terminal")
        recovery_timestamp: When the recovery occurred (defaults to now)
        
    Returns:
        DLQRecord: DLQ record for the step
    """
    return DLQRecord(
        step_id=step.step_id,
        original_payload=step.payload,
        claimed_by=step.claimed_by or "unknown",
        claim_timestamp=step.last_update_ts,
        recovery_timestamp=recovery_timestamp or datetime.utcnow(),
        reason=reason,
        retry_count=step.retry_count,
        max_retries=step.max_retries,
        action=action
    )


# Import concrete implementations for convenience
try:
    from .snowflake_storage import SnowflakeStorage
    from .sqlite_storage import SQLiteStorage
    __all__ = [
        'StorageInterface', 'SQLiteStorageInterface', 'StorageError', 
        'TransactionConflictError', 'ConnectionError', 'VariantSerializer',
        'SnowflakeStorage', 'SQLiteStorage',
        'generate_idempotency_key', 'create_step_from_submission', 
        'create_pod_health_record', 'create_dlq_record'
    ]
except ImportError:
    # Concrete implementations not available (e.g., during testing)
    __all__ = [
        'StorageInterface', 'SQLiteStorageInterface', 'StorageError', 
        'TransactionConflictError', 'ConnectionError', 'VariantSerializer',
        'generate_idempotency_key', 'create_step_from_submission', 
        'create_pod_health_record', 'create_dlq_record'
    ]