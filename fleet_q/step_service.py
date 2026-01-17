"""
Step submission service for FLEET-Q.

Implements the core step submission logic including unique ID generation,
payload validation, and storage operations. Provides idempotency key
generation and handles step creation with proper error handling.

"""

import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from .models import Step, StepPayload, StepStatus, StepSubmissionRequest
from .storage import StorageInterface, create_step_from_submission, generate_idempotency_key
from .config import FleetQConfig

logger = logging.getLogger(__name__)


class StepSubmissionError(Exception):
    """Exception raised when step submission fails."""
    pass


class StepValidationError(StepSubmissionError):
    """Exception raised when step payload validation fails."""
    pass


class StepService:
    """
    Service for handling step submission and management operations.
    
    Provides the core business logic for step creation, validation,
    and storage operations.
    """
    
    def __init__(self, storage: StorageInterface, config: FleetQConfig):
        """
        Initialize the step service.
        
        Args:
            storage: Storage backend for step operations
            config: Application configuration
        """
        self.storage = storage
        self.config = config
    
    def generate_step_id(self) -> str:
        """
        Generate a unique step ID.
        
        Creates a UUID-based unique identifier for a new step.
        The format ensures uniqueness across all pods and time.
        
        Returns:
            str: Unique step identifier
        """
        # Generate a UUID4 for uniqueness
        step_uuid = uuid.uuid4()
        
        # Create a step ID with timestamp prefix for better ordering
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        
        # Format: step_YYYYMMDDHHMMSS_uuid
        step_id = f"step_{timestamp}_{step_uuid.hex[:8]}"
        
        logger.debug(f"Generated step ID: {step_id}")
        return step_id
    
    def validate_step_payload(self, payload: StepPayload) -> None:
        """
        Validate step payload for required fields and constraints.
        
        Args:
            payload: Step payload to validate
            
        Raises:
            StepValidationError: If payload validation fails
        """
        # Validate required fields
        if not payload.type:
            raise StepValidationError("Step type is required and cannot be empty")
        
        if not isinstance(payload.type, str):
            raise StepValidationError("Step type must be a string")
        
        # Validate type format (alphanumeric with underscores and hyphens)
        if not payload.type.replace('_', '').replace('-', '').isalnum():
            raise StepValidationError(
                "Step type must contain only alphanumeric characters, underscores, and hyphens"
            )
        
        # Validate args is a dictionary
        if not isinstance(payload.args, dict):
            raise StepValidationError("Step args must be a dictionary")
        
        # Validate metadata is a dictionary
        if not isinstance(payload.metadata, dict):
            raise StepValidationError("Step metadata must be a dictionary")
        
        # Validate payload size (prevent extremely large payloads)
        # This is a rough estimate - in production might want more sophisticated size checking
        try:
            import json
            payload_size = len(json.dumps({
                "type": payload.type,
                "args": payload.args,
                "metadata": payload.metadata
            }))
            
            # Limit payload to 1MB (1,048,576 bytes)
            max_payload_size = 1024 * 1024
            if payload_size > max_payload_size:
                raise StepValidationError(
                    f"Step payload too large: {payload_size} bytes (max: {max_payload_size})"
                )
        except (TypeError, ValueError) as e:
            raise StepValidationError(f"Step payload contains non-serializable data: {e}")
        
        logger.debug(f"Step payload validation passed for type: {payload.type}")
    
    def create_step_payload(self, request: StepSubmissionRequest) -> StepPayload:
        """
        Create a StepPayload from a submission request.
        
        Args:
            request: Step submission request
            
        Returns:
            StepPayload: Validated step payload
            
        Raises:
            StepValidationError: If request validation fails
        """
        # Create payload from request
        payload = StepPayload(
            type=request.type,
            args=request.args,
            metadata=request.metadata
        )
        
        # Validate the payload
        self.validate_step_payload(payload)
        
        return payload
    
    async def submit_step(
        self,
        request: StepSubmissionRequest,
        step_id: Optional[str] = None
    ) -> str:
        """
        Submit a new step to the queue.
        
        Creates a new step with unique ID, validates the payload,
        and stores it in the STEP_TRACKER table with pending status.
        
        Args:
            request: Step submission request containing type, args, metadata
            step_id: Optional custom step ID (if not provided, one will be generated)
            
        Returns:
            str: The unique step ID of the created step
            
        Raises:
            StepSubmissionError: If step submission fails
            StepValidationError: If payload validation fails
        """
        try:
            # Generate step ID if not provided
            if step_id is None:
                step_id = self.generate_step_id()
            
            logger.info(f"Submitting step {step_id} of type {request.type}")
            
            # Create and validate payload
            payload = self.create_step_payload(request)
            
            # Create step object with pending status
            step = create_step_from_submission(
                step_id=step_id,
                payload=payload,
                priority=request.priority,
                max_retries=request.max_retries
            )
            
            # Store step in database
            await self.storage.create_step(step)
            
            logger.info(
                f"Successfully submitted step {step_id}",
                extra={
                    "step_id": step_id,
                    "step_type": payload.type,
                    "priority": request.priority,
                    "max_retries": request.max_retries
                }
            )
            
            return step_id
            
        except StepValidationError:
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            logger.error(f"Failed to submit step: {e}", exc_info=True)
            raise StepSubmissionError(f"Step submission failed: {e}") from e
    
    async def get_step_status(self, step_id: str) -> Optional[Step]:
        """
        Retrieve the current status of a step.
        
        Args:
            step_id: Unique identifier of the step
            
        Returns:
            Optional[Step]: Step object if found, None otherwise
            
        Raises:
            StepSubmissionError: If retrieval fails
        """
        try:
            step = await self.storage.get_step(step_id)
            
            if step:
                logger.debug(f"Retrieved step {step_id} with status {step.status}")
            else:
                logger.debug(f"Step {step_id} not found")
            
            return step
            
        except Exception as e:
            logger.error(f"Failed to get step status for {step_id}: {e}")
            raise StepSubmissionError(f"Step status retrieval failed: {e}") from e
    
    def get_idempotency_key(self, step_id: str) -> str:
        """
        Generate an idempotency key for a step.
        
        This key can be used by step implementations to ensure idempotent
        execution when steps are requeued due to pod failures.
        
        Args:
            step_id: Unique step identifier
            
        Returns:
            str: Idempotency key derived from step_id
        """
        return generate_idempotency_key(step_id)
    
    async def validate_step_exists(self, step_id: str) -> bool:
        """
        Check if a step exists in the system.
        
        Args:
            step_id: Step identifier to check
            
        Returns:
            bool: True if step exists, False otherwise
        """
        try:
            step = await self.storage.get_step(step_id)
            return step is not None
        except Exception as e:
            logger.error(f"Failed to validate step existence for {step_id}: {e}")
            return False
    
    async def get_queue_statistics(self) -> Dict[str, Any]:
        """
        Get current queue statistics.
        
        Returns:
            Dict[str, Any]: Queue statistics including step counts by status
            
        Raises:
            StepSubmissionError: If statistics retrieval fails
        """
        try:
            stats = await self.storage.get_queue_stats()
            
            # Add total count
            total_steps = sum(stats.values())
            stats['total_steps'] = total_steps
            
            logger.debug(f"Retrieved queue statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get queue statistics: {e}")
            raise StepSubmissionError(f"Queue statistics retrieval failed: {e}") from e
    
    async def complete_step(
        self, 
        step_id: str, 
        result_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a step as completed.
        
        Transitions the step from claimed to completed status with proper
        timestamp updates and optional result metadata.
        
        Args:
            step_id: ID of the step to complete
            result_metadata: Optional metadata about the execution result
            
        Raises:
            StepSubmissionError: If step completion fails
        """
        try:
            logger.info(f"Completing step {step_id}")
            
            await self.storage.complete_step(step_id, result_metadata)
            
            logger.info(
                f"Successfully completed step {step_id}",
                extra={
                    "step_id": step_id,
                    "has_result_metadata": result_metadata is not None
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to complete step {step_id}: {e}")
            raise StepSubmissionError(f"Step completion failed: {e}") from e
    
    async def fail_step(
        self, 
        step_id: str, 
        error_message: Optional[str] = None,
        error_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a step as failed.
        
        Transitions the step from claimed to failed status with proper
        timestamp updates and optional error information.
        
        Args:
            step_id: ID of the step to fail
            error_message: Optional error message describing the failure
            error_metadata: Optional metadata about the failure
            
        Raises:
            StepSubmissionError: If step failure update fails
        """
        try:
            logger.info(f"Failing step {step_id}" + 
                       (f" with error: {error_message}" if error_message else ""))
            
            await self.storage.fail_step(step_id, error_message, error_metadata)
            
            logger.info(
                f"Successfully failed step {step_id}",
                extra={
                    "step_id": step_id,
                    "error_message": error_message,
                    "has_error_metadata": error_metadata is not None
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to fail step {step_id}: {e}")
            raise StepSubmissionError(f"Step failure update failed: {e}") from e
    
    async def execute_step(
        self, 
        step: Step, 
        execution_func: callable,
        *args, 
        **kwargs
    ) -> None:
        """
        Execute a step with proper state transitions and error handling.
        
        This method provides a complete step execution wrapper that handles
        the step lifecycle: execution → completion/failure with proper logging
        and state transitions.
        
        Args:
            step: Step object to execute
            execution_func: Function to execute for this step
            *args: Arguments to pass to execution_func
            **kwargs: Keyword arguments to pass to execution_func
            
        Raises:
            StepSubmissionError: If step execution or state transition fails
        """
        step_id = step.step_id
        
        try:
            logger.info(
                f"Executing step {step_id} of type {step.payload.type}",
                extra={
                    "step_id": step_id,
                    "step_type": step.payload.type,
                    "retry_count": step.retry_count
                }
            )
            
            # Execute the step function (handle both sync and async)
            import asyncio
            if asyncio.iscoroutinefunction(execution_func):
                result = await execution_func(step, *args, **kwargs)
            else:
                result = execution_func(step, *args, **kwargs)
            
            # Determine result metadata
            result_metadata = None
            if isinstance(result, dict):
                result_metadata = result
            elif result is not None:
                result_metadata = {"result": str(result)}
            
            # Mark step as completed
            await self.complete_step(step_id, result_metadata)
            
            logger.info(
                f"Step {step_id} executed successfully",
                extra={
                    "step_id": step_id,
                    "step_type": step.payload.type,
                    "execution_result": "success"
                }
            )
            
        except Exception as e:
            # Mark step as failed
            error_message = str(e)
            error_metadata = {
                "error_type": type(e).__name__,
                "error_message": error_message,
                "retry_count": step.retry_count
            }
            
            try:
                await self.fail_step(step_id, error_message, error_metadata)
            except Exception as fail_error:
                logger.error(f"Failed to mark step {step_id} as failed: {fail_error}")
                # Re-raise the original execution error, not the failure update error
            
            logger.error(
                f"Step {step_id} execution failed: {error_message}",
                extra={
                    "step_id": step_id,
                    "step_type": step.payload.type,
                    "error_type": type(e).__name__,
                    "retry_count": step.retry_count
                },
                exc_info=True
            )
            
            raise StepSubmissionError(f"Step execution failed: {error_message}") from e


def create_step_service(storage: StorageInterface, config: FleetQConfig) -> StepService:
    """
    Factory function to create a StepService instance.
    
    Args:
        storage: Storage backend for step operations
        config: Application configuration
        
    Returns:
        StepService: Configured step service instance
    """
    return StepService(storage, config)