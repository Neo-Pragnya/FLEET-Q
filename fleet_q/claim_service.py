"""
Claim service for FLEET-Q.

Implements the claim loop that continuously attempts to claim available steps
from the queue based on pod capacity. Handles atomic claiming with backoff
retry logic and manages concurrent step execution.

Provides:
- Async claiming task with configurable intervals
- Capacity-aware claiming based on current workload
- Claim attempt scheduling and backoff handling
- Concurrent step execution management
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Callable

from .config import FleetQConfig
from .models import Step, StepStatus
from .storage import StorageInterface
from .backoff import with_backoff
from .logging_config import (
    log_claim_attempt, 
    log_step_transition,
    log_operation_error,
    ErrorContext
)

logger = logging.getLogger(__name__)


class ClaimService:
    """
    Service for managing step claiming and execution operations.
    
    Runs a continuous claiming loop that attempts to claim available work
    from the queue based on pod capacity. Manages concurrent step execution
    and handles claim conflicts with exponential backoff.
    """
    
    def __init__(
        self, 
        storage: StorageInterface, 
        config: FleetQConfig,
        step_executor: Optional[Callable[[Step], Any]] = None
    ):
        """
        Initialize the claim service.
        
        Args:
            storage: Storage backend for step claiming operations
            config: Application configuration
            step_executor: Optional custom step executor function
        """
        self.storage = storage
        self.config = config
        self.pod_id = config.pod_id
        self.step_executor = step_executor or self._default_step_executor
        
        # Control flags for the claim loop
        self._claim_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._is_running = False
        
        # Track currently executing steps
        self._executing_steps: Set[str] = set()
        self._execution_tasks: Dict[str, asyncio.Task] = {}
        self._execution_lock = asyncio.Lock()
        
        # Claim statistics
        self._last_claim_time: Optional[datetime] = None
        self._claim_attempts = 0
        self._successful_claims = 0
        self._total_steps_claimed = 0
        self._total_steps_completed = 0
        self._total_steps_failed = 0
        
        logger.info(f"Claim service initialized for pod {self.pod_id}")
    
    async def _default_step_executor(self, step: Step) -> Any:
        """
        Default step executor that just logs the step execution.
        
        This is a placeholder implementation. In a real application,
        you would replace this with your actual step execution logic.
        
        Args:
            step: Step to execute
            
        Returns:
            Any: Execution result
        """
        logger.info(
            f"Executing step {step.step_id} of type {step.payload.type}",
            extra={
                'step_id': step.step_id,
                'step_type': step.payload.type,
                'retry_count': step.retry_count
            }
        )
        
        # Simulate work with a small delay
        await asyncio.sleep(0.1)
        
        # Return a simple success result
        return {
            'status': 'success',
            'executed_at': datetime.utcnow().isoformat(),
            'pod_id': self.pod_id
        }
    
    def calculate_available_capacity(self) -> int:
        """
        Calculate the number of steps this pod can currently claim.
        
        Uses the configured capacity threshold and subtracts currently
        executing steps to determine available capacity.
        
        Returns:
            int: Number of steps that can be claimed (0 or positive)
        """
        max_concurrent = self.config.max_concurrent_steps
        currently_executing = len(self._executing_steps)
        available = max_concurrent - currently_executing
        
        logger.debug(
            f"Capacity calculation: max={max_concurrent}, executing={currently_executing}, available={available}",
            extra={
                'max_concurrent_steps': max_concurrent,
                'currently_executing': currently_executing,
                'available_capacity': available
            }
        )
        
        return max(0, available)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        jitter=True,
        retry_on=[Exception],  # Retry on any exception during claiming
        on_retry=lambda attempt, delay, error: logger.debug(f"Retrying claim attempt {attempt} after {delay}ms: {error}")
    )
    async def attempt_claim_steps(self, limit: int) -> List[Step]:
        """
        Attempt to claim steps from the queue with backoff retry logic.
        
        Uses atomic database operations to claim steps and handles
        conflicts with exponential backoff. This method is decorated
        with the backoff utility for automatic retry handling.
        
        Args:
            limit: Maximum number of steps to claim
            
        Returns:
            List[Step]: List of successfully claimed steps
            
        Raises:
            Exception: If claiming fails after all retry attempts
        """
        if limit <= 0:
            logger.debug("No capacity available for claiming")
            return []
        
        start_time = time.time()
        conflict_occurred = False
        
        try:
            logger.debug(f"Attempting to claim up to {limit} steps")
            
            # Use storage layer's atomic claiming mechanism
            claimed_steps = await self.storage.claim_steps(self.pod_id, limit)
            
            duration_ms = (time.time() - start_time) * 1000
            
            # Use structured logging for claim attempts
            log_claim_attempt(
                logger,
                self.pod_id,
                limit,
                len(claimed_steps),
                self.get_available_capacity(),
                duration_ms,
                conflict_occurred=conflict_occurred
            )
            
            if claimed_steps:
                step_ids = [step.step_id for step in claimed_steps]
                logger.info(
                    f"Successfully claimed {len(claimed_steps)} steps: {step_ids}",
                    extra={
                        'claimed_count': len(claimed_steps),
                        'claimed_step_ids': step_ids,
                        'pod_id': self.pod_id,
                        'duration_ms': duration_ms
                    }
                )
            else:
                logger.debug("No steps available for claiming")
            
            return claimed_steps
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            
            # Check if this is a conflict error
            if "conflict" in str(e).lower() or "transaction" in str(e).lower():
                conflict_occurred = True
            
            # Log the failed claim attempt with structured logging
            log_claim_attempt(
                logger,
                self.pod_id,
                limit,
                0,  # No steps claimed due to error
                self.get_available_capacity(),
                duration_ms,
                conflict_occurred=conflict_occurred
            )
            
            logger.warning(f"Claim attempt failed: {e}")
            # Re-raise to trigger backoff retry
            raise
    
    async def execute_step_with_lifecycle(self, step: Step) -> None:
        """
        Execute a step with complete lifecycle management.
        
        Handles the full step execution lifecycle including state tracking,
        error handling, and proper cleanup. Updates step status based on
        execution results.
        
        Args:
            step: Step to execute
        """
        step_id = step.step_id
        
        # Add to executing set
        async with self._execution_lock:
            self._executing_steps.add(step_id)
        
        try:
            logger.info(
                f"Starting execution of step {step_id}",
                extra={
                    'step_id': step_id,
                    'step_type': step.payload.type,
                    'retry_count': step.retry_count,
                    'pod_id': self.pod_id
                }
            )
            
            # Log step transition from claimed to executing
            log_step_transition(
                logger,
                step_id,
                "claimed",
                "executing",
                self.pod_id,
                step_type=step.payload.type,
                retry_count=step.retry_count
            )
            
            # Execute the step
            result = await self.step_executor(step)
            
            # Determine result metadata
            result_metadata = None
            if isinstance(result, dict):
                result_metadata = result
            elif result is not None:
                result_metadata = {'result': str(result)}
            
            # Mark step as completed
            await self.storage.complete_step(step_id, result_metadata)
            
            # Log step transition to completed
            log_step_transition(
                logger,
                step_id,
                "executing",
                "completed",
                self.pod_id,
                step_type=step.payload.type,
                execution_result='success'
            )
            
            # Update statistics
            self._total_steps_completed += 1
            
            logger.info(
                f"Successfully completed step {step_id}",
                extra={
                    'step_id': step_id,
                    'step_type': step.payload.type,
                    'execution_result': 'success',
                    'pod_id': self.pod_id
                }
            )
            
        except Exception as e:
            # Handle execution failure
            error_message = str(e)
            error_metadata = {
                'error_type': type(e).__name__,
                'error_message': error_message,
                'retry_count': step.retry_count,
                'pod_id': self.pod_id,
                'failed_at': datetime.utcnow().isoformat()
            }
            
            try:
                await self.storage.fail_step(step_id, error_message, error_metadata)
                
                # Log step transition to failed
                log_step_transition(
                    logger,
                    step_id,
                    "executing",
                    "failed",
                    self.pod_id,
                    step_type=step.payload.type,
                    error_type=type(e).__name__,
                    error_message=error_message,
                    retry_count=step.retry_count
                )
                
                self._total_steps_failed += 1
                
                # Use structured error logging
                log_operation_error(
                    logger,
                    f"step_execution_{step.payload.type}",
                    e,
                    0,  # Duration not tracked here
                    retry_count=step.retry_count,
                    step_id=step_id,
                    step_type=step.payload.type,
                    pod_id=self.pod_id
                )
                
                logger.error(
                    f"Step {step_id} execution failed: {error_message}",
                    extra={
                        'step_id': step_id,
                        'step_type': step.payload.type,
                        'error_type': type(e).__name__,
                        'retry_count': step.retry_count,
                        'pod_id': self.pod_id
                    },
                    exc_info=True
                )
                
            except Exception as fail_error:
                logger.error(
                    f"Failed to mark step {step_id} as failed: {fail_error}",
                    extra={'step_id': step_id, 'original_error': error_message}
                )
        
        finally:
            # Remove from executing set
            async with self._execution_lock:
                self._executing_steps.discard(step_id)
                # Remove from execution tasks if present
                self._execution_tasks.pop(step_id, None)
    
    async def process_claimed_steps(self, claimed_steps: List[Step]) -> None:
        """
        Process a list of claimed steps by starting their execution.
        
        Creates async tasks for each claimed step and manages them
        in the execution tracking system.
        
        Args:
            claimed_steps: List of steps to process
        """
        if not claimed_steps:
            return
        
        logger.info(
            f"Processing {len(claimed_steps)} claimed steps",
            extra={
                'claimed_count': len(claimed_steps),
                'step_ids': [step.step_id for step in claimed_steps]
            }
        )
        
        async with self._execution_lock:
            for step in claimed_steps:
                # Create execution task
                task = asyncio.create_task(
                    self.execute_step_with_lifecycle(step),
                    name=f"execute_step_{step.step_id}"
                )
                
                # Track the task
                self._execution_tasks[step.step_id] = task
                
                logger.debug(
                    f"Started execution task for step {step.step_id}",
                    extra={'step_id': step.step_id, 'task_name': task.get_name()}
                )
    
    async def _claim_loop(self) -> None:
        """
        Main claim loop that runs continuously in the background.
        
        Attempts to claim available steps at the configured interval,
        respecting capacity limits and handling errors gracefully.
        """
        logger.info(
            f"Starting claim loop for pod {self.pod_id} "
            f"(interval: {self.config.claim_interval_seconds}s)"
        )
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Calculate available capacity
                    available_capacity = self.calculate_available_capacity()
                    
                    if available_capacity > 0:
                        # Attempt to claim steps
                        self._claim_attempts += 1
                        claimed_steps = await self.attempt_claim_steps(available_capacity)
                        
                        if claimed_steps:
                            self._successful_claims += 1
                            self._total_steps_claimed += len(claimed_steps)
                            
                            # Process claimed steps
                            await self.process_claimed_steps(claimed_steps)
                        
                        self._last_claim_time = datetime.utcnow()
                    else:
                        logger.debug(
                            f"No available capacity (executing {len(self._executing_steps)} steps)"
                        )
                    
                    # Wait for next claim interval or shutdown signal
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.config.claim_interval_seconds
                        )
                        # If we get here, shutdown was requested
                        break
                    except asyncio.TimeoutError:
                        # Timeout is expected - continue with next claim attempt
                        continue
                        
                except Exception as e:
                    logger.error(f"Error in claim loop: {e}")
                    # Continue the loop even if individual claim attempts fail
                    # Wait a bit before retrying to avoid tight error loops
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=min(5.0, self.config.claim_interval_seconds)
                        )
                        break
                    except asyncio.TimeoutError:
                        continue
        
        except asyncio.CancelledError:
            logger.info(f"Claim loop cancelled for pod {self.pod_id}")
            raise
        except Exception as e:
            logger.error(f"Fatal error in claim loop: {e}")
            raise
        finally:
            logger.info(
                f"Claim loop stopped for pod {self.pod_id} "
                f"(attempts: {self._claim_attempts}, successful: {self._successful_claims})"
            )
    
    async def start(self) -> None:
        """
        Start the claim service.
        
        Starts the background claim loop that continuously attempts to
        claim and execute available work. This method should be called
        during application startup.
        
        Raises:
            RuntimeError: If service is already running
            Exception: If startup fails
        """
        if self._is_running:
            raise RuntimeError(f"Claim service for pod {self.pod_id} is already running")
        
        try:
            logger.info(f"Starting claim service for pod {self.pod_id}")
            
            # Start claim loop
            self._claim_task = asyncio.create_task(self._claim_loop())
            self._is_running = True
            
            logger.info(f"Claim service started for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Failed to start claim service for pod {self.pod_id}: {e}")
            await self.stop()  # Clean up any partial state
            raise
    
    async def stop(self) -> None:
        """
        Stop the claim service gracefully.
        
        Signals the claim loop to stop, waits for executing steps to complete,
        and cleans up resources. This method should be called during
        application shutdown.
        """
        if not self._is_running:
            logger.debug(f"Claim service for pod {self.pod_id} is not running")
            return
        
        try:
            logger.info(f"Stopping claim service for pod {self.pod_id}")
            
            # Signal shutdown to claim loop
            self._shutdown_event.set()
            
            # Wait for claim loop to finish
            if self._claim_task and not self._claim_task.done():
                try:
                    await asyncio.wait_for(self._claim_task, timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Claim loop did not stop within timeout, cancelling")
                    self._claim_task.cancel()
                    try:
                        await self._claim_task
                    except asyncio.CancelledError:
                        pass
            
            # Wait for executing steps to complete (with timeout)
            if self._execution_tasks:
                logger.info(f"Waiting for {len(self._execution_tasks)} executing steps to complete")
                
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._execution_tasks.values(), return_exceptions=True),
                        timeout=30.0  # Give steps 30 seconds to complete
                    )
                except asyncio.TimeoutError:
                    logger.warning("Some executing steps did not complete within timeout, cancelling")
                    
                    # Cancel remaining tasks
                    for task in self._execution_tasks.values():
                        if not task.done():
                            task.cancel()
                    
                    # Wait for cancellation to complete
                    await asyncio.gather(*self._execution_tasks.values(), return_exceptions=True)
            
            # Clean up state
            self._claim_task = None
            self._is_running = False
            self._shutdown_event.clear()
            self._executing_steps.clear()
            self._execution_tasks.clear()
            
            logger.info(f"Claim service stopped for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Error during claim service shutdown: {e}")
            # Ensure we clean up state even if shutdown fails
            self._claim_task = None
            self._is_running = False
            self._shutdown_event.clear()
            self._executing_steps.clear()
            self._execution_tasks.clear()
            raise
    
    @property
    def is_running(self) -> bool:
        """
        Check if the claim service is currently running.
        
        Returns:
            bool: True if service is running, False otherwise
        """
        return self._is_running
    
    @property
    def executing_step_count(self) -> int:
        """
        Get the number of currently executing steps.
        
        Returns:
            int: Number of steps currently being executed
        """
        return len(self._executing_steps)
    
    @property
    def executing_step_ids(self) -> List[str]:
        """
        Get the IDs of currently executing steps.
        
        Returns:
            List[str]: List of step IDs currently being executed
        """
        return list(self._executing_steps)
    
    def get_available_capacity(self) -> int:
        """
        Get the current available capacity for claiming steps.
        
        Returns:
            int: Number of additional steps this pod can claim
        """
        max_concurrent = self.config.max_concurrent_steps
        currently_executing = len(self._executing_steps)
        return max(0, max_concurrent - currently_executing)
    
    async def get_claim_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive claim service statistics.
        
        Returns:
            Dict[str, Any]: Claim statistics including attempt counts, success rates, etc.
        """
        success_rate = 0.0
        if self._claim_attempts > 0:
            success_rate = (self._successful_claims / self._claim_attempts) * 100
        
        return {
            'service_status': 'running' if self._is_running else 'stopped',
            'pod_id': self.pod_id,
            'last_claim_time': self._last_claim_time.isoformat() if self._last_claim_time else None,
            'claim_attempts': self._claim_attempts,
            'successful_claims': self._successful_claims,
            'success_rate_percent': round(success_rate, 2),
            'total_steps_claimed': self._total_steps_claimed,
            'total_steps_completed': self._total_steps_completed,
            'total_steps_failed': self._total_steps_failed,
            'currently_executing': len(self._executing_steps),
            'executing_step_ids': list(self._executing_steps),
            'available_capacity': self.calculate_available_capacity(),
            'max_concurrent_steps': self.config.max_concurrent_steps,
            'claim_interval_seconds': self.config.claim_interval_seconds
        }


def create_claim_service(
    storage: StorageInterface, 
    config: FleetQConfig,
    step_executor: Optional[Callable[[Step], Any]] = None
) -> ClaimService:
    """
    Factory function to create a ClaimService instance.
    
    Args:
        storage: Storage backend for step claiming operations
        config: Application configuration
        step_executor: Optional custom step executor function
        
    Returns:
        ClaimService: Configured claim service instance
    """
    return ClaimService(storage, config, step_executor)