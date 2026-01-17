"""
Recovery service for FLEET-Q.

Implements the recovery system that handles dead pod detection and orphaned step
requeuing. The recovery service runs only on the leader pod and performs periodic
recovery cycles to ensure no work is lost when pods fail.

Provides:
- Orphan step discovery for steps claimed by dead pods
- Recovery cycle management with local DLQ tracking
- Step requeuing logic with retry count management
- Terminal failure detection for poison steps
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from .config import FleetQConfig
from .models import Step, PodHealth, DLQRecord, StepStatus
from .storage import StorageInterface, SQLiteStorageInterface, create_dlq_record
from .sqlite_storage import SQLiteStorage

logger = logging.getLogger(__name__)


class RecoveryService:
    """
    Service for managing recovery operations and orphaned step handling.
    
    Runs only on the leader pod and performs periodic recovery cycles to detect
    dead pods, identify orphaned steps, and requeue eligible work back to the
    pending state. Uses local SQLite DLQ for operational visibility.
    """
    
    def __init__(
        self, 
        storage: StorageInterface, 
        sqlite_storage: SQLiteStorageInterface,
        config: FleetQConfig
    ):
        """
        Initialize the recovery service.
        
        Args:
            storage: Main storage backend (Snowflake) for step and pod operations
            sqlite_storage: Local SQLite storage for DLQ operations
            config: Application configuration
        """
        self.storage = storage
        self.sqlite_storage = sqlite_storage
        self.config = config
        self.pod_id = config.pod_id
        
        # Control flags for the recovery loop
        self._recovery_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._is_running = False
        
        # Recovery statistics
        self._last_recovery_time: Optional[datetime] = None
        self._recovery_count = 0
        self._total_orphans_processed = 0
        self._total_steps_requeued = 0
        self._total_steps_marked_terminal = 0
        
        logger.info(f"Recovery service initialized for pod {self.pod_id}")
    
    async def discover_orphaned_steps(self, dead_pods: List[PodHealth]) -> List[Step]:
        """
        Discover steps that have been orphaned by dead pods.
        
        Finds all steps that are currently claimed by pods that have been
        identified as dead based on stale heartbeats. These steps need to
        be processed for potential requeuing.
        
        Args:
            dead_pods: List of pods that have been identified as dead
            
        Returns:
            List[Step]: List of orphaned steps that need recovery processing
            
        Raises:
            Exception: If orphan discovery fails
        """
        if not dead_pods:
            logger.debug("No dead pods provided for orphan discovery")
            return []
        
        try:
            dead_pod_ids = [pod.pod_id for pod in dead_pods]
            
            logger.info(
                f"Discovering orphaned steps for {len(dead_pod_ids)} dead pods: {dead_pod_ids}",
                extra={
                    'dead_pod_count': len(dead_pod_ids),
                    'dead_pod_ids': dead_pod_ids
                }
            )
            
            # Query for steps claimed by dead pods
            orphaned_steps = await self.storage.get_steps_claimed_by_pods(dead_pod_ids)
            
            logger.info(
                f"Found {len(orphaned_steps)} orphaned steps",
                extra={
                    'orphaned_step_count': len(orphaned_steps),
                    'orphaned_step_ids': [step.step_id for step in orphaned_steps],
                    'dead_pod_ids': dead_pod_ids
                }
            )
            
            return orphaned_steps
            
        except Exception as e:
            logger.error(f"Failed to discover orphaned steps: {e}")
            raise
    
    def categorize_orphaned_step(self, step: Step) -> Tuple[str, str]:
        """
        Categorize an orphaned step and determine the appropriate recovery action.
        
        Analyzes the step's retry count, status, and other factors to determine
        whether it should be requeued or marked as terminally failed.
        
        Args:
            step: Orphaned step to categorize
            
        Returns:
            Tuple[str, str]: (reason, action) where:
                - reason: Why the step is in DLQ ("dead_pod", "timeout", "poison")
                - action: What to do with it ("requeue", "terminal")
        """
        # Determine the reason for orphaning
        reason = "dead_pod"  # Primary reason for orphaning in this context
        
        # Determine the recovery action based on retry count
        if step.retry_count >= step.max_retries:
            action = "terminal"
            logger.debug(
                f"Step {step.step_id} marked as terminal: retry_count={step.retry_count}, max_retries={step.max_retries}",
                extra={
                    'step_id': step.step_id,
                    'retry_count': step.retry_count,
                    'max_retries': step.max_retries,
                    'reason': reason,
                    'action': action
                }
            )
        else:
            action = "requeue"
            logger.debug(
                f"Step {step.step_id} eligible for requeue: retry_count={step.retry_count}, max_retries={step.max_retries}",
                extra={
                    'step_id': step.step_id,
                    'retry_count': step.retry_count,
                    'max_retries': step.max_retries,
                    'reason': reason,
                    'action': action
                }
            )
        
        return reason, action
    
    def determine_recovery_eligibility(self, step: Step) -> bool:
        """
        Determine if an orphaned step is eligible for recovery processing.
        
        Checks various conditions to determine if a step should be included
        in the recovery process. This includes status checks, retry limits,
        and other business logic.
        
        Args:
            step: Step to check for recovery eligibility
            
        Returns:
            bool: True if step is eligible for recovery, False otherwise
        """
        # Only process steps that are in claimed or failed status
        if step.status not in [StepStatus.CLAIMED, StepStatus.FAILED]:
            logger.debug(
                f"Step {step.step_id} not eligible for recovery: status={step.status}",
                extra={
                    'step_id': step.step_id,
                    'status': step.status.value,
                    'eligible': False,
                    'reason': 'invalid_status'
                }
            )
            return False
        
        # Check if step has a claimed_by field (should be set for orphaned steps)
        if not step.claimed_by:
            logger.debug(
                f"Step {step.step_id} not eligible for recovery: no claimed_by field",
                extra={
                    'step_id': step.step_id,
                    'claimed_by': step.claimed_by,
                    'eligible': False,
                    'reason': 'no_claimed_by'
                }
            )
            return False
        
        # Check if retry count is within reasonable bounds
        if step.retry_count < 0:
            logger.warning(
                f"Step {step.step_id} has negative retry count: {step.retry_count}",
                extra={
                    'step_id': step.step_id,
                    'retry_count': step.retry_count,
                    'eligible': False,
                    'reason': 'negative_retry_count'
                }
            )
            return False
        
        # Check if max_retries is reasonable
        if step.max_retries < 0:
            logger.warning(
                f"Step {step.step_id} has negative max_retries: {step.max_retries}",
                extra={
                    'step_id': step.step_id,
                    'max_retries': step.max_retries,
                    'eligible': False,
                    'reason': 'negative_max_retries'
                }
            )
            return False
        
        logger.debug(
            f"Step {step.step_id} is eligible for recovery",
            extra={
                'step_id': step.step_id,
                'status': step.status.value,
                'claimed_by': step.claimed_by,
                'retry_count': step.retry_count,
                'max_retries': step.max_retries,
                'eligible': True
            }
        )
        
        return True
    
    async def process_orphaned_steps(
        self, 
        orphaned_steps: List[Step], 
        cycle_id: str
    ) -> Tuple[List[Step], List[Step]]:
        """
        Process orphaned steps and categorize them for recovery actions.
        
        Analyzes each orphaned step to determine if it should be requeued
        or marked as terminally failed. Records all decisions in the local
        DLQ for operational visibility.
        
        Args:
            orphaned_steps: List of orphaned steps to process
            cycle_id: Recovery cycle identifier for DLQ tracking
            
        Returns:
            Tuple[List[Step], List[Step]]: (steps_to_requeue, terminal_steps)
            
        Raises:
            Exception: If step processing fails
        """
        if not orphaned_steps:
            logger.debug("No orphaned steps to process")
            return [], []
        
        try:
            steps_to_requeue = []
            terminal_steps = []
            recovery_timestamp = datetime.utcnow()
            
            logger.info(
                f"Processing {len(orphaned_steps)} orphaned steps for cycle {cycle_id}",
                extra={
                    'orphaned_step_count': len(orphaned_steps),
                    'cycle_id': cycle_id,
                    'recovery_timestamp': recovery_timestamp.isoformat()
                }
            )
            
            for step in orphaned_steps:
                try:
                    # Check if step is eligible for recovery
                    if not self.determine_recovery_eligibility(step):
                        logger.info(
                            f"Skipping ineligible step {step.step_id}",
                            extra={'step_id': step.step_id, 'cycle_id': cycle_id}
                        )
                        continue
                    
                    # Categorize the step and determine action
                    reason, action = self.categorize_orphaned_step(step)
                    
                    # Create DLQ record for tracking
                    dlq_record = create_dlq_record(
                        step=step,
                        reason=reason,
                        action=action,
                        recovery_timestamp=recovery_timestamp
                    )
                    
                    # Insert into local DLQ
                    await self.sqlite_storage.insert_dlq_record(cycle_id, dlq_record)
                    
                    # Add to appropriate list based on action
                    if action == "requeue":
                        steps_to_requeue.append(step)
                    elif action == "terminal":
                        terminal_steps.append(step)
                    
                    logger.debug(
                        f"Processed orphaned step {step.step_id}: {action}",
                        extra={
                            'step_id': step.step_id,
                            'action': action,
                            'reason': reason,
                            'retry_count': step.retry_count,
                            'max_retries': step.max_retries,
                            'cycle_id': cycle_id
                        }
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Failed to process orphaned step {step.step_id}: {e}",
                        extra={'step_id': step.step_id, 'cycle_id': cycle_id}
                    )
                    # Continue processing other steps even if one fails
                    continue
            
            logger.info(
                f"Completed processing orphaned steps: {len(steps_to_requeue)} to requeue, "
                f"{len(terminal_steps)} terminal",
                extra={
                    'cycle_id': cycle_id,
                    'steps_to_requeue_count': len(steps_to_requeue),
                    'terminal_steps_count': len(terminal_steps),
                    'total_processed': len(steps_to_requeue) + len(terminal_steps)
                }
            )
            
            return steps_to_requeue, terminal_steps
            
        except Exception as e:
            logger.error(f"Failed to process orphaned steps for cycle {cycle_id}: {e}")
            raise
    
    async def requeue_steps_with_retry_management(self, steps_to_requeue: List[Step]) -> Dict[str, int]:
        """
        Requeue steps back to pending status with proper retry count management.
        
        Handles the actual requeuing of orphaned steps, incrementing retry counts
        and marking steps as terminally failed if they exceed maximum retries.
        
        Args:
            steps_to_requeue: List of steps to requeue
            
        Returns:
            Dict[str, int]: Summary with counts of requeued and terminal steps
            
        Raises:
            Exception: If requeuing fails
        """
        if not steps_to_requeue:
            logger.debug("No steps to requeue")
            return {'requeued': 0, 'terminal': 0}
        
        try:
            step_ids = [step.step_id for step in steps_to_requeue]
            
            logger.info(
                f"Requeuing {len(step_ids)} steps with retry management",
                extra={
                    'step_count': len(step_ids),
                    'step_ids': step_ids
                }
            )
            
            # Use the storage layer's requeue_steps method which handles:
            # - Incrementing retry_count
            # - Setting status to 'pending' for eligible steps
            # - Setting status to 'failed' for steps that exceed max_retries
            # - Clearing claimed_by field
            # - Updating last_update_ts
            await self.storage.requeue_steps(step_ids)
            
            # Count how many were actually requeued vs marked terminal
            # We need to check the current state since requeue_steps handles both cases
            requeued_count = 0
            terminal_count = 0
            
            for step in steps_to_requeue:
                if step.retry_count < step.max_retries:
                    requeued_count += 1
                else:
                    terminal_count += 1
            
            logger.info(
                f"Step requeuing completed: {requeued_count} requeued, {terminal_count} marked terminal",
                extra={
                    'requeued_count': requeued_count,
                    'terminal_count': terminal_count,
                    'total_processed': len(step_ids)
                }
            )
            
            return {'requeued': requeued_count, 'terminal': terminal_count}
            
        except Exception as e:
            logger.error(f"Failed to requeue steps: {e}")
            raise
    
    def detect_poison_steps(self, steps: List[Step]) -> List[Step]:
        """
        Detect steps that should be considered poison and not requeued.
        
        Identifies steps that have characteristics indicating they are likely
        to fail repeatedly and should be marked as terminal rather than requeued.
        
        Args:
            steps: List of steps to analyze for poison characteristics
            
        Returns:
            List[Step]: List of steps identified as poison
        """
        poison_steps = []
        
        for step in steps:
            is_poison = False
            poison_reasons = []
            
            # Check if step has already exceeded max retries
            if step.retry_count >= step.max_retries:
                is_poison = True
                poison_reasons.append(f"exceeded_max_retries({step.max_retries})")
            
            # Check for extremely high retry count (potential runaway)
            if step.retry_count > 10:  # Configurable threshold
                is_poison = True
                poison_reasons.append(f"excessive_retries({step.retry_count})")
            
            # Check for steps that have been failing for a very long time
            if step.last_update_ts:
                age_hours = (datetime.utcnow() - step.last_update_ts).total_seconds() / 3600
                if age_hours > 24:  # Steps older than 24 hours
                    is_poison = True
                    poison_reasons.append(f"stale_step({age_hours:.1f}h)")
            
            # Additional poison detection logic could be added here:
            # - Steps with specific error patterns in metadata
            # - Steps from known problematic payload types
            # - Steps that consistently fail on multiple different pods
            
            if is_poison:
                poison_steps.append(step)
                logger.warning(
                    f"Detected poison step {step.step_id}: {', '.join(poison_reasons)}",
                    extra={
                        'step_id': step.step_id,
                        'retry_count': step.retry_count,
                        'max_retries': step.max_retries,
                        'poison_reasons': poison_reasons,
                        'last_update_ts': step.last_update_ts.isoformat() if step.last_update_ts else None
                    }
                )
        
        if poison_steps:
            logger.info(
                f"Detected {len(poison_steps)} poison steps out of {len(steps)} analyzed",
                extra={
                    'poison_step_count': len(poison_steps),
                    'total_steps_analyzed': len(steps),
                    'poison_step_ids': [step.step_id for step in poison_steps]
                }
            )
        
        return poison_steps
    
    async def handle_terminal_failures(self, terminal_steps: List[Step], cycle_id: str) -> None:
        """
        Handle steps that have been marked as terminally failed.
        
        Processes steps that have exceeded their retry limits or been identified
        as poison. Records them in the DLQ for operational visibility but does
        not requeue them.
        
        Args:
            terminal_steps: List of steps marked as terminal
            cycle_id: Recovery cycle identifier for DLQ tracking
            
        Raises:
            Exception: If terminal failure handling fails
        """
        if not terminal_steps:
            logger.debug("No terminal steps to handle")
            return
        
        try:
            logger.info(
                f"Handling {len(terminal_steps)} terminal failures for cycle {cycle_id}",
                extra={
                    'terminal_step_count': len(terminal_steps),
                    'cycle_id': cycle_id,
                    'terminal_step_ids': [step.step_id for step in terminal_steps]
                }
            )
            
            recovery_timestamp = datetime.utcnow()
            
            for step in terminal_steps:
                try:
                    # Create DLQ record for terminal step
                    dlq_record = create_dlq_record(
                        step=step,
                        reason="poison" if step.retry_count > step.max_retries else "max_retries_exceeded",
                        action="terminal",
                        recovery_timestamp=recovery_timestamp
                    )
                    
                    # Insert into local DLQ for tracking
                    await self.sqlite_storage.insert_dlq_record(cycle_id, dlq_record)
                    
                    logger.info(
                        f"Recorded terminal step {step.step_id} in DLQ",
                        extra={
                            'step_id': step.step_id,
                            'retry_count': step.retry_count,
                            'max_retries': step.max_retries,
                            'cycle_id': cycle_id,
                            'reason': dlq_record.reason
                        }
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Failed to handle terminal step {step.step_id}: {e}",
                        extra={'step_id': step.step_id, 'cycle_id': cycle_id}
                    )
                    # Continue processing other terminal steps
                    continue
            
            logger.info(
                f"Completed handling terminal failures for cycle {cycle_id}",
                extra={
                    'terminal_step_count': len(terminal_steps),
                    'cycle_id': cycle_id
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to handle terminal failures for cycle {cycle_id}: {e}")
            raise
    
    async def get_recovery_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive recovery service statistics.
        
        Returns:
            Dict[str, Any]: Recovery statistics including cycle counts, processing totals, etc.
        """
        return {
            'service_status': 'running' if self._is_running else 'stopped',
            'pod_id': self.pod_id,
            'last_recovery_time': self._last_recovery_time.isoformat() if self._last_recovery_time else None,
            'recovery_count': self._recovery_count,
            'total_orphans_processed': self._total_orphans_processed,
            'total_steps_requeued': self._total_steps_requeued,
            'total_steps_marked_terminal': self._total_steps_marked_terminal,
            'recovery_interval_seconds': self.config.recovery_interval_seconds,
            'dead_pod_threshold_seconds': self.config.dead_pod_threshold_seconds
        }
        """
        Get comprehensive recovery service statistics.
        
        Returns:
            Dict[str, Any]: Recovery statistics including cycle counts, processing totals, etc.
        """
        return {
            'service_status': 'running' if self._is_running else 'stopped',
            'pod_id': self.pod_id,
            'last_recovery_time': self._last_recovery_time.isoformat() if self._last_recovery_time else None,
            'recovery_count': self._recovery_count,
            'total_orphans_processed': self._total_orphans_processed,
            'total_steps_requeued': self._total_steps_requeued,
            'total_steps_marked_terminal': self._total_steps_marked_terminal,
            'recovery_interval_seconds': self.config.recovery_interval_seconds,
            'dead_pod_threshold_seconds': self.config.dead_pod_threshold_seconds
        }
    
    @property
    def is_running(self) -> bool:
        """
        Check if the recovery service is currently running.
        
        Returns:
            bool: True if service is running, False otherwise
        """
        return self._is_running
    
    @property
    def last_recovery_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last recovery cycle.
        
        Returns:
            Optional[datetime]: Last recovery time, None if no recovery has run
        """
        return self._last_recovery_time
    
    async def perform_recovery_cycle(self) -> Dict[str, Any]:
        """
        Perform a complete recovery cycle.
        
        Executes the full recovery process including dead pod detection,
        orphan step discovery, DLQ management, and step requeuing. Uses
        local SQLite DLQ for operational visibility and cleanup.
        
        Returns:
            Dict[str, Any]: Recovery cycle summary with statistics
            
        Raises:
            Exception: If recovery cycle fails
        """
        cycle_start_time = datetime.utcnow()
        cycle_id = f"recovery_{int(cycle_start_time.timestamp())}"
        
        logger.info(
            f"Starting recovery cycle {cycle_id}",
            extra={
                'cycle_id': cycle_id,
                'cycle_start_time': cycle_start_time.isoformat(),
                'pod_id': self.pod_id
            }
        )
        
        recovery_summary = {
            'cycle_id': cycle_id,
            'start_time': cycle_start_time,
            'end_time': None,
            'duration_ms': 0,
            'dead_pods_found': 0,
            'dead_pod_ids': [],
            'orphaned_steps_found': 0,
            'steps_requeued': 0,
            'steps_marked_terminal': 0,
            'dlq_records_created': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Step 1: Create local DLQ table for this cycle
            logger.debug(f"Creating DLQ table for cycle {cycle_id}")
            await self.sqlite_storage.create_dlq_table(cycle_id)
            
            # Step 2: Detect dead pods
            logger.debug(f"Detecting dead pods for cycle {cycle_id}")
            dead_pods = await self._detect_and_mark_dead_pods()
            
            recovery_summary.update({
                'dead_pods_found': len(dead_pods),
                'dead_pod_ids': [pod.pod_id for pod in dead_pods]
            })
            
            if not dead_pods:
                logger.info(f"No dead pods found in cycle {cycle_id}")
                recovery_summary['success'] = True
                return recovery_summary
            
            # Step 3: Discover orphaned steps
            logger.debug(f"Discovering orphaned steps for cycle {cycle_id}")
            orphaned_steps = await self.discover_orphaned_steps(dead_pods)
            
            recovery_summary['orphaned_steps_found'] = len(orphaned_steps)
            
            if not orphaned_steps:
                logger.info(f"No orphaned steps found in cycle {cycle_id}")
                recovery_summary['success'] = True
                return recovery_summary
            
            # Step 4: Process orphaned steps and populate DLQ
            logger.debug(f"Processing orphaned steps for cycle {cycle_id}")
            steps_to_requeue, terminal_steps = await self.process_orphaned_steps(
                orphaned_steps, cycle_id
            )
            
            # Step 5: Get DLQ record count for tracking
            dlq_records = await self.sqlite_storage.get_dlq_records(cycle_id)
            recovery_summary['dlq_records_created'] = len(dlq_records)
            
            # Step 6: Requeue eligible steps
            if steps_to_requeue:
                logger.info(f"Requeuing {len(steps_to_requeue)} steps in cycle {cycle_id}")
                requeue_result = await self.requeue_steps_with_retry_management(steps_to_requeue)
                recovery_summary['steps_requeued'] = requeue_result['requeued']
                recovery_summary['steps_marked_terminal'] += requeue_result['terminal']
            
            # Step 7: Handle terminal steps
            if terminal_steps:
                logger.info(f"Handling {len(terminal_steps)} terminal steps in cycle {cycle_id}")
                await self.handle_terminal_failures(terminal_steps, cycle_id)
                # terminal_steps count already included in recovery_summary from step 4
            
            # Update statistics
            self._total_orphans_processed += len(orphaned_steps)
            self._total_steps_requeued += len(steps_to_requeue)
            self._total_steps_marked_terminal += len(terminal_steps)
            
            recovery_summary['success'] = True
            
            logger.info(
                f"Recovery cycle {cycle_id} completed successfully",
                extra={
                    'cycle_id': cycle_id,
                    'dead_pods_found': len(dead_pods),
                    'orphaned_steps_found': len(orphaned_steps),
                    'steps_to_requeue': len(steps_to_requeue),
                    'terminal_steps': len(terminal_steps),
                    'dlq_records_created': len(dlq_records)
                }
            )
            
        except Exception as e:
            recovery_summary['error'] = str(e)
            logger.error(
                f"Recovery cycle {cycle_id} failed: {e}",
                extra={'cycle_id': cycle_id, 'error': str(e)}
            )
            raise
        
        finally:
            # Always cleanup DLQ table
            try:
                logger.debug(f"Cleaning up DLQ table for cycle {cycle_id}")
                await self.sqlite_storage.drop_dlq_table(cycle_id)
                logger.debug(f"DLQ table cleanup completed for cycle {cycle_id}")
            except Exception as cleanup_error:
                logger.error(
                    f"Failed to cleanup DLQ table for cycle {cycle_id}: {cleanup_error}",
                    extra={'cycle_id': cycle_id, 'cleanup_error': str(cleanup_error)}
                )
            
            # Update timing and statistics
            cycle_end_time = datetime.utcnow()
            duration_ms = int((cycle_end_time - cycle_start_time).total_seconds() * 1000)
            
            recovery_summary.update({
                'end_time': cycle_end_time,
                'duration_ms': duration_ms
            })
            
            self._last_recovery_time = cycle_end_time
            self._recovery_count += 1
            
            logger.info(
                f"Recovery cycle {cycle_id} finished in {duration_ms}ms",
                extra={
                    'cycle_id': cycle_id,
                    'duration_ms': duration_ms,
                    'success': recovery_summary['success']
                }
            )
        
        return recovery_summary
    
    async def _detect_and_mark_dead_pods(self) -> List[PodHealth]:
        """
        Detect pods with stale heartbeats and mark them as dead.
        
        This is an internal method that handles the dead pod detection
        and marking process as part of the recovery cycle.
        
        Returns:
            List[PodHealth]: List of pods that were detected and marked as dead
            
        Raises:
            Exception: If dead pod detection or marking fails
        """
        try:
            logger.debug(
                f"Starting dead pod detection with threshold {self.config.dead_pod_threshold_seconds}s"
            )
            
            # Get pods with stale heartbeats
            dead_pods = await self.storage.get_dead_pods(
                self.config.dead_pod_threshold_seconds
            )
            
            if not dead_pods:
                logger.debug("No dead pods detected")
                return []
            
            # Extract pod IDs for marking
            dead_pod_ids = [pod.pod_id for pod in dead_pods]
            
            logger.info(
                f"Detected {len(dead_pods)} dead pods: {dead_pod_ids}",
                extra={
                    'dead_pod_count': len(dead_pods),
                    'dead_pod_ids': dead_pod_ids,
                    'threshold_seconds': self.config.dead_pod_threshold_seconds
                }
            )
            
            # Mark pods as dead in the database
            await self.storage.mark_pods_dead(dead_pod_ids)
            
            logger.info(
                f"Successfully marked {len(dead_pods)} pods as dead",
                extra={
                    'marked_dead_count': len(dead_pods),
                    'marked_dead_pod_ids': dead_pod_ids
                }
            )
            
            return dead_pods
            
        except Exception as e:
            logger.error(f"Failed to detect and mark dead pods: {e}")
            raise
    
    async def _recovery_loop(self) -> None:
        """
        Main recovery loop that runs continuously in the background.
        
        Performs recovery cycles at the configured interval, but only when
        this pod is the current leader. Handles errors gracefully and includes
        leader election checking.
        """
        logger.info(
            f"Starting recovery loop for pod {self.pod_id} "
            f"(interval: {self.config.recovery_interval_seconds}s)"
        )
        
        recovery_cycle_count = 0
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Check if we are the leader
                    is_leader = await self.storage.am_i_leader(
                        self.pod_id,
                        self.config.dead_pod_threshold_seconds
                    )
                    
                    if not is_leader:
                        logger.debug(f"Pod {self.pod_id} is not the leader, skipping recovery cycle")
                    else:
                        logger.debug(f"Pod {self.pod_id} is the leader, performing recovery cycle")
                        
                        # Perform recovery cycle
                        recovery_summary = await self.perform_recovery_cycle()
                        recovery_cycle_count += 1
                        
                        # Log recovery summary
                        if recovery_summary['success']:
                            logger.info(
                                f"Recovery cycle completed: {recovery_summary['dead_pods_found']} dead pods, "
                                f"{recovery_summary['orphaned_steps_found']} orphaned steps, "
                                f"{recovery_summary['steps_requeued']} requeued, "
                                f"{recovery_summary['steps_marked_terminal']} terminal",
                                extra=recovery_summary
                            )
                        else:
                            logger.error(
                                f"Recovery cycle failed: {recovery_summary.get('error', 'unknown error')}",
                                extra=recovery_summary
                            )
                    
                    # Wait for next recovery interval or shutdown signal
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.config.recovery_interval_seconds
                        )
                        # If we get here, shutdown was requested
                        break
                    except asyncio.TimeoutError:
                        # Timeout is expected - continue with next recovery cycle
                        continue
                        
                except Exception as e:
                    logger.error(f"Error in recovery loop: {e}")
                    # Continue the loop even if individual recovery cycles fail
                    # Wait a bit before retrying to avoid tight error loops
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=min(30.0, self.config.recovery_interval_seconds)
                        )
                        break
                    except asyncio.TimeoutError:
                        continue
        
        except asyncio.CancelledError:
            logger.info(f"Recovery loop cancelled for pod {self.pod_id}")
            raise
        except Exception as e:
            logger.error(f"Fatal error in recovery loop: {e}")
            raise
        finally:
            logger.info(
                f"Recovery loop stopped for pod {self.pod_id} "
                f"(performed {recovery_cycle_count} recovery cycles)"
            )
    
    async def start(self) -> None:
        """
        Start the recovery service.
        
        Starts the background recovery loop that performs periodic recovery
        cycles when this pod is the leader. This method should be called
        during application startup.
        
        Raises:
            RuntimeError: If service is already running
            Exception: If startup fails
        """
        if self._is_running:
            raise RuntimeError(f"Recovery service for pod {self.pod_id} is already running")
        
        try:
            logger.info(f"Starting recovery service for pod {self.pod_id}")
            
            # Start recovery loop
            self._recovery_task = asyncio.create_task(self._recovery_loop())
            self._is_running = True
            
            logger.info(f"Recovery service started for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Failed to start recovery service for pod {self.pod_id}: {e}")
            await self.stop()  # Clean up any partial state
            raise
    
    async def stop(self) -> None:
        """
        Stop the recovery service gracefully.
        
        Signals the recovery loop to stop and cleans up resources.
        This method should be called during application shutdown.
        """
        if not self._is_running:
            logger.debug(f"Recovery service for pod {self.pod_id} is not running")
            return
        
        try:
            logger.info(f"Stopping recovery service for pod {self.pod_id}")
            
            # Signal shutdown to recovery loop
            self._shutdown_event.set()
            
            # Wait for recovery loop to finish
            if self._recovery_task and not self._recovery_task.done():
                try:
                    await asyncio.wait_for(self._recovery_task, timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Recovery loop did not stop within timeout, cancelling")
                    self._recovery_task.cancel()
                    try:
                        await self._recovery_task
                    except asyncio.CancelledError:
                        pass
            
            # Clean up state
            self._recovery_task = None
            self._is_running = False
            self._shutdown_event.clear()
            
            logger.info(f"Recovery service stopped for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Error during recovery service shutdown: {e}")
            # Ensure we clean up state even if shutdown fails
            self._recovery_task = None
            self._is_running = False
            self._shutdown_event.clear()
            raise
    
    async def run_manual_recovery(self) -> Dict[str, Any]:
        """
        Run a manual recovery cycle regardless of leader status.
        
        This method can be called via admin endpoints to trigger a recovery
        cycle on demand. It bypasses the normal leader election check.
        
        Returns:
            Dict[str, Any]: Recovery cycle summary
            
        Raises:
            Exception: If manual recovery fails
        """
        logger.info(f"Running manual recovery cycle for pod {self.pod_id}")
        
        try:
            recovery_summary = await self.perform_recovery_cycle()
            
            logger.info(
                f"Manual recovery cycle completed for pod {self.pod_id}",
                extra=recovery_summary
            )
            
            return recovery_summary
            
        except Exception as e:
            logger.error(f"Manual recovery cycle failed for pod {self.pod_id}: {e}")
            raise


def create_recovery_service(
    storage: StorageInterface, 
    sqlite_storage: SQLiteStorageInterface,
    config: FleetQConfig
) -> RecoveryService:
    """
    Factory function to create a RecoveryService instance.
    
    Args:
        storage: Main storage backend for step and pod operations
        sqlite_storage: Local SQLite storage for DLQ operations
        config: Application configuration
        
    Returns:
        RecoveryService: Configured recovery service instance
    """
    return RecoveryService(storage, sqlite_storage, config)