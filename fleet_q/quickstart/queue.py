"""
FLEET-Q Core Queue Operations

Provides clean API for submitting, claiming, completing, and failing steps.
Implements atomic transaction-based claiming with capacity awareness.

Design inspired by Raquel (https://github.com/vduseev/raquel):
- SQL-backed queue semantics: STEP_TRACKER is the single source of truth
- Clean enqueue/dequeue API: submit_step(), claim_steps(), complete_step(), fail_step()
- Transaction-centered correctness: Atomic claims with predictable state transitions
- Retry semantics: Integrated with leader resubmission policy and backoff utility
"""

import json
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from backoff import with_backoff
from config import FleetQConfig
from storage import SnowflakeStorage

logger = logging.getLogger(__name__)


class StepStatus:
    """Step status constants - following Raquel's status naming pattern"""
    PENDING = "pending"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"


class QueueOperations:
    """Core queue operations for FLEET-Q"""
    
    def __init__(self, config: FleetQConfig, storage: SnowflakeStorage):
        self.config = config
        self.storage = storage
        self.pod_id = config.pod_id
    
    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transaction handling.
        
        Inspired by Raquel's transaction-centered correctness approach.
        Ensures that all database operations are properly committed or rolled back.
        """
        try:
            with self.storage.cursor() as cursor:
                yield cursor
                # Commit happens automatically in cursor context manager
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise
    
    def submit_step(
        self,
        payload: Dict[str, Any],
        step_id: Optional[str] = None,
        priority: int = 0
    ) -> str:
        """
        Submit a new step to the queue (enqueue operation).
        
        Raquel-inspired: Clean enqueue API with SQL-backed persistence.
        Steps are immediately visible to all workers via shared STEP_TRACKER table.
        
        Args:
            payload: Step payload (task type, arguments, etc.)
            step_id: Optional custom step ID (auto-generated if not provided)
            priority: Step priority (higher = more important)
            
        Returns:
            Step ID
        """
        if step_id is None:
            step_id = f"step-{uuid.uuid4()}"
        
        query = f"""
            INSERT INTO {self.config.step_tracker_table}
            (step_id, status, payload, priority, created_ts, last_update_ts, retry_count)
            VALUES (%(step_id)s, %(status)s, PARSE_JSON(%(payload)s), %(priority)s, 
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0)
        """
        
        self.storage.execute(query, {
            'step_id': step_id,
            'status': StepStatus.PENDING,
            'payload': json.dumps(payload),
            'priority': priority
        })
        
        logger.info(f"Submitted step {step_id} with priority {priority}")
        return step_id
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=50,
        max_delay_ms=2000,
        jitter=True
    )
    def claim_steps(self, max_count: int) -> List[Dict[str, Any]]:
        """
        Atomically claim pending steps up to max_count (dequeue operation).
        
        Raquel-inspired transaction-centered correctness:
        - Uses MERGE for atomic claim operation (prevents double-claiming)
        - Transaction ensures consistency even under concurrent workers
        - Backoff decorator handles transient conflicts gracefully
        
        This is the core of FLEET-Q's distributed coordination. Multiple workers
        can safely call this concurrently - Snowflake's transaction isolation
        guarantees that each step is claimed by exactly one worker.
        
        Args:
            max_count: Maximum number of steps to claim
            
        Returns:
            List of claimed steps with their details
        """
        if max_count <= 0:
            return []
        
        # Atomic claim using MERGE (Snowflake's transaction-safe approach)
        # This prevents the race condition where two workers try to claim
        # the same step simultaneously
        claim_query = f"""
            MERGE INTO {self.config.step_tracker_table} AS target
            USING (
                SELECT step_id
                FROM {self.config.step_tracker_table}
                WHERE status = %(pending_status)s
                ORDER BY priority DESC, created_ts ASC
                LIMIT %(max_count)s
            ) AS source
            ON target.step_id = source.step_id
            WHEN MATCHED AND target.status = %(pending_status)s THEN
                UPDATE SET 
                    status = %(claimed_status)s,
                    claimed_by = %(pod_id)s,
                    last_update_ts = CURRENT_TIMESTAMP()
        """
        
        self.storage.execute(claim_query, {
            'max_count': max_count,
            'pod_id': self.pod_id,
            'pending_status': StepStatus.PENDING,
            'claimed_status': StepStatus.CLAIMED
        })
        
        # Fetch the claimed steps for this pod
        fetch_query = f"""
            SELECT step_id, payload, retry_count, priority, created_ts
            FROM {self.config.step_tracker_table}
            WHERE claimed_by = %(pod_id)s
            AND status = %(claimed_status)s
        """
        
        claimed = self.storage.execute(fetch_query, {
            'pod_id': self.pod_id,
            'claimed_status': StepStatus.CLAIMED
        })
        
        if claimed:
            logger.info(f"Claimed {len(claimed)} steps")
        
        return claimed
    
    def complete_step(self, step_id: str) -> None:
        """
        Mark a step as completed (success transition).
        
        Raquel-inspired: Clean completion API with predictable state transition.
        Only the worker that claimed the step can complete it (enforced via claimed_by).
        
        Args:
            step_id: Step ID to complete
        """
        query = f"""
            UPDATE {self.config.step_tracker_table}
            SET status = %(completed_status)s,
                last_update_ts = CURRENT_TIMESTAMP()
            WHERE step_id = %(step_id)s
            AND claimed_by = %(pod_id)s
            AND status = %(claimed_status)s
        """
        
        self.storage.execute(query, {
            'step_id': step_id,
            'pod_id': self.pod_id,
            'completed_status': StepStatus.COMPLETED,
            'claimed_status': StepStatus.CLAIMED
        })
        
        logger.info(f"Completed step {step_id}")
    
    def fail_step(self, step_id: str, error_msg: Optional[str] = None) -> None:
        """
        Mark a step as failed (failure transition).
        
        Raquel-inspired: Failed steps remain in the queue for leader recovery.
        The leader will apply retry policy and either resubmit or mark as exhausted.
        Error information is preserved in the payload for debugging.
        
        Args:
            step_id: Step ID to fail
            error_msg: Optional error message
        """
        # Optionally update payload with error info
        payload_update = ""
        if error_msg:
            payload_update = f", payload = OBJECT_INSERT(payload, 'error', %(error_msg)s, true)"
        
        query = f"""
            UPDATE {self.config.step_tracker_table}
            SET status = %(failed_status)s,
                last_update_ts = CURRENT_TIMESTAMP()
                {payload_update}
            WHERE step_id = %(step_id)s
            AND claimed_by = %(pod_id)s
            AND status = %(claimed_status)s
        """
        
        params = {
            'step_id': step_id,
            'pod_id': self.pod_id,
            'failed_status': StepStatus.FAILED,
            'claimed_status': StepStatus.CLAIMED
        }
        if error_msg:
            params['error_msg'] = error_msg
        
        self.storage.execute(query, params)
        
        logger.warning(f"Failed step {step_id}: {error_msg}")
    
    def get_step_status(self, step_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current status of a step
        
        Args:
            step_id: Step ID to query
            
        Returns:
            Step details or None if not found
        """
        query = f"""
            SELECT step_id, status, claimed_by, payload, retry_count, 
                   priority, created_ts, last_update_ts
            FROM {self.config.step_tracker_table}
            WHERE step_id = %(step_id)s
        """
        
        results = self.storage.execute(query, {'step_id': step_id})
        return results[0] if results else None
    
    def get_queue_stats(self) -> Dict[str, int]:
        """
        Get queue statistics
        
        Returns:
            Dictionary with counts by status
        """
        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.config.step_tracker_table}
            GROUP BY status
        """
        
        results = self.storage.execute(query)
        
        stats = {
            'pending': 0,
            'claimed': 0,
            'completed': 0,
            'failed': 0
        }
        
        for row in results:
            stats[row['STATUS']] = row['COUNT']
        
        return stats
    
    def get_inflight_count(self) -> int:
        """
        Get count of steps currently claimed by this pod
        
        Returns:
            Number of inflight steps for this pod
        """
        query = f"""
            SELECT COUNT(*) as count
            FROM {self.config.step_tracker_table}
            WHERE claimed_by = %(pod_id)s
            AND status = 'claimed'
        """
        
        result = self.storage.execute(query, {'pod_id': self.pod_id})
        return result[0]['COUNT'] if result else 0
    
    def calculate_available_capacity(self) -> int:
        """
        Calculate how many steps this pod can claim right now
        
        Returns:
            Number of available slots
        """
        inflight = self.get_inflight_count()
        max_allowed = int(self.config.max_parallelism * self.config.capacity_threshold)
        available = max(0, max_allowed - inflight)
        
        logger.debug(f"Capacity: {inflight}/{max_allowed} inflight, {available} available")
        return available
    
    def requeue_step(self, step_id: str, increment_retry: bool = True) -> None:
        """
        Requeue a step back to pending (reschedule operation).
        
        Raquel-inspired retry semantics:
        - Used by leader recovery to resubmit orphaned or failed steps
        - Increments retry_count to track attempt number
        - Clears claimed_by to make step available for claiming
        - Follows exponential backoff pattern via leader recovery timing
        
        Args:
            step_id: Step ID to requeue
            increment_retry: Whether to increment retry_count
        """
        retry_update = ", retry_count = retry_count + 1" if increment_retry else ""
        
        query = f"""
            UPDATE {self.config.step_tracker_table}
            SET status = %(pending_status)s,
                claimed_by = NULL,
                last_update_ts = CURRENT_TIMESTAMP()
                {retry_update}
            WHERE step_id = %(step_id)s
        """
        
        self.storage.execute(query, {
            'step_id': step_id,
            'pending_status': StepStatus.PENDING
        })
        logger.info(f"Requeued step {step_id}")
    
    def requeue_steps_bulk(self, step_ids: List[str], increment_retry: bool = True) -> int:
        """
        Requeue multiple steps at once (efficient for leader recovery)
        
        Args:
            step_ids: List of step IDs to requeue
            increment_retry: Whether to increment retry_count
            
        Returns:
            Number of steps requeued
        """
        if not step_ids:
            return 0
        
        retry_update = ", retry_count = retry_count + 1" if increment_retry else ""
        
        # Use IN clause for bulk update
        step_ids_str = "','".join(step_ids)
        
        query = f"""
            UPDATE {self.config.step_tracker_table}
            SET status = 'pending',
                claimed_by = NULL,
                last_update_ts = CURRENT_TIMESTAMP()
                {retry_update}
            WHERE step_id IN ('{step_ids_str}')
        """
        
        self.storage.execute(query, {})
        logger.info(f"Requeued {len(step_ids)} steps in bulk")
        return len(step_ids)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Note: This requires actual Snowflake credentials to run
    # This is just to demonstrate the API
    print("QueueOperations module loaded successfully")
    print("Example usage:")
    print("  queue_ops = QueueOperations(config, storage)")
    print("  step_id = queue_ops.submit_step({'task': 'process_data', 'file': 'data.csv'})")
    print("  claimed = queue_ops.claim_steps(max_count=5)")
    print("  queue_ops.complete_step(step_id)")
