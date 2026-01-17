"""
FLEET-Q: Raquel-Inspired Design Patterns

This module demonstrates how FLEET-Q implements design patterns inspired by
Raquel (https://github.com/vduseev/raquel), adapted for distributed,
broker-less, Snowflake-backed task queuing.

Key Patterns from Raquel:
1. SQL-backed queue semantics
2. Transaction-centered correctness
3. Clean enqueue/dequeue API
4. Status-based state machine
5. Retry semantics with backoff

Raquel License: MIT
FLEET-Q adapts these patterns while remaining independent.
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Pattern 1: Status-Based State Machine (inspired by Raquel)
# ============================================================================

class TaskStatus(str, Enum):
    """
    Task status enumeration following Raquel's state machine pattern.
    
    Raquel uses: QUEUED, CLAIMED, SUCCESS, FAILED, EXPIRED, EXHAUSTED, CANCELLED
    FLEET-Q simplifies to: PENDING, CLAIMED, COMPLETED, FAILED
    """
    PENDING = "pending"      # Raquel equivalent: QUEUED
    CLAIMED = "claimed"      # Same as Raquel
    COMPLETED = "completed"  # Raquel equivalent: SUCCESS
    FAILED = "failed"        # Same as Raquel


# ============================================================================
# Pattern 2: Transaction-Centered Correctness (inspired by Raquel)
# ============================================================================

class TransactionContext:
    """
    Context manager for explicit transaction handling.
    
    Raquel Approach:
        with session.begin():
            # Atomic operations
            stmt = update(Job).where(...)
            session.execute(stmt)
    
    FLEET-Q Approach:
        with transaction_context():
            # Atomic operations via Snowflake cursor
            cursor.execute(query, params)
    """
    
    def __init__(self, storage):
        self.storage = storage
        self.cursor = None
    
    def __enter__(self):
        self.cursor = self.storage.cursor()
        return self.cursor.__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.cursor.__exit__(exc_type, exc_val, exc_tb)


# ============================================================================
# Pattern 3: Clean Enqueue/Dequeue API (inspired by Raquel)
# ============================================================================

class RaquelInspiredQueue:
    """
    Demonstrates Raquel-inspired API design for FLEET-Q.
    
    Raquel API:
        raquel.enqueue(fn, *args, **kwargs)
        with raquel.dequeue() as job:
            result = job()
    
    FLEET-Q API:
        queue.submit_step(payload)
        steps = queue.claim_steps(count)
        queue.complete_step(step_id)
    """
    
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.pod_id = config.pod_id
    
    # Enqueue pattern (Raquel: enqueue)
    def submit_step(self, payload: Dict[str, Any], priority: int = 0) -> str:
        """
        Enqueue a task into the queue.
        
        Raquel pattern: Simple, one-method submission.
        """
        step_id = f"step-{self._generate_id()}"
        
        query = """
            INSERT INTO steps (step_id, status, payload, priority)
            VALUES (:step_id, :status, :payload, :priority)
        """
        
        with TransactionContext(self.storage) as cursor:
            cursor.execute(query, {
                'step_id': step_id,
                'status': TaskStatus.PENDING,
                'payload': payload,
                'priority': priority
            })
        
        logger.info(f"Enqueued step {step_id}")
        return step_id
    
    # Dequeue pattern (Raquel: dequeue with context manager)
    @contextmanager
    def claim_and_process(self, max_count: int = 1):
        """
        Context manager for claiming and processing steps.
        
        Raquel pattern:
            with raquel.dequeue() as job:
                if job:
                    result = job()  # Automatically marked success
                # Or job.fail(exception) for failure
        
        FLEET-Q adapts this for batch claiming and explicit processing.
        """
        claimed_steps = []
        
        try:
            # Claim steps atomically
            claimed_steps = self._atomic_claim(max_count)
            
            # Yield to caller for processing
            yield claimed_steps
            
            # If we reach here without exception, mark as completed
            for step in claimed_steps:
                self._mark_completed(step['step_id'])
        
        except Exception as e:
            # On exception, mark all claimed steps as failed
            logger.error(f"Processing failed: {e}")
            for step in claimed_steps:
                self._mark_failed(step['step_id'], str(e))
            raise
    
    def _atomic_claim(self, max_count: int) -> List[Dict]:
        """
        Atomic claim operation using transaction.
        
        Raquel uses SELECT...FOR UPDATE (PostgreSQL row lock):
            SELECT * FROM jobs
            WHERE status = 'queued'
            ORDER BY scheduled_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        
        FLEET-Q uses MERGE (Snowflake-safe atomic operation):
            MERGE INTO steps ...
            WHEN MATCHED AND status = 'pending' THEN UPDATE ...
        """
        query = """
            MERGE INTO steps AS target
            USING (
                SELECT step_id FROM steps
                WHERE status = :pending_status
                ORDER BY priority DESC, created_ts ASC
                LIMIT :max_count
            ) AS source
            ON target.step_id = source.step_id
            WHEN MATCHED AND target.status = :pending_status THEN
                UPDATE SET 
                    status = :claimed_status,
                    claimed_by = :pod_id,
                    claimed_at = CURRENT_TIMESTAMP()
        """
        
        with TransactionContext(self.storage) as cursor:
            cursor.execute(query, {
                'pending_status': TaskStatus.PENDING,
                'claimed_status': TaskStatus.CLAIMED,
                'pod_id': self.pod_id,
                'max_count': max_count
            })
        
        # Fetch claimed steps
        return self._fetch_claimed_steps()
    
    def _mark_completed(self, step_id: str):
        """Mark step as completed (Raquel: success transition)"""
        query = """
            UPDATE steps
            SET status = :completed, finished_at = CURRENT_TIMESTAMP()
            WHERE step_id = :step_id AND claimed_by = :pod_id
        """
        
        with TransactionContext(self.storage) as cursor:
            cursor.execute(query, {
                'completed': TaskStatus.COMPLETED,
                'step_id': step_id,
                'pod_id': self.pod_id
            })
    
    def _mark_failed(self, step_id: str, error: str):
        """Mark step as failed (Raquel: failed transition)"""
        query = """
            UPDATE steps
            SET status = :failed, error = :error, finished_at = CURRENT_TIMESTAMP()
            WHERE step_id = :step_id AND claimed_by = :pod_id
        """
        
        with TransactionContext(self.storage) as cursor:
            cursor.execute(query, {
                'failed': TaskStatus.FAILED,
                'step_id': step_id,
                'pod_id': self.pod_id,
                'error': error
            })
    
    def _fetch_claimed_steps(self) -> List[Dict]:
        """Fetch steps claimed by this pod"""
        # Implementation would fetch from storage
        return []
    
    def _generate_id(self) -> str:
        """Generate unique step ID"""
        import uuid
        return str(uuid.uuid4())


# ============================================================================
# Pattern 4: Retry Semantics with Exponential Backoff (inspired by Raquel)
# ============================================================================

class RetryPolicy:
    """
    Retry policy implementation inspired by Raquel's backoff strategy.
    
    Raquel Implementation:
        planned_delay = job.backoff_base * 2**attempt_num
        actual_delay = min(max(min_delay, planned_delay), max_delay)
        reschedule_at = claimed_at + duration + actual_delay
    
    FLEET-Q Implementation:
        - Leader checks retry_count vs max_retries
        - Requeues eligible steps with incremented retry_count
        - Uses recovery_interval as natural backoff spacing
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        backoff_base_ms: int = 1000,
        min_retry_delay_ms: int = 1000,
        max_retry_delay_ms: int = 60000
    ):
        self.max_retries = max_retries
        self.backoff_base_ms = backoff_base_ms
        self.min_retry_delay_ms = min_retry_delay_ms
        self.max_retry_delay_ms = max_retry_delay_ms
    
    def should_retry(self, retry_count: int) -> bool:
        """
        Determine if step should be retried.
        
        Raquel checks:
            if max_retry_count and attempts >= max_retry_count:
                mark_exhausted()
            else:
                reschedule_with_backoff()
        """
        return retry_count < self.max_retries
    
    def calculate_delay(self, attempt_num: int) -> int:
        """
        Calculate retry delay using exponential backoff.
        
        Raquel formula (adapted):
            delay = min(max(min_delay, base * 2^attempt), max_delay)
        """
        planned_delay = self.backoff_base_ms * (2 ** attempt_num)
        return min(
            max(self.min_retry_delay_ms, planned_delay),
            self.max_retry_delay_ms
        )


# ============================================================================
# Pattern 5: Queue Statistics (inspired by Raquel)
# ============================================================================

class QueueStats:
    """
    Queue statistics tracking inspired by Raquel's QueueStats model.
    
    Raquel exposes:
        - queued: jobs waiting to be processed
        - claimed: jobs currently being processed
        - success: completed jobs
        - failed: failed jobs
        - exhausted: jobs that exceeded retry limit
    
    FLEET-Q exposes:
        - pending: steps waiting to be claimed
        - claimed: steps currently being processed
        - completed: successfully completed steps
        - failed: failed steps (may be retried by leader)
    """
    
    def __init__(self, storage):
        self.storage = storage
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get current queue statistics.
        
        Raquel pattern: Single method that returns counts by status.
        """
        query = """
            SELECT status, COUNT(*) as count
            FROM steps
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
            status = row['status']
            count = row['count']
            if status in stats:
                stats[status] = count
        
        return stats


# ============================================================================
# Usage Example: Raquel-Inspired Patterns in FLEET-Q
# ============================================================================

def example_usage():
    """
    Demonstrates how FLEET-Q uses Raquel-inspired patterns.
    """
    
    # Pattern 1: Clean API
    from config import load_config
    from storage import SnowflakeStorage
    from queue import QueueOperations
    
    config = load_config()
    storage = SnowflakeStorage(config)
    queue = QueueOperations(config, storage)
    
    # Enqueue (Raquel: raquel.enqueue)
    step_id = queue.submit_step({
        'task_type': 'process_data',
        'file': 'data.csv'
    })
    
    # Claim and process (Raquel: with raquel.dequeue())
    steps = queue.claim_steps(max_count=5)
    for step in steps:
        try:
            # Process the step
            result = process_step(step)
            queue.complete_step(step['STEP_ID'])
        except Exception as e:
            queue.fail_step(step['STEP_ID'], str(e))
    
    # Pattern 2: Statistics (Raquel: raquel.stats())
    stats = queue.get_queue_stats()
    print(f"Queue stats: {stats}")
    
    # Pattern 3: Retry policy (Raquel: job retry with backoff)
    retry_policy = RetryPolicy(max_retries=3)
    if retry_policy.should_retry(retry_count=1):
        queue.requeue_step(step_id, increment_retry=True)


def process_step(step: Dict) -> Any:
    """Example step processing function"""
    payload = step['PAYLOAD']
    # ... process the step ...
    return {'status': 'success'}


if __name__ == "__main__":
    print(__doc__)
    print("\nThis module demonstrates Raquel-inspired design patterns in FLEET-Q.")
    print("See the code above for detailed explanations and comparisons.")
