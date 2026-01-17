"""
Snowflake storage backend implementation for FLEET-Q.

Provides concrete implementation of the StorageInterface using Snowflake as the
coordination database. Handles atomic step claiming, pod health management,
and recovery operations with proper transaction handling and retry logic.

"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError
)

from .backoff import with_backoff, create_retry_callback
from .config import FleetQConfig
from .models import Step, PodHealth, StepStatus, PodStatus, StepPayload
from .storage import (
    StorageInterface,
    StorageError,
    TransactionConflictError,
    ConnectionError as StorageConnectionError,
    VariantSerializer
)

logger = logging.getLogger(__name__)


class SnowflakeStorage(StorageInterface):
    """
    Snowflake implementation of the storage interface.
    
    Provides all storage operations needed by FLEET-Q using Snowflake as the
    coordination database. Implements atomic operations, retry logic, and
    proper transaction handling.
    """
    
    def __init__(self, config: FleetQConfig):
        """
        Initialize Snowflake storage backend.
        
        Args:
            config: FleetQ configuration containing Snowflake connection parameters
        """
        self.config = config
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self._connection_lock = asyncio.Lock()
        
        # Create retry callback for logging
        self.retry_callback = create_retry_callback(logger, "snowflake_operation")
    
    async def initialize(self) -> None:
        """
        Initialize the Snowflake storage backend.
        
        Creates database connection and ensures required tables exist.
        
        Raises:
            StorageError: If initialization fails
        """
        try:
            await self._connect()
            await self._ensure_tables_exist()
            logger.info("Snowflake storage backend initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Snowflake storage: {e}")
            raise StorageError(f"Snowflake initialization failed: {e}", e)
    
    async def close(self) -> None:
        """Close the Snowflake connection and clean up resources."""
        async with self._connection_lock:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("Snowflake connection closed")
                except Exception as e:
                    logger.warning(f"Error closing Snowflake connection: {e}")
                finally:
                    self.connection = None
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=200,
        max_delay_ms=10000,
        retry_on=[OperationalError, InterfaceError, DatabaseError, StorageConnectionError]
    )
    async def _connect(self) -> None:
        """
        Establish connection to Snowflake with retry logic.
        
        Raises:
            StorageConnectionError: If connection fails after retries
        """
        async with self._connection_lock:
            try:
                if self.connection:
                    # Test existing connection
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    return
            except Exception:
                # Connection is stale, create new one
                self.connection = None
            
            try:
                # Create new connection
                self.connection = snowflake.connector.connect(
                    **self.config.snowflake_connection_params,
                    autocommit=False  # We want explicit transaction control
                )
                logger.info("Connected to Snowflake successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {e}")
                raise StorageConnectionError(f"Snowflake connection failed: {e}", e)
    
    async def _ensure_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Ensure we have a valid Snowflake connection.
        
        Returns:
            SnowflakeConnection: Active connection
            
        Raises:
            StorageConnectionError: If connection cannot be established
        """
        if not self.connection:
            await self._connect()
        return self.connection
    
    async def _ensure_tables_exist(self) -> None:
        """
        Ensure required tables exist in Snowflake.
        
        Creates POD_HEALTH and STEP_TRACKER tables if they don't exist.
        
        Raises:
            StorageError: If table creation fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Create POD_HEALTH table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS POD_HEALTH (
                    pod_id STRING PRIMARY KEY,
                    birth_ts TIMESTAMP_NTZ NOT NULL,
                    last_heartbeat_ts TIMESTAMP_NTZ NOT NULL,
                    status STRING NOT NULL,
                    meta VARIANT,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Create STEP_TRACKER table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS STEP_TRACKER (
                    step_id STRING PRIMARY KEY,
                    status STRING NOT NULL,
                    claimed_by STRING,
                    last_update_ts TIMESTAMP_NTZ NOT NULL,
                    payload VARIANT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    max_retries INTEGER DEFAULT 3
                )
            """)
            
            connection.commit()
            cursor.close()
            logger.info("Ensured Snowflake tables exist")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create Snowflake tables: {e}")
            raise StorageError(f"Table creation failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def create_step(self, step: Step) -> None:
        """
        Create a new step in the STEP_TRACKER table.
        
        Args:
            step: Step object to create
            
        Raises:
            StorageError: If step creation fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Serialize the payload for VARIANT storage
            payload_json = VariantSerializer.serialize(step.payload)
            
            cursor.execute("""
                INSERT INTO STEP_TRACKER (
                    step_id, status, claimed_by, last_update_ts, payload,
                    retry_count, priority, created_ts, max_retries
                ) VALUES (?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?)
            """, (
                step.step_id,
                step.status.value,
                step.claimed_by,
                step.last_update_ts,
                payload_json,
                step.retry_count,
                step.priority,
                step.created_ts or step.last_update_ts,
                step.max_retries
            ))
            
            connection.commit()
            cursor.close()
            logger.debug(f"Created step {step.step_id} with status {step.status}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create step {step.step_id}: {e}")
            raise StorageError(f"Step creation failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            cursor.execute("""
                SELECT step_id, status, claimed_by, last_update_ts, payload,
                       retry_count, priority, created_ts, max_retries
                FROM STEP_TRACKER
                WHERE step_id = ?
            """, (step_id,))
            
            row = cursor.fetchone()
            cursor.close()
            
            if not row:
                return None
            
            return self._row_to_step(row)
            
        except Exception as e:
            logger.error(f"Failed to get step {step_id}: {e}")
            raise StorageError(f"Step retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=8,
        base_delay_ms=50,
        max_delay_ms=2000,
        retry_on=[OperationalError, InterfaceError, DatabaseError]
    )
    async def claim_steps(self, pod_id: str, limit: int) -> List[Step]:
        """
        Atomically claim available steps for execution.
        
        Uses database transactions to ensure atomicity and prevent race conditions
        between multiple pods attempting to claim the same steps.
        
        Args:
            pod_id: ID of the pod claiming the steps
            limit: Maximum number of steps to claim
            
        Returns:
            List[Step]: List of successfully claimed steps
            
        Raises:
            StorageError: If claiming fails
            TransactionConflictError: If transaction conflicts occur
        """
        if limit <= 0:
            return []
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Start transaction for atomic claiming
            connection.autocommit = False
            
            # Select available steps with row locking (using SELECT FOR UPDATE equivalent)
            # In Snowflake, we use a different approach since it doesn't have FOR UPDATE
            cursor.execute("""
                SELECT step_id, status, claimed_by, last_update_ts, payload,
                       retry_count, priority, created_ts, max_retries
                FROM STEP_TRACKER 
                WHERE status = 'pending'
                ORDER BY priority DESC, created_ts ASC
                LIMIT ?
            """, (limit,))
            
            available_steps = cursor.fetchall()
            
            if not available_steps:
                connection.rollback()
                cursor.close()
                return []
            
            step_ids = [step['STEP_ID'] for step in available_steps]
            
            # Atomically claim the selected steps
            # Use a timestamp-based approach to detect conflicts
            current_time = datetime.utcnow()
            
            cursor.execute(f"""
                UPDATE STEP_TRACKER 
                SET status = 'claimed', 
                    claimed_by = ?, 
                    last_update_ts = ?
                WHERE step_id IN ({','.join(['?'] * len(step_ids))})
                  AND status = 'pending'
            """, [pod_id, current_time] + step_ids)
            
            # Check how many rows were actually updated
            rows_updated = cursor.rowcount
            
            if rows_updated != len(step_ids):
                # Some steps were claimed by another pod, this is a conflict
                connection.rollback()
                cursor.close()
                logger.debug(f"Claiming conflict: requested {len(step_ids)}, updated {rows_updated}")
                raise TransactionConflictError(
                    f"Step claiming conflict: {rows_updated}/{len(step_ids)} steps claimed"
                )
            
            # Commit the transaction
            connection.commit()
            cursor.close()
            
            # Convert the claimed steps to Step objects
            claimed_steps = []
            for step_data in available_steps:
                step = self._row_to_step(step_data)
                # Update the step with the new claim information
                step.status = StepStatus.CLAIMED
                step.claimed_by = pod_id
                step.last_update_ts = current_time
                claimed_steps.append(step)
            
            logger.info(f"Successfully claimed {len(claimed_steps)} steps for pod {pod_id}")
            return claimed_steps
            
        except TransactionConflictError:
            # Re-raise transaction conflicts as-is (rollback already called above)
            raise
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to claim steps for pod {pod_id}: {e}")
            raise StorageError(f"Step claiming failed: {e}", e)
        finally:
            # Restore autocommit
            connection.autocommit = True
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def update_step_status(
        self, 
        step_id: str, 
        status: StepStatus, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update the status of a step with atomic timestamp updates.
        
        Handles step state transitions (claimed → completed/failed) with proper
        timestamp updates and optional metadata storage.
        
        Args:
            step_id: ID of the step to update
            status: New status for the step
            metadata: Optional metadata to include in the update
            
        Raises:
            StorageError: If update fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow()
            
            # Handle different status transitions
            if status in [StepStatus.COMPLETED, StepStatus.FAILED]:
                # For completion/failure, clear claimed_by and update status
                cursor.execute("""
                    UPDATE STEP_TRACKER 
                    SET status = ?, 
                        claimed_by = NULL,
                        last_update_ts = ?
                    WHERE step_id = ?
                """, (status.value, current_time, step_id))
            else:
                # For other status updates, just update status and timestamp
                cursor.execute("""
                    UPDATE STEP_TRACKER 
                    SET status = ?, last_update_ts = ?
                    WHERE step_id = ?
                """, (status.value, current_time, step_id))
            
            if cursor.rowcount == 0:
                logger.warning(f"No step found with ID {step_id} for status update")
            
            connection.commit()
            cursor.close()
            logger.info(f"Updated step {step_id} status to {status}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to update step {step_id} status: {e}")
            raise StorageError(f"Step status update failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow()
            
            # Update step to completed status
            cursor.execute("""
                UPDATE STEP_TRACKER 
                SET status = 'completed',
                    claimed_by = NULL,
                    last_update_ts = ?
                WHERE step_id = ? AND status = 'claimed'
            """, (current_time, step_id))
            
            rows_updated = cursor.rowcount
            
            if rows_updated == 0:
                # Check if step exists and what its current status is
                cursor.execute("""
                    SELECT status, claimed_by FROM STEP_TRACKER WHERE step_id = ?
                """, (step_id,))
                
                row = cursor.fetchone()
                if not row:
                    raise StorageError(f"Step {step_id} not found")
                else:
                    current_status = row[0]
                    claimed_by = row[1]
                    raise StorageError(
                        f"Cannot complete step {step_id}: current status is {current_status}, "
                        f"claimed_by {claimed_by}. Only claimed steps can be completed."
                    )
            
            connection.commit()
            cursor.close()
            logger.info(f"Successfully completed step {step_id}")
            
        except StorageError:
            connection.rollback()
            raise
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to complete step {step_id}: {e}")
            raise StorageError(f"Step completion failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow()
            
            # Update step to failed status
            cursor.execute("""
                UPDATE STEP_TRACKER 
                SET status = 'failed',
                    claimed_by = NULL,
                    last_update_ts = ?
                WHERE step_id = ? AND status = 'claimed'
            """, (current_time, step_id))
            
            rows_updated = cursor.rowcount
            
            if rows_updated == 0:
                # Check if step exists and what its current status is
                cursor.execute("""
                    SELECT status, claimed_by FROM STEP_TRACKER WHERE step_id = ?
                """, (step_id,))
                
                row = cursor.fetchone()
                if not row:
                    raise StorageError(f"Step {step_id} not found")
                else:
                    current_status = row[0]
                    claimed_by = row[1]
                    raise StorageError(
                        f"Cannot fail step {step_id}: current status is {current_status}, "
                        f"claimed_by {claimed_by}. Only claimed steps can be failed."
                    )
            
            connection.commit()
            cursor.close()
            logger.info(f"Successfully failed step {step_id}" + 
                       (f" with error: {error_message}" if error_message else ""))
            
        except StorageError:
            connection.rollback()
            raise
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to fail step {step_id}: {e}")
            raise StorageError(f"Step failure update failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            cursor.execute("""
                SELECT step_id, status, claimed_by, last_update_ts, payload,
                       retry_count, priority, created_ts, max_retries
                FROM STEP_TRACKER
                WHERE status = ?
                ORDER BY priority DESC, created_ts ASC
            """, (status.value,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [self._row_to_step(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get steps by status {status}: {e}")
            raise StorageError(f"Steps retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        if not pod_ids:
            return []
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?'] * len(pod_ids))
            
            cursor.execute(f"""
                SELECT step_id, status, claimed_by, last_update_ts, payload,
                       retry_count, priority, created_ts, max_retries
                FROM STEP_TRACKER
                WHERE claimed_by IN ({placeholders})
                  AND status IN ('claimed', 'failed')
                ORDER BY last_update_ts ASC
            """, pod_ids)
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [self._row_to_step(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get steps claimed by pods {pod_ids}: {e}")
            raise StorageError(f"Orphaned steps retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError, DatabaseError]
    )
    async def requeue_steps(self, step_ids: List[str]) -> None:
        """
        Requeue steps back to pending status with incremented retry count.
        
        Args:
            step_ids: List of step IDs to requeue
            
        Raises:
            StorageError: If requeuing fails
        """
        if not step_ids:
            return
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow()
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?'] * len(step_ids))
            
            cursor.execute(f"""
                UPDATE STEP_TRACKER 
                SET status = 'pending',
                    claimed_by = NULL,
                    retry_count = retry_count + 1,
                    last_update_ts = ?
                WHERE step_id IN ({placeholders})
                  AND retry_count < max_retries
            """, [current_time] + step_ids)
            
            requeued_count = cursor.rowcount
            
            # Mark steps that exceeded max retries as terminally failed
            cursor.execute(f"""
                UPDATE STEP_TRACKER 
                SET status = 'failed',
                    claimed_by = NULL,
                    last_update_ts = ?
                WHERE step_id IN ({placeholders})
                  AND retry_count >= max_retries
                  AND status != 'failed'
            """, [current_time] + step_ids)
            
            terminal_count = cursor.rowcount
            
            connection.commit()
            cursor.close()
            
            logger.info(f"Requeued {requeued_count} steps, marked {terminal_count} as terminal")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to requeue steps {step_ids}: {e}")
            raise StorageError(f"Step requeuing failed: {e}", e)
    
    # Pod Health Management Operations
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def upsert_pod_health(self, pod_health: PodHealth) -> None:
        """
        Insert or update pod health record.
        
        Args:
            pod_health: PodHealth object to upsert
            
        Raises:
            StorageError: If upsert fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Serialize metadata for VARIANT storage
            meta_json = VariantSerializer.serialize(pod_health.meta)
            
            # Use MERGE for upsert operation
            cursor.execute("""
                MERGE INTO POD_HEALTH AS target
                USING (SELECT ? AS pod_id, ? AS birth_ts, ? AS last_heartbeat_ts, 
                              ? AS status, PARSE_JSON(?) AS meta) AS source
                ON target.pod_id = source.pod_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        last_heartbeat_ts = source.last_heartbeat_ts,
                        status = source.status,
                        meta = source.meta,
                        updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (pod_id, birth_ts, last_heartbeat_ts, status, meta)
                    VALUES (source.pod_id, source.birth_ts, source.last_heartbeat_ts, 
                           source.status, source.meta)
            """, (
                pod_health.pod_id,
                pod_health.birth_ts,
                pod_health.last_heartbeat_ts,
                pod_health.status.value,
                meta_json
            ))
            
            connection.commit()
            cursor.close()
            logger.debug(f"Upserted pod health for {pod_health.pod_id}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to upsert pod health for {pod_health.pod_id}: {e}")
            raise StorageError(f"Pod health upsert failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                WHERE pod_id = ?
            """, (pod_id,))
            
            row = cursor.fetchone()
            cursor.close()
            
            if not row:
                return None
            
            return self._row_to_pod_health(row)
            
        except Exception as e:
            logger.error(f"Failed to get pod health for {pod_id}: {e}")
            raise StorageError(f"Pod health retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def get_all_pod_health(self) -> List[PodHealth]:
        """
        Retrieve all pod health records.
        
        Returns:
            List[PodHealth]: List of all pod health records
            
        Raises:
            StorageError: If retrieval fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                ORDER BY birth_ts ASC
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [self._row_to_pod_health(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get all pod health records: {e}")
            raise StorageError(f"Pod health retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Calculate threshold timestamp
            threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
            
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                WHERE status = 'up' 
                  AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
            """, (threshold_time,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [self._row_to_pod_health(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get active pods: {e}")
            raise StorageError(f"Active pods retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        if not self.connection:
            raise StorageError("Storage not initialized")
        
        connection = self.connection
        cursor = None
        
        try:
            cursor = connection.cursor()
            
            # Calculate the threshold timestamp
            threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
            
            # Query for pods with stale heartbeats that are not already marked as down
            sql = """
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                WHERE last_heartbeat_ts < %s
                  AND status != 'down'
                ORDER BY last_heartbeat_ts ASC
            """
            
            cursor.execute(sql, (threshold_time,))
            rows = cursor.fetchall()
            
            dead_pods = []
            for row in rows:
                pod_id, birth_ts, last_heartbeat_ts, status, meta_json = row
                
                # Parse metadata
                try:
                    meta = json.loads(meta_json) if meta_json else {}
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Invalid metadata for pod {pod_id}, using empty dict")
                    meta = {}
                
                # Create PodHealth object
                pod_health = PodHealth(
                    pod_id=pod_id,
                    birth_ts=birth_ts,
                    last_heartbeat_ts=last_heartbeat_ts,
                    status=PodStatus(status),
                    meta=meta
                )
                
                dead_pods.append(pod_health)
            
            logger.info(
                f"Found {len(dead_pods)} dead pods with heartbeat threshold {heartbeat_threshold_seconds}s",
                extra={
                    'dead_pod_count': len(dead_pods),
                    'dead_pod_ids': [p.pod_id for p in dead_pods],
                    'threshold_seconds': heartbeat_threshold_seconds,
                    'threshold_time': threshold_time.isoformat()
                }
            )
            
            return dead_pods
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to get dead pods: {e}")
            raise StorageError(f"Dead pod query failed: {e}", e)
        finally:
            if cursor:
                cursor.close()
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def mark_pods_dead(self, pod_ids: List[str]) -> None:
        """
        Mark specified pods as dead by updating their status.
        
        Args:
            pod_ids: List of pod IDs to mark as dead
            
        Raises:
            StorageError: If update fails
        """
        if not pod_ids:
            return
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?'] * len(pod_ids))
            
            cursor.execute(f"""
                UPDATE POD_HEALTH 
                SET status = 'down', updated_at = CURRENT_TIMESTAMP()
                WHERE pod_id IN ({placeholders})
            """, pod_ids)
            
            updated_count = cursor.rowcount
            connection.commit()
            cursor.close()
            
            logger.info(f"Marked {updated_count} pods as dead: {pod_ids}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to mark pods dead {pod_ids}: {e}")
            raise StorageError(f"Pod status update failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
    async def get_queue_stats(self) -> Dict[str, int]:
        """
        Get queue statistics including step counts by status.
        
        Returns:
            Dict[str, int]: Dictionary with counts for each step status
            
        Raises:
            StorageError: If retrieval fails
        """
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM STEP_TRACKER
                GROUP BY status
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            
            # Initialize all statuses to 0
            stats = {
                'pending': 0,
                'claimed': 0,
                'completed': 0,
                'failed': 0
            }
            
            # Update with actual counts
            for row in rows:
                status = row['STATUS'].lower()
                count = row['COUNT']
                if status in stats:
                    stats[status] = count
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            raise StorageError(f"Queue stats retrieval failed: {e}", e)
    
    # Leader Election Operations
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Calculate threshold timestamp
            threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
            
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                WHERE status = 'up' 
                  AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
                LIMIT 1
            """, (threshold_time,))
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                leader_id = row['POD_ID']
                logger.debug(f"Determined leader: {leader_id}")
                return leader_id
            else:
                logger.debug("No eligible leader found")
                return None
            
        except Exception as e:
            logger.error(f"Failed to determine leader: {e}")
            raise StorageError(f"Leader determination failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        try:
            leader_id = await self.determine_leader(heartbeat_threshold_seconds)
            return leader_id == pod_id
        except Exception as e:
            logger.error(f"Failed to check leader status for pod {pod_id}: {e}")
            raise StorageError(f"Leader status check failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            current_time = datetime.utcnow()
            threshold_time = current_time - timedelta(seconds=heartbeat_threshold_seconds)
            
            # Get all active pods
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
                FROM POD_HEALTH
                WHERE status = 'up' 
                  AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
            """, (threshold_time,))
            
            active_pods = cursor.fetchall()
            cursor.close()
            
            # Build leader info
            leader_info = {
                'current_leader': None,
                'leader_birth_ts': None,
                'leader_last_heartbeat': None,
                'leader_metadata': None,
                'active_pods_count': len(active_pods),
                'active_pod_ids': [pod['POD_ID'] for pod in active_pods],
                'election_timestamp': current_time,
                'heartbeat_threshold_seconds': heartbeat_threshold_seconds
            }
            
            # Set leader information if there are active pods
            if active_pods:
                leader_pod = active_pods[0]  # First pod is eldest (ordered by birth_ts)
                leader_info.update({
                    'current_leader': leader_pod['POD_ID'],
                    'leader_birth_ts': leader_pod['BIRTH_TS'],
                    'leader_last_heartbeat': leader_pod['LAST_HEARTBEAT_TS'],
                    'leader_metadata': self._deserialize_pod_meta(leader_pod['META'])
                })
            
            return leader_info
            
        except Exception as e:
            logger.error(f"Failed to get leader info: {e}")
            raise StorageError(f"Leader info retrieval failed: {e}", e)
    
    def _deserialize_pod_meta(self, meta_data: Any) -> Dict[str, Any]:
        """
        Helper method to deserialize pod metadata.
        
        Args:
            meta_data: Raw metadata from database
            
        Returns:
            Dict[str, Any]: Deserialized metadata
        """
        if isinstance(meta_data, str):
            return VariantSerializer.deserialize_dict(meta_data)
        else:
            return meta_data or {}
    
    def _row_to_step(self, row: Dict[str, Any]) -> Step:
        """
        Convert a database row to a Step object.
        
        Args:
            row: Database row as dictionary
            
        Returns:
            Step: Step object
        """
        # Deserialize payload from VARIANT
        payload_data = row['PAYLOAD']
        if isinstance(payload_data, str):
            payload = VariantSerializer.deserialize_step_payload(payload_data)
        else:
            # If it's already a dict (from Snowflake VARIANT), convert directly
            payload = StepPayload(
                type=payload_data.get('type', ''),
                args=payload_data.get('args', {}),
                metadata=payload_data.get('metadata', {})
            )
        
        return Step(
            step_id=row['STEP_ID'],
            status=StepStatus(row['STATUS']),
            claimed_by=row['CLAIMED_BY'],
            last_update_ts=row['LAST_UPDATE_TS'],
            payload=payload,
            retry_count=row['RETRY_COUNT'],
            priority=row['PRIORITY'],
            created_ts=row['CREATED_TS'],
            max_retries=row['MAX_RETRIES']
        )
    
    def _row_to_pod_health(self, row: Dict[str, Any]) -> PodHealth:
        """
        Convert a database row to a PodHealth object.
        
        Args:
            row: Database row as dictionary
            
        Returns:
            PodHealth: PodHealth object
        """
        # Deserialize metadata from VARIANT
        meta_data = row['META']
        if isinstance(meta_data, str):
            meta = VariantSerializer.deserialize_dict(meta_data)
        else:
            # If it's already a dict (from Snowflake VARIANT), use directly
            meta = meta_data or {}
        
        return PodHealth(
            pod_id=row['POD_ID'],
            birth_ts=row['BIRTH_TS'],
            last_heartbeat_ts=row['LAST_HEARTBEAT_TS'],
            status=PodStatus(row['STATUS']),
            meta=meta
        )
    
    # Leader Election Operations
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Calculate threshold timestamp
            threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
            
            # Find the eldest pod with fresh heartbeat and UP status
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts
                FROM POD_HEALTH
                WHERE status = 'up' 
                  AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
                LIMIT 1
            """, (threshold_time,))
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                leader_id = row['POD_ID']
                logger.debug(
                    f"Determined leader: {leader_id}",
                    extra={
                        'leader_pod_id': leader_id,
                        'leader_birth_ts': row['BIRTH_TS'].isoformat(),
                        'leader_last_heartbeat': row['LAST_HEARTBEAT_TS'].isoformat(),
                        'threshold_time': threshold_time.isoformat()
                    }
                )
                return leader_id
            else:
                logger.warning("No eligible leader found - no active pods with fresh heartbeat")
                return None
            
        except Exception as e:
            logger.error(f"Failed to determine leader: {e}")
            raise StorageError(f"Leader determination failed: {e}", e)
    
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
        try:
            current_leader = await self.determine_leader(heartbeat_threshold_seconds)
            is_leader = current_leader == pod_id
            
            logger.debug(
                f"Leader check for pod {pod_id}: {'YES' if is_leader else 'NO'}",
                extra={
                    'pod_id': pod_id,
                    'current_leader': current_leader,
                    'is_leader': is_leader
                }
            )
            
            return is_leader
            
        except Exception as e:
            logger.error(f"Failed to check leader status for pod {pod_id}: {e}")
            raise StorageError(f"Leader check failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[OperationalError, InterfaceError]
    )
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
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor(DictCursor)
            
            # Calculate threshold timestamp
            threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
            
            # Get all active pods ordered by birth timestamp
            cursor.execute("""
                SELECT pod_id, birth_ts, last_heartbeat_ts, meta
                FROM POD_HEALTH
                WHERE status = 'up' 
                  AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
            """, (threshold_time,))
            
            active_pods = cursor.fetchall()
            cursor.close()
            
            leader_info = {
                'current_leader': None,
                'leader_birth_ts': None,
                'leader_last_heartbeat': None,
                'leader_metadata': None,
                'active_pods_count': len(active_pods),
                'active_pod_ids': [pod['POD_ID'] for pod in active_pods],
                'election_timestamp': datetime.utcnow(),
                'heartbeat_threshold_seconds': heartbeat_threshold_seconds
            }
            
            if active_pods:
                # First pod in the ordered list is the leader (eldest)
                leader_pod = active_pods[0]
                leader_info.update({
                    'current_leader': leader_pod['POD_ID'],
                    'leader_birth_ts': leader_pod['BIRTH_TS'],
                    'leader_last_heartbeat': leader_pod['LAST_HEARTBEAT_TS'],
                    'leader_metadata': leader_pod['META'] if leader_pod['META'] else {}
                })
                
                logger.info(
                    f"Leader election info: {leader_pod['POD_ID']} leads {len(active_pods)} active pods",
                    extra={
                        'current_leader': leader_pod['POD_ID'],
                        'active_pods_count': len(active_pods),
                        'leader_birth_ts': leader_pod['BIRTH_TS'].isoformat(),
                        'threshold_time': threshold_time.isoformat()
                    }
                )
            else:
                logger.warning("No active pods found for leader election")
            
            return leader_info
            
        except Exception as e:
            logger.error(f"Failed to get leader info: {e}")
            raise StorageError(f"Leader info retrieval failed: {e}", e)