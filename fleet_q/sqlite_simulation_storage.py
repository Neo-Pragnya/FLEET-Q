"""
SQLite simulation storage backend for FLEET-Q local development.

Provides a complete SQLite-based implementation that simulates Snowflake behavior
for local development and testing without requiring a Snowflake connection.
Implements both main storage operations and DLQ operations in a single SQLite database.
"""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from .backoff import with_backoff
from .config import FleetQConfig
from .models import Step, StepStatus, PodHealth, PodStatus, StepPayload, DLQRecord
from .storage import StorageInterface, SQLiteStorageInterface, StorageError, VariantSerializer

logger = logging.getLogger(__name__)


class SQLiteSimulationStorage(StorageInterface, SQLiteStorageInterface):
    """
    SQLite-based storage that simulates Snowflake behavior for local development.
    
    This implementation provides all the functionality of SnowflakeStorage but uses
    SQLite as the backend, making it perfect for local development, testing, and
    environments where Snowflake is not available.
    
    Features:
    - Complete step lifecycle management
    - Pod health tracking and heartbeats
    - Leader election support
    - Dead pod detection and recovery
    - Local DLQ operations
    - Atomic transactions for step claiming
    - All operations that SnowflakeStorage provides
    """
    
    def __init__(self, config: FleetQConfig):
        """
        Initialize SQLite simulation storage.
        
        Args:
            config: FleetQ configuration
        """
        self.config = config
        self.connection: Optional[sqlite3.Connection] = None
        self._connection_lock = asyncio.Lock()
        self.db_path = config.sqlite_db_path
        
        # Use a persistent file for simulation mode instead of memory
        if self.db_path == ":memory:":
            self.db_path = "/tmp/fleet_q_simulation.db"
    
    async def initialize(self, db_path: Optional[str] = None) -> None:
        """
        Initialize SQLite database with Snowflake-like schema.
        
        Args:
            db_path: Optional path override for database file
            
        Raises:
            StorageError: If initialization fails
        """
        if db_path:
            self.db_path = db_path
        
        try:
            await self._connect()
            await self._create_schema()
            logger.info(f"SQLite simulation storage initialized with database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite simulation storage: {e}")
            raise StorageError(f"SQLite simulation initialization failed: {e}", e)
    
    async def close(self) -> None:
        """Close SQLite database connection."""
        async with self._connection_lock:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("SQLite simulation connection closed")
                except Exception as e:
                    logger.warning(f"Error closing SQLite simulation connection: {e}")
                finally:
                    self.connection = None
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def _connect(self) -> None:
        """Establish connection to SQLite database."""
        async with self._connection_lock:
            try:
                if self.connection:
                    # Test existing connection
                    self.connection.execute("SELECT 1")
                    return
            except Exception:
                # Connection is stale, create new one
                self.connection = None
            
            try:
                # Create new connection
                self.connection = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,
                    timeout=30.0
                )
                
                # Configure connection for better performance
                self.connection.execute("PRAGMA journal_mode=WAL")
                self.connection.execute("PRAGMA synchronous=NORMAL")
                self.connection.execute("PRAGMA temp_store=MEMORY")
                self.connection.execute("PRAGMA foreign_keys=ON")
                
                logger.debug(f"Connected to SQLite simulation database: {self.db_path}")
                
            except Exception as e:
                logger.error(f"Failed to connect to SQLite simulation: {e}")
                raise StorageError(f"SQLite simulation connection failed: {e}", e)
    
    async def _ensure_connection(self) -> sqlite3.Connection:
        """Ensure we have a valid SQLite connection."""
        if not self.connection:
            raise StorageError("SQLite simulation connection is not initialized")
        return self.connection
    
    async def _create_schema(self) -> None:
        """Create the database schema that simulates Snowflake tables."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Create POD_HEALTH table (simulates Snowflake POD_HEALTH)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS POD_HEALTH (
                    pod_id TEXT PRIMARY KEY,
                    birth_ts TEXT NOT NULL,
                    last_heartbeat_ts TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('up', 'down')),
                    meta TEXT,  -- JSON string to simulate VARIANT
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create STEP_TRACKER table (simulates Snowflake STEP_TRACKER)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS STEP_TRACKER (
                    step_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'completed', 'failed')),
                    claimed_by TEXT,
                    last_update_ts TEXT NOT NULL,
                    payload TEXT NOT NULL,  -- JSON string to simulate VARIANT
                    retry_count INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    created_ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    max_retries INTEGER DEFAULT 3
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pod_health_status ON POD_HEALTH(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pod_health_heartbeat ON POD_HEALTH(last_heartbeat_ts)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_step_status ON STEP_TRACKER(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_step_claimed_by ON STEP_TRACKER(claimed_by)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_step_priority ON STEP_TRACKER(priority DESC, created_ts ASC)")
            
            # Create trigger to update updated_at timestamp
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_pod_health_timestamp 
                AFTER UPDATE ON POD_HEALTH
                BEGIN
                    UPDATE POD_HEALTH SET updated_at = CURRENT_TIMESTAMP WHERE pod_id = NEW.pod_id;
                END
            """)
            
            connection.commit()
            cursor.close()
            
            logger.debug("SQLite simulation schema created successfully")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create SQLite simulation schema: {e}")
            raise StorageError(f"Schema creation failed: {e}", e)
    
    # ========================================================================
    # Main Storage Interface Implementation (simulates SnowflakeStorage)
    # ========================================================================
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def heartbeat(self, pod_id: str, metadata: Dict[str, Any]) -> None:
        """Send heartbeat for a pod (simulates Snowflake heartbeat)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            meta_json = json.dumps(metadata)
            
            # Insert or update pod health record
            cursor.execute("""
                INSERT OR REPLACE INTO POD_HEALTH (
                    pod_id, birth_ts, last_heartbeat_ts, status, meta, updated_at
                ) VALUES (
                    ?, 
                    COALESCE((SELECT birth_ts FROM POD_HEALTH WHERE pod_id = ?), ?),
                    ?, 'up', ?, ?
                )
            """, (pod_id, pod_id, current_time, current_time, meta_json, current_time))
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Heartbeat sent for pod {pod_id}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to send heartbeat for pod {pod_id}: {e}")
            raise StorageError(f"Heartbeat failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def register_pod(self, pod_id: str, metadata: Dict[str, Any]) -> None:
        """Register a new pod (simulates Snowflake pod registration)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            meta_json = json.dumps(metadata)
            
            # Insert new pod record
            cursor.execute("""
                INSERT OR IGNORE INTO POD_HEALTH (
                    pod_id, birth_ts, last_heartbeat_ts, status, meta
                ) VALUES (?, ?, ?, 'up', ?)
            """, (pod_id, current_time, current_time, meta_json))
            
            connection.commit()
            cursor.close()
            
            logger.info(f"Pod {pod_id} registered successfully")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to register pod {pod_id}: {e}")
            raise StorageError(f"Pod registration failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def shutdown_pod(self, pod_id: str) -> None:
        """Mark pod as down (simulates Snowflake pod shutdown)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            
            cursor.execute("""
                UPDATE POD_HEALTH 
                SET status = 'down', last_heartbeat_ts = ?, updated_at = ?
                WHERE pod_id = ?
            """, (current_time, current_time, pod_id))
            
            connection.commit()
            cursor.close()
            
            logger.info(f"Pod {pod_id} marked as down")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to shutdown pod {pod_id}: {e}")
            raise StorageError(f"Pod shutdown failed: {e}", e)
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=100,
        max_delay_ms=5000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def claim_steps(self, pod_id: str, limit: int) -> List[Step]:
        """Atomically claim available steps (simulates Snowflake claiming)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            
            # Begin transaction for atomic claiming
            cursor.execute("BEGIN IMMEDIATE")
            
            # Select available steps with row locking simulation
            cursor.execute("""
                SELECT step_id, payload, priority, retry_count, max_retries, created_ts
                FROM STEP_TRACKER 
                WHERE status = 'pending'
                ORDER BY priority DESC, created_ts ASC
                LIMIT ?
            """, (limit,))
            
            rows = cursor.fetchall()
            
            if not rows:
                cursor.execute("COMMIT")
                cursor.close()
                return []
            
            step_ids = [row[0] for row in rows]
            
            # Atomically claim the selected steps
            placeholders = ','.join(['?'] * len(step_ids))
            cursor.execute(f"""
                UPDATE STEP_TRACKER 
                SET status = 'claimed', 
                    claimed_by = ?, 
                    last_update_ts = ?
                WHERE step_id IN ({placeholders})
                  AND status = 'pending'
            """, [pod_id, current_time] + step_ids)
            
            # Verify we claimed the expected number of steps
            claimed_count = cursor.rowcount
            
            cursor.execute("COMMIT")
            
            # Build Step objects for claimed steps
            claimed_steps = []
            for i, row in enumerate(rows[:claimed_count]):
                step_id, payload_json, priority, retry_count, max_retries, created_ts = row
                
                payload = VariantSerializer.deserialize_step_payload(payload_json)
                
                step = Step(
                    step_id=step_id,
                    status=StepStatus.CLAIMED,
                    claimed_by=pod_id,
                    last_update_ts=datetime.fromisoformat(current_time),
                    payload=payload,
                    retry_count=retry_count,
                    priority=priority,
                    created_ts=datetime.fromisoformat(created_ts),
                    max_retries=max_retries
                )
                claimed_steps.append(step)
            
            cursor.close()
            
            logger.debug(f"Pod {pod_id} claimed {len(claimed_steps)} steps")
            return claimed_steps
            
        except Exception as e:
            try:
                cursor.execute("ROLLBACK")
            except:
                pass
            logger.error(f"Failed to claim steps for pod {pod_id}: {e}")
            raise StorageError(f"Step claiming failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def update_step_status(
        self, 
        step_id: str, 
        status: StepStatus, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update step status (simulates Snowflake step update)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            
            cursor.execute("""
                UPDATE STEP_TRACKER 
                SET status = ?, last_update_ts = ?
                WHERE step_id = ?
            """, (status.value, current_time, step_id))
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Step {step_id} status updated to {status.value}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to update step {step_id} status: {e}")
            raise StorageError(f"Step status update failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def submit_step(self, step: Step) -> None:
        """Submit a new step (simulates Snowflake step submission)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            payload_json = VariantSerializer.serialize(step.payload)
            
            cursor.execute("""
                INSERT INTO STEP_TRACKER (
                    step_id, status, last_update_ts, payload, retry_count, 
                    priority, max_retries, created_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                step.step_id,
                step.status.value,
                current_time,
                payload_json,
                step.retry_count,
                step.priority,
                step.max_retries,
                current_time
            ))
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Step {step.step_id} submitted successfully")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to submit step {step.step_id}: {e}")
            raise StorageError(f"Step submission failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def get_step(self, step_id: str) -> Optional[Step]:
        """Get step by ID (simulates Snowflake step retrieval)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
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
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def get_dead_pods(self, heartbeat_threshold_seconds: int) -> List[str]:
        """Get list of dead pod IDs (simulates Snowflake dead pod detection)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            threshold_time = (datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)).isoformat()
            
            cursor.execute("""
                SELECT pod_id
                FROM POD_HEALTH
                WHERE status = 'up' AND last_heartbeat_ts < ?
            """, (threshold_time,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            dead_pods = [row[0] for row in rows]
            
            if dead_pods:
                # Mark dead pods as down
                await self._mark_pods_as_dead(dead_pods)
            
            return dead_pods
            
        except Exception as e:
            logger.error(f"Failed to get dead pods: {e}")
            raise StorageError(f"Dead pod detection failed: {e}", e)
    
    async def _mark_pods_as_dead(self, pod_ids: List[str]) -> None:
        """Mark pods as dead in the database."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            
            placeholders = ','.join(['?'] * len(pod_ids))
            cursor.execute(f"""
                UPDATE POD_HEALTH 
                SET status = 'down', updated_at = ?
                WHERE pod_id IN ({placeholders})
            """, [current_time] + pod_ids)
            
            connection.commit()
            cursor.close()
            
            logger.info(f"Marked {len(pod_ids)} pods as dead: {pod_ids}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to mark pods as dead: {e}")
            raise StorageError(f"Mark pods as dead failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def get_orphaned_steps(self, dead_pod_ids: List[str]) -> List[Step]:
        """Get steps claimed by dead pods (simulates Snowflake orphan detection)."""
        if not dead_pod_ids:
            return []
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            placeholders = ','.join(['?'] * len(dead_pod_ids))
            cursor.execute(f"""
                SELECT step_id, status, claimed_by, last_update_ts, payload,
                       retry_count, priority, created_ts, max_retries
                FROM STEP_TRACKER
                WHERE status = 'claimed' AND claimed_by IN ({placeholders})
            """, dead_pod_ids)
            
            rows = cursor.fetchall()
            cursor.close()
            
            orphaned_steps = []
            for row in rows:
                orphaned_steps.append(self._row_to_step(row))
            
            logger.debug(f"Found {len(orphaned_steps)} orphaned steps from dead pods")
            return orphaned_steps
            
        except Exception as e:
            logger.error(f"Failed to get orphaned steps: {e}")
            raise StorageError(f"Orphaned steps retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def requeue_steps(self, steps: List[Step]) -> None:
        """Requeue steps back to pending status (simulates Snowflake requeuing)."""
        if not steps:
            return
        
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            current_time = datetime.utcnow().isoformat()
            
            # Begin transaction for atomic requeuing
            cursor.execute("BEGIN")
            
            for step in steps:
                cursor.execute("""
                    UPDATE STEP_TRACKER 
                    SET status = 'pending', 
                        claimed_by = NULL, 
                        last_update_ts = ?,
                        retry_count = ?
                    WHERE step_id = ?
                """, (current_time, step.retry_count, step.step_id))
            
            cursor.execute("COMMIT")
            cursor.close()
            
            logger.info(f"Requeued {len(steps)} steps")
            
        except Exception as e:
            try:
                cursor.execute("ROLLBACK")
            except:
                pass
            logger.error(f"Failed to requeue steps: {e}")
            raise StorageError(f"Step requeuing failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def get_current_leader(self) -> Optional[str]:
        """Get current leader pod ID (simulates Snowflake leader election)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            threshold_time = (datetime.utcnow() - timedelta(seconds=self.config.dead_pod_threshold_seconds)).isoformat()
            
            cursor.execute("""
                SELECT pod_id
                FROM POD_HEALTH
                WHERE status = 'up' AND last_heartbeat_ts > ?
                ORDER BY birth_ts ASC
                LIMIT 1
            """, (threshold_time,))
            
            row = cursor.fetchone()
            cursor.close()
            
            return row[0] if row else None
            
        except Exception as e:
            logger.error(f"Failed to get current leader: {e}")
            raise StorageError(f"Leader detection failed: {e}", e)
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics (simulates Snowflake queue stats)."""
        connection = await self._ensure_connection()
        
        try:
            cursor = connection.cursor()
            
            # Get step counts by status
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM STEP_TRACKER
                GROUP BY status
            """)
            
            step_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get active pod count
            threshold_time = (datetime.utcnow() - timedelta(seconds=self.config.dead_pod_threshold_seconds)).isoformat()
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM POD_HEALTH
                WHERE status = 'up' AND last_heartbeat_ts > ?
            """, (threshold_time,))
            
            active_pods = cursor.fetchone()[0]
            
            cursor.close()
            
            return {
                "pending_steps": step_counts.get("pending", 0),
                "claimed_steps": step_counts.get("claimed", 0),
                "completed_steps": step_counts.get("completed", 0),
                "failed_steps": step_counts.get("failed", 0),
                "active_pods": active_pods,
                "total_steps": sum(step_counts.values())
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            raise StorageError(f"Queue stats retrieval failed: {e}", e)
    
    def _row_to_step(self, row: tuple) -> Step:
        """Convert database row to Step object."""
        (step_id, status, claimed_by, last_update_ts, payload_json,
         retry_count, priority, created_ts, max_retries) = row
        
        payload = VariantSerializer.deserialize_step_payload(payload_json)
        
        return Step(
            step_id=step_id,
            status=StepStatus(status),
            claimed_by=claimed_by,
            last_update_ts=datetime.fromisoformat(last_update_ts),
            payload=payload,
            retry_count=retry_count,
            priority=priority,
            created_ts=datetime.fromisoformat(created_ts),
            max_retries=max_retries
        )
    
    # ========================================================================
    # SQLite Storage Interface Implementation (for DLQ operations)
    # ========================================================================
    
    async def create_dlq_table(self, cycle_id: str) -> None:
        """Create DLQ table for recovery cycle."""
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    step_id TEXT PRIMARY KEY,
                    original_payload TEXT NOT NULL,
                    claimed_by TEXT NOT NULL,
                    claim_timestamp TEXT NOT NULL,
                    recovery_timestamp TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Created DLQ table: {table_name}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create DLQ table {table_name}: {e}")
            raise StorageError(f"DLQ table creation failed: {e}", e)
    
    async def drop_dlq_table(self, cycle_id: str) -> None:
        """Drop DLQ table for recovery cycle."""
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.commit()
            cursor.close()
            
            logger.debug(f"Dropped DLQ table: {table_name}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to drop DLQ table {table_name}: {e}")
            raise StorageError(f"DLQ table drop failed: {e}", e)
    
    async def insert_dlq_record(self, cycle_id: str, record: DLQRecord) -> None:
        """Insert record into DLQ table."""
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            payload_json = VariantSerializer.serialize(record.original_payload)
            
            cursor.execute(f"""
                INSERT OR REPLACE INTO {table_name} (
                    step_id, original_payload, claimed_by, claim_timestamp,
                    recovery_timestamp, reason, retry_count, max_retries, action
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.step_id,
                payload_json,
                record.claimed_by,
                record.claim_timestamp.isoformat(),
                record.recovery_timestamp.isoformat(),
                record.reason,
                record.retry_count,
                record.max_retries,
                record.action
            ))
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Inserted DLQ record for step {record.step_id}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to insert DLQ record: {e}")
            raise StorageError(f"DLQ record insertion failed: {e}", e)
    
    async def get_dlq_records(self, cycle_id: str) -> List[DLQRecord]:
        """Get all DLQ records for a cycle."""
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                cursor.close()
                return []
            
            cursor.execute(f"""
                SELECT step_id, original_payload, claimed_by, claim_timestamp,
                       recovery_timestamp, reason, retry_count, max_retries, action
                FROM {table_name}
                ORDER BY recovery_timestamp ASC
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            
            records = []
            for row in rows:
                records.append(self._row_to_dlq_record(row))
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to get DLQ records: {e}")
            raise StorageError(f"DLQ records retrieval failed: {e}", e)
    
    def _get_dlq_table_name(self, cycle_id: str) -> str:
        """Generate safe DLQ table name."""
        safe_cycle_id = ''.join(c if c.isalnum() else '_' for c in cycle_id)
        if safe_cycle_id and safe_cycle_id[0].isdigit():
            safe_cycle_id = 'c_' + safe_cycle_id
        return f"dlq_orphans_{safe_cycle_id}"
    
    def _row_to_dlq_record(self, row: tuple) -> DLQRecord:
        """Convert database row to DLQRecord."""
        (step_id, original_payload_json, claimed_by, claim_timestamp_str,
         recovery_timestamp_str, reason, retry_count, max_retries, action) = row
        
        original_payload = VariantSerializer.deserialize_step_payload(original_payload_json)
        claim_timestamp = datetime.fromisoformat(claim_timestamp_str)
        recovery_timestamp = datetime.fromisoformat(recovery_timestamp_str)
        
        return DLQRecord(
            step_id=step_id,
            original_payload=original_payload,
            claimed_by=claimed_by,
            claim_timestamp=claim_timestamp,
            recovery_timestamp=recovery_timestamp,
            reason=reason,
            retry_count=retry_count,
            max_retries=max_retries,
            action=action
        )