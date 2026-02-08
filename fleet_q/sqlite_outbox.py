"""
SQLite Outbox Pattern for FLEET-Q In-Pod Execution Fabric

Provides durable boundary for side effects:
- Step updates (status transitions, retries)
- Results (final payloads to persist)
- SharePoint operations (download/upload intents)
- Pressure state (AIMD shared memory)
- Lease management (singleton runner election)

Why Outbox Pattern:
- ZeroMQ is fast but not durable
- Bounded ZMQ queues (HWM) + outbox absorbs bursts
- Separate flushers batch writes efficiently
- Idempotent and replayable within pod lifetime
- Centralized external client management

Tables:
- outbox_step_updates: Status transitions, retries, metadata
- outbox_results: Final payloads to write to Snowflake
- outbox_sharepoint_ops: Download/upload requests
- pressure_state: AIMD configuration (optional)
- control_plane_lease: Singleton runner election
"""

import sqlite3
import json
import time
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime, timedelta
from contextlib import contextmanager
import threading

logger = logging.getLogger(__name__)


# ============================================================================
# Enums and Data Classes
# ============================================================================

class OutboxStatus(str, Enum):
    """Status of outbox entries"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class SharePointOpType(str, Enum):
    """SharePoint operation types"""
    DOWNLOAD = "download"
    UPLOAD = "upload"
    DELETE = "delete"
    LIST = "list"


@dataclass
class StepUpdate:
    """Step update intent for outbox"""
    step_id: str
    status: str
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: Optional[Dict[str, Any]] = None
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class ResultIntent:
    """Result write intent for outbox"""
    step_id: str
    table_name: str
    record_data: Dict[str, Any]
    partition_key: Optional[str] = None
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class SharePointIntent:
    """SharePoint operation intent"""
    operation_id: str
    op_type: SharePointOpType
    site_url: str
    file_path: str
    local_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class PressureState:
    """AIMD pressure state"""
    max_inflight: int
    current_inflight: int
    success_streak: int
    last_throttle_time: float
    updated_at: float


@dataclass
class ControlPlaneLease:
    """Control plane lease for singleton runner"""
    lease_holder: str
    acquired_at: float
    expires_at: float
    heartbeat_at: float
    pod_id: str
    process_id: int


# ============================================================================
# SQLite Outbox Manager
# ============================================================================

class SQLiteOutbox:
    """
    SQLite-based outbox for durable side effect intents.
    
    Thread-safe with connection pooling per thread.
    WAL mode for concurrent reads/writes.
    """
    
    def __init__(self, db_path: str = "/tmp/fleetq_outbox.db", wal_mode: bool = True):
        """
        Initialize outbox database.
        
        Args:
            db_path: Path to SQLite database file
            wal_mode: Enable WAL mode for concurrency (recommended)
        """
        self.db_path = db_path
        self.wal_mode = wal_mode
        self._local = threading.local()
        
        # Initialize schema
        self._init_schema()
        
        logger.info(f"SQLiteOutbox initialized at {db_path} (WAL={wal_mode})")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection"""
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for this connection
            if self.wal_mode:
                self._local.conn.execute("PRAGMA journal_mode=WAL")
            
            # Enable foreign keys
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        
        return self._local.conn
    
    @contextmanager
    def _transaction(self):
        """Context manager for transactions"""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
    
    def _init_schema(self):
        """Initialize database schema"""
        with self._transaction() as conn:
            # Step updates table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox_step_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    step_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    metadata TEXT,
                    created_at REAL NOT NULL,
                    outbox_status TEXT DEFAULT 'pending',
                    processed_at REAL,
                    INDEX idx_outbox_status (outbox_status),
                    INDEX idx_step_id (step_id),
                    INDEX idx_created_at (created_at)
                )
            """)
            
            # Results table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    step_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_data TEXT NOT NULL,
                    partition_key TEXT,
                    created_at REAL NOT NULL,
                    outbox_status TEXT DEFAULT 'pending',
                    processed_at REAL,
                    error_message TEXT,
                    INDEX idx_outbox_status (outbox_status),
                    INDEX idx_step_id (step_id),
                    INDEX idx_table_name (table_name)
                )
            """)
            
            # SharePoint operations table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox_sharepoint_ops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT UNIQUE NOT NULL,
                    op_type TEXT NOT NULL,
                    site_url TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    local_path TEXT,
                    metadata TEXT,
                    created_at REAL NOT NULL,
                    outbox_status TEXT DEFAULT 'pending',
                    processed_at REAL,
                    error_message TEXT,
                    result_data TEXT,
                    INDEX idx_outbox_status (outbox_status),
                    INDEX idx_op_type (op_type),
                    INDEX idx_operation_id (operation_id)
                )
            """)
            
            # Pressure state table (singleton)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pressure_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    max_inflight INTEGER NOT NULL,
                    current_inflight INTEGER NOT NULL,
                    success_streak INTEGER DEFAULT 0,
                    last_throttle_time REAL,
                    updated_at REAL NOT NULL
                )
            """)
            
            # Control plane lease table (singleton)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS control_plane_lease (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    lease_holder TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    heartbeat_at REAL NOT NULL,
                    pod_id TEXT NOT NULL,
                    process_id INTEGER NOT NULL
                )
            """)
            
            logger.info("Outbox schema initialized")
    
    # ========================================================================
    # Step Updates
    # ========================================================================
    
    def enqueue_step_update(self, update: StepUpdate) -> int:
        """
        Enqueue step update intent.
        
        Args:
            update: StepUpdate dataclass
        
        Returns:
            Row ID of inserted record
        """
        with self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO outbox_step_updates 
                (step_id, status, error_message, retry_count, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                update.step_id,
                update.status,
                update.error_message,
                update.retry_count,
                json.dumps(update.metadata) if update.metadata else None,
                update.created_at
            ))
            
            row_id = cursor.lastrowid
            logger.debug(f"Enqueued step update: {update.step_id} -> {update.status}")
            return row_id
    
    def get_pending_step_updates(self, limit: int = 100) -> List[Tuple[int, StepUpdate]]:
        """
        Get pending step updates for flushing.
        
        Args:
            limit: Max number of records to fetch
        
        Returns:
            List of (row_id, StepUpdate) tuples
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT id, step_id, status, error_message, retry_count, metadata, created_at
            FROM outbox_step_updates
            WHERE outbox_status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        
        results = []
        for row in rows:
            update = StepUpdate(
                step_id=row['step_id'],
                status=row['status'],
                error_message=row['error_message'],
                retry_count=row['retry_count'],
                metadata=json.loads(row['metadata']) if row['metadata'] else None,
                created_at=row['created_at']
            )
            results.append((row['id'], update))
        
        return results
    
    def mark_step_update_processed(self, row_id: int, status: OutboxStatus = OutboxStatus.COMPLETED, 
                                   error: Optional[str] = None):
        """Mark step update as processed"""
        with self._transaction() as conn:
            conn.execute("""
                UPDATE outbox_step_updates
                SET outbox_status = ?, processed_at = ?, error_message = ?
                WHERE id = ?
            """, (status.value, time.time(), error, row_id))
    
    # ========================================================================
    # Results
    # ========================================================================
    
    def enqueue_result(self, result: ResultIntent) -> int:
        """
        Enqueue result write intent.
        
        Args:
            result: ResultIntent dataclass
        
        Returns:
            Row ID of inserted record
        """
        with self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO outbox_results
                (step_id, table_name, record_data, partition_key, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                result.step_id,
                result.table_name,
                json.dumps(result.record_data),
                result.partition_key,
                result.created_at
            ))
            
            row_id = cursor.lastrowid
            logger.debug(f"Enqueued result: {result.step_id} -> {result.table_name}")
            return row_id
    
    def get_pending_results(self, limit: int = 100) -> List[Tuple[int, ResultIntent]]:
        """Get pending results for flushing"""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT id, step_id, table_name, record_data, partition_key, created_at
            FROM outbox_results
            WHERE outbox_status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        
        results = []
        for row in rows:
            intent = ResultIntent(
                step_id=row['step_id'],
                table_name=row['table_name'],
                record_data=json.loads(row['record_data']),
                partition_key=row['partition_key'],
                created_at=row['created_at']
            )
            results.append((row['id'], intent))
        
        return results
    
    def mark_result_processed(self, row_id: int, status: OutboxStatus = OutboxStatus.COMPLETED,
                             error: Optional[str] = None):
        """Mark result as processed"""
        with self._transaction() as conn:
            conn.execute("""
                UPDATE outbox_results
                SET outbox_status = ?, processed_at = ?, error_message = ?
                WHERE id = ?
            """, (status.value, time.time(), error, row_id))
    
    # ========================================================================
    # SharePoint Operations
    # ========================================================================
    
    def enqueue_sharepoint_op(self, intent: SharePointIntent) -> int:
        """Enqueue SharePoint operation intent"""
        with self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO outbox_sharepoint_ops
                (operation_id, op_type, site_url, file_path, local_path, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                intent.operation_id,
                intent.op_type.value,
                intent.site_url,
                intent.file_path,
                intent.local_path,
                json.dumps(intent.metadata) if intent.metadata else None,
                intent.created_at
            ))
            
            row_id = cursor.lastrowid
            logger.debug(f"Enqueued SharePoint op: {intent.operation_id} ({intent.op_type})")
            return row_id
    
    def get_pending_sharepoint_ops(self, limit: int = 50) -> List[Tuple[int, SharePointIntent]]:
        """Get pending SharePoint operations"""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT id, operation_id, op_type, site_url, file_path, local_path, metadata, created_at
            FROM outbox_sharepoint_ops
            WHERE outbox_status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        
        results = []
        for row in rows:
            intent = SharePointIntent(
                operation_id=row['operation_id'],
                op_type=SharePointOpType(row['op_type']),
                site_url=row['site_url'],
                file_path=row['file_path'],
                local_path=row['local_path'],
                metadata=json.loads(row['metadata']) if row['metadata'] else None,
                created_at=row['created_at']
            )
            results.append((row['id'], intent))
        
        return results
    
    def mark_sharepoint_op_processed(self, row_id: int, status: OutboxStatus = OutboxStatus.COMPLETED,
                                    error: Optional[str] = None, result_data: Optional[Dict] = None):
        """Mark SharePoint operation as processed"""
        with self._transaction() as conn:
            conn.execute("""
                UPDATE outbox_sharepoint_ops
                SET outbox_status = ?, processed_at = ?, error_message = ?, result_data = ?
                WHERE id = ?
            """, (
                status.value,
                time.time(),
                error,
                json.dumps(result_data) if result_data else None,
                row_id
            ))
    
    # ========================================================================
    # Pressure State (AIMD)
    # ========================================================================
    
    def get_pressure_state(self) -> Optional[PressureState]:
        """Get current pressure state (singleton)"""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT max_inflight, current_inflight, success_streak, last_throttle_time, updated_at
            FROM pressure_state
            WHERE id = 1
        """).fetchone()
        
        if not row:
            return None
        
        return PressureState(
            max_inflight=row['max_inflight'],
            current_inflight=row['current_inflight'],
            success_streak=row['success_streak'],
            last_throttle_time=row['last_throttle_time'],
            updated_at=row['updated_at']
        )
    
    def update_pressure_state(self, state: PressureState):
        """Update or insert pressure state"""
        with self._transaction() as conn:
            conn.execute("""
                INSERT INTO pressure_state (id, max_inflight, current_inflight, success_streak, 
                                           last_throttle_time, updated_at)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    max_inflight = excluded.max_inflight,
                    current_inflight = excluded.current_inflight,
                    success_streak = excluded.success_streak,
                    last_throttle_time = excluded.last_throttle_time,
                    updated_at = excluded.updated_at
            """, (
                state.max_inflight,
                state.current_inflight,
                state.success_streak,
                state.last_throttle_time,
                state.updated_at
            ))
    
    # ========================================================================
    # Control Plane Lease
    # ========================================================================
    
    def try_acquire_lease(self, lease_holder: str, pod_id: str, process_id: int, 
                         ttl_seconds: int = 30) -> bool:
        """
        Try to acquire control plane lease.
        
        Args:
            lease_holder: Identity of lease holder
            pod_id: Pod ID
            process_id: Process ID
            ttl_seconds: Lease TTL
        
        Returns:
            True if lease acquired, False otherwise
        """
        now = time.time()
        expires_at = now + ttl_seconds
        
        with self._transaction() as conn:
            # Check if lease exists and is expired
            row = conn.execute("""
                SELECT expires_at FROM control_plane_lease WHERE id = 1
            """).fetchone()
            
            if row and row['expires_at'] > now:
                # Lease is held by someone else
                return False
            
            # Acquire or renew lease
            conn.execute("""
                INSERT INTO control_plane_lease 
                (id, lease_holder, acquired_at, expires_at, heartbeat_at, pod_id, process_id)
                VALUES (1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    lease_holder = excluded.lease_holder,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at,
                    heartbeat_at = excluded.heartbeat_at,
                    pod_id = excluded.pod_id,
                    process_id = excluded.process_id
            """, (lease_holder, now, expires_at, now, pod_id, process_id))
            
            logger.info(f"Lease acquired by {lease_holder} (pod={pod_id}, pid={process_id})")
            return True
    
    def renew_lease(self, lease_holder: str, ttl_seconds: int = 30) -> bool:
        """
        Renew lease if currently held.
        
        Args:
            lease_holder: Identity of lease holder
            ttl_seconds: Lease TTL
        
        Returns:
            True if renewed, False if not held
        """
        now = time.time()
        expires_at = now + ttl_seconds
        
        with self._transaction() as conn:
            cursor = conn.execute("""
                UPDATE control_plane_lease
                SET expires_at = ?, heartbeat_at = ?
                WHERE id = 1 AND lease_holder = ?
            """, (expires_at, now, lease_holder))
            
            if cursor.rowcount > 0:
                logger.debug(f"Lease renewed by {lease_holder}")
                return True
            else:
                logger.warning(f"Failed to renew lease for {lease_holder}")
                return False
    
    def release_lease(self, lease_holder: str) -> bool:
        """
        Release lease if held.
        
        Args:
            lease_holder: Identity of lease holder
        
        Returns:
            True if released, False if not held
        """
        with self._transaction() as conn:
            cursor = conn.execute("""
                DELETE FROM control_plane_lease
                WHERE id = 1 AND lease_holder = ?
            """, (lease_holder,))
            
            if cursor.rowcount > 0:
                logger.info(f"Lease released by {lease_holder}")
                return True
            else:
                logger.warning(f"Failed to release lease for {lease_holder}")
                return False
    
    def get_current_lease(self) -> Optional[ControlPlaneLease]:
        """Get current lease holder info"""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT lease_holder, acquired_at, expires_at, heartbeat_at, pod_id, process_id
            FROM control_plane_lease
            WHERE id = 1
        """).fetchone()
        
        if not row:
            return None
        
        return ControlPlaneLease(
            lease_holder=row['lease_holder'],
            acquired_at=row['acquired_at'],
            expires_at=row['expires_at'],
            heartbeat_at=row['heartbeat_at'],
            pod_id=row['pod_id'],
            process_id=row['process_id']
        )
    
    def is_lease_holder(self, lease_holder: str) -> bool:
        """Check if given identity holds the lease"""
        lease = self.get_current_lease()
        if not lease:
            return False
        
        now = time.time()
        return lease.lease_holder == lease_holder and lease.expires_at > now
    
    # ========================================================================
    # Cleanup and Maintenance
    # ========================================================================
    
    def cleanup_old_records(self, retention_hours: int = 24):
        """
        Clean up old completed/failed records.
        
        Args:
            retention_hours: Keep records for this many hours
        """
        cutoff_time = time.time() - (retention_hours * 3600)
        
        with self._transaction() as conn:
            # Clean step updates
            cursor = conn.execute("""
                DELETE FROM outbox_step_updates
                WHERE outbox_status IN ('completed', 'failed')
                AND processed_at < ?
            """, (cutoff_time,))
            deleted_updates = cursor.rowcount
            
            # Clean results
            cursor = conn.execute("""
                DELETE FROM outbox_results
                WHERE outbox_status IN ('completed', 'failed')
                AND processed_at < ?
            """, (cutoff_time,))
            deleted_results = cursor.rowcount
            
            # Clean SharePoint ops
            cursor = conn.execute("""
                DELETE FROM outbox_sharepoint_ops
                WHERE outbox_status IN ('completed', 'failed')
                AND processed_at < ?
            """, (cutoff_time,))
            deleted_ops = cursor.rowcount
            
            logger.info(f"Cleanup: deleted {deleted_updates} updates, {deleted_results} results, "
                       f"{deleted_ops} SharePoint ops (older than {retention_hours}h)")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get outbox statistics"""
        conn = self._get_connection()
        
        stats = {}
        
        # Step updates
        row = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN outbox_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN outbox_status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN outbox_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN outbox_status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM outbox_step_updates
        """).fetchone()
        stats['step_updates'] = dict(row)
        
        # Results
        row = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN outbox_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN outbox_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN outbox_status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM outbox_results
        """).fetchone()
        stats['results'] = dict(row)
        
        # SharePoint ops
        row = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN outbox_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN outbox_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN outbox_status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM outbox_sharepoint_ops
        """).fetchone()
        stats['sharepoint_ops'] = dict(row)
        
        # Pressure state
        stats['pressure_state'] = self.get_pressure_state()
        
        # Lease info
        stats['lease'] = self.get_current_lease()
        
        return stats
    
    def close(self):
        """Close all connections"""
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            logger.info("Outbox connections closed")


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    """Example usage of SQLite outbox"""
    
    # Initialize outbox
    outbox = SQLiteOutbox("/tmp/test_outbox.db")
    
    # Enqueue step update
    update = StepUpdate(
        step_id="step-123",
        status="completed",
        metadata={"duration": 5.2}
    )
    outbox.enqueue_step_update(update)
    
    # Enqueue result
    result = ResultIntent(
        step_id="step-123",
        table_name="RESULTS",
        record_data={"output": "success", "score": 0.95}
    )
    outbox.enqueue_result(result)
    
    # Get pending items
    pending_updates = outbox.get_pending_step_updates(limit=10)
    print(f"Pending updates: {len(pending_updates)}")
    
    for row_id, update in pending_updates:
        print(f"  {row_id}: {update.step_id} -> {update.status}")
        outbox.mark_step_update_processed(row_id)
    
    # Lease management
    acquired = outbox.try_acquire_lease("worker-1", "pod-abc", 12345)
    print(f"Lease acquired: {acquired}")
    
    if acquired:
        print(f"Is lease holder: {outbox.is_lease_holder('worker-1')}")
        outbox.renew_lease("worker-1")
        outbox.release_lease("worker-1")
    
    # Get stats
    stats = outbox.get_stats()
    print(f"\nOutbox stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    outbox.close()
