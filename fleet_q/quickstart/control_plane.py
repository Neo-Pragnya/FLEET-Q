"""
FLEET-Q Control Plane Worker

This module implements a control plane worker that runs within the FastAPI application
and handles bulk read/write operations with dynamic scaling:

- Bulk batching with 15-30 second pooling windows
- ORM-agnostic batching (groups by destination/table)
- Dynamic writer scaling based on queue depth
- Support for multiple destinations (Snowflake, SharePoint, Bedrock, etc.)
- Pod ID-based SQLite database paths
- Automatic flushing and maintenance

Architecture:
    FastAPI (uvicorn worker) → Control Plane Worker → Bulk Writers
                                     ↓
                              Write Buffers (by ORM/destination)
                                     ↓
                              Dynamic Writer Pool
"""

import asyncio
import logging
import os
import sqlite3
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from queue import Queue
import threading
import json

logger = logging.getLogger(__name__)


# ============================================================================
# Writer Types and Destinations
# ============================================================================

class WriterType(Enum):
    """Types of bulk writers supported"""
    SNOWFLAKE = "snowflake"
    SHAREPOINT = "sharepoint"
    BEDROCK = "bedrock"
    S3 = "s3"
    LOCAL_DB = "local_db"


@dataclass
class WriteOperation:
    """A single write operation to be batched"""
    operation_id: str
    writer_type: WriterType
    destination: str  # table name, bucket name, etc.
    data: Dict[str, Any]
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    orm_type: Optional[str] = None  # For ORM-agnostic batching


@dataclass
class WriteBatch:
    """Batch of write operations for a specific destination"""
    batch_id: str
    writer_type: WriterType
    destination: str
    orm_type: Optional[str]
    operations: List[WriteOperation]
    created_at: float = field(default_factory=time.time)
    
    def size(self) -> int:
        return len(self.operations)
    
    def age_seconds(self) -> float:
        return time.time() - self.created_at


@dataclass
class WriterPoolStats:
    """Statistics for writer pool"""
    active_writers: int
    pending_operations: int
    buffer_size_by_destination: Dict[str, int]
    operations_completed: int
    operations_failed: int
    last_flush_time: float
    average_batch_size: float


# ============================================================================
# Pod-Aware SQLite Storage
# ============================================================================

class PodLocalStorage:
    """
    Pod-aware SQLite storage with automatic path management.
    
    Creates isolated databases per pod:
    - Path: /tmp/fleetq/{pod_id}/local.db
    - Automatic directory creation
    - Periodic flush and maintenance
    """
    
    def __init__(self, pod_id: str, base_path: str = "/tmp/fleetq"):
        self.pod_id = pod_id
        self.base_path = Path(base_path)
        self.pod_path = self.base_path / pod_id
        self.db_path = self.pod_path / "local.db"
        
        # Ensure directories exist
        self.pod_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized PodLocalStorage for pod {pod_id}")
        logger.info(f"Database path: {self.db_path}")
        
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        with self._get_connection() as conn:
            # Write buffer table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS write_buffer (
                    operation_id TEXT PRIMARY KEY,
                    writer_type TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    orm_type TEXT,
                    data TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    status TEXT DEFAULT 'pending'
                )
            """)
            
            # Write batch history
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_history (
                    batch_id TEXT PRIMARY KEY,
                    writer_type TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    orm_type TEXT,
                    operation_count INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    completed_at REAL,
                    status TEXT DEFAULT 'pending',
                    error TEXT
                )
            """)
            
            # Maintenance log
            conn.execute("""
                CREATE TABLE IF NOT EXISTS maintenance_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    details TEXT
                )
            """)
            
            conn.commit()
            logger.info("Database schema initialized")
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def add_operation(self, operation: WriteOperation):
        """Add operation to write buffer"""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO write_buffer 
                (operation_id, writer_type, destination, orm_type, data, priority, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                operation.operation_id,
                operation.writer_type.value,
                operation.destination,
                operation.orm_type,
                json.dumps(operation.data),
                operation.priority,
                operation.timestamp
            ))
            conn.commit()
    
    def get_pending_operations(
        self, 
        writer_type: Optional[WriterType] = None,
        destination: Optional[str] = None,
        orm_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[WriteOperation]:
        """Get pending operations from buffer"""
        with self._get_connection() as conn:
            query = "SELECT * FROM write_buffer WHERE status = 'pending'"
            params = []
            
            if writer_type:
                query += " AND writer_type = ?"
                params.append(writer_type.value)
            
            if destination:
                query += " AND destination = ?"
                params.append(destination)
            
            if orm_type:
                query += " AND orm_type = ?"
                params.append(orm_type)
            
            query += " ORDER BY priority DESC, created_at ASC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            
            return [
                WriteOperation(
                    operation_id=row['operation_id'],
                    writer_type=WriterType(row['writer_type']),
                    destination=row['destination'],
                    data=json.loads(row['data']),
                    priority=row['priority'],
                    timestamp=row['created_at'],
                    orm_type=row['orm_type']
                )
                for row in rows
            ]
    
    def mark_operations_completed(self, operation_ids: List[str]):
        """Mark operations as completed"""
        with self._get_connection() as conn:
            placeholders = ','.join('?' * len(operation_ids))
            conn.execute(
                f"UPDATE write_buffer SET status = 'completed' WHERE operation_id IN ({placeholders})",
                operation_ids
            )
            conn.commit()
    
    def record_batch(self, batch: WriteBatch, status: str = 'completed', error: Optional[str] = None):
        """Record batch execution in history"""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO batch_history 
                (batch_id, writer_type, destination, orm_type, operation_count, 
                 created_at, completed_at, status, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch.batch_id,
                batch.writer_type.value,
                batch.destination,
                batch.orm_type,
                len(batch.operations),
                batch.created_at,
                time.time(),
                status,
                error
            ))
            conn.commit()
    
    def cleanup_old_records(self, max_age_seconds: float = 86400):
        """Clean up old completed records"""
        cutoff_time = time.time() - max_age_seconds
        
        with self._get_connection() as conn:
            # Delete old completed operations
            result = conn.execute(
                "DELETE FROM write_buffer WHERE status = 'completed' AND created_at < ?",
                (cutoff_time,)
            )
            deleted_ops = result.rowcount
            
            # Delete old batch history
            result = conn.execute(
                "DELETE FROM batch_history WHERE completed_at < ?",
                (cutoff_time,)
            )
            deleted_batches = result.rowcount
            
            # Log maintenance action
            conn.execute("""
                INSERT INTO maintenance_log (action, timestamp, details)
                VALUES (?, ?, ?)
            """, (
                'cleanup',
                time.time(),
                json.dumps({
                    'deleted_operations': deleted_ops,
                    'deleted_batches': deleted_batches,
                    'max_age_seconds': max_age_seconds
                })
            ))
            
            conn.commit()
            logger.info(f"Cleaned up {deleted_ops} operations, {deleted_batches} batches")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        with self._get_connection() as conn:
            # Count by status
            cursor = conn.execute("""
                SELECT status, COUNT(*) as count 
                FROM write_buffer 
                GROUP BY status
            """)
            status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
            
            # Count by destination
            cursor = conn.execute("""
                SELECT destination, COUNT(*) as count 
                FROM write_buffer 
                WHERE status = 'pending'
                GROUP BY destination
            """)
            destination_counts = {row['destination']: row['count'] for row in cursor.fetchall()}
            
            # Database file size
            db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            return {
                'pending_operations': status_counts.get('pending', 0),
                'completed_operations': status_counts.get('completed', 0),
                'destination_counts': destination_counts,
                'database_size_bytes': db_size,
                'database_path': str(self.db_path)
            }


# ============================================================================
# Write Buffer Manager
# ============================================================================

class WriteBufferManager:
    """
    Manages write buffers with ORM-agnostic batching.
    
    Pools operations for 15-30 seconds before flushing to writers.
    Groups by (writer_type, destination, orm_type).
    """
    
    def __init__(
        self,
        storage: PodLocalStorage,
        flush_interval: float = 20.0,  # Configurable: 15-30 seconds
        max_batch_size: int = 1000
    ):
        self.storage = storage
        self.flush_interval = flush_interval
        self.max_batch_size = max_batch_size
        
        # In-memory buffers grouped by (writer_type, destination, orm_type)
        self.buffers: Dict[tuple, List[WriteOperation]] = defaultdict(list)
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        
        logger.info(f"WriteBufferManager initialized (flush_interval={flush_interval}s)")
    
    def add_operation(self, operation: WriteOperation):
        """Add operation to buffer"""
        # Persist to SQLite first
        self.storage.add_operation(operation)
        
        # Add to in-memory buffer
        buffer_key = (
            operation.writer_type,
            operation.destination,
            operation.orm_type or "default"
        )
        
        with self.buffer_lock:
            self.buffers[buffer_key].append(operation)
            
        logger.debug(
            f"Added operation {operation.operation_id} to buffer "
            f"{buffer_key} (size={len(self.buffers[buffer_key])})"
        )
    
    def should_flush(self) -> bool:
        """Check if any buffer should be flushed"""
        elapsed = time.time() - self.last_flush_time
        
        # Flush if interval passed
        if elapsed >= self.flush_interval:
            return True
        
        # Flush if any buffer is full
        with self.buffer_lock:
            for buffer in self.buffers.values():
                if len(buffer) >= self.max_batch_size:
                    return True
        
        return False
    
    def get_batches_to_flush(self) -> List[WriteBatch]:
        """Get batches ready to flush"""
        batches = []
        
        with self.buffer_lock:
            for (writer_type, destination, orm_type), operations in self.buffers.items():
                if operations:
                    batch_id = f"batch_{int(time.time())}_{writer_type.value}_{destination}"
                    batch = WriteBatch(
                        batch_id=batch_id,
                        writer_type=writer_type,
                        destination=destination,
                        orm_type=orm_type if orm_type != "default" else None,
                        operations=operations.copy()
                    )
                    batches.append(batch)
            
            # Clear buffers
            self.buffers.clear()
            self.last_flush_time = time.time()
        
        return batches
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        with self.buffer_lock:
            return {
                'total_buffered': sum(len(ops) for ops in self.buffers.values()),
                'buffer_count': len(self.buffers),
                'buffers': {
                    f"{writer_type.value}:{destination}:{orm_type}": len(ops)
                    for (writer_type, destination, orm_type), ops in self.buffers.items()
                },
                'last_flush_ago': time.time() - self.last_flush_time
            }


# ============================================================================
# Bulk Writer Implementations
# ============================================================================

class BulkWriter:
    """Base class for bulk writers"""
    
    def __init__(self, writer_type: WriterType):
        self.writer_type = writer_type
        self.operations_completed = 0
        self.operations_failed = 0
    
    async def write_batch(self, batch: WriteBatch) -> bool:
        """Write a batch of operations. To be implemented by subclasses."""
        raise NotImplementedError


class SnowflakeBulkWriter(BulkWriter):
    """Bulk writer for Snowflake"""
    
    def __init__(self, storage_conn):
        super().__init__(WriterType.SNOWFLAKE)
        self.storage = storage_conn
    
    async def write_batch(self, batch: WriteBatch) -> bool:
        """Write batch to Snowflake"""
        try:
            logger.info(
                f"Writing batch {batch.batch_id} to Snowflake "
                f"(destination={batch.destination}, size={batch.size()})"
            )
            
            # Group operations by insert/update/delete
            inserts = []
            for op in batch.operations:
                inserts.append(op.data)
            
            if inserts:
                # Build bulk insert query
                # For ORM-specific handling, check batch.orm_type
                table = batch.destination
                
                if batch.orm_type:
                    logger.info(f"Using ORM type: {batch.orm_type}")
                
                # Example: bulk insert (adapt to your schema)
                if self.storage:
                    # Use existing storage connection
                    # Adapt this to your actual Snowflake storage API
                    query = f"INSERT INTO {table} (data) SELECT PARSE_JSON(?) FROM TABLE(FLATTEN(input => PARSE_JSON(?)))"
                    
                    # In practice, use executemany or COPY INTO for best performance
                    logger.info(f"Bulk inserting {len(inserts)} records into {table}")
                    # self.storage.execute_many(query, inserts)
                
                # Simulate write
                await asyncio.sleep(0.1)
            
            self.operations_completed += batch.size()
            logger.info(f"Successfully wrote batch {batch.batch_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write batch {batch.batch_id}: {e}", exc_info=True)
            self.operations_failed += batch.size()
            return False


class SharePointBulkWriter(BulkWriter):
    """Bulk writer for SharePoint"""
    
    def __init__(self):
        super().__init__(WriterType.SHAREPOINT)
    
    async def write_batch(self, batch: WriteBatch) -> bool:
        """Write batch to SharePoint"""
        try:
            logger.info(
                f"Writing batch {batch.batch_id} to SharePoint "
                f"(destination={batch.destination}, size={batch.size()})"
            )
            
            # Implement SharePoint bulk upload logic
            # This would use SharePoint API/SDK
            
            await asyncio.sleep(0.2)  # Simulate network call
            
            self.operations_completed += batch.size()
            return True
            
        except Exception as e:
            logger.error(f"Failed to write batch {batch.batch_id}: {e}", exc_info=True)
            self.operations_failed += batch.size()
            return False


class BedrockBulkWriter(BulkWriter):
    """Bulk writer for Bedrock API (batch inference)"""
    
    def __init__(self):
        super().__init__(WriterType.BEDROCK)
    
    async def write_batch(self, batch: WriteBatch) -> bool:
        """Process batch through Bedrock"""
        try:
            logger.info(
                f"Processing batch {batch.batch_id} through Bedrock "
                f"(size={batch.size()})"
            )
            
            # Implement Bedrock batch inference logic
            # This would use boto3 bedrock-runtime API
            
            await asyncio.sleep(0.5)  # Simulate API call
            
            self.operations_completed += batch.size()
            return True
            
        except Exception as e:
            logger.error(f"Failed to process batch {batch.batch_id}: {e}", exc_info=True)
            self.operations_failed += batch.size()
            return False


# ============================================================================
# Dynamic Writer Pool
# ============================================================================

class DynamicWriterPool:
    """
    Pool of bulk writers that scales up/down based on queue depth.
    
    Scaling policy:
    - queue_depth < 100: 1 writer
    - queue_depth 100-500: 2-3 writers
    - queue_depth 500-1000: 4-5 writers
    - queue_depth > 1000: 6-8 writers (max)
    """
    
    def __init__(
        self,
        min_writers: int = 1,
        max_writers: int = 8,
        scale_up_threshold: int = 100,
        scale_down_threshold: int = 50
    ):
        self.min_writers = min_writers
        self.max_writers = max_writers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        
        # Writer instances by type
        self.snowflake_writers: List[SnowflakeBulkWriter] = []
        self.sharepoint_writers: List[SharePointBulkWriter] = []
        self.bedrock_writers: List[BedrockBulkWriter] = []
        
        # Active writer threads
        self.active_workers: Set[asyncio.Task] = set()
        
        # Queue for batches
        self.batch_queue: asyncio.Queue = asyncio.Queue()
        
        logger.info(
            f"DynamicWriterPool initialized "
            f"(min={min_writers}, max={max_writers})"
        )
    
    def get_desired_writer_count(self) -> int:
        """Calculate desired number of writers based on queue depth"""
        queue_depth = self.batch_queue.qsize()
        
        if queue_depth < self.scale_down_threshold:
            return self.min_writers
        elif queue_depth < 100:
            return 1
        elif queue_depth < 500:
            return min(3, self.max_writers)
        elif queue_depth < 1000:
            return min(5, self.max_writers)
        else:
            return self.max_writers
    
    async def submit_batch(self, batch: WriteBatch):
        """Submit batch to writer pool"""
        await self.batch_queue.put(batch)
        logger.debug(f"Batch {batch.batch_id} queued (queue_size={self.batch_queue.qsize()})")
    
    async def writer_worker(self, worker_id: int, storage_conn=None):
        """Worker that processes batches"""
        logger.info(f"Writer worker {worker_id} started")
        
        # Create writer instances
        writers = {
            WriterType.SNOWFLAKE: SnowflakeBulkWriter(storage_conn),
            WriterType.SHAREPOINT: SharePointBulkWriter(),
            WriterType.BEDROCK: BedrockBulkWriter()
        }
        
        while True:
            try:
                # Get batch from queue with timeout
                try:
                    batch = await asyncio.wait_for(
                        self.batch_queue.get(),
                        timeout=30.0
                    )
                except asyncio.TimeoutError:
                    # No work for 30 seconds, check if we should scale down
                    if len(self.active_workers) > self.min_writers:
                        logger.info(f"Worker {worker_id} scaling down due to low load")
                        break
                    continue
                
                # Process batch with appropriate writer
                writer = writers.get(batch.writer_type)
                if writer:
                    success = await writer.write_batch(batch)
                    
                    if success:
                        logger.info(f"Worker {worker_id} completed batch {batch.batch_id}")
                    else:
                        logger.error(f"Worker {worker_id} failed batch {batch.batch_id}")
                else:
                    logger.error(f"No writer available for type {batch.writer_type}")
                
                self.batch_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info(f"Writer worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Writer worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        logger.info(f"Writer worker {worker_id} stopped")
    
    async def scale_workers(self, storage_conn=None):
        """Dynamically scale number of active workers"""
        desired_count = self.get_desired_writer_count()
        current_count = len(self.active_workers)
        
        if desired_count > current_count:
            # Scale up
            for i in range(current_count, desired_count):
                task = asyncio.create_task(
                    self.writer_worker(i, storage_conn)
                )
                self.active_workers.add(task)
                logger.info(f"Scaled up: added writer {i} (total={len(self.active_workers)})")
        
        elif desired_count < current_count:
            # Scale down by cancelling excess workers
            workers_to_remove = current_count - desired_count
            for _ in range(workers_to_remove):
                if self.active_workers:
                    task = self.active_workers.pop()
                    task.cancel()
                    logger.info(f"Scaled down: removed 1 writer (total={len(self.active_workers)})")
    
    async def start(self, storage_conn=None):
        """Start minimum number of workers"""
        for i in range(self.min_writers):
            task = asyncio.create_task(self.writer_worker(i, storage_conn))
            self.active_workers.add(task)
        
        logger.info(f"Started {self.min_writers} initial workers")
    
    async def stop(self):
        """Stop all workers"""
        for task in self.active_workers:
            task.cancel()
        
        if self.active_workers:
            await asyncio.gather(*self.active_workers, return_exceptions=True)
        
        logger.info("All writer workers stopped")


# ============================================================================
# Control Plane Worker
# ============================================================================

class ControlPlaneWorker:
    """
    Main control plane worker that coordinates bulk operations.
    
    Runs as a background task within FastAPI uvicorn worker.
    Handles all bulk read/write operations with dynamic scaling.
    """
    
    def __init__(
        self,
        pod_id: str,
        storage_conn=None,
        flush_interval: float = 20.0,
        maintenance_interval: float = 3600.0,  # 1 hour
        base_path: str = "/tmp/fleetq"
    ):
        self.pod_id = pod_id
        self.storage_conn = storage_conn
        self.flush_interval = flush_interval
        self.maintenance_interval = maintenance_interval
        
        # Initialize components
        self.local_storage = PodLocalStorage(pod_id, base_path)
        self.buffer_manager = WriteBufferManager(
            self.local_storage,
            flush_interval=flush_interval
        )
        self.writer_pool = DynamicWriterPool()
        
        # Control flags
        self.running = False
        self.tasks: List[asyncio.Task] = []
        
        logger.info(f"ControlPlaneWorker initialized for pod {pod_id}")
    
    async def submit_write_operation(self, operation: WriteOperation):
        """Submit a write operation for batching"""
        self.buffer_manager.add_operation(operation)
        logger.debug(f"Submitted write operation {operation.operation_id}")
    
    async def flush_loop(self):
        """Periodic flush loop"""
        logger.info("Flush loop started")
        
        while self.running:
            try:
                # Check if flush needed
                if self.buffer_manager.should_flush():
                    batches = self.buffer_manager.get_batches_to_flush()
                    
                    logger.info(f"Flushing {len(batches)} batches")
                    
                    for batch in batches:
                        # Submit to writer pool
                        await self.writer_pool.submit_batch(batch)
                        
                        # Record in history
                        self.local_storage.record_batch(batch, status='queued')
                    
                    # Scale workers if needed
                    await self.writer_pool.scale_workers(self.storage_conn)
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Flush loop error: {e}", exc_info=True)
                await asyncio.sleep(5)
        
        logger.info("Flush loop stopped")
    
    async def maintenance_loop(self):
        """Periodic maintenance loop"""
        logger.info("Maintenance loop started")
        
        while self.running:
            try:
                await asyncio.sleep(self.maintenance_interval)
                
                logger.info("Running maintenance cycle")
                
                # Cleanup old records
                self.local_storage.cleanup_old_records(max_age_seconds=86400)
                
                # Log statistics
                stats = self.get_stats()
                logger.info(f"Control plane stats: {stats}")
                
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}", exc_info=True)
                await asyncio.sleep(60)
        
        logger.info("Maintenance loop stopped")
    
    async def start(self):
        """Start control plane worker"""
        if self.running:
            logger.warning("Control plane worker already running")
            return
        
        self.running = True
        
        # Start writer pool
        await self.writer_pool.start(self.storage_conn)
        
        # Start background loops
        self.tasks = [
            asyncio.create_task(self.flush_loop()),
            asyncio.create_task(self.maintenance_loop())
        ]
        
        logger.info("Control plane worker started")
    
    async def stop(self):
        """Stop control plane worker"""
        if not self.running:
            return
        
        logger.info("Stopping control plane worker...")
        
        self.running = False
        
        # Cancel background tasks
        for task in self.tasks:
            task.cancel()
        
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Flush any remaining operations
        batches = self.buffer_manager.get_batches_to_flush()
        for batch in batches:
            await self.writer_pool.submit_batch(batch)
        
        # Wait for queue to empty
        await self.writer_pool.batch_queue.join()
        
        # Stop writer pool
        await self.writer_pool.stop()
        
        logger.info("Control plane worker stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get control plane statistics"""
        return {
            'pod_id': self.pod_id,
            'running': self.running,
            'buffer_stats': self.buffer_manager.get_buffer_stats(),
            'storage_stats': self.local_storage.get_stats(),
            'writer_pool': {
                'active_workers': len(self.writer_pool.active_workers),
                'queue_depth': self.writer_pool.batch_queue.qsize()
            }
        }


# ============================================================================
# Convenience Functions
# ============================================================================

# Global control plane instance
_control_plane: Optional[ControlPlaneWorker] = None


def initialize_control_plane(
    pod_id: str,
    storage_conn=None,
    **kwargs
) -> ControlPlaneWorker:
    """Initialize global control plane instance"""
    global _control_plane
    _control_plane = ControlPlaneWorker(pod_id, storage_conn, **kwargs)
    return _control_plane


def get_control_plane() -> Optional[ControlPlaneWorker]:
    """Get global control plane instance"""
    return _control_plane


async def submit_bulk_write(
    writer_type: WriterType,
    destination: str,
    data: Dict[str, Any],
    orm_type: Optional[str] = None,
    priority: int = 0
) -> str:
    """
    Submit a write operation to control plane.
    
    Returns operation_id for tracking.
    """
    if not _control_plane:
        raise RuntimeError("Control plane not initialized")
    
    operation_id = f"op_{int(time.time() * 1000)}_{os.urandom(4).hex()}"
    
    operation = WriteOperation(
        operation_id=operation_id,
        writer_type=writer_type,
        destination=destination,
        data=data,
        orm_type=orm_type,
        priority=priority
    )
    
    await _control_plane.submit_write_operation(operation)
    
    return operation_id
