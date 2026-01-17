"""
SQLite storage backend implementation for FLEET-Q local DLQ operations.

Provides concrete implementation of the SQLiteStorageInterface for ephemeral
storage during recovery cycles. Used for Dead Letter Queue (DLQ) management
with automatic cleanup.

"""

import asyncio
import logging
import sqlite3
from datetime import datetime
from typing import List, Optional

from .backoff import with_backoff
from .config import FleetQConfig
from .models import DLQRecord, StepPayload
from .storage import SQLiteStorageInterface, StorageError, VariantSerializer

logger = logging.getLogger(__name__)


class SQLiteStorage(SQLiteStorageInterface):
    """
    SQLite implementation for local DLQ storage operations.
    
    Provides ephemeral storage for recovery cycles, particularly for Dead Letter
    Queue (DLQ) management. Tables are created and dropped per recovery cycle
    to ensure clean separation and automatic cleanup.
    """
    
    def __init__(self, config: FleetQConfig):
        """
        Initialize SQLite storage backend.
        
        Args:
            config: FleetQ configuration containing SQLite settings
        """
        self.config = config
        self.connection: Optional[sqlite3.Connection] = None
        self._connection_lock = asyncio.Lock()
        self.db_path = config.sqlite_db_path
    
    async def initialize(self, db_path: Optional[str] = None) -> None:
        """
        Initialize SQLite database connection.
        
        Args:
            db_path: Path to SQLite database file (or ":memory:" for in-memory)
                    If None, uses config.sqlite_db_path
            
        Raises:
            StorageError: If initialization fails
        """
        if db_path:
            self.db_path = db_path
        
        try:
            await self._connect()
            logger.info(f"SQLite storage initialized with database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite storage: {e}")
            raise StorageError(f"SQLite initialization failed: {e}", e)
    
    async def close(self) -> None:
        """Close SQLite database connection."""
        async with self._connection_lock:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("SQLite connection closed")
                except Exception as e:
                    logger.warning(f"Error closing SQLite connection: {e}")
                finally:
                    self.connection = None
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def _connect(self) -> None:
        """
        Establish connection to SQLite database.
        
        Raises:
            StorageError: If connection fails
        """
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
                    check_same_thread=False,  # Allow use from different threads
                    timeout=30.0  # 30 second timeout for database locks
                )
                
                # Configure connection for better performance and reliability
                self.connection.execute("PRAGMA journal_mode=WAL")
                self.connection.execute("PRAGMA synchronous=NORMAL")
                self.connection.execute("PRAGMA temp_store=MEMORY")
                self.connection.execute("PRAGMA mmap_size=268435456")  # 256MB
                
                logger.debug(f"Connected to SQLite database: {self.db_path}")
                
            except Exception as e:
                logger.error(f"Failed to connect to SQLite: {e}")
                raise StorageError(f"SQLite connection failed: {e}", e)
    
    async def _ensure_connection(self) -> sqlite3.Connection:
        """
        Ensure we have a valid SQLite connection.
        
        Returns:
            sqlite3.Connection: Active connection
            
        Raises:
            StorageError: If connection cannot be established
        """
        if not self.connection:
            raise StorageError("SQLite connection is not initialized")
        return self.connection
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def create_dlq_table(self, cycle_id: str) -> None:
        """
        Create a DLQ table for a specific recovery cycle.
        
        Args:
            cycle_id: Unique identifier for the recovery cycle
            
        Raises:
            StorageError: If table creation fails
        """
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Create the DLQ table with all required fields
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
            
            # Create index for faster queries
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_action 
                ON {table_name}(action)
            """)
            
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_reason 
                ON {table_name}(reason)
            """)
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Created DLQ table: {table_name}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create DLQ table {table_name}: {e}")
            raise StorageError(f"DLQ table creation failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def drop_dlq_table(self, cycle_id: str) -> None:
        """
        Drop a DLQ table for a specific recovery cycle.
        
        Args:
            cycle_id: Unique identifier for the recovery cycle
            
        Raises:
            StorageError: If table drop fails
        """
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Drop the table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            connection.commit()
            cursor.close()
            
            logger.debug(f"Dropped DLQ table: {table_name}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to drop DLQ table {table_name}: {e}")
            raise StorageError(f"DLQ table drop failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
    async def insert_dlq_record(self, cycle_id: str, record: DLQRecord) -> None:
        """
        Insert a record into the DLQ table.
        
        Args:
            cycle_id: Recovery cycle identifier
            record: DLQ record to insert
            
        Raises:
            StorageError: If insertion fails
        """
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Serialize the payload for storage
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
            
            logger.debug(f"Inserted DLQ record for step {record.step_id} in table {table_name}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to insert DLQ record for step {record.step_id}: {e}")
            raise StorageError(f"DLQ record insertion failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
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
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                cursor.close()
                return []  # Table doesn't exist, return empty list
            
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
            logger.error(f"Failed to get DLQ records from table {table_name}: {e}")
            raise StorageError(f"DLQ records retrieval failed: {e}", e)
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=2000,
        retry_on=[sqlite3.OperationalError, sqlite3.DatabaseError]
    )
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
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                cursor.close()
                return []  # Table doesn't exist, return empty list
            
            cursor.execute(f"""
                SELECT step_id, original_payload, claimed_by, claim_timestamp,
                       recovery_timestamp, reason, retry_count, max_retries, action
                FROM {table_name}
                WHERE action = ?
                ORDER BY recovery_timestamp ASC
            """, (action,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            records = []
            for row in rows:
                records.append(self._row_to_dlq_record(row))
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to get DLQ records by action {action} from table {table_name}: {e}")
            raise StorageError(f"DLQ records retrieval failed: {e}", e)
    
    def _get_dlq_table_name(self, cycle_id: str) -> str:
        """
        Generate a safe table name for a DLQ cycle.
        
        Args:
            cycle_id: Recovery cycle identifier
            
        Returns:
            str: Safe table name
        """
        # Sanitize cycle_id to ensure it's safe for SQL
        # Only allow alphanumeric characters and underscores
        safe_cycle_id = ''.join(c if c.isalnum() else '_' for c in cycle_id)
        # Ensure it doesn't start with a number
        if safe_cycle_id and safe_cycle_id[0].isdigit():
            safe_cycle_id = 'c_' + safe_cycle_id
        return f"dlq_orphans_{safe_cycle_id}"
    
    def _row_to_dlq_record(self, row: tuple) -> DLQRecord:
        """
        Convert a database row to a DLQRecord object.
        
        Args:
            row: Database row as tuple
            
        Returns:
            DLQRecord: DLQ record object
        """
        (step_id, original_payload_json, claimed_by, claim_timestamp_str,
         recovery_timestamp_str, reason, retry_count, max_retries, action) = row
        
        # Deserialize the payload
        original_payload = VariantSerializer.deserialize_step_payload(original_payload_json)
        
        # Parse timestamps
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
    
    async def get_table_count(self, cycle_id: str) -> int:
        """
        Get the number of records in a DLQ table.
        
        Args:
            cycle_id: Recovery cycle identifier
            
        Returns:
            int: Number of records in the table
            
        Raises:
            StorageError: If count retrieval fails
        """
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                cursor.close()
                return 0  # Table doesn't exist
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            
            return count
            
        except Exception as e:
            logger.error(f"Failed to get table count for {table_name}: {e}")
            raise StorageError(f"Table count retrieval failed: {e}", e)
    
    async def table_exists(self, cycle_id: str) -> bool:
        """
        Check if a DLQ table exists for a cycle.
        
        Args:
            cycle_id: Recovery cycle identifier
            
        Returns:
            bool: True if table exists, False otherwise
            
        Raises:
            StorageError: If check fails
        """
        connection = await self._ensure_connection()
        table_name = self._get_dlq_table_name(cycle_id)
        
        try:
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            exists = cursor.fetchone() is not None
            cursor.close()
            
            return exists
            
        except Exception as e:
            logger.error(f"Failed to check if table {table_name} exists: {e}")
            raise StorageError(f"Table existence check failed: {e}", e)
    
    async def get_dead_pods(self, heartbeat_threshold_seconds: int) -> List:
        """
        Get dead pods - not implemented for SQLite storage.
        
        SQLite storage is only used for local DLQ operations, not pod health management.
        This method is required by the interface but always returns an empty list.
        
        Args:
            heartbeat_threshold_seconds: Ignored
            
        Returns:
            List: Always empty list
        """
        return []