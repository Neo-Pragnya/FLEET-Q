"""
FLEET-Q Storage Layer

Abstractions for Snowflake (global coordination) and SQLite (local ephemeral state).
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from config import FleetQConfig

logger = logging.getLogger(__name__)


class SnowflakeStorage:
    """Wrapper for Snowflake operations with connection pooling"""
    
    def __init__(self, config: FleetQConfig):
        self.config = config
        self._connection = None
    
    def _get_connection(self):
        """Get or create Snowflake connection"""
        if self._connection is None or self._connection.is_closed():
            sf_config = self.config.snowflake
            self._connection = snowflake.connector.connect(
                account=sf_config.account,
                user=sf_config.user,
                password=sf_config.password,
                database=sf_config.database,
                schema=sf_config.schema,
                warehouse=sf_config.warehouse,
                role=sf_config.role,
            )
            logger.info("Snowflake connection established")
        return self._connection
    
    @contextmanager
    def cursor(self):
        """Context manager for Snowflake cursor"""
        conn = self._get_connection()
        cur = conn.cursor(DictCursor)
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Snowflake transaction failed: {e}")
            raise
        finally:
            cur.close()
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute a query and return results as list of dicts
        
        Args:
            query: SQL query with %(param)s placeholders
            params: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        with self.cursor() as cur:
            cur.execute(query, params or {})
            if cur.description:
                return cur.fetchall()
            return []
    
    def execute_many(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a query multiple times with different parameters
        
        Args:
            query: SQL query with %(param)s placeholders
            params_list: List of parameter dictionaries
        """
        with self.cursor() as cur:
            cur.executemany(query, params_list)
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            logger.info("Snowflake connection closed")


class LocalStorage:
    """Wrapper for local SQLite operations (ephemeral, used by leader for DLQ)"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize local SQLite database"""
        with self._get_connection() as conn:
            # Create DLQ table structure (ephemeral, recreated as needed)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq_metadata (
                    table_name TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    processed BOOLEAN DEFAULT 0
                )
            """)
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for SQLite connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def create_dlq_table(self, table_name: str) -> None:
        """
        Create an ephemeral DLQ table for orphaned steps
        
        Args:
            table_name: Name for the DLQ table (e.g., dlq_orphans_20260117_123456)
        """
        with self._get_connection() as conn:
            # Create table for orphaned steps
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    step_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    claimed_by TEXT NOT NULL,
                    last_update_ts TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    discovered_at TEXT NOT NULL
                )
            """)
            
            # Record metadata
            conn.execute("""
                INSERT OR REPLACE INTO dlq_metadata (table_name, created_at)
                VALUES (?, ?)
            """, (table_name, datetime.utcnow().isoformat()))
            
            conn.commit()
            logger.info(f"Created DLQ table: {table_name}")
    
    def insert_dlq_records(self, table_name: str, records: List[Dict[str, Any]]) -> None:
        """
        Insert orphaned step records into DLQ table
        
        Args:
            table_name: DLQ table name
            records: List of step records to insert
        """
        if not records:
            return
        
        with self._get_connection() as conn:
            conn.executemany(f"""
                INSERT INTO {table_name} 
                (step_id, payload, claimed_by, last_update_ts, retry_count, reason, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    r['step_id'],
                    json.dumps(r['payload']),
                    r['claimed_by'],
                    r['last_update_ts'],
                    r['retry_count'],
                    r['reason'],
                    r['discovered_at']
                )
                for r in records
            ])
            conn.commit()
            logger.info(f"Inserted {len(records)} records into DLQ table {table_name}")
    
    def get_dlq_records(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all records from a DLQ table
        
        Args:
            table_name: DLQ table name
            
        Returns:
            List of DLQ records
        """
        with self._get_connection() as conn:
            cursor = conn.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            return [
                {
                    'step_id': row['step_id'],
                    'payload': json.loads(row['payload']),
                    'claimed_by': row['claimed_by'],
                    'last_update_ts': row['last_update_ts'],
                    'retry_count': row['retry_count'],
                    'reason': row['reason'],
                    'discovered_at': row['discovered_at']
                }
                for row in rows
            ]
    
    def drop_dlq_table(self, table_name: str) -> None:
        """
        Drop a DLQ table after processing
        
        Args:
            table_name: DLQ table name to drop
        """
        with self._get_connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute("DELETE FROM dlq_metadata WHERE table_name = ?", (table_name,))
            conn.commit()
            logger.info(f"Dropped DLQ table: {table_name}")
    
    def list_dlq_tables(self) -> List[str]:
        """
        List all DLQ tables
        
        Returns:
            List of DLQ table names
        """
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT table_name FROM dlq_metadata WHERE processed = 0")
            return [row['table_name'] for row in cursor.fetchall()]
    
    def mark_dlq_processed(self, table_name: str) -> None:
        """
        Mark a DLQ table as processed
        
        Args:
            table_name: DLQ table name
        """
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE dlq_metadata SET processed = 1 WHERE table_name = ?",
                (table_name,)
            )
            conn.commit()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Test local storage
    local = LocalStorage("/tmp/test_fleet_q.db")
    
    # Create DLQ table
    dlq_name = "dlq_test_20260117_123456"
    local.create_dlq_table(dlq_name)
    
    # Insert records
    test_records = [
        {
            'step_id': 'step-001',
            'payload': {'task': 'test'},
            'claimed_by': 'pod-001',
            'last_update_ts': datetime.utcnow().isoformat(),
            'retry_count': 1,
            'reason': 'pod died',
            'discovered_at': datetime.utcnow().isoformat()
        }
    ]
    local.insert_dlq_records(dlq_name, test_records)
    
    # Retrieve records
    records = local.get_dlq_records(dlq_name)
    print(f"Retrieved {len(records)} records")
    
    # Clean up
    local.drop_dlq_table(dlq_name)
    print("Test completed successfully")
