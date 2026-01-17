"""
Unit tests for SQLite storage backend implementation.

Tests the SQLiteStorage class functionality for local DLQ operations
during recovery cycles.
"""

import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import Mock

from fleet_q.config import FleetQConfig
from fleet_q.models import DLQRecord, StepPayload
from fleet_q.storage import StorageError
from fleet_q.sqlite_storage import SQLiteStorage


@pytest.fixture
def mock_config():
    """Create a mock FleetQConfig for testing."""
    config = Mock(spec=FleetQConfig)
    config.sqlite_db_path = ":memory:"  # Use in-memory database for tests
    return config


@pytest.fixture
async def storage(mock_config):
    """Create a SQLiteStorage instance for testing."""
    storage = SQLiteStorage(mock_config)
    await storage.initialize()
    yield storage
    await storage.close()


class TestSQLiteStorageInitialization:
    """Test SQLite storage initialization and connection management."""
    
    async def test_initialize_memory_database(self, mock_config):
        """Test initialization with in-memory database."""
        storage = SQLiteStorage(mock_config)
        
        await storage.initialize()
        
        assert storage.connection is not None
        assert storage.db_path == ":memory:"
        
        await storage.close()
    
    async def test_initialize_file_database(self, mock_config):
        """Test initialization with file database."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            db_path = tmp_file.name
        
        try:
            storage = SQLiteStorage(mock_config)
            await storage.initialize(db_path)
            
            assert storage.connection is not None
            assert storage.db_path == db_path
            assert os.path.exists(db_path)
            
            await storage.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    async def test_close_connection(self, storage):
        """Test connection closing."""
        assert storage.connection is not None
        
        await storage.close()
        
        assert storage.connection is None


class TestDLQTableManagement:
    """Test DLQ table creation and management."""
    
    async def test_create_dlq_table(self, storage):
        """Test DLQ table creation."""
        cycle_id = "test_cycle_123"
        
        await storage.create_dlq_table(cycle_id)
        
        # Verify table exists
        table_exists = await storage.table_exists(cycle_id)
        assert table_exists is True
    
    async def test_drop_dlq_table(self, storage):
        """Test DLQ table dropping."""
        cycle_id = "test_cycle_456"
        
        # Create table first
        await storage.create_dlq_table(cycle_id)
        assert await storage.table_exists(cycle_id) is True
        
        # Drop table
        await storage.drop_dlq_table(cycle_id)
        assert await storage.table_exists(cycle_id) is False
    
    async def test_drop_nonexistent_table(self, storage):
        """Test dropping a table that doesn't exist (should not raise error)."""
        cycle_id = "nonexistent_cycle"
        
        # Should not raise an error
        await storage.drop_dlq_table(cycle_id)
    
    async def test_table_name_sanitization(self, storage):
        """Test that table names are properly sanitized."""
        cycle_id = "test-cycle_with.special@chars!"
        
        await storage.create_dlq_table(cycle_id)
        
        # Should create table with sanitized name
        table_exists = await storage.table_exists(cycle_id)
        assert table_exists is True


class TestDLQRecordOperations:
    """Test DLQ record insertion and retrieval."""
    
    async def test_insert_and_get_dlq_record(self, storage):
        """Test inserting and retrieving a DLQ record."""
        cycle_id = "test_cycle_789"
        await storage.create_dlq_table(cycle_id)
        
        # Create test record
        record = DLQRecord(
            step_id="test-step-123",
            original_payload=StepPayload(
                type="test_task",
                args={"param": "value"},
                metadata={"info": "test"}
            ),
            claimed_by="test-pod-456",
            claim_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            recovery_timestamp=datetime(2024, 1, 1, 12, 5, 0),
            reason="dead_pod",
            retry_count=1,
            max_retries=3,
            action="requeue"
        )
        
        # Insert record
        await storage.insert_dlq_record(cycle_id, record)
        
        # Retrieve records
        records = await storage.get_dlq_records(cycle_id)
        
        assert len(records) == 1
        retrieved_record = records[0]
        
        assert retrieved_record.step_id == record.step_id
        assert retrieved_record.original_payload.type == record.original_payload.type
        assert retrieved_record.original_payload.args == record.original_payload.args
        assert retrieved_record.claimed_by == record.claimed_by
        assert retrieved_record.claim_timestamp == record.claim_timestamp
        assert retrieved_record.recovery_timestamp == record.recovery_timestamp
        assert retrieved_record.reason == record.reason
        assert retrieved_record.retry_count == record.retry_count
        assert retrieved_record.max_retries == record.max_retries
        assert retrieved_record.action == record.action
    
    async def test_insert_multiple_records(self, storage):
        """Test inserting multiple DLQ records."""
        cycle_id = "test_cycle_multi"
        await storage.create_dlq_table(cycle_id)
        
        # Create multiple test records
        records = []
        for i in range(3):
            record = DLQRecord(
                step_id=f"test-step-{i}",
                original_payload=StepPayload(
                    type=f"test_task_{i}",
                    args={"index": i},
                    metadata={}
                ),
                claimed_by=f"test-pod-{i}",
                claim_timestamp=datetime(2024, 1, 1, 12, i, 0),
                recovery_timestamp=datetime(2024, 1, 1, 12, i + 5, 0),
                reason="dead_pod",
                retry_count=i,
                max_retries=3,
                action="requeue" if i < 2 else "terminal"
            )
            records.append(record)
            await storage.insert_dlq_record(cycle_id, record)
        
        # Retrieve all records
        retrieved_records = await storage.get_dlq_records(cycle_id)
        
        assert len(retrieved_records) == 3
        
        # Records should be ordered by recovery_timestamp
        for i, retrieved_record in enumerate(retrieved_records):
            assert retrieved_record.step_id == f"test-step-{i}"
            assert retrieved_record.original_payload.type == f"test_task_{i}"
    
    async def test_get_dlq_records_by_action(self, storage):
        """Test retrieving DLQ records filtered by action."""
        cycle_id = "test_cycle_filter"
        await storage.create_dlq_table(cycle_id)
        
        # Insert records with different actions
        requeue_record = DLQRecord(
            step_id="requeue-step",
            original_payload=StepPayload(type="test", args={}, metadata={}),
            claimed_by="pod-1",
            claim_timestamp=datetime.utcnow(),
            recovery_timestamp=datetime.utcnow(),
            reason="dead_pod",
            retry_count=1,
            max_retries=3,
            action="requeue"
        )
        
        terminal_record = DLQRecord(
            step_id="terminal-step",
            original_payload=StepPayload(type="test", args={}, metadata={}),
            claimed_by="pod-2",
            claim_timestamp=datetime.utcnow(),
            recovery_timestamp=datetime.utcnow(),
            reason="poison",
            retry_count=3,
            max_retries=3,
            action="terminal"
        )
        
        await storage.insert_dlq_record(cycle_id, requeue_record)
        await storage.insert_dlq_record(cycle_id, terminal_record)
        
        # Get records by action
        requeue_records = await storage.get_dlq_records_by_action(cycle_id, "requeue")
        terminal_records = await storage.get_dlq_records_by_action(cycle_id, "terminal")
        
        assert len(requeue_records) == 1
        assert len(terminal_records) == 1
        assert requeue_records[0].step_id == "requeue-step"
        assert terminal_records[0].step_id == "terminal-step"
    
    async def test_get_records_from_nonexistent_table(self, storage):
        """Test getting records from a table that doesn't exist."""
        cycle_id = "nonexistent_cycle"
        
        # Should return empty list, not raise error
        records = await storage.get_dlq_records(cycle_id)
        assert records == []
        
        records_by_action = await storage.get_dlq_records_by_action(cycle_id, "requeue")
        assert records_by_action == []
    
    async def test_insert_record_replace_existing(self, storage):
        """Test that inserting a record with same step_id replaces existing."""
        cycle_id = "test_cycle_replace"
        await storage.create_dlq_table(cycle_id)
        
        # Insert initial record
        record1 = DLQRecord(
            step_id="duplicate-step",
            original_payload=StepPayload(type="test", args={}, metadata={}),
            claimed_by="pod-1",
            claim_timestamp=datetime.utcnow(),
            recovery_timestamp=datetime.utcnow(),
            reason="dead_pod",
            retry_count=1,
            max_retries=3,
            action="requeue"
        )
        await storage.insert_dlq_record(cycle_id, record1)
        
        # Insert record with same step_id but different data
        record2 = DLQRecord(
            step_id="duplicate-step",
            original_payload=StepPayload(type="test", args={}, metadata={}),
            claimed_by="pod-2",
            claim_timestamp=datetime.utcnow(),
            recovery_timestamp=datetime.utcnow(),
            reason="timeout",
            retry_count=2,
            max_retries=3,
            action="terminal"
        )
        await storage.insert_dlq_record(cycle_id, record2)
        
        # Should only have one record (the replacement)
        records = await storage.get_dlq_records(cycle_id)
        assert len(records) == 1
        assert records[0].claimed_by == "pod-2"
        assert records[0].reason == "timeout"
        assert records[0].action == "terminal"


class TestUtilityMethods:
    """Test utility methods."""
    
    async def test_get_table_count(self, storage):
        """Test getting the count of records in a table."""
        cycle_id = "test_cycle_count"
        await storage.create_dlq_table(cycle_id)
        
        # Initially empty
        count = await storage.get_table_count(cycle_id)
        assert count == 0
        
        # Add some records
        for i in range(5):
            record = DLQRecord(
                step_id=f"step-{i}",
                original_payload=StepPayload(type="test", args={}, metadata={}),
                claimed_by="pod-1",
                claim_timestamp=datetime.utcnow(),
                recovery_timestamp=datetime.utcnow(),
                reason="dead_pod",
                retry_count=0,
                max_retries=3,
                action="requeue"
            )
            await storage.insert_dlq_record(cycle_id, record)
        
        # Should have 5 records
        count = await storage.get_table_count(cycle_id)
        assert count == 5
    
    async def test_get_table_count_nonexistent(self, storage):
        """Test getting count from nonexistent table."""
        cycle_id = "nonexistent_cycle"
        
        count = await storage.get_table_count(cycle_id)
        assert count == 0
    
    async def test_table_exists(self, storage):
        """Test checking if table exists."""
        cycle_id = "test_cycle_exists"
        
        # Initially doesn't exist
        exists = await storage.table_exists(cycle_id)
        assert exists is False
        
        # Create table
        await storage.create_dlq_table(cycle_id)
        exists = await storage.table_exists(cycle_id)
        assert exists is True
        
        # Drop table
        await storage.drop_dlq_table(cycle_id)
        exists = await storage.table_exists(cycle_id)
        assert exists is False


class TestErrorHandling:
    """Test error handling scenarios."""
    
    async def test_operation_on_closed_connection(self, mock_config):
        """Test operations on closed connection."""
        storage = SQLiteStorage(mock_config)
        await storage.initialize()
        await storage.close()
        
        # Operations on closed connection should raise StorageError
        with pytest.raises(StorageError):
            await storage.create_dlq_table("test_cycle")
    
    async def test_invalid_cycle_id_characters(self, storage):
        """Test handling of cycle IDs with special characters."""
        # These should be sanitized and work fine
        problematic_ids = [
            "cycle with spaces",
            "cycle-with-dashes",
            "cycle_with_underscores",
            "cycle.with.dots",
            "cycle@with#special$chars%"
        ]
        
        for cycle_id in problematic_ids:
            await storage.create_dlq_table(cycle_id)
            exists = await storage.table_exists(cycle_id)
            assert exists is True
            await storage.drop_dlq_table(cycle_id)


class TestDataSerialization:
    """Test data serialization and deserialization."""
    
    async def test_complex_payload_serialization(self, storage):
        """Test serialization of complex step payloads."""
        cycle_id = "test_cycle_complex"
        await storage.create_dlq_table(cycle_id)
        
        # Create record with complex payload
        complex_payload = StepPayload(
            type="complex_task",
            args={
                "string_param": "test value",
                "int_param": 42,
                "float_param": 3.14,
                "bool_param": True,
                "list_param": [1, 2, 3, "four"],
                "dict_param": {
                    "nested_key": "nested_value",
                    "nested_list": [{"deep": "value"}]
                }
            },
            metadata={
                "priority": "high",
                "timeout": 300,
                "tags": ["urgent", "customer-facing"],
                "config": {
                    "retries": 3,
                    "backoff": "exponential"
                }
            }
        )
        
        record = DLQRecord(
            step_id="complex-step",
            original_payload=complex_payload,
            claimed_by="test-pod",
            claim_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            recovery_timestamp=datetime(2024, 1, 1, 12, 5, 0),
            reason="dead_pod",
            retry_count=1,
            max_retries=3,
            action="requeue"
        )
        
        # Insert and retrieve
        await storage.insert_dlq_record(cycle_id, record)
        records = await storage.get_dlq_records(cycle_id)
        
        assert len(records) == 1
        retrieved_record = records[0]
        
        # Verify complex payload was preserved
        payload = retrieved_record.original_payload
        assert payload.type == "complex_task"
        assert payload.args["string_param"] == "test value"
        assert payload.args["int_param"] == 42
        assert payload.args["float_param"] == 3.14
        assert payload.args["bool_param"] is True
        assert payload.args["list_param"] == [1, 2, 3, "four"]
        assert payload.args["dict_param"]["nested_key"] == "nested_value"
        assert payload.args["dict_param"]["nested_list"] == [{"deep": "value"}]
        
        assert payload.metadata["priority"] == "high"
        assert payload.metadata["timeout"] == 300
        assert payload.metadata["tags"] == ["urgent", "customer-facing"]
        assert payload.metadata["config"]["retries"] == 3
        assert payload.metadata["config"]["backoff"] == "exponential"
    
    async def test_timestamp_serialization(self, storage):
        """Test that timestamps are properly serialized and deserialized."""
        cycle_id = "test_cycle_timestamps"
        await storage.create_dlq_table(cycle_id)
        
        # Use specific timestamps
        claim_time = datetime(2024, 1, 1, 12, 30, 45, 123456)
        recovery_time = datetime(2024, 1, 1, 12, 35, 50, 654321)
        
        record = DLQRecord(
            step_id="timestamp-step",
            original_payload=StepPayload(type="test", args={}, metadata={}),
            claimed_by="test-pod",
            claim_timestamp=claim_time,
            recovery_timestamp=recovery_time,
            reason="dead_pod",
            retry_count=1,
            max_retries=3,
            action="requeue"
        )
        
        await storage.insert_dlq_record(cycle_id, record)
        records = await storage.get_dlq_records(cycle_id)
        
        assert len(records) == 1
        retrieved_record = records[0]
        
        # Timestamps should be preserved (note: microseconds might be truncated by SQLite)
        assert retrieved_record.claim_timestamp.replace(microsecond=0) == claim_time.replace(microsecond=0)
        assert retrieved_record.recovery_timestamp.replace(microsecond=0) == recovery_time.replace(microsecond=0)