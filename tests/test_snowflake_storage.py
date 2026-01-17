"""
Unit tests for Snowflake storage backend implementation.

Tests the SnowflakeStorage class functionality using mocked connections
to avoid requiring actual Snowflake infrastructure during testing.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from contextlib import asynccontextmanager

from fleet_q.config import FleetQConfig
from fleet_q.models import Step, StepPayload, StepStatus, PodHealth, PodStatus
from fleet_q.storage import StorageError, TransactionConflictError, ConnectionError as StorageConnectionError
from fleet_q.snowflake_storage import SnowflakeStorage


@pytest.fixture
def mock_config():
    """Create a mock FleetQConfig for testing."""
    config = Mock(spec=FleetQConfig)
    config.snowflake_connection_params = {
        "account": "test_account",
        "user": "test_user", 
        "password": "test_password",
        "database": "test_db",
        "schema": "test_schema"
    }
    config.default_max_attempts = 5
    config.default_base_delay_ms = 100
    config.default_max_delay_ms = 30000
    return config


@pytest.fixture
def mock_connection():
    """Create a mock Snowflake connection."""
    connection = Mock()
    connection.autocommit = True
    connection.commit = Mock()
    connection.rollback = Mock()
    connection.close = Mock()
    
    # Mock cursor
    cursor = Mock()
    cursor.execute = Mock()
    cursor.fetchone = Mock()
    cursor.fetchall = Mock()
    cursor.close = Mock()
    cursor.rowcount = 1
    
    connection.cursor = Mock(return_value=cursor)
    
    return connection


@pytest.fixture
def storage(mock_config):
    """Create a SnowflakeStorage instance for testing."""
    return SnowflakeStorage(mock_config)


class TestSnowflakeStorageInitialization:
    """Test storage initialization and connection management."""
    
    @patch('fleet_q.snowflake_storage.snowflake.connector.connect')
    async def test_initialize_success(self, mock_connect, storage, mock_connection):
        """Test successful storage initialization."""
        mock_connect.return_value = mock_connection
        
        await storage.initialize()
        
        # Should have called connect with correct parameters
        mock_connect.assert_called_once_with(
            **storage.config.snowflake_connection_params,
            autocommit=False
        )
        
        # Should have created tables
        cursor_calls = mock_connection.cursor.return_value.execute.call_args_list
        assert len(cursor_calls) >= 2  # At least POD_HEALTH and STEP_TRACKER tables
        
        # Check that table creation SQL was called
        table_creation_calls = [call for call in cursor_calls if 'CREATE TABLE' in str(call)]
        assert len(table_creation_calls) >= 2
    
    @patch('fleet_q.snowflake_storage.snowflake.connector.connect')
    async def test_initialize_connection_failure(self, mock_connect, storage):
        """Test initialization failure due to connection error."""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(StorageError, match="Snowflake initialization failed"):
            await storage.initialize()
    
    async def test_close_connection(self, storage, mock_connection):
        """Test connection closing."""
        storage.connection = mock_connection
        
        await storage.close()
        
        mock_connection.close.assert_called_once()
        assert storage.connection is None
    
    async def test_close_connection_with_error(self, storage, mock_connection):
        """Test connection closing with error (should not raise)."""
        storage.connection = mock_connection
        mock_connection.close.side_effect = Exception("Close error")
        
        # Should not raise exception
        await storage.close()
        assert storage.connection is None


class TestStepOperations:
    """Test step-related storage operations."""
    
    async def test_create_step_success(self, storage, mock_connection):
        """Test successful step creation."""
        storage.connection = mock_connection
        
        step = Step(
            step_id="test-step-123",
            status=StepStatus.PENDING,
            claimed_by=None,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test_task", args={"param": "value"}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        await storage.create_step(step)
        
        # Should have executed INSERT statement
        cursor = mock_connection.cursor.return_value
        cursor.execute.assert_called_once()
        
        # Check that the SQL contains INSERT INTO STEP_TRACKER
        sql_call = cursor.execute.call_args[0][0]
        assert "INSERT INTO STEP_TRACKER" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_create_step_failure(self, storage, mock_connection):
        """Test step creation failure."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.execute.side_effect = Exception("Database error")
        
        step = Step(
            step_id="test-step-456",
            status=StepStatus.PENDING,
            claimed_by=None,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test_task", args={}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        with pytest.raises(StorageError, match="Step creation failed"):
            await storage.create_step(step)
        
        mock_connection.rollback.assert_called_once()
    
    async def test_get_step_found(self, storage, mock_connection):
        """Test successful step retrieval."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock database row
        mock_row = {
            'STEP_ID': 'test-step-123',
            'STATUS': 'pending',
            'CLAIMED_BY': None,
            'LAST_UPDATE_TS': datetime.utcnow(),
            'PAYLOAD': '{"type": "test_task", "args": {}, "metadata": {}}',
            'RETRY_COUNT': 0,
            'PRIORITY': 0,
            'CREATED_TS': datetime.utcnow(),
            'MAX_RETRIES': 3
        }
        cursor.fetchone.return_value = mock_row
        
        result = await storage.get_step("test-step-123")
        
        assert result is not None
        assert result.step_id == "test-step-123"
        assert result.status == StepStatus.PENDING
        assert result.claimed_by is None
    
    async def test_get_step_not_found(self, storage, mock_connection):
        """Test step retrieval when step doesn't exist."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None
        
        result = await storage.get_step("nonexistent-step")
        
        assert result is None
    
    async def test_claim_steps_success(self, storage, mock_connection):
        """Test successful step claiming."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock available steps
        mock_steps = [
            {
                'STEP_ID': 'step-1',
                'STATUS': 'pending',
                'CLAIMED_BY': None,
                'LAST_UPDATE_TS': datetime.utcnow(),
                'PAYLOAD': '{"type": "test_task", "args": {}, "metadata": {}}',
                'RETRY_COUNT': 0,
                'PRIORITY': 0,
                'CREATED_TS': datetime.utcnow(),
                'MAX_RETRIES': 3
            },
            {
                'STEP_ID': 'step-2',
                'STATUS': 'pending',
                'CLAIMED_BY': None,
                'LAST_UPDATE_TS': datetime.utcnow(),
                'PAYLOAD': '{"type": "test_task", "args": {}, "metadata": {}}',
                'RETRY_COUNT': 0,
                'PRIORITY': 0,
                'CREATED_TS': datetime.utcnow(),
                'MAX_RETRIES': 3
            }
        ]
        cursor.fetchall.return_value = mock_steps
        cursor.rowcount = 2  # Both steps were updated
        
        result = await storage.claim_steps("test-pod-123", 2)
        
        assert len(result) == 2
        assert all(step.status == StepStatus.CLAIMED for step in result)
        assert all(step.claimed_by == "test-pod-123" for step in result)
        
        mock_connection.commit.assert_called_once()
    
    async def test_claim_steps_conflict(self, storage, mock_connection):
        """Test step claiming with conflict (some steps already claimed)."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock available steps
        mock_steps = [
            {
                'STEP_ID': 'step-1',
                'STATUS': 'pending',
                'CLAIMED_BY': None,
                'LAST_UPDATE_TS': datetime.utcnow(),
                'PAYLOAD': '{"type": "test_task", "args": {}, "metadata": {}}',
                'RETRY_COUNT': 0,
                'PRIORITY': 0,
                'CREATED_TS': datetime.utcnow(),
                'MAX_RETRIES': 3
            }
        ]
        cursor.fetchall.return_value = mock_steps
        cursor.rowcount = 0  # No steps were actually updated (conflict)
        
        with pytest.raises(TransactionConflictError, match="Step claiming conflict"):
            await storage.claim_steps("test-pod-123", 1)
        
        mock_connection.rollback.assert_called_once()
    
    async def test_claim_steps_no_available(self, storage, mock_connection):
        """Test step claiming when no steps are available."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = []  # No available steps
        
        result = await storage.claim_steps("test-pod-123", 5)
        
        assert result == []
        mock_connection.rollback.assert_called_once()
    
    async def test_update_step_status_success(self, storage, mock_connection):
        """Test successful step status update."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1  # One row updated
        
        await storage.update_step_status("test-step-123", StepStatus.COMPLETED)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "UPDATE STEP_TRACKER" in sql_call
        assert "status = ?" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_update_step_status_completed_clears_claimed_by(self, storage, mock_connection):
        """Test that completed status clears claimed_by field."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1
        
        await storage.update_step_status("test-step-123", StepStatus.COMPLETED)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "claimed_by = NULL" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_update_step_status_failed_clears_claimed_by(self, storage, mock_connection):
        """Test that failed status clears claimed_by field."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1
        
        await storage.update_step_status("test-step-123", StepStatus.FAILED)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "claimed_by = NULL" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_update_step_status_pending_keeps_claimed_by(self, storage, mock_connection):
        """Test that pending status doesn't clear claimed_by field."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1
        
        await storage.update_step_status("test-step-123", StepStatus.PENDING)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "claimed_by = NULL" not in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_complete_step_success(self, storage, mock_connection):
        """Test successful step completion."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1  # One row updated
        
        result_metadata = {"result": "success", "duration": 1500}
        
        await storage.complete_step("test-step-123", result_metadata)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        
        assert "UPDATE STEP_TRACKER" in sql_call
        assert "status = 'completed'" in sql_call
        assert "claimed_by = NULL" in sql_call
        assert "WHERE step_id = ? AND status = 'claimed'" in sql_call
        assert params[1] == "test-step-123"
        
        mock_connection.commit.assert_called_once()
    
    async def test_complete_step_not_claimed(self, storage, mock_connection):
        """Test step completion when step is not in claimed status."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 0  # No rows updated
        cursor.fetchone.return_value = ("pending", None)  # Step exists but not claimed
        
        with pytest.raises(StorageError, match="Cannot complete step.*current status is pending"):
            await storage.complete_step("test-step-123")
        
        mock_connection.rollback.assert_called_once()
    
    async def test_complete_step_not_found(self, storage, mock_connection):
        """Test step completion when step doesn't exist."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 0  # No rows updated
        cursor.fetchone.return_value = None  # Step doesn't exist
        
        with pytest.raises(StorageError, match="Step test-step-123 not found"):
            await storage.complete_step("test-step-123")
        
        mock_connection.rollback.assert_called_once()
    
    async def test_fail_step_success(self, storage, mock_connection):
        """Test successful step failure."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 1  # One row updated
        
        error_message = "Task execution failed"
        error_metadata = {"error_code": "EXEC_001"}
        
        await storage.fail_step("test-step-123", error_message, error_metadata)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        
        assert "UPDATE STEP_TRACKER" in sql_call
        assert "status = 'failed'" in sql_call
        assert "claimed_by = NULL" in sql_call
        assert "WHERE step_id = ? AND status = 'claimed'" in sql_call
        assert params[1] == "test-step-123"
        
        mock_connection.commit.assert_called_once()
    
    async def test_fail_step_not_claimed(self, storage, mock_connection):
        """Test step failure when step is not in claimed status."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 0  # No rows updated
        cursor.fetchone.return_value = ("completed", "other_pod")  # Step already completed
        
        with pytest.raises(StorageError, match="Cannot fail step.*current status is completed"):
            await storage.fail_step("test-step-123")
        
        mock_connection.rollback.assert_called_once()
    
    async def test_fail_step_database_error(self, storage, mock_connection):
        """Test step failure with database error."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.execute.side_effect = Exception("Database error")
        
        with pytest.raises(StorageError, match="Step failure update failed"):
            await storage.fail_step("test-step-123")
        
        mock_connection.rollback.assert_called_once()
    
    async def test_requeue_steps_success(self, storage, mock_connection):
        """Test successful step requeuing."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 2  # Two steps requeued
        
        step_ids = ["step-1", "step-2"]
        await storage.requeue_steps(step_ids)
        
        # Should have called execute twice (requeue + terminal check)
        assert cursor.execute.call_count == 2
        
        mock_connection.commit.assert_called_once()
    
    async def test_requeue_steps_empty_list(self, storage, mock_connection):
        """Test requeuing with empty step list."""
        storage.connection = mock_connection
        
        await storage.requeue_steps([])
        
        # Should not have made any database calls
        mock_connection.cursor.assert_not_called()


class TestPodHealthOperations:
    """Test pod health related storage operations."""
    
    async def test_upsert_pod_health_success(self, storage, mock_connection):
        """Test successful pod health upsert."""
        storage.connection = mock_connection
        
        pod_health = PodHealth(
            pod_id="test-pod-123",
            birth_ts=datetime.utcnow(),
            last_heartbeat_ts=datetime.utcnow(),
            status=PodStatus.UP,
            meta={"cpu_count": 4, "version": "1.0.0"}
        )
        
        await storage.upsert_pod_health(pod_health)
        
        cursor = mock_connection.cursor.return_value
        cursor.execute.assert_called_once()
        
        # Check that MERGE statement was used
        sql_call = cursor.execute.call_args[0][0]
        assert "MERGE INTO POD_HEALTH" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_get_active_pods_success(self, storage, mock_connection):
        """Test successful active pods retrieval."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock active pods
        mock_pods = [
            {
                'POD_ID': 'pod-1',
                'BIRTH_TS': datetime.utcnow() - timedelta(hours=1),
                'LAST_HEARTBEAT_TS': datetime.utcnow() - timedelta(seconds=30),
                'STATUS': 'up',
                'META': '{"cpu_count": 4}'
            },
            {
                'POD_ID': 'pod-2',
                'BIRTH_TS': datetime.utcnow() - timedelta(minutes=30),
                'LAST_HEARTBEAT_TS': datetime.utcnow() - timedelta(seconds=15),
                'STATUS': 'up',
                'META': '{"cpu_count": 2}'
            }
        ]
        cursor.fetchall.return_value = mock_pods
        
        result = await storage.get_active_pods(180)  # 3 minutes threshold
        
        assert len(result) == 2
        assert all(pod.status == PodStatus.UP for pod in result)
        
        # Check that query used correct threshold
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "last_heartbeat_ts > ?" in sql_call
    
    async def test_mark_pods_dead_success(self, storage, mock_connection):
        """Test successful marking of pods as dead."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.rowcount = 2  # Two pods marked as dead
        
        pod_ids = ["pod-1", "pod-2"]
        await storage.mark_pods_dead(pod_ids)
        
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "UPDATE POD_HEALTH" in sql_call
        assert "status = 'down'" in sql_call
        
        mock_connection.commit.assert_called_once()
    
    async def test_mark_pods_dead_empty_list(self, storage, mock_connection):
        """Test marking pods as dead with empty list."""
        storage.connection = mock_connection
        
        await storage.mark_pods_dead([])
        
        # Should not have made any database calls
        mock_connection.cursor.assert_not_called()
    
    async def test_get_dead_pods_success(self, storage, mock_connection):
        """Test successful retrieval of dead pods."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock database response
        cursor.fetchall.return_value = [
            ("pod-1", datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 9, 0, 0), "up", '{"test": "data1"}'),
            ("pod-2", datetime(2024, 1, 1, 10, 30, 0), datetime(2024, 1, 1, 9, 30, 0), "up", '{"test": "data2"}')
        ]
        
        dead_pods = await storage.get_dead_pods(300)  # 5 minutes threshold
        
        assert len(dead_pods) == 2
        assert dead_pods[0].pod_id == "pod-1"
        assert dead_pods[1].pod_id == "pod-2"
        assert dead_pods[0].status == PodStatus.UP
        assert dead_pods[1].status == PodStatus.UP
        
        # Verify SQL query was called correctly
        cursor.execute.assert_called_once()
        sql_call = cursor.execute.call_args[0][0]
        assert "last_heartbeat_ts < %s" in sql_call
        assert "status != 'down'" in sql_call
        
        mock_connection.commit.assert_not_called()  # Read operation, no commit needed
    
    async def test_get_dead_pods_empty_result(self, storage, mock_connection):
        """Test get_dead_pods with no dead pods found."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = []
        
        dead_pods = await storage.get_dead_pods(300)
        
        assert len(dead_pods) == 0
        cursor.execute.assert_called_once()
    
    async def test_get_dead_pods_invalid_metadata(self, storage, mock_connection):
        """Test get_dead_pods with invalid JSON metadata."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock database response with invalid JSON
        cursor.fetchall.return_value = [
            ("pod-1", datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 9, 0, 0), "up", "invalid-json"),
        ]
        
        dead_pods = await storage.get_dead_pods(300)
        
        assert len(dead_pods) == 1
        assert dead_pods[0].pod_id == "pod-1"
        assert dead_pods[0].meta == {}  # Should default to empty dict on invalid JSON


class TestQueueStatistics:
    """Test queue statistics operations."""
    
    async def test_get_queue_stats_success(self, storage, mock_connection):
        """Test successful queue statistics retrieval."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock statistics data
        mock_stats = [
            {'STATUS': 'pending', 'COUNT': 10},
            {'STATUS': 'claimed', 'COUNT': 5},
            {'STATUS': 'completed', 'COUNT': 100},
            {'STATUS': 'failed', 'COUNT': 2}
        ]
        cursor.fetchall.return_value = mock_stats
        
        result = await storage.get_queue_stats()
        
        expected = {
            'pending': 10,
            'claimed': 5,
            'completed': 100,
            'failed': 2
        }
        assert result == expected
    
    async def test_get_queue_stats_partial_data(self, storage, mock_connection):
        """Test queue statistics with partial data (some statuses missing)."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        
        # Mock partial statistics (only some statuses present)
        mock_stats = [
            {'STATUS': 'pending', 'COUNT': 5},
            {'STATUS': 'completed', 'COUNT': 50}
        ]
        cursor.fetchall.return_value = mock_stats
        
        result = await storage.get_queue_stats()
        
        expected = {
            'pending': 5,
            'claimed': 0,  # Should default to 0
            'completed': 50,
            'failed': 0  # Should default to 0
        }
        assert result == expected


class TestErrorHandling:
    """Test error handling and retry logic."""
    
    @patch('fleet_q.snowflake_storage.snowflake.connector.connect')
    async def test_connection_retry_on_failure(self, mock_connect, storage):
        """Test that connection failures trigger retries."""
        from snowflake.connector.errors import OperationalError
        
        # Mock connection that will succeed on second attempt
        mock_connection = Mock()
        cursor = Mock()
        cursor.execute = Mock()
        cursor.close = Mock()
        mock_connection.cursor.return_value = cursor
        mock_connection.commit = Mock()
        
        # First call fails with Snowflake error, second succeeds
        mock_connect.side_effect = [OperationalError("Connection failed"), mock_connection]
        
        # Mock the sleep to speed up test
        with patch('asyncio.sleep'):
            await storage.initialize()
        
        # Should have retried the connection (2 calls total)
        assert mock_connect.call_count == 2
    
    async def test_operation_rollback_on_error(self, storage, mock_connection):
        """Test that operations rollback on error."""
        storage.connection = mock_connection
        cursor = mock_connection.cursor.return_value
        cursor.execute.side_effect = Exception("Database error")
        
        step = Step(
            step_id="test-step",
            status=StepStatus.PENDING,
            claimed_by=None,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        with pytest.raises(StorageError):
            await storage.create_step(step)
        
        # Should have rolled back the transaction
        mock_connection.rollback.assert_called_once()


class TestDataConversion:
    """Test data conversion helper methods."""
    
    def test_row_to_step_conversion(self, storage):
        """Test conversion of database row to Step object."""
        mock_row = {
            'STEP_ID': 'test-step-123',
            'STATUS': 'claimed',
            'CLAIMED_BY': 'test-pod-456',
            'LAST_UPDATE_TS': datetime(2024, 1, 1, 12, 0, 0),
            'PAYLOAD': '{"type": "test_task", "args": {"param": "value"}, "metadata": {"info": "test"}}',
            'RETRY_COUNT': 1,
            'PRIORITY': 5,
            'CREATED_TS': datetime(2024, 1, 1, 11, 0, 0),
            'MAX_RETRIES': 3
        }
        
        result = storage._row_to_step(mock_row)
        
        assert isinstance(result, Step)
        assert result.step_id == 'test-step-123'
        assert result.status == StepStatus.CLAIMED
        assert result.claimed_by == 'test-pod-456'
        assert result.last_update_ts == datetime(2024, 1, 1, 12, 0, 0)
        assert result.payload.type == 'test_task'
        assert result.payload.args == {"param": "value"}
        assert result.payload.metadata == {"info": "test"}
        assert result.retry_count == 1
        assert result.priority == 5
        assert result.created_ts == datetime(2024, 1, 1, 11, 0, 0)
        assert result.max_retries == 3
    
    def test_row_to_pod_health_conversion(self, storage):
        """Test conversion of database row to PodHealth object."""
        mock_row = {
            'POD_ID': 'test-pod-123',
            'BIRTH_TS': datetime(2024, 1, 1, 10, 0, 0),
            'LAST_HEARTBEAT_TS': datetime(2024, 1, 1, 12, 0, 0),
            'STATUS': 'up',
            'META': '{"cpu_count": 4, "version": "1.0.0"}'
        }
        
        result = storage._row_to_pod_health(mock_row)
        
        assert isinstance(result, PodHealth)
        assert result.pod_id == 'test-pod-123'
        assert result.birth_ts == datetime(2024, 1, 1, 10, 0, 0)
        assert result.last_heartbeat_ts == datetime(2024, 1, 1, 12, 0, 0)
        assert result.status == PodStatus.UP
        assert result.meta == {"cpu_count": 4, "version": "1.0.0"}
    
    def test_row_to_step_with_dict_payload(self, storage):
        """Test conversion when payload is already a dict (from Snowflake VARIANT)."""
        mock_row = {
            'STEP_ID': 'test-step-456',
            'STATUS': 'pending',
            'CLAIMED_BY': None,
            'LAST_UPDATE_TS': datetime(2024, 1, 1, 12, 0, 0),
            'PAYLOAD': {"type": "test_task", "args": {"param": "value"}, "metadata": {}},  # Already a dict
            'RETRY_COUNT': 0,
            'PRIORITY': 0,
            'CREATED_TS': datetime(2024, 1, 1, 12, 0, 0),
            'MAX_RETRIES': 3
        }
        
        result = storage._row_to_step(mock_row)
        
        assert result.payload.type == 'test_task'
        assert result.payload.args == {"param": "value"}
        assert result.payload.metadata == {}
    
    def test_row_to_pod_health_with_dict_meta(self, storage):
        """Test conversion when meta is already a dict (from Snowflake VARIANT)."""
        mock_row = {
            'POD_ID': 'test-pod-456',
            'BIRTH_TS': datetime(2024, 1, 1, 10, 0, 0),
            'LAST_HEARTBEAT_TS': datetime(2024, 1, 1, 12, 0, 0),
            'STATUS': 'up',
            'META': {"cpu_count": 2, "threads": 8}  # Already a dict
        }
        
        result = storage._row_to_pod_health(mock_row)
        
        assert result.meta == {"cpu_count": 2, "threads": 8}