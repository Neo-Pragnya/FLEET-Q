"""
Unit tests for step submission service.

Tests the core step submission logic including ID generation,
payload validation, and storage operations.
"""

import pytest
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

from fleet_q.step_service import (
    StepService, 
    StepSubmissionError, 
    StepValidationError,
    create_step_service
)
from fleet_q.models import StepSubmissionRequest, StepPayload, Step, StepStatus
from fleet_q.config import FleetQConfig
from fleet_q.storage import StorageError


class TestStepService:
    """Test step service functionality."""
    
    @pytest.fixture
    def mock_storage(self):
        """Create a mock storage backend."""
        storage = AsyncMock()
        return storage
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test"
        )
    
    @pytest.fixture
    def step_service(self, mock_storage, config):
        """Create a step service instance."""
        return StepService(mock_storage, config)
    
    def test_generate_step_id(self, step_service):
        """Test step ID generation."""
        step_id = step_service.generate_step_id()
        
        # Should start with "step_"
        assert step_id.startswith("step_")
        
        # Should contain timestamp and UUID parts
        parts = step_id.split("_")
        assert len(parts) == 3
        assert parts[0] == "step"
        assert len(parts[1]) == 14  # YYYYMMDDHHMMSS
        assert len(parts[2]) == 8   # First 8 chars of UUID hex
        
        # Should be unique
        step_id2 = step_service.generate_step_id()
        assert step_id != step_id2
    
    def test_validate_step_payload_success(self, step_service):
        """Test successful payload validation."""
        payload = StepPayload(
            type="test_task",
            args={"param1": "value1", "param2": 42},
            metadata={"source": "test", "priority": "high"}
        )
        
        # Should not raise any exception
        step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_empty_type(self, step_service):
        """Test payload validation with empty type."""
        payload = StepPayload(type="", args={}, metadata={})
        
        with pytest.raises(StepValidationError, match="Step type is required"):
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_invalid_type_format(self, step_service):
        """Test payload validation with invalid type format."""
        payload = StepPayload(type="invalid@type!", args={}, metadata={})
        
        with pytest.raises(StepValidationError, match="must contain only alphanumeric"):
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_non_string_type(self, step_service):
        """Test payload validation with non-string type."""
        payload = StepPayload(type=123, args={}, metadata={})
        
        with pytest.raises(StepValidationError, match="Step type must be a string"):
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_invalid_args(self, step_service):
        """Test payload validation with invalid args."""
        payload = StepPayload(type="test", args="not_a_dict", metadata={})
        
        with pytest.raises(StepValidationError, match="Step args must be a dictionary"):
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_invalid_metadata(self, step_service):
        """Test payload validation with invalid metadata."""
        payload = StepPayload(type="test", args={}, metadata="not_a_dict")
        
        with pytest.raises(StepValidationError, match="Step metadata must be a dictionary"):
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_too_large(self, step_service):
        """Test payload validation with oversized payload."""
        # Create a large payload (over 1MB)
        large_data = "x" * (1024 * 1024 + 1)  # 1MB + 1 byte
        payload = StepPayload(
            type="test",
            args={"large_data": large_data},
            metadata={}
        )
        
        with pytest.raises(StepValidationError, match="Step payload too large"):
            step_service.validate_step_payload(payload)
    
    def test_create_step_payload_success(self, step_service):
        """Test successful step payload creation from request."""
        request = StepSubmissionRequest(
            type="test_task",
            args={"param": "value"},
            metadata={"source": "test"},
            priority=5,
            max_retries=3
        )
        
        payload = step_service.create_step_payload(request)
        
        assert payload.type == "test_task"
        assert payload.args == {"param": "value"}
        assert payload.metadata == {"source": "test"}
    
    def test_create_step_payload_validation_failure(self, step_service):
        """Test step payload creation with validation failure."""
        request = StepSubmissionRequest(
            type="",  # Invalid empty type
            args={},
            metadata={}
        )
        
        with pytest.raises(StepValidationError):
            step_service.create_step_payload(request)
    
    @pytest.mark.asyncio
    async def test_submit_step_success(self, step_service, mock_storage):
        """Test successful step submission."""
        request = StepSubmissionRequest(
            type="test_task",
            args={"param": "value"},
            metadata={"source": "test"},
            priority=5,
            max_retries=3
        )
        
        # Mock storage create_step to succeed
        mock_storage.create_step.return_value = None
        
        step_id = await step_service.submit_step(request)
        
        # Should return a valid step ID
        assert step_id.startswith("step_")
        
        # Should have called storage.create_step
        mock_storage.create_step.assert_called_once()
        
        # Check the step object passed to storage
        call_args = mock_storage.create_step.call_args[0]
        step = call_args[0]
        
        assert isinstance(step, Step)
        assert step.step_id == step_id
        assert step.status == StepStatus.PENDING
        assert step.payload.type == "test_task"
        assert step.priority == 5
        assert step.max_retries == 3
    
    @pytest.mark.asyncio
    async def test_submit_step_with_custom_id(self, step_service, mock_storage):
        """Test step submission with custom step ID."""
        request = StepSubmissionRequest(type="test_task", args={}, metadata={})
        custom_id = "custom_step_123"
        
        mock_storage.create_step.return_value = None
        
        step_id = await step_service.submit_step(request, step_id=custom_id)
        
        assert step_id == custom_id
        
        # Check the step object
        call_args = mock_storage.create_step.call_args[0]
        step = call_args[0]
        assert step.step_id == custom_id
    
    @pytest.mark.asyncio
    async def test_submit_step_validation_error(self, step_service, mock_storage):
        """Test step submission with validation error."""
        request = StepSubmissionRequest(
            type="",  # Invalid empty type
            args={},
            metadata={}
        )
        
        with pytest.raises(StepValidationError):
            await step_service.submit_step(request)
        
        # Storage should not be called
        mock_storage.create_step.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_submit_step_storage_error(self, step_service, mock_storage):
        """Test step submission with storage error."""
        request = StepSubmissionRequest(type="test_task", args={}, metadata={})
        
        # Mock storage to raise an error
        mock_storage.create_step.side_effect = StorageError("Database error")
        
        with pytest.raises(StepSubmissionError, match="Step submission failed"):
            await step_service.submit_step(request)
    
    @pytest.mark.asyncio
    async def test_get_step_status_success(self, step_service, mock_storage):
        """Test successful step status retrieval."""
        step_id = "test_step_123"
        expected_step = Step(
            step_id=step_id,
            status=StepStatus.PENDING,
            claimed_by=None,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        mock_storage.get_step.return_value = expected_step
        
        result = await step_service.get_step_status(step_id)
        
        assert result == expected_step
        mock_storage.get_step.assert_called_once_with(step_id)
    
    @pytest.mark.asyncio
    async def test_get_step_status_not_found(self, step_service, mock_storage):
        """Test step status retrieval when step not found."""
        step_id = "nonexistent_step"
        
        mock_storage.get_step.return_value = None
        
        result = await step_service.get_step_status(step_id)
        
        assert result is None
        mock_storage.get_step.assert_called_once_with(step_id)
    
    @pytest.mark.asyncio
    async def test_get_step_status_storage_error(self, step_service, mock_storage):
        """Test step status retrieval with storage error."""
        step_id = "test_step_123"
        
        mock_storage.get_step.side_effect = StorageError("Database error")
        
        with pytest.raises(StepSubmissionError, match="Step status retrieval failed"):
            await step_service.get_step_status(step_id)
    
    def test_get_idempotency_key(self, step_service):
        """Test idempotency key generation."""
        step_id = "test_step_123"
        
        key = step_service.get_idempotency_key(step_id)
        
        # Should be consistent
        key2 = step_service.get_idempotency_key(step_id)
        assert key == key2
        
        # Should be different for different step IDs
        key3 = step_service.get_idempotency_key("different_step")
        assert key != key3
        
        # Should follow expected format
        assert key == f"idempotent_{step_id}"
    
    @pytest.mark.asyncio
    async def test_validate_step_exists_true(self, step_service, mock_storage):
        """Test step existence validation when step exists."""
        step_id = "existing_step"
        mock_step = Mock()
        
        mock_storage.get_step.return_value = mock_step
        
        result = await step_service.validate_step_exists(step_id)
        
        assert result is True
        mock_storage.get_step.assert_called_once_with(step_id)
    
    @pytest.mark.asyncio
    async def test_validate_step_exists_false(self, step_service, mock_storage):
        """Test step existence validation when step doesn't exist."""
        step_id = "nonexistent_step"
        
        mock_storage.get_step.return_value = None
        
        result = await step_service.validate_step_exists(step_id)
        
        assert result is False
        mock_storage.get_step.assert_called_once_with(step_id)
    
    @pytest.mark.asyncio
    async def test_validate_step_exists_error(self, step_service, mock_storage):
        """Test step existence validation with storage error."""
        step_id = "test_step"
        
        mock_storage.get_step.side_effect = StorageError("Database error")
        
        result = await step_service.validate_step_exists(step_id)
        
        # Should return False on error
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_queue_statistics_success(self, step_service, mock_storage):
        """Test successful queue statistics retrieval."""
        expected_stats = {
            'pending': 10,
            'claimed': 5,
            'completed': 20,
            'failed': 2
        }
        
        mock_storage.get_queue_stats.return_value = expected_stats
        
        result = await step_service.get_queue_statistics()
        
        # Should include original stats plus total
        assert result['pending'] == 10
        assert result['claimed'] == 5
        assert result['completed'] == 20
        assert result['failed'] == 2
        assert result['total_steps'] == 37  # Sum of all
        
        mock_storage.get_queue_stats.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_queue_statistics_error(self, step_service, mock_storage):
        """Test queue statistics retrieval with storage error."""
        mock_storage.get_queue_stats.side_effect = StorageError("Database error")
        
        with pytest.raises(StepSubmissionError, match="Queue statistics retrieval failed"):
            await step_service.get_queue_statistics()


class TestStepServiceFactory:
    """Test step service factory function."""
    
    def test_create_step_service(self):
        """Test step service factory function."""
        mock_storage = AsyncMock()
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test", 
            snowflake_password="test",
            snowflake_database="test"
        )
        
        service = create_step_service(mock_storage, config)
        
        assert isinstance(service, StepService)
        assert service.storage == mock_storage
        assert service.config == config


class TestStepValidationEdgeCases:
    """Test edge cases for step validation."""
    
    @pytest.fixture
    def step_service(self):
        """Create a step service for testing."""
        mock_storage = AsyncMock()
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test", 
            snowflake_database="test"
        )
        return StepService(mock_storage, config)
    
    def test_validate_step_payload_valid_type_formats(self, step_service):
        """Test various valid step type formats."""
        valid_types = [
            "simple_task",
            "complex-task",
            "task123",
            "UPPERCASE_TASK",
            "mixed_Case-Task123"
        ]
        
        for step_type in valid_types:
            payload = StepPayload(type=step_type, args={}, metadata={})
            # Should not raise exception
            step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_invalid_type_formats(self, step_service):
        """Test various invalid step type formats."""
        invalid_types = [
            "task with spaces",
            "task@symbol",
            "task.dot",
            "task/slash",
            "task\\backslash",
            "task:colon"
        ]
        
        for step_type in invalid_types:
            payload = StepPayload(type=step_type, args={}, metadata={})
            with pytest.raises(StepValidationError, match="must contain only alphanumeric"):
                step_service.validate_step_payload(payload)
    
    def test_validate_step_payload_non_serializable_data(self, step_service):
        """Test payload validation with non-serializable data."""
        # Create a payload with non-serializable data
        payload = StepPayload(
            type="test",
            args={"func": lambda x: x},  # Functions are not JSON serializable
            metadata={}
        )
        
        with pytest.raises(StepValidationError, match="non-serializable data"):
            step_service.validate_step_payload(payload)