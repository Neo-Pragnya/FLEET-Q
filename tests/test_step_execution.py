"""
Unit tests for step execution functionality.

Tests the step execution logic including state transitions, error handling,
and proper timestamp updates for completed and failed steps.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, Mock

from fleet_q.step_service import StepService, StepSubmissionError
from fleet_q.models import Step, StepPayload, StepStatus
from fleet_q.config import FleetQConfig
from fleet_q.storage import StorageError


class TestStepExecution:
    """Test step execution functionality."""
    
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
    
    @pytest.fixture
    def sample_step(self):
        """Create a sample step for testing."""
        return Step(
            step_id="test_step_123",
            status=StepStatus.CLAIMED,
            claimed_by="test_pod",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(
                type="test_task",
                args={"param": "value"},
                metadata={"source": "test"}
            ),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
    
    @pytest.mark.asyncio
    async def test_complete_step_success(self, step_service, mock_storage):
        """Test successful step completion."""
        step_id = "test_step_123"
        result_metadata = {"result": "success", "duration_ms": 1500}
        
        # Mock storage complete_step to succeed
        mock_storage.complete_step.return_value = None
        
        await step_service.complete_step(step_id, result_metadata)
        
        # Should have called storage.complete_step
        mock_storage.complete_step.assert_called_once_with(step_id, result_metadata)
    
    @pytest.mark.asyncio
    async def test_complete_step_without_metadata(self, step_service, mock_storage):
        """Test step completion without result metadata."""
        step_id = "test_step_123"
        
        mock_storage.complete_step.return_value = None
        
        await step_service.complete_step(step_id)
        
        mock_storage.complete_step.assert_called_once_with(step_id, None)
    
    @pytest.mark.asyncio
    async def test_complete_step_storage_error(self, step_service, mock_storage):
        """Test step completion with storage error."""
        step_id = "test_step_123"
        
        mock_storage.complete_step.side_effect = StorageError("Database error")
        
        with pytest.raises(StepSubmissionError, match="Step completion failed"):
            await step_service.complete_step(step_id)
    
    @pytest.mark.asyncio
    async def test_fail_step_success(self, step_service, mock_storage):
        """Test successful step failure."""
        step_id = "test_step_123"
        error_message = "Task execution failed"
        error_metadata = {"error_code": "EXEC_001", "retry_count": 1}
        
        mock_storage.fail_step.return_value = None
        
        await step_service.fail_step(step_id, error_message, error_metadata)
        
        mock_storage.fail_step.assert_called_once_with(step_id, error_message, error_metadata)
    
    @pytest.mark.asyncio
    async def test_fail_step_without_error_info(self, step_service, mock_storage):
        """Test step failure without error information."""
        step_id = "test_step_123"
        
        mock_storage.fail_step.return_value = None
        
        await step_service.fail_step(step_id)
        
        mock_storage.fail_step.assert_called_once_with(step_id, None, None)
    
    @pytest.mark.asyncio
    async def test_fail_step_storage_error(self, step_service, mock_storage):
        """Test step failure with storage error."""
        step_id = "test_step_123"
        
        mock_storage.fail_step.side_effect = StorageError("Database error")
        
        with pytest.raises(StepSubmissionError, match="Step failure update failed"):
            await step_service.fail_step(step_id)
    
    @pytest.mark.asyncio
    async def test_execute_step_success(self, step_service, mock_storage, sample_step):
        """Test successful step execution."""
        # Mock execution function
        async def mock_execution_func(step):
            return {"result": "success", "processed_items": 42}
        
        mock_storage.complete_step.return_value = None
        
        await step_service.execute_step(sample_step, mock_execution_func)
        
        # Should have called complete_step with result metadata
        mock_storage.complete_step.assert_called_once_with(
            sample_step.step_id, 
            {"result": "success", "processed_items": 42}
        )
    
    @pytest.mark.asyncio
    async def test_execute_step_success_no_result(self, step_service, mock_storage, sample_step):
        """Test successful step execution with no return value."""
        # Mock execution function that returns None
        async def mock_execution_func(step):
            return None
        
        mock_storage.complete_step.return_value = None
        
        await step_service.execute_step(sample_step, mock_execution_func)
        
        # Should have called complete_step with None metadata
        mock_storage.complete_step.assert_called_once_with(sample_step.step_id, None)
    
    @pytest.mark.asyncio
    async def test_execute_step_success_string_result(self, step_service, mock_storage, sample_step):
        """Test successful step execution with string result."""
        # Mock execution function that returns a string
        async def mock_execution_func(step):
            return "Task completed successfully"
        
        mock_storage.complete_step.return_value = None
        
        await step_service.execute_step(sample_step, mock_execution_func)
        
        # Should have called complete_step with string result wrapped in metadata
        mock_storage.complete_step.assert_called_once_with(
            sample_step.step_id, 
            {"result": "Task completed successfully"}
        )
    
    @pytest.mark.asyncio
    async def test_execute_step_execution_failure(self, step_service, mock_storage, sample_step):
        """Test step execution with execution function failure."""
        # Mock execution function that raises an exception
        async def mock_execution_func(step):
            raise ValueError("Invalid input parameter")
        
        mock_storage.fail_step.return_value = None
        
        with pytest.raises(StepSubmissionError, match="Step execution failed"):
            await step_service.execute_step(sample_step, mock_execution_func)
        
        # Should have called fail_step with error information
        mock_storage.fail_step.assert_called_once_with(
            sample_step.step_id,
            "Invalid input parameter",
            {
                "error_type": "ValueError",
                "error_message": "Invalid input parameter",
                "retry_count": 0
            }
        )
    
    @pytest.mark.asyncio
    async def test_execute_step_with_args_kwargs(self, step_service, mock_storage, sample_step):
        """Test step execution with additional arguments."""
        # Mock execution function that uses additional arguments
        async def mock_execution_func(step, extra_param, keyword_param=None):
            return {
                "step_id": step.step_id,
                "extra_param": extra_param,
                "keyword_param": keyword_param
            }
        
        mock_storage.complete_step.return_value = None
        
        await step_service.execute_step(
            sample_step, 
            mock_execution_func, 
            "extra_value",
            keyword_param="keyword_value"
        )
        
        # Should have called complete_step with the function result
        expected_result = {
            "step_id": sample_step.step_id,
            "extra_param": "extra_value",
            "keyword_param": "keyword_value"
        }
        mock_storage.complete_step.assert_called_once_with(sample_step.step_id, expected_result)
    
    @pytest.mark.asyncio
    async def test_execute_step_fail_step_error(self, step_service, mock_storage, sample_step):
        """Test step execution where both execution and fail_step fail."""
        # Mock execution function that raises an exception
        async def mock_execution_func(step):
            raise RuntimeError("Execution failed")
        
        # Mock fail_step to also fail
        mock_storage.fail_step.side_effect = StorageError("Database error")
        
        # Should still raise the original execution error
        with pytest.raises(StepSubmissionError, match="Step execution failed"):
            await step_service.execute_step(sample_step, mock_execution_func)
        
        # Should have attempted to call fail_step
        mock_storage.fail_step.assert_called_once()


class TestStepExecutionEdgeCases:
    """Test edge cases for step execution."""
    
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
    
    @pytest.mark.asyncio
    async def test_execute_step_sync_function(self, step_service):
        """Test step execution with synchronous function."""
        # Mock synchronous execution function
        def sync_execution_func(step):
            return {"sync_result": True}
        
        sample_step = Step(
            step_id="sync_step",
            status=StepStatus.CLAIMED,
            claimed_by="test_pod",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="sync_task", args={}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        step_service.storage.complete_step.return_value = None
        
        # Should handle sync function (though in practice we expect async)
        await step_service.execute_step(sample_step, sync_execution_func)
        
        step_service.storage.complete_step.assert_called_once_with(
            "sync_step", 
            {"sync_result": True}
        )
    
    @pytest.mark.asyncio
    async def test_complete_step_empty_step_id(self, step_service):
        """Test step completion with empty step ID."""
        step_service.storage.complete_step.side_effect = StorageError("Step not found")
        
        with pytest.raises(StepSubmissionError):
            await step_service.complete_step("")
    
    @pytest.mark.asyncio
    async def test_fail_step_empty_step_id(self, step_service):
        """Test step failure with empty step ID."""
        step_service.storage.fail_step.side_effect = StorageError("Step not found")
        
        with pytest.raises(StepSubmissionError):
            await step_service.fail_step("")