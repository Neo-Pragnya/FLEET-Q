"""
Tests for the claim service.

Tests the claim loop functionality including capacity calculation,
step claiming, and execution management.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

from fleet_q.claim_service import ClaimService, create_claim_service
from fleet_q.config import FleetQConfig
from fleet_q.models import Step, StepPayload, StepStatus


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    return FleetQConfig(
        snowflake_account="test",
        snowflake_user="test",
        snowflake_password="test",
        snowflake_database="test",
        max_parallelism=10,
        capacity_threshold=0.8,
        claim_interval_seconds=1  # Short interval for testing
    )


@pytest.fixture
def mock_storage():
    """Create a mock storage interface for testing."""
    storage = Mock()
    storage.claim_steps = AsyncMock(return_value=[])
    storage.complete_step = AsyncMock()
    storage.fail_step = AsyncMock()
    return storage


@pytest.fixture
def claim_service(mock_storage, mock_config):
    """Create a claim service instance for testing."""
    return ClaimService(mock_storage, mock_config)


class TestClaimService:
    """Test cases for the ClaimService class."""
    
    def test_initialization(self, claim_service, mock_config):
        """Test claim service initialization."""
        assert claim_service.pod_id == mock_config.pod_id
        assert claim_service.config == mock_config
        assert not claim_service.is_running
        assert claim_service.executing_step_count == 0
    
    def test_calculate_available_capacity(self, claim_service):
        """Test capacity calculation logic."""
        # Initially no executing steps
        assert claim_service.calculate_available_capacity() == 8  # 10 * 0.8
        
        # Add some executing steps
        claim_service._executing_steps.add("step1")
        claim_service._executing_steps.add("step2")
        assert claim_service.calculate_available_capacity() == 6  # 8 - 2
        
        # Fill to capacity
        for i in range(3, 9):  # Add 6 more steps
            claim_service._executing_steps.add(f"step{i}")
        assert claim_service.calculate_available_capacity() == 0
        
        # Over capacity
        claim_service._executing_steps.add("step9")
        assert claim_service.calculate_available_capacity() == 0  # Never negative
    
    @pytest.mark.asyncio
    async def test_attempt_claim_steps_no_capacity(self, claim_service, mock_storage):
        """Test claiming when no capacity is available."""
        result = await claim_service.attempt_claim_steps(0)
        assert result == []
        mock_storage.claim_steps.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_attempt_claim_steps_success(self, claim_service, mock_storage):
        """Test successful step claiming."""
        # Create mock steps
        mock_steps = [
            Step(
                step_id="step1",
                status=StepStatus.CLAIMED,
                claimed_by=claim_service.pod_id,
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=0,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            )
        ]
        
        mock_storage.claim_steps.return_value = mock_steps
        
        result = await claim_service.attempt_claim_steps(2)
        assert result == mock_steps
        mock_storage.claim_steps.assert_called_once_with(claim_service.pod_id, 2)
    
    @pytest.mark.asyncio
    async def test_attempt_claim_steps_failure_with_retry(self, claim_service, mock_storage):
        """Test claim failure with backoff retry."""
        # Mock storage to fail then succeed
        mock_storage.claim_steps.side_effect = [
            Exception("Database error"),
            Exception("Another error"),
            []  # Success on third attempt
        ]
        
        result = await claim_service.attempt_claim_steps(1)
        assert result == []
        assert mock_storage.claim_steps.call_count == 3
    
    @pytest.mark.asyncio
    async def test_execute_step_with_lifecycle_success(self, claim_service, mock_storage):
        """Test successful step execution."""
        step = Step(
            step_id="test_step",
            status=StepStatus.CLAIMED,
            claimed_by=claim_service.pod_id,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={"key": "value"}, metadata={}),
            retry_count=0,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        # Mock successful execution
        async def mock_executor(step):
            return {"status": "success"}
        
        claim_service.step_executor = mock_executor
        
        await claim_service.execute_step_with_lifecycle(step)
        
        # Verify step was completed
        mock_storage.complete_step.assert_called_once_with(
            "test_step", 
            {"status": "success"}
        )
        mock_storage.fail_step.assert_not_called()
        
        # Verify step is no longer in executing set
        assert "test_step" not in claim_service._executing_steps
    
    @pytest.mark.asyncio
    async def test_execute_step_with_lifecycle_failure(self, claim_service, mock_storage):
        """Test step execution failure."""
        step = Step(
            step_id="test_step",
            status=StepStatus.CLAIMED,
            claimed_by=claim_service.pod_id,
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        # Mock failing execution
        async def mock_executor(step):
            raise ValueError("Execution failed")
        
        claim_service.step_executor = mock_executor
        
        await claim_service.execute_step_with_lifecycle(step)
        
        # Verify step was marked as failed
        mock_storage.fail_step.assert_called_once()
        call_args = mock_storage.fail_step.call_args
        assert call_args[0][0] == "test_step"  # step_id
        assert "Execution failed" in call_args[0][1]  # error_message
        
        mock_storage.complete_step.assert_not_called()
        
        # Verify step is no longer in executing set
        assert "test_step" not in claim_service._executing_steps
    
    @pytest.mark.asyncio
    async def test_process_claimed_steps(self, claim_service):
        """Test processing of claimed steps."""
        steps = [
            Step(
                step_id=f"step{i}",
                status=StepStatus.CLAIMED,
                claimed_by=claim_service.pod_id,
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=0,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            )
            for i in range(3)
        ]
        
        await claim_service.process_claimed_steps(steps)
        
        # Verify tasks were created for each step
        assert len(claim_service._execution_tasks) == 3
        for i in range(3):
            assert f"step{i}" in claim_service._execution_tasks
    
    @pytest.mark.asyncio
    async def test_start_and_stop(self, claim_service):
        """Test service start and stop lifecycle."""
        assert not claim_service.is_running
        
        # Start the service
        await claim_service.start()
        assert claim_service.is_running
        assert claim_service._claim_task is not None
        
        # Stop the service
        await claim_service.stop()
        assert not claim_service.is_running
        assert claim_service._claim_task is None
    
    @pytest.mark.asyncio
    async def test_start_already_running(self, claim_service):
        """Test starting service when already running."""
        await claim_service.start()
        
        with pytest.raises(RuntimeError, match="already running"):
            await claim_service.start()
        
        await claim_service.stop()
    
    @pytest.mark.asyncio
    async def test_get_claim_statistics(self, claim_service):
        """Test claim statistics retrieval."""
        # Set some test statistics
        claim_service._claim_attempts = 10
        claim_service._successful_claims = 8
        claim_service._total_steps_claimed = 15
        claim_service._total_steps_completed = 12
        claim_service._total_steps_failed = 3
        claim_service._executing_steps.add("step1")
        claim_service._executing_steps.add("step2")
        
        stats = await claim_service.get_claim_statistics()
        
        assert stats['service_status'] == 'stopped'
        assert stats['pod_id'] == claim_service.pod_id
        assert stats['claim_attempts'] == 10
        assert stats['successful_claims'] == 8
        assert stats['success_rate_percent'] == 80.0
        assert stats['total_steps_claimed'] == 15
        assert stats['total_steps_completed'] == 12
        assert stats['total_steps_failed'] == 3
        assert stats['currently_executing'] == 2
        assert sorted(stats['executing_step_ids']) == sorted(['step1', 'step2'])
        assert stats['available_capacity'] == 6  # 8 - 2
    
    def test_factory_function(self, mock_storage, mock_config):
        """Test the factory function."""
        service = create_claim_service(mock_storage, mock_config)
        assert isinstance(service, ClaimService)
        assert service.storage == mock_storage
        assert service.config == mock_config
    
    def test_properties(self, claim_service):
        """Test service properties."""
        # Test executing_step_count
        assert claim_service.executing_step_count == 0
        claim_service._executing_steps.add("step1")
        assert claim_service.executing_step_count == 1
        
        # Test executing_step_ids
        assert claim_service.executing_step_ids == ["step1"]
        claim_service._executing_steps.add("step2")
        assert set(claim_service.executing_step_ids) == {"step1", "step2"}


class TestClaimServiceIntegration:
    """Integration tests for the claim service."""
    
    @pytest.mark.asyncio
    async def test_claim_loop_integration(self, mock_storage, mock_config):
        """Test the complete claim loop integration."""
        # Create steps to be claimed
        mock_steps = [
            Step(
                step_id="integration_step",
                status=StepStatus.CLAIMED,
                claimed_by="test_pod",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=0,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            )
        ]
        
        mock_storage.claim_steps.return_value = mock_steps
        
        # Create service with very short interval
        config = mock_config
        config.claim_interval_seconds = 0.1
        
        service = ClaimService(mock_storage, config)
        
        # Start service
        await service.start()
        
        # Let it run for a short time
        await asyncio.sleep(0.3)
        
        # Stop service
        await service.stop()
        
        # Verify claims were attempted
        assert mock_storage.claim_steps.call_count >= 1
        assert service._claim_attempts >= 1