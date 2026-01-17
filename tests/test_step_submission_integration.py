"""
Integration tests for step submission functionality.

Tests the complete step submission flow from API request to storage,
verifying that all components work together correctly.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient

from fleet_q.api import create_app
from fleet_q.models import StepSubmissionRequest, Step, StepStatus, StepPayload
from fleet_q.config import FleetQConfig
from fleet_q.storage import StorageError


class TestStepSubmissionIntegration:
    """Integration tests for step submission."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test"
        )
    
    @pytest.fixture
    def mock_storage(self):
        """Create mock storage backend."""
        storage = AsyncMock()
        return storage
    
    @pytest.fixture
    def app(self, mock_storage, config):
        """Create FastAPI test application."""
        return create_app(mock_storage, config)
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app)
    
    def test_submit_step_success(self, client, mock_storage):
        """Test successful step submission via API."""
        # Mock storage to succeed
        mock_storage.create_step.return_value = None
        
        # Submit step via API
        response = client.post("/submit", json={
            "type": "test_task",
            "args": {"param1": "value1", "param2": 42},
            "metadata": {"source": "integration_test"},
            "priority": 5,
            "max_retries": 3
        })
        
        # Should return success
        assert response.status_code == 200
        data = response.json()
        
        assert "step_id" in data
        assert data["status"] == "submitted"
        assert "submitted successfully" in data["message"]
        assert data["step_id"].startswith("step_")
        
        # Storage should have been called
        mock_storage.create_step.assert_called_once()
        
        # Verify the step object passed to storage
        call_args = mock_storage.create_step.call_args[0]
        step = call_args[0]
        
        assert isinstance(step, Step)
        assert step.step_id == data["step_id"]
        assert step.status == StepStatus.PENDING
        assert step.payload.type == "test_task"
        assert step.payload.args == {"param1": "value1", "param2": 42}
        assert step.payload.metadata == {"source": "integration_test"}
        assert step.priority == 5
        assert step.max_retries == 3
        assert step.retry_count == 0
        assert step.claimed_by is None
    
    def test_submit_step_validation_error(self, client, mock_storage):
        """Test step submission with validation error."""
        # Submit invalid step (empty type)
        response = client.post("/submit", json={
            "type": "",  # Invalid empty type
            "args": {},
            "metadata": {}
        })
        
        # Should return validation error
        assert response.status_code == 400
        data = response.json()
        assert "Step type is required" in data["detail"]
        
        # Storage should not have been called
        mock_storage.create_step.assert_not_called()
    
    def test_submit_step_storage_error(self, client, mock_storage):
        """Test step submission with storage error."""
        # Mock storage to fail
        mock_storage.create_step.side_effect = StorageError("Database connection failed")
        
        # Submit valid step
        response = client.post("/submit", json={
            "type": "test_task",
            "args": {},
            "metadata": {}
        })
        
        # Should return server error
        assert response.status_code == 500
        data = response.json()
        assert "Step submission failed" in data["detail"]
    
    def test_get_step_status_success(self, client, mock_storage):
        """Test successful step status retrieval."""
        step_id = "test_step_123"
        test_step = Step(
            step_id=step_id,
            status=StepStatus.PENDING,
            claimed_by=None,
            last_update_ts=datetime(2024, 1, 1, 12, 0, 0),
            payload=StepPayload(type="test_task", args={"param": "value"}, metadata={}),
            retry_count=0,
            priority=5,
            created_ts=datetime(2024, 1, 1, 12, 0, 0),
            max_retries=3
        )
        
        mock_storage.get_step.return_value = test_step
        
        response = client.get(f"/status/{step_id}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["step_id"] == step_id
        assert data["status"] == "pending"
        assert data["claimed_by"] is None
        assert data["retry_count"] == 0
        assert data["priority"] == 5
        assert data["max_retries"] == 3
        
        mock_storage.get_step.assert_called_once_with(step_id)
    
    def test_get_step_status_not_found(self, client, mock_storage):
        """Test step status retrieval for non-existent step."""
        step_id = "nonexistent_step"
        
        mock_storage.get_step.return_value = None
        
        response = client.get(f"/status/{step_id}")
        
        assert response.status_code == 404
        data = response.json()
        assert f"Step {step_id} not found" in data["detail"]
    
    def test_get_idempotency_key_success(self, client, mock_storage):
        """Test successful idempotency key retrieval."""
        step_id = "test_step_123"
        
        # Mock step exists
        mock_storage.get_step.return_value = Mock()  # Non-None indicates step exists
        
        response = client.get(f"/idempotency/{step_id}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["step_id"] == step_id
        assert data["idempotency_key"] == f"idempotent_{step_id}"
    
    def test_get_idempotency_key_not_found(self, client, mock_storage):
        """Test idempotency key retrieval for non-existent step."""
        step_id = "nonexistent_step"
        
        # Mock step doesn't exist
        mock_storage.get_step.return_value = None
        
        response = client.get(f"/idempotency/{step_id}")
        
        assert response.status_code == 404
        data = response.json()
        assert f"Step {step_id} not found" in data["detail"]
    
    def test_health_endpoint(self, client, mock_storage):
        """Test health endpoint."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "pod_id" in data
        assert data["status"] == "healthy"
        assert "uptime_seconds" in data
        assert data["max_capacity"] > 0
        assert "version" in data
    
    def test_queue_stats_endpoint(self, client, mock_storage):
        """Test queue statistics endpoint."""
        # Mock queue stats
        mock_storage.get_queue_stats.return_value = {
            'pending': 10,
            'claimed': 5,
            'completed': 20,
            'failed': 2
        }
        
        # Mock leader info for the queue stats endpoint
        mock_storage.get_leader_info.return_value = {
            'current_leader': 'test-pod-123',
            'active_pods_count': 3,
            'leader_birth_ts': None,
            'election_timestamp': None
        }
        
        response = client.get("/admin/queue")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["pending_steps"] == 10
        assert data["claimed_steps"] == 5
        assert data["completed_steps"] == 20
        assert data["failed_steps"] == 2
        assert data["active_pods"] == 3
        assert data["current_leader"] == "test-pod-123"
    
    def test_submit_step_with_defaults(self, client, mock_storage):
        """Test step submission with default values."""
        mock_storage.create_step.return_value = None
        
        # Submit minimal step (only required fields)
        response = client.post("/submit", json={
            "type": "simple_task"
        })
        
        assert response.status_code == 200
        
        # Verify defaults were applied
        call_args = mock_storage.create_step.call_args[0]
        step = call_args[0]
        
        assert step.payload.args == {}  # Default empty dict
        assert step.payload.metadata == {}  # Default empty dict
        assert step.priority == 0  # Default priority
        assert step.max_retries == 3  # Default max retries
    
    def test_submit_step_invalid_json(self, client, mock_storage):
        """Test step submission with invalid JSON."""
        response = client.post("/submit", data="invalid json")
        
        # Should return 422 for invalid JSON
        assert response.status_code == 422
    
    def test_submit_step_missing_required_field(self, client, mock_storage):
        """Test step submission missing required type field."""
        response = client.post("/submit", json={
            "args": {},
            "metadata": {}
            # Missing required "type" field
        })
        
        # Should return 422 for missing required field
        assert response.status_code == 422


class TestStepSubmissionEndToEnd:
    """End-to-end tests using real storage abstractions."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test"
        )
    
    @pytest.mark.asyncio
    async def test_step_submission_flow(self, config):
        """Test complete step submission flow with real service objects."""
        from fleet_q.step_service import StepService
        from fleet_q.models import StepSubmissionRequest
        
        # Create mock storage
        mock_storage = AsyncMock()
        mock_storage.create_step.return_value = None
        
        # Create step service
        step_service = StepService(mock_storage, config)
        
        # Create submission request
        request = StepSubmissionRequest(
            type="integration_test_task",
            args={"test_param": "test_value"},
            metadata={"test_source": "end_to_end"},
            priority=10,
            max_retries=5
        )
        
        # Submit step
        step_id = await step_service.submit_step(request)
        
        # Verify step ID format
        assert step_id.startswith("step_")
        parts = step_id.split("_")
        assert len(parts) == 3
        
        # Verify storage was called correctly
        mock_storage.create_step.assert_called_once()
        
        # Verify step object
        call_args = mock_storage.create_step.call_args[0]
        step = call_args[0]
        
        assert step.step_id == step_id
        assert step.status == StepStatus.PENDING
        assert step.payload.type == "integration_test_task"
        assert step.payload.args == {"test_param": "test_value"}
        assert step.payload.metadata == {"test_source": "end_to_end"}
        assert step.priority == 10
        assert step.max_retries == 5
        assert step.retry_count == 0
        assert step.claimed_by is None
        assert step.created_ts is not None
        assert step.last_update_ts is not None
        
        # Test idempotency key generation
        idempotency_key = step_service.get_idempotency_key(step_id)
        assert idempotency_key == f"idempotent_{step_id}"