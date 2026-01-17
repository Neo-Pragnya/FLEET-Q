"""
Tests for administrative endpoints in FLEET-Q API.

Tests the administrative endpoints including leader information,
queue statistics, and manual recovery functionality.
"""

import pytest
from unittest.mock import AsyncMock, Mock
from datetime import datetime

from fastapi.testclient import TestClient

from fleet_q.api import create_app
from fleet_q.config import FleetQConfig
from fleet_q.models import RecoverySummary


class TestAdminEndpoints:
    """Test suite for administrative API endpoints."""
    
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
    def mock_sqlite_storage(self):
        """Create mock SQLite storage backend."""
        sqlite_storage = AsyncMock()
        return sqlite_storage
    
    @pytest.fixture
    def app(self, mock_storage, config, mock_sqlite_storage):
        """Create FastAPI test application."""
        return create_app(mock_storage, config, mock_sqlite_storage)
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app)
    
    def test_leader_info_endpoint(self, client, mock_storage):
        """Test leader information endpoint."""
        # Mock leader info
        mock_storage.get_leader_info.return_value = {
            'current_leader': 'pod-leader-123',
            'leader_birth_ts': datetime(2024, 1, 1, 12, 0, 0),
            'active_pods_count': 5,
            'election_timestamp': datetime(2024, 1, 1, 12, 5, 0)
        }
        
        response = client.get("/admin/leader")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["current_leader"] == "pod-leader-123"
        assert data["active_pods"] == 5
        assert "leader_birth_ts" in data
        assert "last_election_check" in data
    
    def test_leader_info_no_leader(self, client, mock_storage):
        """Test leader information endpoint when no leader exists."""
        # Mock no leader scenario
        mock_storage.get_leader_info.return_value = {
            'current_leader': None,
            'leader_birth_ts': None,
            'active_pods_count': 0,
            'election_timestamp': datetime(2024, 1, 1, 12, 5, 0)
        }
        
        response = client.get("/admin/leader")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["current_leader"] is None
        assert data["active_pods"] == 0
        assert data["leader_birth_ts"] is None
    
    def test_manual_recovery_as_leader(self, client, mock_storage, mock_sqlite_storage):
        """Test manual recovery endpoint when called by leader."""
        # Mock this pod as leader
        mock_storage.am_i_leader.return_value = True
        
        # Mock recovery summary
        recovery_summary = RecoverySummary(
            cycle_id="recovery_123456789",
            dead_pods=["pod-dead-1", "pod-dead-2"],
            orphaned_steps_found=10,
            steps_requeued=8,
            steps_marked_terminal=2,
            recovery_duration_ms=1500
        )
        
        # We need to mock the recovery service creation and execution
        # This is a bit complex due to the dynamic import, so we'll mock at the module level
        import fleet_q.recovery_service
        
        with pytest.MonkeyPatch().context() as m:
            # Mock the recovery service
            mock_recovery_service = AsyncMock()
            mock_recovery_service.run_recovery_cycle.return_value = recovery_summary
            
            # Mock the create_recovery_service function
            def mock_create_recovery_service(storage, sqlite_storage, config):
                return mock_recovery_service
            
            m.setattr(fleet_q.recovery_service, "create_recovery_service", mock_create_recovery_service)
            
            response = client.post("/admin/recovery/run")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "completed"
        assert "recovery_summary" in data
        assert data["recovery_summary"]["dead_pods_found"] == 2
        assert data["recovery_summary"]["orphaned_steps_found"] == 10
        assert data["recovery_summary"]["steps_requeued"] == 8
        assert data["recovery_summary"]["steps_marked_terminal"] == 2
    
    def test_manual_recovery_not_leader(self, client, mock_storage):
        """Test manual recovery endpoint when called by non-leader."""
        # Mock this pod as not leader
        mock_storage.am_i_leader.return_value = False
        mock_storage.determine_leader.return_value = "pod-leader-456"
        
        response = client.post("/admin/recovery/run")
        
        assert response.status_code == 403
        data = response.json()
        
        assert "leader pod" in data["detail"]
        assert "pod-leader-456" in data["detail"]
    
    def test_manual_recovery_no_leader(self, client, mock_storage):
        """Test manual recovery endpoint when no leader exists."""
        # Mock this pod as not leader and no leader exists
        mock_storage.am_i_leader.return_value = False
        mock_storage.determine_leader.return_value = None
        
        response = client.post("/admin/recovery/run")
        
        assert response.status_code == 403
        data = response.json()
        
        assert "leader pod" in data["detail"]
        assert "none" in data["detail"]
    
    def test_health_endpoint_with_leader_status(self, client, mock_storage):
        """Test health endpoint includes leader status."""
        # Mock leader status
        mock_storage.am_i_leader.return_value = True
        
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["is_leader"] is True
        assert "pod_id" in data
        assert "status" in data
        assert "uptime_seconds" in data
        assert "current_capacity" in data
        assert "max_capacity" in data
        assert "version" in data
    
    def test_health_endpoint_not_leader(self, client, mock_storage):
        """Test health endpoint when pod is not leader."""
        # Mock not leader status
        mock_storage.am_i_leader.return_value = False
        
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["is_leader"] is False