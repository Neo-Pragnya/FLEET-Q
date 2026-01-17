"""
Tests for the health service module.

Tests pod health tracking functionality including heartbeat loops,
pod registration, and graceful shutdown.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from fleet_q.config import FleetQConfig
from fleet_q.health_service import HealthService, create_health_service
from fleet_q.models import PodHealth, PodStatus
from fleet_q.storage import StorageInterface


class MockStorage(StorageInterface):
    """Mock storage implementation for testing."""
    
    def __init__(self):
        self.pod_health_records = {}
        self.upsert_calls = []
        self.initialize_called = False
        self.close_called = False
    
    async def initialize(self):
        self.initialize_called = True
    
    async def close(self):
        self.close_called = True
    
    async def upsert_pod_health(self, pod_health: PodHealth):
        self.upsert_calls.append(pod_health)
        self.pod_health_records[pod_health.pod_id] = pod_health
    
    async def get_pod_health(self, pod_id: str):
        return self.pod_health_records.get(pod_id)
    
    async def get_active_pods(self, heartbeat_threshold_seconds: int):
        threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
        active_pods = []
        for pod_health in self.pod_health_records.values():
            if (pod_health.status == PodStatus.UP and 
                pod_health.last_heartbeat_ts > threshold_time):
                active_pods.append(pod_health)
        return active_pods
    
    async def get_dead_pods(self, heartbeat_threshold_seconds: int):
        threshold_time = datetime.utcnow() - timedelta(seconds=heartbeat_threshold_seconds)
        dead_pods = []
        for pod_health in self.pod_health_records.values():
            if (pod_health.status != PodStatus.DOWN and 
                pod_health.last_heartbeat_ts <= threshold_time):
                dead_pods.append(pod_health)
        return dead_pods
    
    # Stub implementations for other required methods
    async def create_step(self, step): pass
    async def get_step(self, step_id): return None
    async def claim_steps(self, pod_id, limit): return []
    async def update_step_status(self, step_id, status, metadata=None): pass
    async def complete_step(self, step_id, result_metadata=None): pass
    async def fail_step(self, step_id, error_message=None, error_metadata=None): pass
    async def get_steps_by_status(self, status): return []
    async def get_steps_claimed_by_pods(self, pod_ids): return []
    async def requeue_steps(self, step_ids): pass
    async def get_all_pod_health(self): return list(self.pod_health_records.values())
    async def mark_pods_dead(self, pod_ids): pass
    async def get_queue_stats(self): return {}
    
    # Leader election methods
    async def determine_leader(self, heartbeat_threshold_seconds): return None
    async def am_i_leader(self, pod_id, heartbeat_threshold_seconds): return False
    async def get_leader_info(self, heartbeat_threshold_seconds): return {}


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = MagicMock(spec=FleetQConfig)
    config.pod_id = "test-pod-123"
    config.max_parallelism = 10
    config.capacity_threshold = 0.8
    config.max_concurrent_steps = 8
    config.heartbeat_interval_seconds = 1  # Short interval for testing
    config.dead_pod_threshold_seconds = 60
    return config


@pytest.fixture
def mock_storage():
    """Create a mock storage for testing."""
    return MockStorage()


@pytest.fixture
def health_service(mock_storage, mock_config):
    """Create a health service for testing."""
    return HealthService(mock_storage, mock_config)


class TestHealthService:
    """Test cases for the HealthService class."""
    
    def test_initialization(self, health_service, mock_config):
        """Test health service initialization."""
        assert health_service.pod_id == mock_config.pod_id
        assert health_service.config == mock_config
        assert not health_service.is_running
        assert isinstance(health_service.birth_ts, datetime)
    
    def test_factory_function(self, mock_storage, mock_config):
        """Test the create_health_service factory function."""
        service = create_health_service(mock_storage, mock_config)
        assert isinstance(service, HealthService)
        assert service.storage == mock_storage
        assert service.config == mock_config
    
    @patch('fleet_q.health_service.psutil')
    @patch('fleet_q.health_service.platform')
    def test_collect_pod_metadata(self, mock_platform, mock_psutil, health_service):
        """Test pod metadata collection."""
        # Mock system information
        mock_psutil.cpu_count.return_value = 4
        mock_memory = MagicMock()
        mock_memory.total = 8 * 1024**3  # 8GB
        mock_memory.available = 4 * 1024**3  # 4GB
        mock_psutil.virtual_memory.return_value = mock_memory
        
        mock_process = MagicMock()
        mock_process.as_dict.return_value = {
            'pid': 1234,
            'ppid': 1,
            'name': 'python',
            'create_time': 1234567890,
            'num_threads': 5
        }
        mock_psutil.Process.return_value = mock_process
        
        mock_platform.system.return_value = 'Linux'
        mock_platform.release.return_value = '5.4.0'
        mock_platform.machine.return_value = 'x86_64'
        mock_platform.python_version.return_value = '3.9.0'
        
        # Collect metadata
        metadata = health_service.collect_pod_metadata()
        
        # Verify structure
        assert 'system' in metadata
        assert 'process' in metadata
        assert 'fleet_q' in metadata
        assert 'collected_at' in metadata
        
        # Verify system info
        assert metadata['system']['cpu_count'] == 4
        assert metadata['system']['memory_total_gb'] == 8.0
        assert metadata['system']['memory_available_gb'] == 4.0
        assert metadata['system']['system'] == 'Linux'
        
        # Verify fleet_q info
        assert metadata['fleet_q']['max_parallelism'] == 10
        assert metadata['fleet_q']['capacity_threshold'] == 0.8
        assert metadata['fleet_q']['max_concurrent_steps'] == 8
    
    def test_collect_pod_metadata_error_handling(self, health_service):
        """Test metadata collection with system errors."""
        with patch('fleet_q.health_service.psutil.cpu_count', side_effect=Exception("System error")):
            metadata = health_service.collect_pod_metadata()
            
            # Should still return basic fleet_q metadata
            assert 'fleet_q' in metadata
            assert 'error' in metadata
            assert metadata['fleet_q']['max_parallelism'] == 10
    
    def test_invalidate_metadata_cache(self, health_service):
        """Test metadata cache invalidation."""
        # First call should populate cache
        with patch('fleet_q.health_service.psutil'):
            metadata1 = health_service.collect_pod_metadata()
        
        # Second call should return cached data
        with patch('fleet_q.health_service.psutil') as mock_psutil:
            metadata2 = health_service.collect_pod_metadata()
            # psutil should not be called again
            mock_psutil.cpu_count.assert_not_called()
        
        # Invalidate cache
        health_service.invalidate_metadata_cache()
        
        # Third call should collect fresh data
        with patch('fleet_q.health_service.psutil') as mock_psutil:
            mock_psutil.cpu_count.return_value = 8
            metadata3 = health_service.collect_pod_metadata()
            # psutil should be called again
            mock_psutil.cpu_count.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_register_pod(self, health_service, mock_storage):
        """Test pod registration."""
        with patch.object(health_service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'test': 'metadata'}
            
            await health_service.register_pod()
            
            # Verify storage was called
            assert len(mock_storage.upsert_calls) == 1
            pod_health = mock_storage.upsert_calls[0]
            
            assert pod_health.pod_id == "test-pod-123"
            assert pod_health.status == PodStatus.UP
            assert pod_health.birth_ts == health_service.birth_ts
            assert pod_health.meta == {'test': 'metadata'}
    
    @pytest.mark.asyncio
    async def test_send_heartbeat(self, health_service, mock_storage):
        """Test heartbeat sending."""
        with patch.object(health_service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'heartbeat': 'data'}
            
            await health_service.send_heartbeat()
            
            # Verify storage was called
            assert len(mock_storage.upsert_calls) == 1
            pod_health = mock_storage.upsert_calls[0]
            
            assert pod_health.pod_id == "test-pod-123"
            assert pod_health.status == PodStatus.UP
            assert pod_health.birth_ts == health_service.birth_ts
            assert pod_health.meta == {'heartbeat': 'data'}
            # last_heartbeat_ts should be recent
            assert (datetime.utcnow() - pod_health.last_heartbeat_ts).total_seconds() < 1
    
    @pytest.mark.asyncio
    async def test_send_heartbeat_error_handling(self, health_service, mock_storage):
        """Test heartbeat error handling."""
        # Mock storage to raise an error
        mock_storage.upsert_pod_health = AsyncMock(side_effect=Exception("Storage error"))
        
        # Should not raise exception (heartbeat failures are logged but not fatal)
        await health_service.send_heartbeat()
        
        # No calls should have been recorded in upsert_calls
        assert len(mock_storage.upsert_calls) == 0
    
    @pytest.mark.asyncio
    async def test_mark_pod_down(self, health_service, mock_storage):
        """Test marking pod as down."""
        with patch.object(health_service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'shutdown': 'data'}
            
            await health_service.mark_pod_down()
            
            # Verify storage was called
            assert len(mock_storage.upsert_calls) == 1
            pod_health = mock_storage.upsert_calls[0]
            
            assert pod_health.pod_id == "test-pod-123"
            assert pod_health.status == PodStatus.DOWN
            assert pod_health.birth_ts == health_service.birth_ts
            assert 'shutdown_ts' in pod_health.meta
    
    @pytest.mark.asyncio
    async def test_start_and_stop(self, health_service, mock_storage):
        """Test starting and stopping the health service."""
        with patch.object(health_service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'test': 'data'}
            
            # Start the service
            await health_service.start()
            
            assert health_service.is_running
            assert health_service._heartbeat_task is not None
            
            # Verify registration occurred
            assert len(mock_storage.upsert_calls) >= 1
            assert mock_storage.upsert_calls[0].status == PodStatus.UP
            
            # Wait a bit for heartbeat loop to run
            await asyncio.sleep(0.1)
            
            # Stop the service
            await health_service.stop()
            
            assert not health_service.is_running
            assert health_service._heartbeat_task is None
            
            # Verify shutdown occurred
            final_call = mock_storage.upsert_calls[-1]
            assert final_call.status == PodStatus.DOWN
    
    @pytest.mark.asyncio
    async def test_start_already_running(self, health_service):
        """Test starting service when already running."""
        with patch.object(health_service, 'collect_pod_metadata'):
            await health_service.start()
            
            # Try to start again
            with pytest.raises(RuntimeError, match="already running"):
                await health_service.start()
            
            await health_service.stop()
    
    @pytest.mark.asyncio
    async def test_stop_not_running(self, health_service):
        """Test stopping service when not running."""
        # Should not raise an error
        await health_service.stop()
        assert not health_service.is_running
    
    @pytest.mark.asyncio
    async def test_heartbeat_loop_with_multiple_beats(self, health_service, mock_storage):
        """Test heartbeat loop sends multiple heartbeats."""
        with patch.object(health_service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'test': 'data'}
            
            # Start the service
            await health_service.start()
            
            # Wait for multiple heartbeat intervals
            await asyncio.sleep(0.25)  # Should allow 2-3 heartbeats with 0.1s interval
            
            # Stop the service
            await health_service.stop()
            
            # Should have registration + multiple heartbeats + shutdown
            assert len(mock_storage.upsert_calls) >= 3
            
            # All but the last should be UP status
            for call in mock_storage.upsert_calls[:-1]:
                assert call.status == PodStatus.UP
            
            # Last should be DOWN status
            assert mock_storage.upsert_calls[-1].status == PodStatus.DOWN
    
    def test_uptime_seconds(self, health_service):
        """Test uptime calculation."""
        # Should be very small since just created
        uptime = health_service.uptime_seconds
        assert 0 <= uptime < 1
        
        # Mock an older birth time
        health_service.birth_ts = datetime.utcnow() - timedelta(seconds=100)
        uptime = health_service.uptime_seconds
        assert 99 <= uptime <= 101  # Allow some tolerance
    
    @pytest.mark.asyncio
    async def test_get_pod_health(self, health_service, mock_storage):
        """Test getting pod health from storage."""
        # Add a pod health record to mock storage
        pod_health = PodHealth(
            pod_id="test-pod-123",
            birth_ts=datetime.utcnow(),
            last_heartbeat_ts=datetime.utcnow(),
            status=PodStatus.UP,
            meta={}
        )
        mock_storage.pod_health_records["test-pod-123"] = pod_health
        
        result = await health_service.get_pod_health()
        assert result == pod_health
    
    @pytest.mark.asyncio
    async def test_get_all_active_pods(self, health_service, mock_storage):
        """Test getting all active pods."""
        # Add some pod health records
        now = datetime.utcnow()
        
        # Active pod
        active_pod = PodHealth(
            pod_id="active-pod",
            birth_ts=now - timedelta(minutes=5),
            last_heartbeat_ts=now - timedelta(seconds=10),
            status=PodStatus.UP,
            meta={}
        )
        
        # Inactive pod (stale heartbeat)
        inactive_pod = PodHealth(
            pod_id="inactive-pod",
            birth_ts=now - timedelta(minutes=10),
            last_heartbeat_ts=now - timedelta(minutes=5),
            status=PodStatus.UP,
            meta={}
        )
        
        # Down pod
        down_pod = PodHealth(
            pod_id="down-pod",
            birth_ts=now - timedelta(minutes=10),
            last_heartbeat_ts=now - timedelta(seconds=10),
            status=PodStatus.DOWN,
            meta={}
        )
        
        mock_storage.pod_health_records.update({
            "active-pod": active_pod,
            "inactive-pod": inactive_pod,
            "down-pod": down_pod
        })
        
        active_pods = await health_service.get_all_active_pods()
        
        # Should only return the active pod
        assert len(active_pods) == 1
        assert active_pods[0].pod_id == "active-pod"
    
    @pytest.mark.asyncio
    async def test_detect_and_mark_dead_pods(self, health_service, mock_storage):
        """Test detecting and marking dead pods."""
        now = datetime.utcnow()
        
        # Active pod
        active_pod = PodHealth(
            pod_id="active-pod",
            birth_ts=now - timedelta(minutes=5),
            last_heartbeat_ts=now - timedelta(seconds=10),
            status=PodStatus.UP,
            meta={}
        )
        
        # Dead pod (stale heartbeat)
        dead_pod = PodHealth(
            pod_id="dead-pod",
            birth_ts=now - timedelta(minutes=10),
            last_heartbeat_ts=now - timedelta(minutes=5),
            status=PodStatus.UP,
            meta={}
        )
        
        # Already down pod (should not be marked again)
        down_pod = PodHealth(
            pod_id="down-pod",
            birth_ts=now - timedelta(minutes=10),
            last_heartbeat_ts=now - timedelta(minutes=5),
            status=PodStatus.DOWN,
            meta={}
        )
        
        mock_storage.pod_health_records.update({
            "active-pod": active_pod,
            "dead-pod": dead_pod,
            "down-pod": down_pod
        })
        
        # Track calls to mark_pods_dead
        marked_dead_calls = []
        original_mark_pods_dead = mock_storage.mark_pods_dead
        
        async def track_mark_pods_dead(pod_ids):
            marked_dead_calls.append(pod_ids)
            return await original_mark_pods_dead(pod_ids)
        
        mock_storage.mark_pods_dead = track_mark_pods_dead
        
        # Detect and mark dead pods
        dead_pods = await health_service.detect_and_mark_dead_pods()
        
        # Should have found 1 dead pod (not the already down one)
        assert len(dead_pods) == 1
        assert dead_pods[0].pod_id == "dead-pod"
        
        # Should have called mark_pods_dead with the correct pod ID
        assert len(marked_dead_calls) == 1
        assert marked_dead_calls[0] == ["dead-pod"]
    
    @pytest.mark.asyncio
    async def test_detect_and_mark_dead_pods_no_dead_pods(self, health_service, mock_storage):
        """Test detecting dead pods when there are none."""
        now = datetime.utcnow()
        
        # All pods are active
        active_pod1 = PodHealth(
            pod_id="active-pod-1",
            birth_ts=now - timedelta(minutes=5),
            last_heartbeat_ts=now - timedelta(seconds=10),
            status=PodStatus.UP,
            meta={}
        )
        
        active_pod2 = PodHealth(
            pod_id="active-pod-2",
            birth_ts=now - timedelta(minutes=3),
            last_heartbeat_ts=now - timedelta(seconds=5),
            status=PodStatus.UP,
            meta={}
        )
        
        mock_storage.pod_health_records.update({
            "active-pod-1": active_pod1,
            "active-pod-2": active_pod2
        })
        
        # Track calls to mark_pods_dead
        marked_dead_calls = []
        
        async def track_mark_pods_dead(pod_ids):
            marked_dead_calls.append(pod_ids)
        
        mock_storage.mark_pods_dead = track_mark_pods_dead
        
        # Detect and mark dead pods
        dead_pods = await health_service.detect_and_mark_dead_pods()
        
        # Should have found no dead pods
        assert len(dead_pods) == 0
        
        # Should not have called mark_pods_dead
        assert len(marked_dead_calls) == 0
    
    @pytest.mark.asyncio
    async def test_get_dead_pods(self, health_service, mock_storage):
        """Test getting dead pods directly."""
        now = datetime.utcnow()
        
        # Active pod
        active_pod = PodHealth(
            pod_id="active-pod",
            birth_ts=now - timedelta(minutes=5),
            last_heartbeat_ts=now - timedelta(seconds=10),
            status=PodStatus.UP,
            meta={}
        )
        
        # Dead pod (stale heartbeat)
        dead_pod = PodHealth(
            pod_id="dead-pod",
            birth_ts=now - timedelta(minutes=10),
            last_heartbeat_ts=now - timedelta(minutes=5),
            status=PodStatus.UP,
            meta={}
        )
        
        mock_storage.pod_health_records.update({
            "active-pod": active_pod,
            "dead-pod": dead_pod
        })
        
        dead_pods = await health_service.get_dead_pods()
        
        # Should only return the dead pod
        assert len(dead_pods) == 1
        assert dead_pods[0].pod_id == "dead-pod"


class TestHealthServiceIntegration:
    """Integration tests for health service."""
    
    @pytest.mark.asyncio
    async def test_full_lifecycle(self, mock_storage, mock_config):
        """Test complete health service lifecycle."""
        service = HealthService(mock_storage, mock_config)
        
        with patch.object(service, 'collect_pod_metadata') as mock_collect:
            mock_collect.return_value = {'integration': 'test'}
            
            # Start service
            await service.start()
            assert service.is_running
            
            # Let it run for a short time
            await asyncio.sleep(0.1)
            
            # Check that heartbeats are being sent
            assert len(mock_storage.upsert_calls) >= 2  # Registration + at least one heartbeat
            
            # Stop service
            await service.stop()
            assert not service.is_running
            
            # Verify final state is DOWN
            assert mock_storage.upsert_calls[-1].status == PodStatus.DOWN