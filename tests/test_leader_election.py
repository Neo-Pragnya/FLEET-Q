"""
Tests for leader election functionality.

Tests the leader election algorithm implementation including deterministic
leader selection, leader status checking, and leader information retrieval.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from fleet_q.config import FleetQConfig
from fleet_q.leader_service import LeaderElectionService, create_leader_service
from fleet_q.models import PodHealth, PodStatus
from fleet_q.storage import StorageInterface


class MockStorage(StorageInterface):
    """Mock storage implementation for testing leader election."""
    
    def __init__(self):
        self.pods = {}
        self.leader_calls = []
    
    async def initialize(self):
        pass
    
    async def close(self):
        pass
    
    # Implement required abstract methods (minimal for testing)
    async def create_step(self, step):
        pass
    
    async def get_step(self, step_id):
        return None
    
    async def claim_steps(self, pod_id, limit):
        return []
    
    async def update_step_status(self, step_id, status, metadata=None):
        pass
    
    async def complete_step(self, step_id, result_metadata=None):
        pass
    
    async def fail_step(self, step_id, error_message=None, error_metadata=None):
        pass
    
    async def get_steps_by_status(self, status):
        return []
    
    async def get_steps_claimed_by_pods(self, pod_ids):
        return []
    
    async def requeue_steps(self, step_ids):
        pass
    
    async def upsert_pod_health(self, pod_health):
        self.pods[pod_health.pod_id] = pod_health
    
    async def get_pod_health(self, pod_id):
        return self.pods.get(pod_id)
    
    async def get_all_pod_health(self):
        return list(self.pods.values())
    
    async def get_active_pods(self, heartbeat_threshold_seconds):
        current_time = datetime.utcnow()
        threshold = current_time - timedelta(seconds=heartbeat_threshold_seconds)
        
        active_pods = []
        for pod in self.pods.values():
            if (pod.status == PodStatus.UP and 
                pod.last_heartbeat_ts > threshold):
                active_pods.append(pod)
        
        # Sort by birth_ts for deterministic ordering
        return sorted(active_pods, key=lambda p: p.birth_ts)
    
    async def mark_pods_dead(self, pod_ids):
        current_time = datetime.utcnow()
        for pod_id in pod_ids:
            if pod_id in self.pods:
                self.pods[pod_id].status = PodStatus.DOWN
                # Also update heartbeat to make it stale
                self.pods[pod_id].last_heartbeat_ts = current_time - timedelta(minutes=10)
    
    async def get_dead_pods(self, heartbeat_threshold_seconds):
        current_time = datetime.utcnow()
        threshold = current_time - timedelta(seconds=heartbeat_threshold_seconds)
        
        dead_pods = []
        for pod in self.pods.values():
            if (pod.status != PodStatus.DOWN and 
                pod.last_heartbeat_ts <= threshold):
                dead_pods.append(pod)
        
        return dead_pods
    
    async def get_queue_stats(self):
        return {'pending': 0, 'claimed': 0, 'completed': 0, 'failed': 0}
    
    # Leader election methods
    async def determine_leader(self, heartbeat_threshold_seconds):
        self.leader_calls.append(('determine_leader', heartbeat_threshold_seconds))
        
        active_pods = await self.get_active_pods(heartbeat_threshold_seconds)
        if active_pods:
            # Return the eldest pod (first in sorted list)
            return active_pods[0].pod_id
        return None
    
    async def am_i_leader(self, pod_id, heartbeat_threshold_seconds):
        self.leader_calls.append(('am_i_leader', pod_id, heartbeat_threshold_seconds))
        
        leader_id = await self.determine_leader(heartbeat_threshold_seconds)
        return leader_id == pod_id
    
    async def get_leader_info(self, heartbeat_threshold_seconds):
        self.leader_calls.append(('get_leader_info', heartbeat_threshold_seconds))
        
        active_pods = await self.get_active_pods(heartbeat_threshold_seconds)
        current_time = datetime.utcnow()
        
        leader_info = {
            'current_leader': None,
            'leader_birth_ts': None,
            'leader_last_heartbeat': None,
            'leader_metadata': None,
            'active_pods_count': len(active_pods),
            'active_pod_ids': [pod.pod_id for pod in active_pods],
            'election_timestamp': current_time,
            'heartbeat_threshold_seconds': heartbeat_threshold_seconds
        }
        
        if active_pods:
            leader_pod = active_pods[0]
            leader_info.update({
                'current_leader': leader_pod.pod_id,
                'leader_birth_ts': leader_pod.birth_ts,
                'leader_last_heartbeat': leader_pod.last_heartbeat_ts,
                'leader_metadata': leader_pod.meta
            })
        
        return leader_info


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = MagicMock(spec=FleetQConfig)
    config.pod_id = "test-pod-1"
    config.dead_pod_threshold_seconds = 180
    config.heartbeat_interval_seconds = 30
    return config


@pytest.fixture
def mock_storage():
    """Create a mock storage for testing."""
    return MockStorage()


@pytest.fixture
def leader_service(mock_storage, mock_config):
    """Create a leader election service for testing."""
    return LeaderElectionService(mock_storage, mock_config)


class TestLeaderElectionService:
    """Test cases for the LeaderElectionService class."""
    
    @pytest.mark.asyncio
    async def test_determine_leader_no_pods(self, leader_service, mock_storage):
        """Test leader determination when no pods are active."""
        leader_id = await leader_service.determine_leader()
        assert leader_id is None
        assert ('determine_leader', 180) in mock_storage.leader_calls
    
    @pytest.mark.asyncio
    async def test_determine_leader_single_pod(self, leader_service, mock_storage):
        """Test leader determination with a single active pod."""
        # Add a single active pod
        current_time = datetime.utcnow()
        pod_health = PodHealth(
            pod_id="pod-1",
            birth_ts=current_time - timedelta(minutes=5),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={"version": "1.0.0"}
        )
        await mock_storage.upsert_pod_health(pod_health)
        
        leader_id = await leader_service.determine_leader()
        assert leader_id == "pod-1"
    
    @pytest.mark.asyncio
    async def test_determine_leader_multiple_pods_eldest_wins(self, leader_service, mock_storage):
        """Test that the eldest pod becomes the leader."""
        current_time = datetime.utcnow()
        
        # Add multiple pods with different birth times
        pods = [
            PodHealth(
                pod_id="pod-new",
                birth_ts=current_time - timedelta(minutes=1),  # Newest
                last_heartbeat_ts=current_time - timedelta(seconds=10),
                status=PodStatus.UP,
                meta={}
            ),
            PodHealth(
                pod_id="pod-old",
                birth_ts=current_time - timedelta(minutes=10),  # Oldest
                last_heartbeat_ts=current_time - timedelta(seconds=15),
                status=PodStatus.UP,
                meta={}
            ),
            PodHealth(
                pod_id="pod-middle",
                birth_ts=current_time - timedelta(minutes=5),  # Middle
                last_heartbeat_ts=current_time - timedelta(seconds=20),
                status=PodStatus.UP,
                meta={}
            )
        ]
        
        for pod in pods:
            await mock_storage.upsert_pod_health(pod)
        
        leader_id = await leader_service.determine_leader()
        assert leader_id == "pod-old"  # Eldest pod should be leader
    
    @pytest.mark.asyncio
    async def test_determine_leader_ignores_stale_heartbeat(self, leader_service, mock_storage):
        """Test that pods with stale heartbeats are not considered for leadership."""
        current_time = datetime.utcnow()
        
        # Add pods with fresh and stale heartbeats
        pods = [
            PodHealth(
                pod_id="pod-stale",
                birth_ts=current_time - timedelta(minutes=10),  # Oldest but stale
                last_heartbeat_ts=current_time - timedelta(minutes=5),  # Stale heartbeat
                status=PodStatus.UP,
                meta={}
            ),
            PodHealth(
                pod_id="pod-fresh",
                birth_ts=current_time - timedelta(minutes=5),  # Newer but fresh
                last_heartbeat_ts=current_time - timedelta(seconds=30),  # Fresh heartbeat
                status=PodStatus.UP,
                meta={}
            )
        ]
        
        for pod in pods:
            await mock_storage.upsert_pod_health(pod)
        
        leader_id = await leader_service.determine_leader()
        assert leader_id == "pod-fresh"  # Only pod with fresh heartbeat
    
    @pytest.mark.asyncio
    async def test_determine_leader_ignores_down_pods(self, leader_service, mock_storage):
        """Test that pods with DOWN status are not considered for leadership."""
        current_time = datetime.utcnow()
        
        # Add pods with UP and DOWN status
        pods = [
            PodHealth(
                pod_id="pod-down",
                birth_ts=current_time - timedelta(minutes=10),  # Oldest but down
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.DOWN,
                meta={}
            ),
            PodHealth(
                pod_id="pod-up",
                birth_ts=current_time - timedelta(minutes=5),  # Newer but up
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.UP,
                meta={}
            )
        ]
        
        for pod in pods:
            await mock_storage.upsert_pod_health(pod)
        
        leader_id = await leader_service.determine_leader()
        assert leader_id == "pod-up"  # Only UP pod
    
    @pytest.mark.asyncio
    async def test_am_i_leader_true(self, leader_service, mock_storage, mock_config):
        """Test am_i_leader returns True when this pod is the leader."""
        current_time = datetime.utcnow()
        
        # Add this pod as the eldest active pod
        pod_health = PodHealth(
            pod_id=mock_config.pod_id,
            birth_ts=current_time - timedelta(minutes=10),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(pod_health)
        
        is_leader = await leader_service.am_i_leader()
        assert is_leader is True
    
    @pytest.mark.asyncio
    async def test_am_i_leader_false(self, leader_service, mock_storage, mock_config):
        """Test am_i_leader returns False when another pod is the leader."""
        current_time = datetime.utcnow()
        
        # Add another pod as the eldest active pod
        pods = [
            PodHealth(
                pod_id="other-pod",
                birth_ts=current_time - timedelta(minutes=10),  # Older
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.UP,
                meta={}
            ),
            PodHealth(
                pod_id=mock_config.pod_id,
                birth_ts=current_time - timedelta(minutes=5),  # Newer
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.UP,
                meta={}
            )
        ]
        
        for pod in pods:
            await mock_storage.upsert_pod_health(pod)
        
        is_leader = await leader_service.am_i_leader()
        assert is_leader is False
    
    @pytest.mark.asyncio
    async def test_get_leader_info(self, leader_service, mock_storage):
        """Test getting comprehensive leader information."""
        current_time = datetime.utcnow()
        
        # Add multiple active pods
        pods = [
            PodHealth(
                pod_id="pod-leader",
                birth_ts=current_time - timedelta(minutes=10),
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.UP,
                meta={"version": "1.0.0", "cpu_count": 4}
            ),
            PodHealth(
                pod_id="pod-follower",
                birth_ts=current_time - timedelta(minutes=5),
                last_heartbeat_ts=current_time - timedelta(seconds=20),
                status=PodStatus.UP,
                meta={"version": "1.0.0", "cpu_count": 2}
            )
        ]
        
        for pod in pods:
            await mock_storage.upsert_pod_health(pod)
        
        leader_info = await leader_service.get_leader_info()
        
        assert leader_info['current_leader'] == "pod-leader"
        assert leader_info['active_pods_count'] == 2
        assert "pod-leader" in leader_info['active_pod_ids']
        assert "pod-follower" in leader_info['active_pod_ids']
        assert leader_info['this_pod_id'] == "test-pod-1"
        assert leader_info['this_pod_is_leader'] is False
    
    @pytest.mark.asyncio
    async def test_leader_caching(self, leader_service, mock_storage, mock_config):
        """Test that leader election results are cached appropriately."""
        current_time = datetime.utcnow()
        
        # Add this pod as leader
        pod_health = PodHealth(
            pod_id=mock_config.pod_id,
            birth_ts=current_time - timedelta(minutes=10),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(pod_health)
        
        # First call should query storage
        is_leader_1 = await leader_service.am_i_leader()
        call_count_1 = len(mock_storage.leader_calls)
        
        # Second call within cache duration should use cache
        is_leader_2 = await leader_service.am_i_leader()
        call_count_2 = len(mock_storage.leader_calls)
        
        assert is_leader_1 is True
        assert is_leader_2 is True
        assert call_count_2 == call_count_1  # No additional storage calls
        
        # Check cached status
        cached_status = leader_service.cached_leader_status
        assert cached_status is True
        
        # Invalidate cache and check again
        leader_service.invalidate_cache()
        cached_status_after_invalidation = leader_service.cached_leader_status
        assert cached_status_after_invalidation is None
    
    @pytest.mark.asyncio
    async def test_wait_for_leadership_success(self, leader_service, mock_storage, mock_config):
        """Test waiting for leadership when pod becomes leader."""
        current_time = datetime.utcnow()
        
        # Add this pod as the only active pod (should become leader immediately)
        this_pod = PodHealth(
            pod_id=mock_config.pod_id,
            birth_ts=current_time - timedelta(minutes=5),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(this_pod)
        
        # Wait for leadership - should succeed immediately
        became_leader = await leader_service.wait_for_leadership(timeout_seconds=1.0)
        assert became_leader is True
    
    @pytest.mark.asyncio
    async def test_wait_for_leadership_timeout(self, leader_service, mock_storage, mock_config):
        """Test waiting for leadership times out when pod doesn't become leader."""
        current_time = datetime.utcnow()
        
        # Add another pod as permanent leader
        other_pod = PodHealth(
            pod_id="other-pod",
            birth_ts=current_time - timedelta(minutes=10),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(other_pod)
        
        # Add this pod as newer (won't become leader)
        this_pod = PodHealth(
            pod_id=mock_config.pod_id,
            birth_ts=current_time - timedelta(minutes=5),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(this_pod)
        
        # Wait for leadership with short timeout
        became_leader = await leader_service.wait_for_leadership(timeout_seconds=0.2)
        assert became_leader is False
    
    @pytest.mark.asyncio
    async def test_ensure_leader_or_wait_success(self, leader_service, mock_storage):
        """Test ensuring a leader exists when one is available."""
        current_time = datetime.utcnow()
        
        # Add an active pod
        pod_health = PodHealth(
            pod_id="leader-pod",
            birth_ts=current_time - timedelta(minutes=10),
            last_heartbeat_ts=current_time - timedelta(seconds=30),
            status=PodStatus.UP,
            meta={}
        )
        await mock_storage.upsert_pod_health(pod_health)
        
        leader_id = await leader_service.ensure_leader_or_wait(max_wait_seconds=1.0)
        assert leader_id == "leader-pod"
    
    @pytest.mark.asyncio
    async def test_ensure_leader_or_wait_timeout(self, leader_service, mock_storage):
        """Test ensuring a leader times out when no pods are available."""
        # No pods added - should timeout
        with pytest.raises(RuntimeError, match="No leader elected within"):
            await leader_service.ensure_leader_or_wait(max_wait_seconds=0.2)


class TestLeaderElectionFactory:
    """Test cases for the leader election factory function."""
    
    def test_create_leader_service(self, mock_storage, mock_config):
        """Test the factory function creates a proper service instance."""
        service = create_leader_service(mock_storage, mock_config)
        
        assert isinstance(service, LeaderElectionService)
        assert service.storage is mock_storage
        assert service.config is mock_config
        assert service.pod_id == mock_config.pod_id


class TestLeaderElectionIntegration:
    """Integration tests for leader election with multiple pods."""
    
    @pytest.mark.asyncio
    async def test_leader_election_scenario(self, mock_storage):
        """Test a complete leader election scenario with multiple pods."""
        current_time = datetime.utcnow()
        
        # Create multiple leader services for different pods
        configs = []
        services = []
        
        for i in range(3):
            config = MagicMock(spec=FleetQConfig)
            config.pod_id = f"pod-{i+1}"
            config.dead_pod_threshold_seconds = 180
            config.heartbeat_interval_seconds = 30
            configs.append(config)
            
            service = LeaderElectionService(mock_storage, config)
            services.append(service)
        
        # Add pods with different birth times
        birth_times = [
            current_time - timedelta(minutes=10),  # pod-1: oldest
            current_time - timedelta(minutes=5),   # pod-2: middle
            current_time - timedelta(minutes=1),   # pod-3: newest
        ]
        
        for i, (config, birth_time) in enumerate(zip(configs, birth_times)):
            pod_health = PodHealth(
                pod_id=config.pod_id,
                birth_ts=birth_time,
                last_heartbeat_ts=current_time - timedelta(seconds=30),
                status=PodStatus.UP,
                meta={"index": i}
            )
            await mock_storage.upsert_pod_health(pod_health)
        
        # Check leader election results
        leader_results = []
        for service in services:
            is_leader = await service.am_i_leader()
            leader_results.append(is_leader)
        
        # Only pod-1 (oldest) should be leader
        assert leader_results == [True, False, False]
        
        # Get leader info from each service
        for service in services:
            leader_info = await service.get_leader_info()
            assert leader_info['current_leader'] == "pod-1"
            assert leader_info['active_pods_count'] == 3
        
        # Simulate pod-1 going down
        await mock_storage.mark_pods_dead(["pod-1"])
        
        # Invalidate caches to force fresh election
        for service in services:
            service.invalidate_cache()
        
        # Check new leader election results
        new_leader_results = []
        for service in services:
            is_leader = await service.am_i_leader()
            new_leader_results.append(is_leader)
        
        # Now pod-2 (next oldest) should be leader
        assert new_leader_results == [False, True, False]