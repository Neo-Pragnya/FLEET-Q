"""
Tests for the recovery service functionality.

Tests the recovery service's ability to discover orphaned steps, categorize them,
and manage recovery cycles with local DLQ tracking.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from fleet_q.config import FleetQConfig
from fleet_q.models import Step, StepStatus, StepPayload, PodHealth, PodStatus, DLQRecord
from fleet_q.recovery_service import RecoveryService
from fleet_q.storage import StorageInterface, SQLiteStorageInterface


class MockStorage(StorageInterface):
    """Mock storage for testing recovery service."""
    
    def __init__(self):
        self.dead_pods = []
        self.orphaned_steps = []
        self.requeued_steps = []
        
    async def initialize(self):
        pass
        
    async def close(self):
        pass
        
    async def get_dead_pods(self, threshold_seconds):
        return self.dead_pods
        
    async def mark_pods_dead(self, pod_ids):
        pass
        
    async def get_steps_claimed_by_pods(self, pod_ids):
        return self.orphaned_steps
        
    async def requeue_steps(self, step_ids):
        self.requeued_steps.extend(step_ids)
        
    async def am_i_leader(self, pod_id, threshold_seconds):
        return True  # Always leader for testing
        
    # Implement other required methods as no-ops
    async def create_step(self, step): pass
    async def get_step(self, step_id): return None
    async def claim_steps(self, pod_id, limit): return []
    async def update_step_status(self, step_id, status, metadata=None): pass
    async def complete_step(self, step_id, result_metadata=None): pass
    async def fail_step(self, step_id, error_message=None, error_metadata=None): pass
    async def get_steps_by_status(self, status): return []
    async def upsert_pod_health(self, pod_health): pass
    async def get_pod_health(self, pod_id): return None
    async def get_all_pod_health(self): return []
    async def get_active_pods(self, threshold_seconds): return []
    async def determine_leader(self, threshold_seconds): return "test-pod"
    async def get_leader_info(self, threshold_seconds): return {}
    async def get_queue_stats(self): return {}


class MockSQLiteStorage(SQLiteStorageInterface):
    """Mock SQLite storage for testing recovery service."""
    
    def __init__(self):
        self.dlq_tables = {}
        self.dlq_records = {}
        
    async def initialize(self, db_path=None):
        pass
        
    async def close(self):
        pass
        
    async def create_dlq_table(self, cycle_id):
        self.dlq_tables[cycle_id] = True
        self.dlq_records[cycle_id] = []
        
    async def drop_dlq_table(self, cycle_id):
        if cycle_id in self.dlq_tables:
            del self.dlq_tables[cycle_id]
        if cycle_id in self.dlq_records:
            del self.dlq_records[cycle_id]
            
    async def insert_dlq_record(self, cycle_id, record):
        if cycle_id not in self.dlq_records:
            self.dlq_records[cycle_id] = []
        self.dlq_records[cycle_id].append(record)
        
    async def get_dlq_records(self, cycle_id):
        return self.dlq_records.get(cycle_id, [])
        
    async def get_dlq_records_by_action(self, cycle_id, action):
        records = self.dlq_records.get(cycle_id, [])
        return [r for r in records if r.action == action]


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = MagicMock(spec=FleetQConfig)
    config.pod_id = "test-pod"
    config.recovery_interval_seconds = 300
    config.dead_pod_threshold_seconds = 180
    return config


@pytest.fixture
def mock_storage():
    """Create a mock storage for testing."""
    return MockStorage()


@pytest.fixture
def mock_sqlite_storage():
    """Create a mock SQLite storage for testing."""
    return MockSQLiteStorage()


@pytest.fixture
def recovery_service(mock_storage, mock_sqlite_storage, mock_config):
    """Create a recovery service for testing."""
    return RecoveryService(mock_storage, mock_sqlite_storage, mock_config)


class TestOrphanStepDiscovery:
    """Test orphan step discovery functionality."""
    
    @pytest.mark.asyncio
    async def test_discover_orphaned_steps_empty(self, recovery_service, mock_storage):
        """Test discovering orphaned steps with no dead pods."""
        dead_pods = []
        orphaned_steps = await recovery_service.discover_orphaned_steps(dead_pods)
        assert orphaned_steps == []
    
    @pytest.mark.asyncio
    async def test_discover_orphaned_steps_with_dead_pods(self, recovery_service, mock_storage):
        """Test discovering orphaned steps with dead pods."""
        # Set up dead pods
        dead_pods = [
            PodHealth(
                pod_id="dead-pod-1",
                birth_ts=datetime.utcnow() - timedelta(hours=1),
                last_heartbeat_ts=datetime.utcnow() - timedelta(minutes=10),
                status=PodStatus.UP,
                meta={}
            )
        ]
        
        # Set up orphaned steps
        mock_storage.orphaned_steps = [
            Step(
                step_id="orphan-step-1",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow() - timedelta(minutes=5),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=1,
                priority=0,
                created_ts=datetime.utcnow() - timedelta(minutes=10),
                max_retries=3
            )
        ]
        
        orphaned_steps = await recovery_service.discover_orphaned_steps(dead_pods)
        assert len(orphaned_steps) == 1
        assert orphaned_steps[0].step_id == "orphan-step-1"
        assert orphaned_steps[0].claimed_by == "dead-pod-1"


class TestStepCategorization:
    """Test step categorization and eligibility determination."""
    
    def test_categorize_orphaned_step_requeue(self, recovery_service):
        """Test categorizing a step that should be requeued."""
        step = Step(
            step_id="requeue-step",
            status=StepStatus.CLAIMED,
            claimed_by="dead-pod-1",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        reason, action = recovery_service.categorize_orphaned_step(step)
        assert reason == "dead_pod"
        assert action == "requeue"
    
    def test_categorize_orphaned_step_terminal(self, recovery_service):
        """Test categorizing a step that should be marked terminal."""
        step = Step(
            step_id="terminal-step",
            status=StepStatus.CLAIMED,
            claimed_by="dead-pod-1",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=5,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        reason, action = recovery_service.categorize_orphaned_step(step)
        assert reason == "dead_pod"
        assert action == "terminal"
    
    def test_determine_recovery_eligibility_valid(self, recovery_service):
        """Test eligibility determination for a valid step."""
        step = Step(
            step_id="valid-step",
            status=StepStatus.CLAIMED,
            claimed_by="dead-pod-1",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        eligible = recovery_service.determine_recovery_eligibility(step)
        assert eligible is True
    
    def test_determine_recovery_eligibility_invalid_status(self, recovery_service):
        """Test eligibility determination for a step with invalid status."""
        step = Step(
            step_id="invalid-step",
            status=StepStatus.COMPLETED,  # Not eligible for recovery
            claimed_by="dead-pod-1",
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        eligible = recovery_service.determine_recovery_eligibility(step)
        assert eligible is False
    
    def test_determine_recovery_eligibility_no_claimed_by(self, recovery_service):
        """Test eligibility determination for a step without claimed_by."""
        step = Step(
            step_id="unclaimed-step",
            status=StepStatus.CLAIMED,
            claimed_by=None,  # Not eligible for recovery
            last_update_ts=datetime.utcnow(),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            priority=0,
            created_ts=datetime.utcnow(),
            max_retries=3
        )
        
        eligible = recovery_service.determine_recovery_eligibility(step)
        assert eligible is False


class TestRecoveryCycle:
    """Test recovery cycle functionality."""
    
    @pytest.mark.asyncio
    async def test_process_orphaned_steps(self, recovery_service, mock_sqlite_storage):
        """Test processing orphaned steps and creating DLQ records."""
        # Create test steps
        orphaned_steps = [
            Step(
                step_id="requeue-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=1,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            ),
            Step(
                step_id="terminal-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=5,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            )
        ]
        
        cycle_id = "test-cycle"
        await mock_sqlite_storage.create_dlq_table(cycle_id)
        
        steps_to_requeue, terminal_steps = await recovery_service.process_orphaned_steps(
            orphaned_steps, cycle_id
        )
        
        # Check results
        assert len(steps_to_requeue) == 1
        assert steps_to_requeue[0].step_id == "requeue-step"
        
        assert len(terminal_steps) == 1
        assert terminal_steps[0].step_id == "terminal-step"
        
        # Check DLQ records were created
        dlq_records = await mock_sqlite_storage.get_dlq_records(cycle_id)
        assert len(dlq_records) == 2
        
        # Check record contents
        requeue_records = [r for r in dlq_records if r.action == "requeue"]
        terminal_records = [r for r in dlq_records if r.action == "terminal"]
        
        assert len(requeue_records) == 1
        assert requeue_records[0].step_id == "requeue-step"
        
        assert len(terminal_records) == 1
        assert terminal_records[0].step_id == "terminal-step"


class TestPoisonStepDetection:
    """Test poison step detection functionality."""
    
    def test_detect_poison_steps_max_retries(self, recovery_service):
        """Test detecting poison steps based on max retries exceeded."""
        steps = [
            Step(
                step_id="poison-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=5,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            ),
            Step(
                step_id="normal-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=1,
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=3
            )
        ]
        
        poison_steps = recovery_service.detect_poison_steps(steps)
        assert len(poison_steps) == 1
        assert poison_steps[0].step_id == "poison-step"
    
    def test_detect_poison_steps_excessive_retries(self, recovery_service):
        """Test detecting poison steps based on excessive retry count."""
        steps = [
            Step(
                step_id="excessive-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=datetime.utcnow(),
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=15,  # Excessive retries
                priority=0,
                created_ts=datetime.utcnow(),
                max_retries=20
            )
        ]
        
        poison_steps = recovery_service.detect_poison_steps(steps)
        assert len(poison_steps) == 1
        assert poison_steps[0].step_id == "excessive-step"
    
    def test_detect_poison_steps_stale_step(self, recovery_service):
        """Test detecting poison steps based on age."""
        old_timestamp = datetime.utcnow() - timedelta(hours=25)  # Older than 24 hours
        
        steps = [
            Step(
                step_id="stale-step",
                status=StepStatus.CLAIMED,
                claimed_by="dead-pod-1",
                last_update_ts=old_timestamp,
                payload=StepPayload(type="test", args={}, metadata={}),
                retry_count=1,
                priority=0,
                created_ts=old_timestamp,
                max_retries=3
            )
        ]
        
        poison_steps = recovery_service.detect_poison_steps(steps)
        assert len(poison_steps) == 1
        assert poison_steps[0].step_id == "stale-step"


class TestRecoveryStatistics:
    """Test recovery service statistics."""
    
    @pytest.mark.asyncio
    async def test_get_recovery_statistics(self, recovery_service):
        """Test getting recovery service statistics."""
        stats = await recovery_service.get_recovery_statistics()
        
        assert stats['service_status'] == 'stopped'  # Not started yet
        assert stats['pod_id'] == 'test-pod'
        assert stats['recovery_count'] == 0
        assert stats['total_orphans_processed'] == 0
        assert stats['total_steps_requeued'] == 0
        assert stats['total_steps_marked_terminal'] == 0
        assert stats['recovery_interval_seconds'] == 300
        assert stats['dead_pod_threshold_seconds'] == 180
        assert stats['last_recovery_time'] is None
    
    def test_service_properties(self, recovery_service):
        """Test recovery service properties."""
        assert recovery_service.is_running is False
        assert recovery_service.last_recovery_time is None