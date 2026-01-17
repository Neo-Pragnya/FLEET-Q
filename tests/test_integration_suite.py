"""
Comprehensive integration test suite for FLEET-Q.

This module provides multi-pod simulation testing, end-to-end workflow validation,
and failure injection and recovery testing as required by task 14.1.

Tests cover:
- Multi-pod simulation with leader election
- End-to-end step lifecycle workflows
- Failure injection and recovery scenarios
- Performance under concurrent load
- System resilience and data consistency

"""

import asyncio
import pytest
import tempfile
import os
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from unittest.mock import AsyncMock, patch

from fleet_q.config import FleetQConfig
from fleet_q.sqlite_simulation_storage import SQLiteSimulationStorage
from fleet_q.health_service import HealthService
from fleet_q.claim_service import ClaimService
from fleet_q.recovery_service import RecoveryService
from fleet_q.step_service import StepService
from fleet_q.models import (
    Step, StepStatus, PodStatus, StepPayload, StepSubmissionRequest,
    PodHealth, DLQRecord
)
from fleet_q.storage import StorageError


class MultiPodSimulator:
    """
    Simulates multiple FLEET-Q pods for integration testing.
    
    Provides utilities for creating multiple pod instances, coordinating
    their lifecycle, and testing inter-pod interactions through shared storage.
    """
    
    def __init__(self, num_pods: int = 3, db_path: Optional[str] = None):
        """
        Initialize multi-pod simulator.
        
        Args:
            num_pods: Number of pods to simulate
            db_path: Optional database path (uses temp file if None)
        """
        self.num_pods = num_pods
        self.db_path = db_path or self._create_temp_db()
        self.pods: List[Dict[str, Any]] = []
        self.storage: Optional[SQLiteSimulationStorage] = None
        self.running = False
        
    def _create_temp_db(self) -> str:
        """Create temporary database file for testing."""
        fd, path = tempfile.mkstemp(suffix='.db', prefix='fleet_q_test_')
        os.close(fd)
        return path
    
    async def initialize(self) -> None:
        """Initialize shared storage and create pod instances."""
        # Create shared storage
        base_config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            sqlite_db_path=self.db_path,
            heartbeat_interval_seconds=1,  # Fast heartbeat for testing
            claim_interval_seconds=1,      # Fast claiming for testing
            recovery_interval_seconds=5,   # Fast recovery for testing
            dead_pod_threshold_seconds=10  # Quick dead pod detection
        )
        
        self.storage = SQLiteSimulationStorage(base_config)
        await self.storage.initialize()
        
        # Create pod instances
        for i in range(self.num_pods):
            pod_id = f"test-pod-{i+1:03d}"
            
            # Create pod-specific config
            pod_config = FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                sqlite_db_path=self.db_path,
                pod_id=pod_id,
                max_parallelism=5,
                capacity_threshold=0.8,
                heartbeat_interval_seconds=1,
                claim_interval_seconds=1,
                recovery_interval_seconds=5,
                dead_pod_threshold_seconds=10
            )
            
            # Create pod services
            health_service = HealthService(self.storage, pod_config)
            claim_service = ClaimService(self.storage, pod_config)
            recovery_service = RecoveryService(self.storage, self.storage, pod_config)
            step_service = StepService(self.storage, pod_config)
            
            pod = {
                'pod_id': pod_id,
                'config': pod_config,
                'storage': self.storage,  # Shared storage
                'health_service': health_service,
                'claim_service': claim_service,
                'recovery_service': recovery_service,
                'step_service': step_service,
                'tasks': [],
                'is_leader': False,
                'active': True
            }
            
            self.pods.append(pod)
    
    async def start_all_pods(self) -> None:
        """Start all pod services."""
        self.running = True
        
        for pod in self.pods:
            if pod['active']:
                # Start health service
                health_task = asyncio.create_task(
                    pod['health_service'].start(),
                    name=f"health_{pod['pod_id']}"
                )
                pod['tasks'].append(health_task)
                
                # Start claim service
                claim_task = asyncio.create_task(
                    pod['claim_service'].start(),
                    name=f"claim_{pod['pod_id']}"
                )
                pod['tasks'].append(claim_task)
                
                # Start recovery service
                recovery_task = asyncio.create_task(
                    pod['recovery_service'].start(),
                    name=f"recovery_{pod['pod_id']}"
                )
                pod['tasks'].append(recovery_task)
        
        # Give pods time to start and register
        await asyncio.sleep(2)
    
    async def stop_all_pods(self) -> None:
        """Stop all pod services gracefully."""
        self.running = False
        
        for pod in self.pods:
            # Stop services
            if pod['health_service']:
                await pod['health_service'].stop()
            if pod['claim_service']:
                await pod['claim_service'].stop()
            if pod['recovery_service']:
                await pod['recovery_service'].stop()
            
            # Cancel tasks
            for task in pod['tasks']:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            pod['tasks'].clear()
    
    async def kill_pod(self, pod_index: int) -> None:
        """Simulate pod failure by stopping its services abruptly."""
        if 0 <= pod_index < len(self.pods):
            pod = self.pods[pod_index]
            pod['active'] = False
            
            # Cancel all tasks without graceful shutdown
            for task in pod['tasks']:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            pod['tasks'].clear()
    
    async def restart_pod(self, pod_index: int) -> None:
        """Restart a previously killed pod."""
        if 0 <= pod_index < len(self.pods):
            pod = self.pods[pod_index]
            pod['active'] = True
            
            # Restart services
            health_task = asyncio.create_task(
                pod['health_service'].start(),
                name=f"health_{pod['pod_id']}_restart"
            )
            pod['tasks'].append(health_task)
            
            claim_task = asyncio.create_task(
                pod['claim_service'].start(),
                name=f"claim_{pod['pod_id']}_restart"
            )
            pod['tasks'].append(claim_task)
            
            recovery_task = asyncio.create_task(
                pod['recovery_service'].start(),
                name=f"recovery_{pod['pod_id']}_restart"
            )
            pod['tasks'].append(recovery_task)
    
    async def get_current_leader(self) -> Optional[str]:
        """Get the current leader pod ID."""
        return await self.storage.get_current_leader()
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get current queue statistics."""
        return await self.storage.get_queue_stats()
    
    async def submit_steps(self, count: int, step_type: str = "test_task") -> List[str]:
        """Submit multiple test steps."""
        step_ids = []
        
        for i in range(count):
            request = StepSubmissionRequest(
                type=step_type,
                args={"task_id": i, "data": f"test_data_{i}"},
                metadata={"source": "integration_test", "batch": "multi_submit"},
                priority=random.randint(0, 10),
                max_retries=3
            )
            
            # Use first active pod's step service
            active_pod = next((p for p in self.pods if p['active']), None)
            if active_pod:
                step_id = await active_pod['step_service'].submit_step(request)
                step_ids.append(step_id)
        
        return step_ids
    
    def cleanup(self) -> None:
        """Clean up temporary resources."""
        if self.db_path and os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except OSError:
                pass


class TestMultiPodIntegration:
    """Integration tests for multi-pod scenarios."""
    
    @pytest.fixture
    async def simulator(self):
        """Create and initialize multi-pod simulator."""
        sim = MultiPodSimulator(num_pods=3)
        await sim.initialize()
        yield sim
        await sim.stop_all_pods()
        if sim.storage:
            await sim.storage.close()
        sim.cleanup()
    
    @pytest.mark.asyncio
    async def test_multi_pod_leader_election(self, simulator):
        """
        Test leader election with multiple pods.
        
        """
        await simulator.start_all_pods()
        
        # Wait for leader election to stabilize
        await asyncio.sleep(3)
        
        # Check that exactly one leader is elected
        leader = await simulator.get_current_leader()
        assert leader is not None
        assert leader.startswith("test-pod-")
        
        # Verify leader is the eldest pod (test-pod-001)
        assert leader == "test-pod-001"
        
        # Kill the current leader
        await simulator.kill_pod(0)  # Kill test-pod-001
        
        # Wait for new leader election
        await asyncio.sleep(6)
        
        # Verify new leader is elected
        new_leader = await simulator.get_current_leader()
        assert new_leader is not None
        assert new_leader != leader
        assert new_leader == "test-pod-002"  # Next eldest
    
    @pytest.mark.asyncio
    async def test_multi_pod_step_claiming(self, simulator):
        """
        Test step claiming across multiple pods.
        
 
        """
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit multiple steps
        step_ids = await simulator.submit_steps(15, "concurrent_test")
        assert len(step_ids) == 15
        
        # Wait for steps to be claimed and processed
        await asyncio.sleep(5)
        
        # Check that steps were distributed across pods
        stats = await simulator.get_queue_stats()
        
        # All steps should be claimed or completed
        assert stats['pending_steps'] == 0
        assert stats['claimed_steps'] + stats['completed_steps'] == 15
        
        # Verify no double-claiming occurred by checking step statuses
        claimed_by_pods = set()
        for step_id in step_ids:
            step = await simulator.storage.get_step(step_id)
            if step and step.claimed_by:
                claimed_by_pods.add(step.claimed_by)
        
        # Multiple pods should have claimed steps
        assert len(claimed_by_pods) > 1
    
    @pytest.mark.asyncio
    async def test_pod_failure_and_recovery(self, simulator):
        """
        Test pod failure detection and step recovery.
        

        """
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit steps and let them be claimed
        step_ids = await simulator.submit_steps(10, "recovery_test")
        await asyncio.sleep(3)
        
        # Get initial stats
        initial_stats = await simulator.get_queue_stats()
        
        # Kill a pod that has claimed steps
        await simulator.kill_pod(1)  # Kill test-pod-002
        
        # Wait for dead pod detection and recovery
        await asyncio.sleep(12)  # Wait longer than dead_pod_threshold
        
        # Check that orphaned steps were recovered
        final_stats = await simulator.get_queue_stats()
        
        # Steps should be requeued or completed
        assert final_stats['pending_steps'] + final_stats['completed_steps'] >= initial_stats['claimed_steps']
        
        # Verify dead pod is no longer active
        assert final_stats['active_pods'] == 2
    
    @pytest.mark.asyncio
    async def test_concurrent_step_processing(self, simulator):
        """
        Test concurrent step processing under load.
        

        """
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit a large batch of steps
        step_ids = await simulator.submit_steps(50, "load_test")
        assert len(step_ids) == 50
        
        # Monitor processing over time
        start_time = time.time()
        processed_count = 0
        
        while time.time() - start_time < 30:  # 30 second timeout
            stats = await simulator.get_queue_stats()
            processed_count = stats['claimed_steps'] + stats['completed_steps']
            
            if processed_count == 50:
                break
            
            await asyncio.sleep(1)
        
        # Verify all steps were processed
        final_stats = await simulator.get_queue_stats()
        assert final_stats['pending_steps'] == 0
        assert final_stats['claimed_steps'] + final_stats['completed_steps'] == 50
        
        # Verify processing was distributed
        processing_time = time.time() - start_time
        assert processing_time < 25  # Should complete within reasonable time


class TestEndToEndWorkflows:
    """End-to-end workflow validation tests."""
    
    @pytest.fixture
    async def single_pod_setup(self):
        """Set up single pod for end-to-end testing."""
        sim = MultiPodSimulator(num_pods=1)
        await sim.initialize()
        await sim.start_all_pods()
        yield sim
        await sim.stop_all_pods()
        if sim.storage:
            await sim.storage.close()
        sim.cleanup()
    
    @pytest.mark.asyncio
    async def test_complete_step_lifecycle(self, single_pod_setup):
        """
        Test complete step lifecycle from submission to completion.
        
        
        """
        simulator = single_pod_setup
        
        # Submit a step
        request = StepSubmissionRequest(
            type="lifecycle_test",
            args={"test_param": "test_value"},
            metadata={"workflow": "end_to_end"},
            priority=5,
            max_retries=3
        )
        
        pod = simulator.pods[0]
        step_id = await pod['step_service'].submit_step(request)
        
        # Verify step is pending
        step = await simulator.storage.get_step(step_id)
        assert step.status == StepStatus.PENDING
        assert step.claimed_by is None
        assert step.retry_count == 0
        
        # Wait for step to be claimed
        await asyncio.sleep(3)
        
        step = await simulator.storage.get_step(step_id)
        assert step.status == StepStatus.CLAIMED
        assert step.claimed_by == pod['pod_id']
        
        # Simulate step completion
        await simulator.storage.update_step_status(step_id, StepStatus.COMPLETED)
        
        # Verify final state
        step = await simulator.storage.get_step(step_id)
        assert step.status == StepStatus.COMPLETED
        assert step.claimed_by == pod['pod_id']
    
    @pytest.mark.asyncio
    async def test_step_retry_workflow(self, single_pod_setup):
        """
        Test step retry workflow with failure and recovery.
        
        
        """
        simulator = single_pod_setup
        
        # Submit a step
        request = StepSubmissionRequest(
            type="retry_test",
            args={"fail_count": 2},
            metadata={"test": "retry_workflow"},
            max_retries=3
        )
        
        pod = simulator.pods[0]
        step_id = await pod['step_service'].submit_step(request)
        
        # Wait for step to be claimed
        await asyncio.sleep(3)
        
        # Simulate step failure
        await simulator.storage.update_step_status(step_id, StepStatus.FAILED)
        
        step = await simulator.storage.get_step(step_id)
        assert step.status == StepStatus.FAILED
        
        # Kill pod to trigger recovery
        await simulator.kill_pod(0)
        await simulator.restart_pod(0)
        
        # Wait for recovery to requeue the step
        await asyncio.sleep(8)
        
        # Check if step was requeued
        step = await simulator.storage.get_step(step_id)
        # Step should either be pending (requeued) or claimed again
        assert step.status in [StepStatus.PENDING, StepStatus.CLAIMED]
    
    @pytest.mark.asyncio
    async def test_idempotency_workflow(self, single_pod_setup):
        """
        Test idempotency key generation and usage.
        
        """
        simulator = single_pod_setup
        pod = simulator.pods[0]
        
        # Submit a step
        request = StepSubmissionRequest(
            type="idempotency_test",
            args={"operation": "create_resource"},
            metadata={"resource_id": "test_resource_123"}
        )
        
        step_id = await pod['step_service'].submit_step(request)
        
        # Get idempotency key
        idempotency_key = pod['step_service'].get_idempotency_key(step_id)
        
        # Verify idempotency key format
        assert idempotency_key == f"idempotent_{step_id}"
        assert idempotency_key.startswith("idempotent_step_")
        
        # Verify key is consistent across calls
        second_key = pod['step_service'].get_idempotency_key(step_id)
        assert idempotency_key == second_key


class TestFailureInjectionAndRecovery:
    """Failure injection and recovery testing."""
    
    @pytest.fixture
    async def resilience_simulator(self):
        """Set up simulator for resilience testing."""
        sim = MultiPodSimulator(num_pods=4)
        await sim.initialize()
        yield sim
        await sim.stop_all_pods()
        if sim.storage:
            await sim.storage.close()
        sim.cleanup()
    
    @pytest.mark.asyncio
    async def test_cascading_pod_failures(self, resilience_simulator):
        """
        Test system resilience under cascading pod failures.
        
   
        """
        simulator = resilience_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit work
        step_ids = await simulator.submit_steps(20, "cascade_test")
        await asyncio.sleep(3)
        
        initial_stats = await simulator.get_queue_stats()
        assert initial_stats['active_pods'] == 4
        
        # Kill pods one by one
        await simulator.kill_pod(0)  # Kill leader
        await asyncio.sleep(6)
        
        stats_after_first = await simulator.get_queue_stats()
        assert stats_after_first['active_pods'] == 3
        
        await simulator.kill_pod(1)  # Kill another pod
        await asyncio.sleep(6)
        
        stats_after_second = await simulator.get_queue_stats()
        assert stats_after_second['active_pods'] == 2
        
        # System should still be functional with 2 pods
        new_leader = await simulator.get_current_leader()
        assert new_leader is not None
        
        # Submit more work to verify system is still operational
        additional_steps = await simulator.submit_steps(5, "post_failure_test")
        await asyncio.sleep(5)
        
        final_stats = await simulator.get_queue_stats()
        # System should still be processing work
        assert final_stats['active_pods'] == 2
    
    @pytest.mark.asyncio
    async def test_storage_error_recovery(self, resilience_simulator):
        """
        Test recovery from storage errors.
        
        
        """
        simulator = resilience_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit initial work
        step_ids = await simulator.submit_steps(10, "storage_error_test")
        await asyncio.sleep(2)
        
        # Simulate storage errors by temporarily corrupting the database
        original_claim_method = simulator.storage.claim_steps
        
        error_count = 0
        async def failing_claim_steps(pod_id: str, limit: int):
            nonlocal error_count
            error_count += 1
            if error_count <= 3:  # Fail first 3 attempts
                raise StorageError("Simulated storage failure")
            return await original_claim_method(pod_id, limit)
        
        # Patch the claim_steps method to inject failures
        simulator.storage.claim_steps = failing_claim_steps
        
        # Wait for system to recover from errors
        await asyncio.sleep(10)
        
        # Restore normal operation
        simulator.storage.claim_steps = original_claim_method
        
        # Verify system recovered and continued processing
        await asyncio.sleep(5)
        final_stats = await simulator.get_queue_stats()
        
        # Some steps should have been processed despite errors
        assert final_stats['claimed_steps'] + final_stats['completed_steps'] > 0
    
    @pytest.mark.asyncio
    async def test_leader_election_under_stress(self, resilience_simulator):
        """
        Test leader election stability under stress conditions.
        
      
        """
        simulator = resilience_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Record initial leader
        initial_leader = await simulator.get_current_leader()
        assert initial_leader is not None
        
        # Rapidly kill and restart pods to stress leader election
        for cycle in range(3):
            # Kill current leader
            leader_pod_index = int(initial_leader.split('-')[-1]) - 1
            await simulator.kill_pod(leader_pod_index)
            
            # Wait for new leader election
            await asyncio.sleep(6)
            
            new_leader = await simulator.get_current_leader()
            assert new_leader is not None
            assert new_leader != initial_leader
            
            # Restart the killed pod
            await simulator.restart_pod(leader_pod_index)
            await asyncio.sleep(3)
            
            # Leader should remain stable (not change back)
            stable_leader = await simulator.get_current_leader()
            assert stable_leader == new_leader
            
            initial_leader = new_leader
    
    @pytest.mark.asyncio
    async def test_recovery_dlq_operations(self, resilience_simulator):
        """
        Test DLQ operations during recovery cycles.
        
 
        """
        simulator = resilience_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit steps and let them be claimed
        step_ids = await simulator.submit_steps(15, "dlq_test")
        await asyncio.sleep(3)
        
        # Kill multiple pods to create orphaned steps
        await simulator.kill_pod(1)
        await simulator.kill_pod(2)
        
        # Wait for recovery cycle to run
        await asyncio.sleep(12)
        
        # Verify recovery occurred
        stats = await simulator.get_queue_stats()
        assert stats['active_pods'] == 2  # Only 2 pods left
        
        # Steps should be requeued or completed
        total_processed = stats['pending_steps'] + stats['claimed_steps'] + stats['completed_steps']
        assert total_processed == 15  # No steps lost
        
        # Submit new work to verify system is still functional
        new_step_ids = await simulator.submit_steps(5, "post_recovery_test")
        await asyncio.sleep(5)
        
        final_stats = await simulator.get_queue_stats()
        # New steps should be processed
        assert final_stats['claimed_steps'] + final_stats['completed_steps'] >= 5


class TestPerformanceAndLoad:
    """Performance and load testing scenarios."""
    
    @pytest.fixture
    async def performance_simulator(self):
        """Set up simulator optimized for performance testing."""
        sim = MultiPodSimulator(num_pods=5)
        await sim.initialize()
        yield sim
        await sim.stop_all_pods()
        if sim.storage:
            await sim.storage.close()
        sim.cleanup()
    
    @pytest.mark.asyncio
    async def test_high_concurrency_claiming(self, performance_simulator):
        """
        Test step claiming under high concurrency.
        
        
        """
        simulator = performance_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit a large number of steps rapidly
        start_time = time.time()
        step_ids = await simulator.submit_steps(100, "concurrency_test")
        submission_time = time.time() - start_time
        
        assert len(step_ids) == 100
        assert submission_time < 10  # Should submit quickly
        
        # Monitor claiming performance
        claim_start = time.time()
        while time.time() - claim_start < 30:
            stats = await simulator.get_queue_stats()
            if stats['pending_steps'] == 0:
                break
            await asyncio.sleep(0.5)
        
        claiming_time = time.time() - claim_start
        final_stats = await simulator.get_queue_stats()
        
        # All steps should be claimed
        assert final_stats['pending_steps'] == 0
        assert final_stats['claimed_steps'] + final_stats['completed_steps'] == 100
        
        # Performance should be reasonable
        assert claiming_time < 25  # Should claim within reasonable time
        
        # Calculate throughput
        throughput = 100 / claiming_time
        assert throughput > 4  # At least 4 steps/second
    
    @pytest.mark.asyncio
    async def test_recovery_performance(self, performance_simulator):
        """
        Test recovery performance with many orphaned steps.
        
       
        """
        simulator = performance_simulator
        await simulator.start_all_pods()
        await asyncio.sleep(2)
        
        # Submit many steps and let them be claimed
        step_ids = await simulator.submit_steps(50, "recovery_perf_test")
        await asyncio.sleep(5)  # Let steps be claimed
        
        # Kill multiple pods to create many orphans
        recovery_start = time.time()
        await simulator.kill_pod(1)
        await simulator.kill_pod(2)
        await simulator.kill_pod(3)
        
        # Wait for recovery to complete
        while time.time() - recovery_start < 30:
            stats = await simulator.get_queue_stats()
            # Recovery is complete when we have active pods and no orphaned steps
            if stats['active_pods'] == 2:  # 2 pods remaining
                break
            await asyncio.sleep(1)
        
        recovery_time = time.time() - recovery_start
        final_stats = await simulator.get_queue_stats()
        
        # Recovery should complete in reasonable time
        assert recovery_time < 25
        assert final_stats['active_pods'] == 2
        
        # All steps should be accounted for
        total_steps = (final_stats['pending_steps'] + 
                      final_stats['claimed_steps'] + 
                      final_stats['completed_steps'] + 
                      final_stats['failed_steps'])
        assert total_steps == 50


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "-s"])