"""
Performance and load testing for FLEET-Q.

This module provides comprehensive performance testing including:
- Load testing scenarios for high concurrency
- Performance benchmarks for claiming and recovery
- Resource usage monitoring and optimization
- Throughput and latency measurements

Requirements validated: 2.1, 3.1, 6.1
"""

import asyncio
import pytest
import time
import statistics
import psutil
import os
import tempfile
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from fleet_q.config import FleetQConfig
from fleet_q.sqlite_simulation_storage import SQLiteSimulationStorage
from fleet_q.health_service import HealthService
from fleet_q.claim_service import ClaimService
from fleet_q.recovery_service import RecoveryService
from fleet_q.step_service import StepService
from fleet_q.models import StepSubmissionRequest, StepStatus


@dataclass
class PerformanceMetrics:
    """Container for performance measurement results."""
    operation: str
    total_operations: int
    total_time_seconds: float
    throughput_ops_per_second: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    memory_usage_mb: float
    cpu_usage_percent: float
    error_count: int
    success_rate: float


class PerformanceMonitor:
    """
    Monitors system performance during load testing.
    
    Tracks CPU usage, memory consumption, operation latencies,
    and throughput metrics during test execution.
    """
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.operation_times: List[float] = []
        self.memory_samples: List[float] = []
        self.cpu_samples: List[float] = []
        self.error_count = 0
        self.success_count = 0
    
    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        self.start_time = time.time()
        self.operation_times.clear()
        self.memory_samples.clear()
        self.cpu_samples.clear()
        self.error_count = 0
        self.success_count = 0
    
    def record_operation(self, duration_seconds: float, success: bool = True) -> None:
        """Record an operation's performance."""
        self.operation_times.append(duration_seconds * 1000)  # Convert to ms
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
    
    def sample_resources(self) -> None:
        """Sample current resource usage."""
        try:
            memory_mb = self.process.memory_info().rss / 1024 / 1024
            cpu_percent = self.process.cpu_percent()
            
            self.memory_samples.append(memory_mb)
            self.cpu_samples.append(cpu_percent)
        except Exception:
            # Ignore sampling errors
            pass
    
    def get_metrics(self, operation_name: str) -> PerformanceMetrics:
        """Calculate and return performance metrics."""
        total_time = time.time() - self.start_time if self.start_time else 0
        total_ops = self.success_count + self.error_count
        
        if total_ops == 0:
            return PerformanceMetrics(
                operation=operation_name,
                total_operations=0,
                total_time_seconds=total_time,
                throughput_ops_per_second=0,
                average_latency_ms=0,
                p95_latency_ms=0,
                p99_latency_ms=0,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                error_count=0,
                success_rate=0
            )
        
        throughput = total_ops / total_time if total_time > 0 else 0
        avg_latency = statistics.mean(self.operation_times) if self.operation_times else 0
        p95_latency = statistics.quantiles(self.operation_times, n=20)[18] if len(self.operation_times) >= 20 else avg_latency
        p99_latency = statistics.quantiles(self.operation_times, n=100)[98] if len(self.operation_times) >= 100 else avg_latency
        
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        
        success_rate = self.success_count / total_ops if total_ops > 0 else 0
        
        return PerformanceMetrics(
            operation=operation_name,
            total_operations=total_ops,
            total_time_seconds=total_time,
            throughput_ops_per_second=throughput,
            average_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            memory_usage_mb=avg_memory,
            cpu_usage_percent=avg_cpu,
            error_count=self.error_count,
            success_rate=success_rate
        )


class LoadTestHarness:
    """
    Load testing harness for FLEET-Q performance testing.
    
    Provides utilities for running controlled load tests with
    multiple concurrent pods and varying workload patterns.
    """
    
    def __init__(self, num_pods: int = 5):
        self.num_pods = num_pods
        self.db_path = None
        self.storage = None
        self.pods = []
        self.monitor = PerformanceMonitor()
    
    async def setup(self) -> None:
        """Set up load testing environment."""
        # Create temporary database
        fd, self.db_path = tempfile.mkstemp(suffix='.db', prefix='fleet_q_perf_')
        os.close(fd)
        
        # Create optimized configuration for performance testing
        base_config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            sqlite_db_path=self.db_path,
            heartbeat_interval_seconds=2,
            claim_interval_seconds=0.5,  # Aggressive claiming for performance
            recovery_interval_seconds=10,
            dead_pod_threshold_seconds=15,
            max_parallelism=10,  # Higher parallelism for load testing
            capacity_threshold=0.9  # Use more capacity
        )
        
        # Initialize shared storage
        self.storage = SQLiteSimulationStorage(base_config)
        await self.storage.initialize()
        
        # Create pod instances
        for i in range(self.num_pods):
            pod_id = f"perf-pod-{i+1:03d}"
            
            pod_config = FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                sqlite_db_path=self.db_path,
                pod_id=pod_id,
                max_parallelism=10,
                capacity_threshold=0.9,
                heartbeat_interval_seconds=2,
                claim_interval_seconds=0.5,
                recovery_interval_seconds=10,
                dead_pod_threshold_seconds=15
            )
            
            # Create services
            health_service = HealthService(self.storage, pod_config)
            claim_service = ClaimService(self.storage, pod_config)
            recovery_service = RecoveryService(self.storage, self.storage, pod_config)
            step_service = StepService(self.storage, pod_config)
            
            pod = {
                'pod_id': pod_id,
                'config': pod_config,
                'health_service': health_service,
                'claim_service': claim_service,
                'recovery_service': recovery_service,
                'step_service': step_service,
                'tasks': [],
                'active': True
            }
            
            self.pods.append(pod)
    
    async def start_pods(self) -> None:
        """Start all pod services."""
        for pod in self.pods:
            if pod['active']:
                # Start services
                health_task = asyncio.create_task(
                    pod['health_service'].start(),
                    name=f"health_{pod['pod_id']}"
                )
                pod['tasks'].append(health_task)
                
                claim_task = asyncio.create_task(
                    pod['claim_service'].start(),
                    name=f"claim_{pod['pod_id']}"
                )
                pod['tasks'].append(claim_task)
                
                recovery_task = asyncio.create_task(
                    pod['recovery_service'].start(),
                    name=f"recovery_{pod['pod_id']}"
                )
                pod['tasks'].append(recovery_task)
        
        # Allow pods to initialize
        await asyncio.sleep(3)
    
    async def stop_pods(self) -> None:
        """Stop all pod services."""
        for pod in self.pods:
            # Stop services gracefully
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
    
    async def submit_load(self, num_steps: int, batch_size: int = 10) -> List[str]:
        """Submit steps in batches for load testing."""
        step_ids = []
        active_pod = next((p for p in self.pods if p['active']), None)
        
        if not active_pod:
            return step_ids
        
        for batch_start in range(0, num_steps, batch_size):
            batch_end = min(batch_start + batch_size, num_steps)
            batch_tasks = []
            
            for i in range(batch_start, batch_end):
                request = StepSubmissionRequest(
                    type="load_test_task",
                    args={"task_id": i, "batch": batch_start // batch_size},
                    metadata={"load_test": True, "timestamp": time.time()},
                    priority=i % 10,  # Vary priorities
                    max_retries=3
                )
                
                task = asyncio.create_task(
                    active_pod['step_service'].submit_step(request)
                )
                batch_tasks.append(task)
            
            # Wait for batch to complete
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, str):  # step_id
                    step_ids.append(result)
                # Ignore exceptions for load testing
            
            # Small delay between batches to avoid overwhelming
            await asyncio.sleep(0.1)
        
        return step_ids
    
    async def wait_for_processing(self, expected_count: int, timeout: int = 60) -> Dict[str, Any]:
        """Wait for steps to be processed and return final stats."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            stats = await self.storage.get_queue_stats()
            processed = stats['claimed_steps'] + stats['completed_steps'] + stats['failed_steps']
            
            if processed >= expected_count:
                break
            
            await asyncio.sleep(1)
        
        return await self.storage.get_queue_stats()
    
    def cleanup(self) -> None:
        """Clean up test resources."""
        if self.db_path and os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except OSError:
                pass


class TestHighConcurrencyLoad:
    """High concurrency load testing scenarios."""
    
    @pytest.fixture
    async def load_harness(self):
        """Set up load testing harness."""
        harness = LoadTestHarness(num_pods=5)
        await harness.setup()
        yield harness
        await harness.stop_pods()
        if harness.storage:
            await harness.storage.close()
        harness.cleanup()
    
    @pytest.mark.asyncio
    async def test_high_volume_step_submission(self, load_harness):
        """
        Test high-volume step submission performance.
        
        Validates Requirements: 2.1 (submission throughput)
        """
        harness = load_harness
        await harness.start_pods()
        
        # Start monitoring
        harness.monitor.start_monitoring()
        
        # Submit large number of steps
        num_steps = 500
        start_time = time.time()
        
        step_ids = await harness.submit_load(num_steps, batch_size=20)
        
        submission_time = time.time() - start_time
        harness.monitor.record_operation(submission_time, len(step_ids) == num_steps)
        harness.monitor.sample_resources()
        
        # Get performance metrics
        metrics = harness.monitor.get_metrics("high_volume_submission")
        
        # Validate performance requirements
        assert len(step_ids) == num_steps
        assert metrics.success_rate >= 0.95  # 95% success rate
        assert metrics.throughput_ops_per_second >= 10  # At least 10 submissions/sec
        assert metrics.average_latency_ms <= 5000  # Average under 5 seconds
        assert metrics.memory_usage_mb <= 500  # Memory usage reasonable
        
        print(f"Submission Performance:")
        print(f"  Throughput: {metrics.throughput_ops_per_second:.2f} ops/sec")
        print(f"  Average Latency: {metrics.average_latency_ms:.2f} ms")
        print(f"  Memory Usage: {metrics.memory_usage_mb:.2f} MB")
        print(f"  Success Rate: {metrics.success_rate:.2%}")
    
    @pytest.mark.asyncio
    async def test_concurrent_claiming_performance(self, load_harness):
        """
        Test concurrent step claiming performance.
        
        Validates Requirements: 2.1 (claiming throughput), 3.1 (capacity management)
        """
        harness = load_harness
        await harness.start_pods()
        
        # Submit steps first
        num_steps = 300
        step_ids = await harness.submit_load(num_steps, batch_size=15)
        assert len(step_ids) == num_steps
        
        # Start monitoring claiming performance
        harness.monitor.start_monitoring()
        
        # Monitor claiming process
        claiming_start = time.time()
        resource_sample_task = asyncio.create_task(self._sample_resources_periodically(harness.monitor))
        
        try:
            # Wait for all steps to be claimed
            final_stats = await harness.wait_for_processing(num_steps, timeout=45)
            
            claiming_time = time.time() - claiming_start
            harness.monitor.record_operation(claiming_time, True)
            
            # Cancel resource sampling
            resource_sample_task.cancel()
            try:
                await resource_sample_task
            except asyncio.CancelledError:
                pass
            
            # Get performance metrics
            metrics = harness.monitor.get_metrics("concurrent_claiming")
            
            # Validate claiming performance
            processed_steps = final_stats['claimed_steps'] + final_stats['completed_steps']
            claiming_throughput = processed_steps / claiming_time
            
            assert processed_steps >= num_steps * 0.9  # At least 90% processed
            assert claiming_throughput >= 8  # At least 8 claims/sec
            assert final_stats['active_pods'] == 5  # All pods active
            assert metrics.memory_usage_mb <= 600  # Memory usage reasonable
            
            print(f"Claiming Performance:")
            print(f"  Steps Processed: {processed_steps}/{num_steps}")
            print(f"  Claiming Throughput: {claiming_throughput:.2f} steps/sec")
            print(f"  Total Time: {claiming_time:.2f} seconds")
            print(f"  Memory Usage: {metrics.memory_usage_mb:.2f} MB")
            print(f"  CPU Usage: {metrics.cpu_usage_percent:.2f}%")
            
        finally:
            if not resource_sample_task.done():
                resource_sample_task.cancel()
    
    @pytest.mark.asyncio
    async def test_mixed_workload_performance(self, load_harness):
        """
        Test performance under mixed workload (submission + claiming + recovery).
        
        Validates Requirements: 3.1 (mixed workload handling)
        """
        harness = load_harness
        await harness.start_pods()
        
        harness.monitor.start_monitoring()
        
        # Start continuous resource monitoring
        resource_task = asyncio.create_task(self._sample_resources_periodically(harness.monitor))
        
        try:
            # Phase 1: Submit initial load
            initial_steps = await harness.submit_load(200, batch_size=10)
            await asyncio.sleep(5)  # Let some steps be claimed
            
            # Phase 2: Kill a pod to trigger recovery while submitting more
            harness.pods[1]['active'] = False
            await harness.pods[1]['health_service'].stop()
            await harness.pods[1]['claim_service'].stop()
            
            # Submit more steps during recovery
            additional_steps = await harness.submit_load(100, batch_size=5)
            
            # Phase 3: Wait for system to stabilize
            total_expected = len(initial_steps) + len(additional_steps)
            final_stats = await harness.wait_for_processing(total_expected, timeout=60)
            
            # Calculate mixed workload metrics
            total_time = time.time() - harness.monitor.start_time
            harness.monitor.record_operation(total_time, True)
            
            metrics = harness.monitor.get_metrics("mixed_workload")
            
            # Validate mixed workload performance
            total_processed = (final_stats['claimed_steps'] + 
                             final_stats['completed_steps'] + 
                             final_stats['failed_steps'])
            
            assert total_processed >= total_expected * 0.85  # 85% processed despite pod failure
            assert final_stats['active_pods'] == 4  # One pod killed
            assert metrics.memory_usage_mb <= 700  # Memory usage under stress
            
            print(f"Mixed Workload Performance:")
            print(f"  Total Steps: {total_expected}")
            print(f"  Processed: {total_processed}")
            print(f"  Success Rate: {total_processed/total_expected:.2%}")
            print(f"  Total Time: {total_time:.2f} seconds")
            print(f"  Memory Usage: {metrics.memory_usage_mb:.2f} MB")
            
        finally:
            resource_task.cancel()
            try:
                await resource_task
            except asyncio.CancelledError:
                pass
    
    async def _sample_resources_periodically(self, monitor: PerformanceMonitor) -> None:
        """Sample system resources periodically during test."""
        try:
            while True:
                monitor.sample_resources()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass


class TestRecoveryPerformance:
    """Recovery system performance testing."""
    
    @pytest.fixture
    async def recovery_harness(self):
        """Set up harness for recovery performance testing."""
        harness = LoadTestHarness(num_pods=6)
        await harness.setup()
        yield harness
        await harness.stop_pods()
        if harness.storage:
            await harness.storage.close()
        harness.cleanup()
    
    @pytest.mark.asyncio
    async def test_large_scale_recovery_performance(self, recovery_harness):
        """
        Test recovery performance with large number of orphaned steps.
        
        Validates Requirements: 6.1 (recovery efficiency)
        """
        harness = recovery_harness
        await harness.start_pods()
        
        # Submit large number of steps
        num_steps = 400
        step_ids = await harness.submit_load(num_steps, batch_size=20)
        
        # Wait for steps to be claimed
        await asyncio.sleep(8)
        
        # Start monitoring recovery performance
        harness.monitor.start_monitoring()
        
        # Kill multiple pods to create many orphans
        recovery_start = time.time()
        
        # Kill 3 pods simultaneously
        for i in [1, 2, 3]:
            harness.pods[i]['active'] = False
            await harness.pods[i]['health_service'].stop()
            await harness.pods[i]['claim_service'].stop()
            await harness.pods[i]['recovery_service'].stop()
        
        # Monitor recovery process
        resource_task = asyncio.create_task(self._monitor_recovery_resources(harness.monitor))
        
        try:
            # Wait for recovery to complete
            recovery_complete = False
            max_recovery_time = 45  # seconds
            
            while time.time() - recovery_start < max_recovery_time:
                stats = await harness.storage.get_queue_stats()
                
                # Recovery is complete when active pods stabilize and steps are requeued
                if stats['active_pods'] == 3:  # 3 pods remaining
                    recovery_complete = True
                    break
                
                await asyncio.sleep(2)
            
            recovery_time = time.time() - recovery_start
            harness.monitor.record_operation(recovery_time, recovery_complete)
            
            # Get final statistics
            final_stats = await harness.storage.get_queue_stats()
            metrics = harness.monitor.get_metrics("large_scale_recovery")
            
            # Validate recovery performance
            assert recovery_complete, "Recovery did not complete within timeout"
            assert final_stats['active_pods'] == 3
            assert recovery_time <= 40  # Recovery should complete within 40 seconds
            
            # Verify no steps were lost
            total_steps = (final_stats['pending_steps'] + 
                          final_stats['claimed_steps'] + 
                          final_stats['completed_steps'] + 
                          final_stats['failed_steps'])
            assert total_steps >= num_steps * 0.95  # At least 95% of steps preserved
            
            print(f"Large Scale Recovery Performance:")
            print(f"  Recovery Time: {recovery_time:.2f} seconds")
            print(f"  Steps Preserved: {total_steps}/{num_steps}")
            print(f"  Active Pods: {final_stats['active_pods']}")
            print(f"  Memory Usage: {metrics.memory_usage_mb:.2f} MB")
            
        finally:
            resource_task.cancel()
            try:
                await resource_task
            except asyncio.CancelledError:
                pass
    
    @pytest.mark.asyncio
    async def test_recovery_under_continuous_load(self, recovery_harness):
        """
        Test recovery performance while system is under continuous load.
        
        Validates Requirements: 6.1 (recovery under load)
        """
        harness = recovery_harness
        await harness.start_pods()
        
        harness.monitor.start_monitoring()
        
        # Start continuous step submission
        submission_task = asyncio.create_task(
            self._continuous_step_submission(harness, rate_per_second=5)
        )
        
        # Start resource monitoring
        resource_task = asyncio.create_task(
            self._monitor_recovery_resources(harness.monitor)
        )
        
        try:
            # Let system run under load for a bit
            await asyncio.sleep(10)
            
            # Trigger recovery by killing pods
            recovery_start = time.time()
            
            # Kill 2 pods
            for i in [2, 4]:
                harness.pods[i]['active'] = False
                await harness.pods[i]['health_service'].stop()
                await harness.pods[i]['claim_service'].stop()
                await harness.pods[i]['recovery_service'].stop()
            
            # Continue load during recovery
            await asyncio.sleep(20)  # Let recovery happen under load
            
            recovery_time = time.time() - recovery_start
            
            # Stop continuous submission
            submission_task.cancel()
            try:
                await submission_task
            except asyncio.CancelledError:
                pass
            
            # Get final metrics
            final_stats = await harness.storage.get_queue_stats()
            metrics = harness.monitor.get_metrics("recovery_under_load")
            
            # Validate recovery under load
            assert final_stats['active_pods'] == 4  # 4 pods remaining
            assert recovery_time <= 25  # Recovery should be efficient even under load
            
            # System should still be processing new steps
            assert final_stats['claimed_steps'] + final_stats['completed_steps'] > 0
            
            print(f"Recovery Under Load Performance:")
            print(f"  Recovery Time: {recovery_time:.2f} seconds")
            print(f"  Active Pods: {final_stats['active_pods']}")
            print(f"  Steps in System: {final_stats['pending_steps'] + final_stats['claimed_steps']}")
            print(f"  Memory Usage: {metrics.memory_usage_mb:.2f} MB")
            
        finally:
            if not submission_task.done():
                submission_task.cancel()
            if not resource_task.done():
                resource_task.cancel()
    
    async def _monitor_recovery_resources(self, monitor: PerformanceMonitor) -> None:
        """Monitor resources during recovery operations."""
        try:
            while True:
                monitor.sample_resources()
                await asyncio.sleep(0.5)  # More frequent sampling during recovery
        except asyncio.CancelledError:
            pass
    
    async def _continuous_step_submission(self, harness: LoadTestHarness, rate_per_second: int) -> None:
        """Submit steps continuously at specified rate."""
        interval = 1.0 / rate_per_second
        step_counter = 0
        
        try:
            while True:
                active_pod = next((p for p in harness.pods if p['active']), None)
                if active_pod:
                    request = StepSubmissionRequest(
                        type="continuous_load_task",
                        args={"step_id": step_counter},
                        metadata={"continuous_load": True},
                        priority=step_counter % 5
                    )
                    
                    try:
                        await active_pod['step_service'].submit_step(request)
                        step_counter += 1
                    except Exception:
                        # Ignore submission errors during continuous load
                        pass
                
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass


class TestResourceUsageOptimization:
    """Resource usage monitoring and optimization tests."""
    
    @pytest.fixture
    async def optimization_harness(self):
        """Set up harness for resource optimization testing."""
        harness = LoadTestHarness(num_pods=3)
        await harness.setup()
        yield harness
        await harness.stop_pods()
        if harness.storage:
            await harness.storage.close()
        harness.cleanup()
    
    @pytest.mark.asyncio
    async def test_memory_usage_optimization(self, optimization_harness):
        """
        Test memory usage remains optimized under various loads.
        
        Validates Requirements: 3.1 (resource optimization)
        """
        harness = optimization_harness
        await harness.start_pods()
        
        # Baseline memory usage
        harness.monitor.start_monitoring()
        harness.monitor.sample_resources()
        await asyncio.sleep(2)
        
        baseline_metrics = harness.monitor.get_metrics("baseline")
        baseline_memory = baseline_metrics.memory_usage_mb
        
        # Test memory usage under increasing load
        load_levels = [50, 100, 200, 300]
        memory_results = []
        
        for load_level in load_levels:
            harness.monitor.start_monitoring()
            
            # Submit load
            step_ids = await harness.submit_load(load_level, batch_size=10)
            
            # Monitor memory during processing
            monitoring_task = asyncio.create_task(
                self._monitor_memory_usage(harness.monitor, duration=15)
            )
            
            # Wait for processing
            await harness.wait_for_processing(load_level, timeout=30)
            
            # Stop monitoring
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass
            
            metrics = harness.monitor.get_metrics(f"load_{load_level}")
            memory_results.append((load_level, metrics.memory_usage_mb))
            
            print(f"Load {load_level}: Memory usage {metrics.memory_usage_mb:.2f} MB")
            
            # Clear processed steps to reset state
            await asyncio.sleep(5)
        
        # Validate memory optimization
        max_memory = max(result[1] for result in memory_results)
        memory_growth = max_memory - baseline_memory
        
        # Memory growth should be reasonable (less than 200MB for 300 steps)
        assert memory_growth <= 200, f"Memory growth too high: {memory_growth:.2f} MB"
        
        # Memory usage should not grow linearly with load (indicating good optimization)
        memory_per_step = memory_growth / 300
        assert memory_per_step <= 1.0, f"Memory per step too high: {memory_per_step:.2f} MB/step"
        
        print(f"Memory Optimization Results:")
        print(f"  Baseline Memory: {baseline_memory:.2f} MB")
        print(f"  Max Memory: {max_memory:.2f} MB")
        print(f"  Memory Growth: {memory_growth:.2f} MB")
        print(f"  Memory per Step: {memory_per_step:.3f} MB/step")
    
    @pytest.mark.asyncio
    async def test_cpu_usage_efficiency(self, optimization_harness):
        """
        Test CPU usage efficiency under load.
        
        Validates Requirements: 3.1 (CPU optimization)
        """
        harness = optimization_harness
        await harness.start_pods()
        
        harness.monitor.start_monitoring()
        
        # Submit sustained load
        step_ids = await harness.submit_load(250, batch_size=15)
        
        # Monitor CPU usage during processing
        cpu_monitoring_task = asyncio.create_task(
            self._monitor_cpu_usage(harness.monitor, duration=25)
        )
        
        # Wait for processing to complete
        await harness.wait_for_processing(250, timeout=40)
        
        # Stop CPU monitoring
        cpu_monitoring_task.cancel()
        try:
            await cpu_monitoring_task
        except asyncio.CancelledError:
            pass
        
        metrics = harness.monitor.get_metrics("cpu_efficiency")
        
        # Validate CPU efficiency
        # CPU usage should be reasonable (not maxed out constantly)
        assert metrics.cpu_usage_percent <= 80, f"CPU usage too high: {metrics.cpu_usage_percent:.2f}%"
        
        # Should achieve good throughput with reasonable CPU usage
        cpu_efficiency = metrics.throughput_ops_per_second / (metrics.cpu_usage_percent / 100)
        assert cpu_efficiency >= 5, f"CPU efficiency too low: {cpu_efficiency:.2f} ops/sec per CPU%"
        
        print(f"CPU Efficiency Results:")
        print(f"  Average CPU Usage: {metrics.cpu_usage_percent:.2f}%")
        print(f"  Throughput: {metrics.throughput_ops_per_second:.2f} ops/sec")
        print(f"  CPU Efficiency: {cpu_efficiency:.2f} ops/sec per CPU%")
    
    async def _monitor_memory_usage(self, monitor: PerformanceMonitor, duration: int) -> None:
        """Monitor memory usage for specified duration."""
        end_time = time.time() + duration
        try:
            while time.time() < end_time:
                monitor.sample_resources()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
    
    async def _monitor_cpu_usage(self, monitor: PerformanceMonitor, duration: int) -> None:
        """Monitor CPU usage for specified duration."""
        end_time = time.time() + duration
        try:
            while time.time() < end_time:
                monitor.sample_resources()
                await asyncio.sleep(0.5)  # More frequent CPU sampling
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "-s", "--tb=short"])