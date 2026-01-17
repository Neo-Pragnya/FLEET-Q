#!/usr/bin/env python3
"""
Performance benchmarking script for FLEET-Q.

This script provides comprehensive performance benchmarks for:
- Step claiming throughput and latency
- Recovery system performance
- Resource usage optimization
- Concurrent processing capabilities

Usage:
    python scripts/benchmark_performance.py [options]

Options:
    --benchmark TYPE    Run specific benchmark (claiming, recovery, resource, all)
    --pods N           Number of pods to simulate (default: 5)
    --steps N          Number of steps for load testing (default: 1000)
    --duration SECS    Duration for sustained load tests (default: 60)
    --output FILE      Output file for benchmark results (JSON format)
    --verbose          Enable verbose output
"""

import argparse
import asyncio
import json
import time
import statistics
import psutil
import tempfile
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fleet_q.config import FleetQConfig
from fleet_q.sqlite_simulation_storage import SQLiteSimulationStorage
from fleet_q.health_service import HealthService
from fleet_q.claim_service import ClaimService
from fleet_q.recovery_service import RecoveryService
from fleet_q.step_service import StepService
from fleet_q.models import StepSubmissionRequest


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    benchmark_name: str
    timestamp: datetime
    configuration: Dict[str, Any]
    metrics: Dict[str, float]
    details: Dict[str, Any]
    success: bool
    error_message: str = ""


class PerformanceBenchmark:
    """
    Comprehensive performance benchmarking suite for FLEET-Q.
    
    Provides standardized benchmarks for measuring system performance
    across different scenarios and configurations.
    """
    
    def __init__(self, num_pods: int = 5, verbose: bool = False):
        self.num_pods = num_pods
        self.verbose = verbose
        self.db_path = None
        self.storage = None
        self.pods = []
        self.results: List[BenchmarkResult] = []
        
    async def setup(self) -> None:
        """Set up benchmarking environment."""
        # Create temporary database
        fd, self.db_path = tempfile.mkstemp(suffix='.db', prefix='fleet_q_benchmark_')
        os.close(fd)
        
        # Create optimized configuration for benchmarking
        base_config = FleetQConfig(
            snowflake_account="benchmark",
            snowflake_user="benchmark",
            snowflake_password="benchmark",
            snowflake_database="benchmark",
            sqlite_db_path=self.db_path,
            heartbeat_interval_seconds=1,
            claim_interval_seconds=0.1,  # Very aggressive for benchmarking
            recovery_interval_seconds=5,
            dead_pod_threshold_seconds=10,
            max_parallelism=20,  # High parallelism for benchmarking
            capacity_threshold=0.95  # Use almost full capacity
        )
        
        # Initialize shared storage
        self.storage = SQLiteSimulationStorage(base_config)
        await self.storage.initialize()
        
        # Create pod instances
        for i in range(self.num_pods):
            pod_id = f"benchmark-pod-{i+1:03d}"
            
            pod_config = FleetQConfig(
                snowflake_account="benchmark",
                snowflake_user="benchmark",
                snowflake_password="benchmark",
                snowflake_database="benchmark",
                sqlite_db_path=self.db_path,
                pod_id=pod_id,
                max_parallelism=20,
                capacity_threshold=0.95,
                heartbeat_interval_seconds=1,
                claim_interval_seconds=0.1,
                recovery_interval_seconds=5,
                dead_pod_threshold_seconds=10
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
    
    async def cleanup(self) -> None:
        """Clean up benchmarking resources."""
        # Stop all pods
        for pod in self.pods:
            if pod['health_service']:
                await pod['health_service'].stop()
            if pod['claim_service']:
                await pod['claim_service'].stop()
            if pod['recovery_service']:
                await pod['recovery_service'].stop()
            
            for task in pod['tasks']:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
        
        # Close storage
        if self.storage:
            await self.storage.close()
        
        # Remove temporary database
        if self.db_path and os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except OSError:
                pass
    
    async def start_pods(self) -> None:
        """Start all pod services."""
        for pod in self.pods:
            if pod['active']:
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
        
        # Wait for pods to initialize
        await asyncio.sleep(2)
    
    async def benchmark_step_claiming(self, num_steps: int = 1000) -> BenchmarkResult:
        """
        Benchmark step claiming performance.
        
        Measures throughput and latency of atomic step claiming operations.
        """
        print(f"🔥 Benchmarking step claiming with {num_steps} steps...")
        
        try:
            await self.start_pods()
            
            # Submit all steps first
            submission_start = time.time()
            step_ids = []
            
            active_pod = self.pods[0]  # Use first pod for submission
            
            for i in range(num_steps):
                request = StepSubmissionRequest(
                    type="benchmark_task",
                    args={"task_id": i},
                    metadata={"benchmark": "claiming"},
                    priority=i % 10
                )
                
                step_id = await active_pod['step_service'].submit_step(request)
                step_ids.append(step_id)
            
            submission_time = time.time() - submission_start
            
            # Measure claiming performance
            claiming_start = time.time()
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024
            
            # Wait for all steps to be claimed
            while True:
                stats = await self.storage.get_queue_stats()
                if stats['pending_steps'] == 0:
                    break
                await asyncio.sleep(0.1)
            
            claiming_end = time.time()
            final_memory = process.memory_info().rss / 1024 / 1024
            
            claiming_time = claiming_end - claiming_start
            total_time = claiming_end - submission_start
            
            # Calculate metrics
            submission_throughput = num_steps / submission_time
            claiming_throughput = num_steps / claiming_time
            total_throughput = num_steps / total_time
            memory_usage = final_memory - initial_memory
            
            # Get final statistics
            final_stats = await self.storage.get_queue_stats()
            
            result = BenchmarkResult(
                benchmark_name="step_claiming",
                timestamp=datetime.now(),
                configuration={
                    "num_pods": self.num_pods,
                    "num_steps": num_steps,
                    "max_parallelism": self.pods[0]['config'].max_parallelism,
                    "capacity_threshold": self.pods[0]['config'].capacity_threshold
                },
                metrics={
                    "submission_throughput_ops_per_sec": submission_throughput,
                    "claiming_throughput_ops_per_sec": claiming_throughput,
                    "total_throughput_ops_per_sec": total_throughput,
                    "submission_time_seconds": submission_time,
                    "claiming_time_seconds": claiming_time,
                    "total_time_seconds": total_time,
                    "memory_usage_mb": memory_usage,
                    "average_latency_ms": (claiming_time / num_steps) * 1000
                },
                details={
                    "steps_submitted": len(step_ids),
                    "steps_claimed": final_stats['claimed_steps'],
                    "steps_completed": final_stats['completed_steps'],
                    "active_pods": final_stats['active_pods'],
                    "initial_memory_mb": initial_memory,
                    "final_memory_mb": final_memory
                },
                success=True
            )
            
            if self.verbose:
                print(f"  Submission: {submission_throughput:.2f} ops/sec")
                print(f"  Claiming: {claiming_throughput:.2f} ops/sec")
                print(f"  Memory usage: {memory_usage:.2f} MB")
            
            return result
            
        except Exception as e:
            return BenchmarkResult(
                benchmark_name="step_claiming",
                timestamp=datetime.now(),
                configuration={"num_pods": self.num_pods, "num_steps": num_steps},
                metrics={},
                details={},
                success=False,
                error_message=str(e)
            )
    
    async def benchmark_recovery_performance(self, num_steps: int = 500) -> BenchmarkResult:
        """
        Benchmark recovery system performance.
        
        Measures recovery time and efficiency when pods fail.
        """
        print(f"🔄 Benchmarking recovery performance with {num_steps} steps...")
        
        try:
            await self.start_pods()
            
            # Submit steps and let them be claimed
            active_pod = self.pods[0]
            step_ids = []
            
            for i in range(num_steps):
                request = StepSubmissionRequest(
                    type="recovery_benchmark_task",
                    args={"task_id": i},
                    metadata={"benchmark": "recovery"}
                )
                
                step_id = await active_pod['step_service'].submit_step(request)
                step_ids.append(step_id)
            
            # Wait for steps to be claimed
            await asyncio.sleep(5)
            
            initial_stats = await self.storage.get_queue_stats()
            
            # Kill multiple pods to trigger recovery
            recovery_start = time.time()
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024
            
            # Kill 3 pods
            killed_pods = []
            for i in [1, 2, 3]:
                pod = self.pods[i]
                pod['active'] = False
                await pod['health_service'].stop()
                await pod['claim_service'].stop()
                await pod['recovery_service'].stop()
                killed_pods.append(pod['pod_id'])
            
            # Wait for recovery to complete
            recovery_complete = False
            max_recovery_time = 60
            
            while time.time() - recovery_start < max_recovery_time:
                stats = await self.storage.get_queue_stats()
                if stats['active_pods'] == self.num_pods - 3:  # 3 pods killed
                    recovery_complete = True
                    break
                await asyncio.sleep(1)
            
            recovery_end = time.time()
            final_memory = process.memory_info().rss / 1024 / 1024
            
            recovery_time = recovery_end - recovery_start
            memory_usage = final_memory - initial_memory
            
            # Get final statistics
            final_stats = await self.storage.get_queue_stats()
            
            # Calculate recovery metrics
            steps_recovered = (final_stats['pending_steps'] + 
                             final_stats['claimed_steps'] + 
                             final_stats['completed_steps'])
            recovery_efficiency = steps_recovered / num_steps
            
            result = BenchmarkResult(
                benchmark_name="recovery_performance",
                timestamp=datetime.now(),
                configuration={
                    "num_pods": self.num_pods,
                    "num_steps": num_steps,
                    "pods_killed": len(killed_pods)
                },
                metrics={
                    "recovery_time_seconds": recovery_time,
                    "recovery_efficiency": recovery_efficiency,
                    "steps_per_second_recovered": steps_recovered / recovery_time if recovery_time > 0 else 0,
                    "memory_usage_mb": memory_usage
                },
                details={
                    "initial_active_pods": initial_stats['active_pods'],
                    "final_active_pods": final_stats['active_pods'],
                    "killed_pods": killed_pods,
                    "steps_before_recovery": num_steps,
                    "steps_after_recovery": steps_recovered,
                    "recovery_complete": recovery_complete,
                    "initial_memory_mb": initial_memory,
                    "final_memory_mb": final_memory
                },
                success=recovery_complete
            )
            
            if self.verbose:
                print(f"  Recovery time: {recovery_time:.2f} seconds")
                print(f"  Recovery efficiency: {recovery_efficiency:.2%}")
                print(f"  Steps recovered: {steps_recovered}/{num_steps}")
            
            return result
            
        except Exception as e:
            return BenchmarkResult(
                benchmark_name="recovery_performance",
                timestamp=datetime.now(),
                configuration={"num_pods": self.num_pods, "num_steps": num_steps},
                metrics={},
                details={},
                success=False,
                error_message=str(e)
            )
    
    async def benchmark_resource_usage(self, duration_seconds: int = 60) -> BenchmarkResult:
        """
        Benchmark resource usage under sustained load.
        
        Measures CPU and memory usage during sustained operation.
        """
        print(f"📊 Benchmarking resource usage for {duration_seconds} seconds...")
        
        try:
            await self.start_pods()
            
            process = psutil.Process()
            
            # Baseline measurements
            baseline_memory = process.memory_info().rss / 1024 / 1024
            baseline_cpu = process.cpu_percent()
            
            # Start sustained load
            load_start = time.time()
            memory_samples = []
            cpu_samples = []
            
            # Submit steps continuously
            step_counter = 0
            active_pod = self.pods[0]
            
            # Resource monitoring task
            async def monitor_resources():
                while time.time() - load_start < duration_seconds:
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    cpu_percent = process.cpu_percent()
                    
                    memory_samples.append(memory_mb)
                    cpu_samples.append(cpu_percent)
                    
                    await asyncio.sleep(1)
            
            # Step submission task
            async def submit_steps():
                nonlocal step_counter
                while time.time() - load_start < duration_seconds:
                    request = StepSubmissionRequest(
                        type="resource_benchmark_task",
                        args={"task_id": step_counter},
                        metadata={"benchmark": "resource_usage"}
                    )
                    
                    try:
                        await active_pod['step_service'].submit_step(request)
                        step_counter += 1
                    except Exception:
                        pass  # Ignore submission errors during sustained load
                    
                    await asyncio.sleep(0.1)  # 10 submissions per second
            
            # Run monitoring and submission concurrently
            await asyncio.gather(
                monitor_resources(),
                submit_steps()
            )
            
            load_end = time.time()
            actual_duration = load_end - load_start
            
            # Calculate resource metrics
            avg_memory = statistics.mean(memory_samples) if memory_samples else baseline_memory
            max_memory = max(memory_samples) if memory_samples else baseline_memory
            avg_cpu = statistics.mean(cpu_samples) if cpu_samples else baseline_cpu
            max_cpu = max(cpu_samples) if cpu_samples else baseline_cpu
            
            memory_growth = max_memory - baseline_memory
            
            # Get final statistics
            final_stats = await self.storage.get_queue_stats()
            
            result = BenchmarkResult(
                benchmark_name="resource_usage",
                timestamp=datetime.now(),
                configuration={
                    "num_pods": self.num_pods,
                    "duration_seconds": duration_seconds,
                    "submission_rate": 10  # steps per second
                },
                metrics={
                    "baseline_memory_mb": baseline_memory,
                    "average_memory_mb": avg_memory,
                    "max_memory_mb": max_memory,
                    "memory_growth_mb": memory_growth,
                    "average_cpu_percent": avg_cpu,
                    "max_cpu_percent": max_cpu,
                    "steps_submitted": step_counter,
                    "submission_rate_actual": step_counter / actual_duration
                },
                details={
                    "actual_duration": actual_duration,
                    "memory_samples": len(memory_samples),
                    "cpu_samples": len(cpu_samples),
                    "final_queue_stats": final_stats
                },
                success=True
            )
            
            if self.verbose:
                print(f"  Average memory: {avg_memory:.2f} MB")
                print(f"  Memory growth: {memory_growth:.2f} MB")
                print(f"  Average CPU: {avg_cpu:.2f}%")
                print(f"  Steps submitted: {step_counter}")
            
            return result
            
        except Exception as e:
            return BenchmarkResult(
                benchmark_name="resource_usage",
                timestamp=datetime.now(),
                configuration={"num_pods": self.num_pods, "duration_seconds": duration_seconds},
                metrics={},
                details={},
                success=False,
                error_message=str(e)
            )
    
    async def benchmark_concurrent_processing(self, num_steps: int = 2000) -> BenchmarkResult:
        """
        Benchmark concurrent processing capabilities.
        
        Measures system performance under high concurrent load.
        """
        print(f"⚡ Benchmarking concurrent processing with {num_steps} steps...")
        
        try:
            await self.start_pods()
            
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024
            
            # Submit large batch of steps rapidly
            submission_start = time.time()
            
            # Submit in parallel batches
            batch_size = 50
            batches = [num_steps // batch_size] * (num_steps // batch_size)
            if num_steps % batch_size:
                batches.append(num_steps % batch_size)
            
            async def submit_batch(batch_start: int, batch_size: int):
                active_pod = self.pods[batch_start % len(self.pods)]  # Distribute across pods
                batch_ids = []
                
                for i in range(batch_size):
                    request = StepSubmissionRequest(
                        type="concurrent_benchmark_task",
                        args={"task_id": batch_start + i},
                        metadata={"benchmark": "concurrent", "batch": batch_start}
                    )
                    
                    step_id = await active_pod['step_service'].submit_step(request)
                    batch_ids.append(step_id)
                
                return batch_ids
            
            # Submit all batches concurrently
            batch_tasks = []
            batch_start = 0
            for batch_size in batches:
                task = asyncio.create_task(submit_batch(batch_start, batch_size))
                batch_tasks.append(task)
                batch_start += batch_size
            
            batch_results = await asyncio.gather(*batch_tasks)
            
            submission_end = time.time()
            submission_time = submission_end - submission_start
            
            # Count total submitted steps
            total_submitted = sum(len(batch) for batch in batch_results)
            
            # Wait for processing to complete
            processing_start = time.time()
            
            while True:
                stats = await self.storage.get_queue_stats()
                processed = stats['claimed_steps'] + stats['completed_steps']
                
                if processed >= total_submitted * 0.95:  # 95% processed
                    break
                
                # Timeout after 2 minutes
                if time.time() - processing_start > 120:
                    break
                
                await asyncio.sleep(1)
            
            processing_end = time.time()
            final_memory = process.memory_info().rss / 1024 / 1024
            
            processing_time = processing_end - processing_start
            total_time = processing_end - submission_start
            
            # Get final statistics
            final_stats = await self.storage.get_queue_stats()
            
            # Calculate concurrent processing metrics
            submission_throughput = total_submitted / submission_time
            processing_throughput = (final_stats['claimed_steps'] + final_stats['completed_steps']) / processing_time
            total_throughput = (final_stats['claimed_steps'] + final_stats['completed_steps']) / total_time
            memory_usage = final_memory - initial_memory
            
            result = BenchmarkResult(
                benchmark_name="concurrent_processing",
                timestamp=datetime.now(),
                configuration={
                    "num_pods": self.num_pods,
                    "num_steps": num_steps,
                    "batch_size": 50,
                    "concurrent_batches": len(batches)
                },
                metrics={
                    "submission_throughput_ops_per_sec": submission_throughput,
                    "processing_throughput_ops_per_sec": processing_throughput,
                    "total_throughput_ops_per_sec": total_throughput,
                    "submission_time_seconds": submission_time,
                    "processing_time_seconds": processing_time,
                    "total_time_seconds": total_time,
                    "memory_usage_mb": memory_usage,
                    "concurrent_efficiency": processing_throughput / submission_throughput
                },
                details={
                    "steps_submitted": total_submitted,
                    "steps_processed": final_stats['claimed_steps'] + final_stats['completed_steps'],
                    "processing_success_rate": (final_stats['claimed_steps'] + final_stats['completed_steps']) / total_submitted,
                    "final_queue_stats": final_stats,
                    "initial_memory_mb": initial_memory,
                    "final_memory_mb": final_memory
                },
                success=True
            )
            
            if self.verbose:
                print(f"  Submission: {submission_throughput:.2f} ops/sec")
                print(f"  Processing: {processing_throughput:.2f} ops/sec")
                print(f"  Memory usage: {memory_usage:.2f} MB")
                print(f"  Success rate: {result.details['processing_success_rate']:.2%}")
            
            return result
            
        except Exception as e:
            return BenchmarkResult(
                benchmark_name="concurrent_processing",
                timestamp=datetime.now(),
                configuration={"num_pods": self.num_pods, "num_steps": num_steps},
                metrics={},
                details={},
                success=False,
                error_message=str(e)
            )
    
    async def run_all_benchmarks(self, num_steps: int = 1000, duration: int = 60) -> List[BenchmarkResult]:
        """Run all performance benchmarks."""
        print("🚀 Starting comprehensive FLEET-Q performance benchmarks...")
        
        benchmarks = [
            ("Step Claiming", self.benchmark_step_claiming(num_steps)),
            ("Recovery Performance", self.benchmark_recovery_performance(num_steps // 2)),
            ("Resource Usage", self.benchmark_resource_usage(duration)),
            ("Concurrent Processing", self.benchmark_concurrent_processing(num_steps * 2))
        ]
        
        results = []
        
        for name, benchmark_coro in benchmarks:
            print(f"\n--- Running {name} Benchmark ---")
            
            try:
                # Reset environment for each benchmark
                await self.cleanup()
                await self.setup()
                
                result = await benchmark_coro
                results.append(result)
                
                if result.success:
                    print(f"✅ {name} benchmark completed successfully")
                else:
                    print(f"❌ {name} benchmark failed: {result.error_message}")
                    
            except Exception as e:
                error_result = BenchmarkResult(
                    benchmark_name=name.lower().replace(" ", "_"),
                    timestamp=datetime.now(),
                    configuration={"num_pods": self.num_pods},
                    metrics={},
                    details={},
                    success=False,
                    error_message=str(e)
                )
                results.append(error_result)
                print(f"💥 {name} benchmark crashed: {e}")
        
        self.results = results
        return results
    
    def save_results(self, output_file: str) -> None:
        """Save benchmark results to JSON file."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "configuration": {
                "num_pods": self.num_pods,
                "system_info": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                    "platform": os.name
                }
            },
            "benchmarks": [asdict(result) for result in self.results],
            "summary": self._generate_summary()
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📊 Benchmark results saved to: {output_file}")
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary of benchmark results."""
        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]
        
        summary = {
            "total_benchmarks": len(self.results),
            "successful_benchmarks": len(successful),
            "failed_benchmarks": len(failed),
            "success_rate": len(successful) / len(self.results) if self.results else 0
        }
        
        # Add key performance metrics
        if successful:
            for result in successful:
                if result.benchmark_name == "step_claiming":
                    summary["claiming_throughput_ops_per_sec"] = result.metrics.get("claiming_throughput_ops_per_sec", 0)
                elif result.benchmark_name == "recovery_performance":
                    summary["recovery_time_seconds"] = result.metrics.get("recovery_time_seconds", 0)
                elif result.benchmark_name == "concurrent_processing":
                    summary["concurrent_throughput_ops_per_sec"] = result.metrics.get("processing_throughput_ops_per_sec", 0)
        
        return summary


async def main():
    """Main entry point for performance benchmarking."""
    parser = argparse.ArgumentParser(
        description="Run FLEET-Q performance benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--benchmark',
        choices=['claiming', 'recovery', 'resource', 'concurrent', 'all'],
        default='all',
        help='Benchmark to run (default: all)'
    )
    
    parser.add_argument(
        '--pods',
        type=int,
        default=5,
        help='Number of pods to simulate (default: 5)'
    )
    
    parser.add_argument(
        '--steps',
        type=int,
        default=1000,
        help='Number of steps for load testing (default: 1000)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Duration for sustained load tests in seconds (default: 60)'
    )
    
    parser.add_argument(
        '--output',
        default='benchmark_results.json',
        help='Output file for benchmark results (default: benchmark_results.json)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Create benchmark instance
    benchmark = PerformanceBenchmark(num_pods=args.pods, verbose=args.verbose)
    
    try:
        await benchmark.setup()
        
        # Run requested benchmarks
        if args.benchmark == 'claiming':
            result = await benchmark.benchmark_step_claiming(args.steps)
            benchmark.results = [result]
        elif args.benchmark == 'recovery':
            result = await benchmark.benchmark_recovery_performance(args.steps // 2)
            benchmark.results = [result]
        elif args.benchmark == 'resource':
            result = await benchmark.benchmark_resource_usage(args.duration)
            benchmark.results = [result]
        elif args.benchmark == 'concurrent':
            result = await benchmark.benchmark_concurrent_processing(args.steps * 2)
            benchmark.results = [result]
        else:  # all
            await benchmark.run_all_benchmarks(args.steps, args.duration)
        
        # Save results
        benchmark.save_results(args.output)
        
        # Print summary
        summary = benchmark._generate_summary()
        print(f"\n🏁 Benchmark Summary:")
        print(f"  Successful: {summary['successful_benchmarks']}/{summary['total_benchmarks']}")
        print(f"  Success Rate: {summary['success_rate']:.1%}")
        
        if 'claiming_throughput_ops_per_sec' in summary:
            print(f"  Claiming Throughput: {summary['claiming_throughput_ops_per_sec']:.2f} ops/sec")
        if 'recovery_time_seconds' in summary:
            print(f"  Recovery Time: {summary['recovery_time_seconds']:.2f} seconds")
        if 'concurrent_throughput_ops_per_sec' in summary:
            print(f"  Concurrent Throughput: {summary['concurrent_throughput_ops_per_sec']:.2f} ops/sec")
        
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
    except Exception as e:
        print(f"\n💥 Benchmark error: {e}")
    finally:
        await benchmark.cleanup()


if __name__ == "__main__":
    asyncio.run(main())