"""
aiomultiprocess Integration for IOHub

This module demonstrates how to use aiomultiprocess with IOHub for HTTP-heavy workloads.
aiomultiprocess allows running async functions across multiple processes, each with its own event loop.

Key benefits:
- Each process handles many concurrent HTTP calls (async)
- IOHub coordinates permits across all processes (shared AIMD)
- Optimal for Bedrock API calls (high latency, low CPU)
- Adaptive worker count based on pod CPU resources
"""

import asyncio
import time
from typing import Dict, Any, List
import multiprocessing as mp

try:
    import aiomultiprocess
    AIOMULTIPROCESS_AVAILABLE = True
except ImportError:
    AIOMULTIPROCESS_AVAILABLE = False
    print("aiomultiprocess not available. Install with: pip install aiomultiprocess")

from iohub import IOHubZMQBased, IOHubClientZMQ

# Import pod resource utilities
try:
    from cgroup_aware_resources import (
        recommended_aiomultiprocess_workers,
        recommended_async_concurrency,
        get_pod_resources
    )
    CGROUP_AWARE = True
except ImportError:
    CGROUP_AWARE = False
    print("Warning: cgroup_aware_resources not available. Using defaults.")


# ============================================================================
# Async Worker Function (runs in aiomultiprocess pool)
# ============================================================================

async def bedrock_worker_async(
    task: Dict[str, Any],
    iohub_address: str = "tcp://127.0.0.1:5555"
) -> Dict[str, Any]:
    """
    Async worker function for aiomultiprocess pool.
    
    This function:
    1. Creates IOHub client (DEALER socket)
    2. Requests permit before Bedrock call
    3. Executes async Bedrock call
    4. Reports outcome to IOHub
    5. Enqueues result write
    6. Releases permit
    
    Args:
        task: Task dictionary with step_id and payload
        iohub_address: IOHub ZMQ address
    
    Returns:
        Result dictionary
    """
    worker_id = f"worker-{mp.current_process().pid}"
    
    # Create IOHub client for this worker
    client = IOHubClientZMQ(iohub_address, worker_id)
    
    try:
        # Request permit (async wait if needed)
        permit_acquired = False
        for attempt in range(5):
            if client.request_permit(timeout=2.0):
                permit_acquired = True
                break
            await asyncio.sleep(0.5)  # Async wait before retry
        
        if not permit_acquired:
            return {
                "status": "error",
                "error": "Permit timeout",
                "task_id": task.get('step_id')
            }
        
        # Execute Bedrock call (async)
        start_time = time.time()
        result = await mock_bedrock_call_async(task['payload'])
        latency = time.time() - start_time
        
        # Report success
        client.report_outcome('success', latency=latency)
        
        # Enqueue write
        client.enqueue_write(
            step_id=task['step_id'],
            table_name='RESULTS',
            record_data={
                'step_id': task['step_id'],
                'result': result,
                'latency': latency,
                'worker_id': worker_id
            }
        )
        
        return {
            "status": "success",
            "result": result,
            "latency": latency,
            "worker_id": worker_id
        }
    
    except Exception as e:
        # Report error
        if "ThrottlingException" in str(e):
            client.report_outcome('throttle')
        else:
            client.report_outcome('error')
        
        return {
            "status": "error",
            "error": str(e),
            "task_id": task.get('step_id')
        }
    
    finally:
        # Always release permit
        client.release_permit()
        client.close()


async def mock_bedrock_call_async(payload: Dict[str, Any]) -> str:
    """
    Mock async Bedrock API call.
    
    Simulates:
    - Variable latency (100-500ms)
    - Occasional throttles (5%)
    - Realistic async behavior
    """
    # Simulate latency
    latency = 0.1 + 0.4 * (hash(str(payload)) % 100) / 100
    await asyncio.sleep(latency)
    
    # Simulate occasional throttles
    if hash(str(payload)) % 20 == 0:
        raise Exception("ThrottlingException: Rate exceeded")
    
    return f"Bedrock response for: {payload.get('prompt', 'N/A')}"


# ============================================================================
# aiomultiprocess Pool Manager
# ============================================================================

class AIOMultiprocessIOHubExecutor:
    """
    Executor using aiomultiprocess.Pool with IOHub coordination.
    
    This is the optimal pattern for Bedrock-heavy workloads:
    - Auto-detects CPU cores from cgroups (Kubernetes-aware)
    - N processes × M concurrent per process = optimal throughput
    - IOHub limits to safe pod-wide level → stable throughput
    - Each process has async event loop → efficient I/O
    """
    
    def __init__(
        self,
        iohub_address: str = "tcp://127.0.0.1:5555",
        processes: int = None,  # Auto-detect if None
        max_tasks_per_child: int = 100
    ):
        if not AIOMULTIPROCESS_AVAILABLE:
            raise ImportError("aiomultiprocess is required. Install with: pip install aiomultiprocess")
        
        self.iohub_address = iohub_address
        self.max_tasks_per_child = max_tasks_per_child
        
        # Auto-detect optimal process count from pod resources
        if processes is None and CGROUP_AWARE:
            self.processes = recommended_aiomultiprocess_workers()
            print(f"[AIOMultiprocess] Auto-detected {self.processes} processes from pod CPU limits")
        else:
            self.processes = processes or 4
            if processes is None:
                print(f"[AIOMultiprocess] Using default {self.processes} processes (cgroup detection unavailable)")
        
        self.pool = None
    
    def start(self):
        """Start aiomultiprocess pool"""
        self.pool = aiomultiprocess.Pool(
            processes=self.processes,
            maxtasksperchild=self.max_tasks_per_child
        )
        print(f"[AIOMultiprocess] Started pool with {self.processes} processes")
    
    def close(self):
        """Close pool gracefully"""
        if self.pool:
            self.pool.close()
            self.pool.join()
            print("[AIOMultiprocess] Pool closed")
    
    async def execute_batch(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute batch of tasks using aiomultiprocess pool.
        
        Args:
            tasks: List of task dictionaries
        
        Returns:
            List of results
        """
        if not self.pool:
            raise RuntimeError("Pool not started. Call start() first.")
        
        # Map tasks to worker function
        # Each task is executed by bedrock_worker_async in a separate process
        results = await self.pool.map(
            bedrock_worker_async,
            [(task, self.iohub_address) for task in tasks]
        )
        
        return results


# ============================================================================
# Alternative: Simple Async Pool (no aiomultiprocess)
# ============================================================================

class SimpleAsyncIOHubExecutor:
    """
    Simple async executor without multiprocessing.
    
    Use when:
    - Single process is sufficient
    - Want simplicity over multi-process
    - Testing or low-volume workloads
    """
    
    def __init__(self, iohub_address: str = "tcp://127.0.0.1:5555"):
        self.iohub_address = iohub_address
    
    async def execute_batch(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute batch of tasks concurrently in single process.
        
        Args:
            tasks: List of task dictionaries
        
        Returns:
            List of results
        """
        # Execute all tasks concurrently (asyncio.gather)
        results = await asyncio.gather(*[
            bedrock_worker_async(task, self.iohub_address)
            for task in tasks
        ])
        
        return results


# ============================================================================
# Demo Functions
# ============================================================================

async def demo_aiomultiprocess():
    """
    Demo using aiomultiprocess.Pool with IOHub.
    
    This shows the production pattern:
    - Auto-detects CPU cores from pod cgroups
    - IOHub coordinates permits across N processes
    - Each process handles M tasks concurrently
    - Total: N×M tasks with controlled pod-wide concurrency
    """
    if not AIOMULTIPROCESS_AVAILABLE:
        print("Skipping aiomultiprocess demo (not installed)")
        return
    
    print("\n" + "=" * 70)
    print("aiomultiprocess + IOHub Demo (Adaptive)")
    print("=" * 70 + "\n")
    
    # Show pod resources if available
    if CGROUP_AWARE:
        resources = get_pod_resources()
        print(f"[Resources] Detected {resources.cpu_cores:.2f} CPU cores")
        print(f"[Resources] Recommended workers: {resources.recommended_aiomultiprocess}")
        if resources.memory_limit_gb:
            print(f"[Resources] Memory limit: {resources.memory_limit_gb:.2f} GB")
        print()
    
    # Start IOHub
    iohub_address = "tcp://127.0.0.1:5555"
    hub = IOHubZMQBased(bind_address=iohub_address)
    hub_process = mp.Process(target=hub.run, name="IOHub")
    hub_process.start()
    
    await asyncio.sleep(1)  # Wait for IOHub startup
    print(f"[Demo] IOHub started (PID: {hub_process.pid})\n")
    
    # Create executor (auto-detects optimal process count)
    executor = AIOMultiprocessIOHubExecutor(iohub_address=iohub_address)
    executor.start()
    print(f"[Demo] Using {executor.processes} worker processes\n")
    
    # Create tasks (adjust based on worker count)
    num_tasks = executor.processes * 20  # 20 tasks per process
    tasks = [
        {
            'step_id': f'task-{i:03d}',
            'payload': {
                'prompt': f'Test prompt {i}',
                'task_type': 'bedrock'
            }
        }
        for i in range(num_tasks)
    ]
    
    # Execute batch
    print(f"[Demo] Executing {len(tasks)} tasks across {executor.processes} processes...\n")
    start = time.time()
    results = await executor.execute_batch(tasks)
    elapsed = time.time() - start
    
    # Summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'error')
    
    print("\n" + "=" * 70)
    print("Results")
    print("=" * 70)
    print(f"Tasks completed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {len(results) / elapsed:.2f} tasks/sec")
    print(f"Worker efficiency: {successful / executor.processes:.1f} tasks/worker")
    print("=" * 70 + "\n")
    
    # Cleanup
    executor.close()
    hub_process.terminate()
    hub_process.join()


async def demo_simple_async():
    """
    Demo using simple async executor (no multiprocessing).
    
    This shows:
    - Single process with asyncio.gather
    - Simpler but lower total concurrency
    - Good for testing or low-volume
    """
    print("\n" + "=" * 70)
    print("Simple Async + IOHub Demo")
    print("=" * 70 + "\n")
    
    # Start IOHub
    iohub_address = "tcp://127.0.0.1:5555"
    hub = IOHubZMQBased(bind_address=iohub_address)
    hub_process = mp.Process(target=hub.run, name="IOHub")
    hub_process.start()
    
    await asyncio.sleep(1)
    print(f"[Demo] IOHub started (PID: {hub_process.pid})\n")
    
    # Create executor
    executor = SimpleAsyncIOHubExecutor(iohub_address=iohub_address)
    
    # Create tasks
    tasks = [
        {
            'step_id': f'task-{i:03d}',
            'payload': {
                'prompt': f'Test prompt {i}',
                'task_type': 'bedrock'
            }
        }
        for i in range(40)
    ]
    
    # Execute batch
    print(f"[Demo] Executing {len(tasks)} tasks in single process...\n")
    start = time.time()
    results = await executor.execute_batch(tasks)
    elapsed = time.time() - start
    
    # Summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'error')
    
    print("\n" + "=" * 70)
    print("Results")
    print("=" * 70)
    print(f"Tasks completed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {len(results) / elapsed:.2f} tasks/sec")
    print("=" * 70 + "\n")
    
    # Cleanup
    hub_process.terminate()
    hub_process.join()


async def demo_comparison():
    """
    Compare aiomultiprocess vs simple async.
    
    Shows the performance difference between:
    - 4 processes × async event loops
    - 1 process × async event loop
    """
    print("\n" + "=" * 70)
    print("Performance Comparison: aiomultiprocess vs Simple Async")
    print("=" * 70 + "\n")
    
    if AIOMULTIPROCESS_AVAILABLE:
        print("Running aiomultiprocess demo...")
        await demo_aiomultiprocess()
    
    print("\nRunning simple async demo...")
    await demo_simple_async()
    
    print("\n" + "=" * 70)
    print("Comparison Summary")
    print("=" * 70)
    print("aiomultiprocess:")
    print("  - 4 processes × async event loops")
    print("  - Higher total concurrency")
    print("  - Better for high-volume workloads")
    print("  - Requires aiomultiprocess package")
    print()
    print("Simple Async:")
    print("  - 1 process × async event loop")
    print("  - Lower total concurrency")
    print("  - Simpler code")
    print("  - Good for testing/low-volume")
    print("=" * 70 + "\n")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point"""
    print("\n" + "=" * 70)
    print("aiomultiprocess + IOHub Integration")
    print("=" * 70)
    print()
    print("Choose demo:")
    print("1. aiomultiprocess.Pool (production pattern)")
    print("2. Simple async (single process)")
    print("3. Comparison (both)")
    print()
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == '1':
        await demo_aiomultiprocess()
    elif choice == '2':
        await demo_simple_async()
    elif choice == '3':
        await demo_comparison()
    else:
        print("Invalid choice")


if __name__ == "__main__":
    """
    Run demo.
    
    Usage:
        python aiomultiprocess_iohub.py
    
    Requirements:
        pip install aiomultiprocess pyzmq
    """
    asyncio.run(main())
